package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/largevalue"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	valueLogRefCountsFileName = "vlog_ref_counts.meta"
	valueLogRefCountsVersion  = uint32(1)
)

var (
	valueLogRefCountsMagic = [8]byte{'T', 'V', 'R', 'E', 'F', 'C', 'N', 'T'}
	errValueLogRefCorrupt  = errors.New("treedb: corrupt value-log ref counters metadata")
)

type valueLogRefDelta struct {
	changes map[uint32]int64
}

func newValueLogRefDelta() *valueLogRefDelta {
	return &valueLogRefDelta{changes: make(map[uint32]int64)}
}

func (d *valueLogRefDelta) add(fileID uint32, delta int64) {
	if d == nil || delta == 0 {
		return
	}
	next := d.changes[fileID] + delta
	if next == 0 {
		delete(d.changes, fileID)
		return
	}
	d.changes[fileID] = next
}

type valueLogRefTracker struct {
	mu        sync.RWMutex
	commitSeq uint64
	counts    map[uint32]uint64
	valid     bool
	dirty     bool
}

func newValueLogRefTracker() *valueLogRefTracker {
	return &valueLogRefTracker{
		counts: make(map[uint32]uint64),
	}
}

func (t *valueLogRefTracker) canTrack(baseSeq uint64) bool {
	if t == nil {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.valid && t.commitSeq == baseSeq
}

func (t *valueLogRefTracker) replace(counts map[uint32]uint64, commitSeq uint64, dirty bool) {
	if t == nil {
		return
	}
	next := make(map[uint32]uint64, len(counts))
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		next[fileID] = n
	}
	t.mu.Lock()
	t.counts = next
	t.commitSeq = commitSeq
	t.valid = true
	t.dirty = dirty
	t.mu.Unlock()
}

func (t *valueLogRefTracker) invalidate() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.valid = false
	t.mu.Unlock()
}

func (t *valueLogRefTracker) applyDelta(nextCommitSeq uint64, delta *valueLogRefDelta) error {
	if t == nil {
		return nil
	}
	if delta == nil {
		return errors.New("treedb: missing value-log ref delta")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.valid {
		return errors.New("treedb: value-log ref tracker invalid")
	}
	if nextCommitSeq != t.commitSeq+1 {
		return fmt.Errorf("treedb: value-log ref tracker sequence mismatch: have=%d next=%d", t.commitSeq, nextCommitSeq)
	}

	for fileID, change := range delta.changes {
		switch {
		case change > 0:
			t.counts[fileID] += uint64(change)
		case change < 0:
			cur := t.counts[fileID]
			dec := uint64(-change)
			if dec > cur {
				return fmt.Errorf("treedb: value-log ref tracker underflow: file=%d have=%d dec=%d", fileID, cur, dec)
			}
			cur -= dec
			if cur == 0 {
				delete(t.counts, fileID)
			} else {
				t.counts[fileID] = cur
			}
		}
	}

	t.commitSeq = nextCommitSeq
	t.dirty = true
	return nil
}

func (t *valueLogRefTracker) referencedSet(commitSeq uint64) (map[uint32]struct{}, bool) {
	if t == nil {
		return nil, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.valid || t.commitSeq != commitSeq {
		return nil, false
	}
	out := make(map[uint32]struct{}, len(t.counts))
	for fileID, n := range t.counts {
		if n == 0 {
			continue
		}
		out[fileID] = struct{}{}
	}
	return out, true
}

func (t *valueLogRefTracker) dirtySnapshot() (uint64, map[uint32]uint64, bool) {
	if t == nil {
		return 0, nil, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.valid || !t.dirty {
		return 0, nil, false
	}
	out := make(map[uint32]uint64, len(t.counts))
	for fileID, n := range t.counts {
		if n == 0 {
			continue
		}
		out[fileID] = n
	}
	return t.commitSeq, out, true
}

func (t *valueLogRefTracker) markClean(commitSeq uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.valid && t.commitSeq == commitSeq {
		t.dirty = false
	}
	t.mu.Unlock()
}

func (db *DB) valueLogRefCountsPath() string {
	if db == nil || db.dir == "" {
		return ""
	}
	return filepath.Join(db.dir, valueLogRefCountsFileName)
}

func (db *DB) currentCommitSeq() uint64 {
	if db == nil {
		return 0
	}
	if state := db.state.Load(); state != nil {
		return state.CommitSeq
	}
	db.mu.RLock()
	seq := db.meta.CommitSeq
	db.mu.RUnlock()
	return seq
}

func (db *DB) initValueLogRefTracker() error {
	if db == nil || db.valueLogRefTracker == nil {
		return nil
	}
	loaded, err := db.loadValueLogRefTracker(db.currentCommitSeq())
	if err != nil {
		return err
	}
	if loaded {
		return nil
	}
	counts, commitSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		return err
	}
	db.valueLogRefTracker.replace(counts, commitSeq, true)
	return db.persistValueLogRefTracker()
}

func (db *DB) loadValueLogRefTracker(commitSeq uint64) (bool, error) {
	path := db.valueLogRefCountsPath()
	if path == "" {
		return false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	disk, err := decodeValueLogRefCounts(data)
	if err != nil {
		// Corrupt metadata is non-fatal: rebuild from full scan.
		if errors.Is(err, errValueLogRefCorrupt) {
			return false, nil
		}
		return false, err
	}
	if disk.commitSeq != commitSeq {
		return false, nil
	}
	db.valueLogRefTracker.replace(disk.counts, disk.commitSeq, false)
	return true, nil
}

func (db *DB) persistValueLogRefTracker() error {
	if db == nil || db.valueLogRefTracker == nil {
		return nil
	}
	path := db.valueLogRefCountsPath()
	if path == "" {
		return nil
	}
	commitSeq, counts, ok := db.valueLogRefTracker.dirtySnapshot()
	if !ok {
		return nil
	}
	blob, err := encodeValueLogRefCounts(commitSeq, counts)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, blob, 0o644); err != nil {
		return err
	}
	db.valueLogRefTracker.markClean(commitSeq)
	return nil
}

func (db *DB) persistValueLogRefTrackerBestEffort() {
	if err := db.persistValueLogRefTracker(); err != nil {
		db.reportError(err)
	}
}

func (db *DB) referencedValueLogSegments(ctx context.Context) (map[uint32]struct{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	seq := db.currentCommitSeq()
	if db.valueLogRefTracker != nil {
		if refs, ok := db.valueLogRefTracker.referencedSet(seq); ok {
			return refs, nil
		}
	}
	counts, scannedSeq, err := db.scanValueLogRefCounts(ctx)
	if err != nil {
		return nil, err
	}
	if db.valueLogRefTracker != nil {
		db.valueLogRefTracker.replace(counts, scannedSeq, true)
	}
	refs := make(map[uint32]struct{}, len(counts))
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		refs[fileID] = struct{}{}
	}
	return refs, nil
}

func (db *DB) scanValueLogRefCounts(ctx context.Context) (map[uint32]uint64, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return nil, 0, fmt.Errorf("missing db state")
	}
	commitSeq := snap.state.CommitSeq
	counts := make(map[uint32]uint64)

	userIter := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := collectValueLogRefCounts(ctx, db, userIter, counts); err != nil {
		_ = userIter.Close()
		_ = snap.Close()
		return nil, 0, err
	}
	_ = userIter.Close()

	sysIter := tree.New(snap.idx.pager, newValueReader(snap.state.ValueLogSet, false), snap.state.SystemRootPageID).
		IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := collectValueLogRefCounts(ctx, db, sysIter, counts); err != nil {
		_ = sysIter.Close()
		_ = snap.Close()
		return nil, 0, err
	}
	_ = sysIter.Close()

	if err := snap.Close(); err != nil {
		return nil, 0, err
	}
	return counts, commitSeq, nil
}

func collectValueLogRefCounts(ctx context.Context, db *DB, it iterator.UnsafeIterator, refs map[uint32]uint64) error {
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
			refs[ptr.FileID]++
			if db != nil {
				nested, err := db.outerLeafNestedBlobRefCounts(ptr)
				if err != nil {
					return err
				}
				for fileID, n := range nested {
					if n == 0 {
						continue
					}
					refs[fileID] += n
				}
			}
		}
		it.Next()
	}
	return it.Error()
}

func (db *DB) outerLeafNestedBlobRefCounts(ptr page.ValuePtr) (map[uint32]uint64, error) {
	if db == nil || db.valueLogManager == nil {
		return nil, nil
	}
	payload, err := db.valueLogManager.Read(ptr)
	if err != nil {
		return nil, err
	}
	manifest, isManifest, err := largevalue.DecodeManifest(payload)
	if err != nil {
		return nil, err
	}
	if isManifest {
		refs := make(map[uint32]uint64, len(manifest.Chunks))
		for i := range manifest.Chunks {
			chunkPtr := manifest.Chunks[i]
			if !page.IsValueLogFileID(chunkPtr.FileID) {
				return nil, fmt.Errorf("treedb: invalid large-value chunk pointer file %d", chunkPtr.FileID)
			}
			refs[chunkPtr.FileID]++
		}
		return refs, nil
	}
	if !outerleaf.HasMagic(payload) {
		return nil, nil
	}
	block, err := outerleaf.DecodeBlockLease(payload)
	if err != nil {
		return nil, err
	}
	defer block.Release()
	refs := make(map[uint32]uint64, 4)
	if err := block.VisitTypedEntries(func(_ []byte, kind outerleaf.EntryKind, _ []byte, blobPtr page.ValuePtr) error {
		if kind != outerleaf.EntryKindBlobRef {
			return nil
		}
		return db.addNestedBlobRefCounts(blobPtr, refs)
	}); err != nil {
		return nil, err
	}
	return refs, nil
}

func (db *DB) addNestedBlobRefCounts(blobPtr page.ValuePtr, refs map[uint32]uint64) error {
	if !page.IsValueLogFileID(blobPtr.FileID) {
		return fmt.Errorf("treedb: invalid nested blob pointer file %d", blobPtr.FileID)
	}
	refs[blobPtr.FileID]++
	if db == nil || db.valueLogManager == nil {
		return nil
	}
	payload, err := db.valueLogManager.Read(blobPtr)
	if err != nil {
		return err
	}
	manifest, isManifest, err := largevalue.DecodeManifest(payload)
	if err != nil {
		return err
	}
	if !isManifest {
		return nil
	}
	for i := range manifest.Chunks {
		chunkPtr := manifest.Chunks[i]
		if !page.IsValueLogFileID(chunkPtr.FileID) {
			return fmt.Errorf("treedb: invalid large-value chunk pointer file %d", chunkPtr.FileID)
		}
		refs[chunkPtr.FileID]++
	}
	return nil
}

func (db *DB) buildValueLogRefDelta(p *pager.Pager, root uint64, baseSeq uint64, ops []batchpkg.Entry) (*valueLogRefDelta, error) {
	if db == nil || db.valueLogRefTracker == nil || !db.valueLogRefTracker.canTrack(baseSeq) {
		return nil, nil
	}
	// Implement an exact delta by diffing old/new pointers for touched keys.
	// This is more expensive than earlier fast paths, but it keeps the incremental
	// tracker correct (including nested blob refs).
	return db.buildValueLogRefDeltaExact(baseSeq, p, root, ops)
}

func (db *DB) buildValueLogRefDeltaExact(baseSeq uint64, p *pager.Pager, root uint64, ops []batchpkg.Entry) (*valueLogRefDelta, error) {
	if db == nil || db.valueLogRefTracker == nil || !db.valueLogRefTracker.canTrack(baseSeq) {
		return nil, nil
	}
	if p == nil {
		return nil, fmt.Errorf("treedb: missing pager for value-log delta build")
	}

	tr := tree.New(p, nil, root)
	delta := newValueLogRefDelta()

	// Coalesce duplicate keys (last-wins) to avoid double-counting old pointers.
	coalesced := ops
	if len(ops) > 1 {
		coalesced = coalesced[:0]
		for i := 0; i < len(ops); {
			j := i + 1
			for j < len(ops) && bytes.Equal(ops[j].Key, ops[i].Key) {
				j++
			}
			coalesced = append(coalesced, ops[j-1])
			i = j
		}
	}

	for i := range coalesced {
		op := coalesced[i]
		if len(op.Key) == 0 {
			continue
		}

		var oldPtr page.ValuePtr
		oldPtrOK := false
		if entry, err := tr.GetEntryExact(op.Key); err == nil {
			if entry.Flags&node.FlagPointer != 0 && page.IsValueLogFileID(entry.ValuePtr.FileID) {
				oldPtr = entry.ValuePtr
				oldPtrOK = true
			}
		} else if !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}

		var newPtr page.ValuePtr
		newPtrOK := false
		if op.Type == batchpkg.OpPut && op.IsPtr && page.IsValueLogFileID(op.ValuePtr.FileID) {
			newPtr = op.ValuePtr
			newPtrOK = true
		}

		if oldPtrOK && newPtrOK && oldPtr == newPtr {
			continue
		}

		if oldPtrOK {
			delta.add(oldPtr.FileID, -1)
			nested, err := db.outerLeafNestedBlobRefCounts(oldPtr)
			if err != nil {
				return nil, err
			}
			for fileID, n := range nested {
				if n == 0 {
					continue
				}
				delta.add(fileID, -int64(n))
			}
		}
		if newPtrOK {
			delta.add(newPtr.FileID, 1)
			nested, err := db.outerLeafNestedBlobRefCounts(newPtr)
			if err != nil {
				return nil, err
			}
			for fileID, n := range nested {
				if n == 0 {
					continue
				}
				delta.add(fileID, int64(n))
			}
		}
	}
	return delta, nil
}

type valueLogRefCountsDisk struct {
	commitSeq uint64
	counts    map[uint32]uint64
}

func encodeValueLogRefCounts(commitSeq uint64, counts map[uint32]uint64) ([]byte, error) {
	ids := make([]uint32, 0, len(counts))
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		ids = append(ids, fileID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > int(^uint32(0)) {
		return nil, fmt.Errorf("too many value-log refs: %d", len(ids))
	}

	size := len(valueLogRefCountsMagic) + 4 + 8 + 4 + len(ids)*(4+8) + 4
	buf := make([]byte, 0, size)
	buf = append(buf, valueLogRefCountsMagic[:]...)
	var tmp4 [4]byte
	var tmp8 [8]byte
	binary.LittleEndian.PutUint32(tmp4[:], valueLogRefCountsVersion)
	buf = append(buf, tmp4[:]...)
	binary.LittleEndian.PutUint64(tmp8[:], commitSeq)
	buf = append(buf, tmp8[:]...)
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(ids)))
	buf = append(buf, tmp4[:]...)
	for _, fileID := range ids {
		binary.LittleEndian.PutUint32(tmp4[:], fileID)
		buf = append(buf, tmp4[:]...)
		binary.LittleEndian.PutUint64(tmp8[:], counts[fileID])
		buf = append(buf, tmp8[:]...)
	}
	crc := crc32.ChecksumIEEE(buf)
	binary.LittleEndian.PutUint32(tmp4[:], crc)
	buf = append(buf, tmp4[:]...)
	return buf, nil
}

func decodeValueLogRefCounts(data []byte) (valueLogRefCountsDisk, error) {
	const base = 8 + 4 + 8 + 4 + 4 // magic + version + seq + count + crc
	if len(data) < base {
		return valueLogRefCountsDisk{}, errValueLogRefCorrupt
	}
	if len(data) < len(valueLogRefCountsMagic)+4 {
		return valueLogRefCountsDisk{}, errValueLogRefCorrupt
	}
	if got := data[:len(valueLogRefCountsMagic)]; string(got) != string(valueLogRefCountsMagic[:]) {
		return valueLogRefCountsDisk{}, errValueLogRefCorrupt
	}
	payload := data[:len(data)-4]
	gotCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(payload) != gotCRC {
		return valueLogRefCountsDisk{}, errValueLogRefCorrupt
	}
	off := len(valueLogRefCountsMagic)
	ver := binary.LittleEndian.Uint32(payload[off : off+4])
	off += 4
	if ver != valueLogRefCountsVersion {
		return valueLogRefCountsDisk{}, errValueLogRefCorrupt
	}
	commitSeq := binary.LittleEndian.Uint64(payload[off : off+8])
	off += 8
	n := binary.LittleEndian.Uint32(payload[off : off+4])
	off += 4
	expected := len(valueLogRefCountsMagic) + 4 + 8 + 4 + int(n)*(4+8) + 4
	if len(data) != expected {
		return valueLogRefCountsDisk{}, errValueLogRefCorrupt
	}
	counts := make(map[uint32]uint64, int(n))
	for i := uint32(0); i < n; i++ {
		fileID := binary.LittleEndian.Uint32(payload[off : off+4])
		off += 4
		refCount := binary.LittleEndian.Uint64(payload[off : off+8])
		off += 8
		if refCount == 0 {
			continue
		}
		counts[fileID] = refCount
	}
	return valueLogRefCountsDisk{
		commitSeq: commitSeq,
		counts:    counts,
	}, nil
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	if path == "" {
		return nil
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
