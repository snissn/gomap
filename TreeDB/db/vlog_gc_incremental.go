package db

import (
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
	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	valueLogRefCountsFileName = "vlog_ref_counts.meta"
	// valueLogRefCountsVersion is the on-disk version for vlog_ref_counts.meta.
	//
	// Version 3 includes nested value-log pointers embedded inside leaf pages
	// stored in the value log, in addition to direct LeafRef reachability.
	// Older metadata is intentionally treated as stale/corrupt and rebuilt from
	// a full scan. TreeDB is still pre-alpha, so we do not preserve old
	// vlog_ref_counts.meta encodings yet.
	valueLogRefCountsVersion = uint32(3)
)

var (
	valueLogRefCountsMagic = [8]byte{'T', 'V', 'R', 'E', 'F', 'C', 'N', 'T'}
	errValueLogRefCorrupt  = errors.New("treedb: corrupt value-log ref counters metadata")
)

const (
	leafRefStateUnknown uint32 = iota
	leafRefStateAbsent
	leafRefStatePresent
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

type valueLogReferencedSetSnapshot struct {
	commitSeq uint64
	refs      map[uint32]struct{}
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
			if err := db.mergeLeafRefValueLogRefs(ctx, refs); err != nil {
				return nil, err
			}
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

func cloneReferencedValueLogSegments(in map[uint32]struct{}) map[uint32]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[uint32]struct{}, len(in))
	for fileID := range in {
		out[fileID] = struct{}{}
	}
	return out
}

func (db *DB) cacheLastValueLogGCReferencedSegments(commitSeq uint64, refs map[uint32]struct{}) {
	if db == nil {
		return
	}
	db.lastValueLogGCRefsMu.Lock()
	db.lastValueLogGCRefs.commitSeq = commitSeq
	db.lastValueLogGCRefs.refs = cloneReferencedValueLogSegments(refs)
	db.lastValueLogGCRefsMu.Unlock()
}

// LastValueLogGCReferencedSegments returns the referenced value-log segment IDs
// from the most recent GC pass when they still match the current commit
// sequence.
func (db *DB) LastValueLogGCReferencedSegments() (map[uint32]struct{}, bool) {
	if db == nil {
		return nil, false
	}
	seq := db.currentCommitSeq()
	db.lastValueLogGCRefsMu.RLock()
	defer db.lastValueLogGCRefsMu.RUnlock()
	if db.lastValueLogGCRefs.commitSeq == 0 || db.lastValueLogGCRefs.commitSeq != seq {
		return nil, false
	}
	return cloneReferencedValueLogSegments(db.lastValueLogGCRefs.refs), true
}

// ReferencedValueLogSegments returns the currently reachable value-log segment
// IDs. This may use the incremental ref tracker when available.
func (db *DB) ReferencedValueLogSegments(ctx context.Context) (map[uint32]struct{}, error) {
	return db.referencedValueLogSegments(ctx)
}

func (db *DB) mergeLeafRefValueLogRefs(ctx context.Context, refs map[uint32]struct{}) error {
	if db == nil || refs == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !db.shouldScanLeafRefValueLogRefs() {
		return nil
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		return fmt.Errorf("acquire snapshot: nil")
	}
	if snap.idx == nil || snap.idx.pager == nil || snap.state == nil {
		_ = snap.Close()
		return fmt.Errorf("missing db state")
	}
	counts := make(map[uint32]uint64, 8)
	reader := ValueReaderForState(snap.state)
	userFound, err := collectLeafRefValueLogRefCounts(ctx, snap.idx.pager, snap.state.RootPageID, reader, counts)
	if err != nil {
		_ = snap.Close()
		return err
	}
	systemFound, err := collectLeafRefValueLogRefCounts(ctx, snap.idx.pager, snap.state.SystemRootPageID, reader, counts)
	if err != nil {
		_ = snap.Close()
		return err
	}
	if err := snap.Close(); err != nil {
		return err
	}
	db.noteLeafRefValueLogReachability(userFound || systemFound)
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		refs[fileID] = struct{}{}
	}
	return nil
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
	reader := ValueReaderForState(snap.state)

	userIter := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := collectValueLogRefCounts(ctx, db, userIter, counts); err != nil {
		_ = userIter.Close()
		_ = snap.Close()
		return nil, 0, err
	}
	_ = userIter.Close()

	sysIter := tree.New(snap.idx.pager, newValueReader(snap.state.ValueLogSet), snap.state.SystemRootPageID).
		IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := collectValueLogRefCounts(ctx, db, sysIter, counts); err != nil {
		_ = sysIter.Close()
		_ = snap.Close()
		return nil, 0, err
	}
	_ = sysIter.Close()

	if snap.idx != nil && snap.idx.pager != nil {
		userFound, err := collectLeafRefValueLogRefCounts(ctx, snap.idx.pager, snap.state.RootPageID, reader, counts)
		if err != nil {
			_ = snap.Close()
			return nil, 0, err
		}
		systemFound, err := collectLeafRefValueLogRefCounts(ctx, snap.idx.pager, snap.state.SystemRootPageID, reader, counts)
		if err != nil {
			_ = snap.Close()
			return nil, 0, err
		}
		db.noteLeafRefValueLogReachability(userFound || systemFound)
	}

	if err := snap.Close(); err != nil {
		return nil, 0, err
	}
	return counts, commitSeq, nil
}

func collectLeafRefValueLogRefCounts(ctx context.Context, p *pager.Pager, rootID uint64, reader tree.SlabReader, refs map[uint32]uint64) (bool, error) {
	if p == nil || rootID == 0 || refs == nil {
		return false, nil
	}
	verifyAlways := p.VerifyOnRead()
	verifyLeafPageChecksums := true
	if cap, ok := reader.(readChecksumCapability); ok {
		verifyLeafPageChecksums = cap.ReadChecksumEnabled()
	}
	found := false
	var leafScratch []byte
	err := leafrefscan.Walk(ctx, rootID, p.Get, func(pageID uint64, n node.Node) error {
		if verifyAlways || !p.IsVerified(pageID) {
			if !n.VerifyChecksum() {
				return fmt.Errorf("checksum mismatch on page %d", pageID)
			}
			if !verifyAlways {
				p.MarkVerified(pageID)
			}
		}
		return nil
	}, func(ptr page.ValuePtr) error {
		if !page.IsValueLogFileID(ptr.FileID) {
			return nil
		}
		refs[ptr.FileID]++
		found = true
		return collectNestedLeafPageValueLogRefCounts(ptr, reader, refs, &leafScratch, verifyLeafPageChecksums)
	})
	return found, err
}

func collectNestedLeafPageValueLogRefCounts(ptr page.ValuePtr, reader tree.SlabReader, refs map[uint32]uint64, scratch *[]byte, verifyLeafPageChecksums bool) error {
	if refs == nil || !page.IsValueLogFileID(ptr.FileID) || reader == nil {
		return nil
	}
	var (
		leafPage []byte
		err      error
	)
	if toer, ok := reader.(unsafeToReader); ok && scratch != nil {
		if cap(*scratch) < page.PageSize {
			*scratch = make([]byte, 0, page.PageSize)
		} else {
			*scratch = (*scratch)[:0]
		}
		leafPage, _, err = toer.ReadUnsafeTo(ptr, (*scratch)[:0])
	} else {
		leafPage, err = reader.ReadUnsafe(ptr)
	}
	if err != nil {
		return err
	}
	if len(leafPage) != page.PageSize {
		return fmt.Errorf("treedb: invalid leaf page size in value log file=%d offset=%d got=%d want=%d", ptr.FileID, ptr.Offset, len(leafPage), page.PageSize)
	}
	n := node.NewNodeView(leafPage)
	if n.Type() != page.PageTypeLeaf {
		return fmt.Errorf("treedb: expected leaf page in value log file=%d offset=%d, got type=%d", ptr.FileID, ptr.Offset, n.Type())
	}
	if verifyLeafPageChecksums && !n.VerifyChecksum() {
		return fmt.Errorf("treedb: checksum mismatch for value-log leaf page file=%d offset=%d", ptr.FileID, ptr.Offset)
	}
	// Nested leaf pages are a terminal reachability source here. We count the
	// payload pointers embedded in that page, but we do not recurse again through
	// those payloads as if they were more leaf pages.
	count := n.Count()
	for i := uint16(0); i < count; i++ {
		_, valPtr, flags, err := n.GetLeafValueView(i)
		if err != nil {
			return err
		}
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(valPtr.FileID) {
			continue
		}
		refs[valPtr.FileID]++
	}
	return nil
}

func (db *DB) shouldScanLeafRefValueLogRefs() bool {
	if db == nil {
		return false
	}
	if db.indexOuterLeavesInValueLog || db.leafPageLog != nil {
		return true
	}
	return db.leafRefState.Load() != leafRefStateAbsent
}

func (db *DB) noteLeafRefValueLogReachability(found bool) {
	if db == nil {
		return
	}
	if db.indexOuterLeavesInValueLog || db.leafPageLog != nil {
		if found {
			db.leafRefState.Store(leafRefStatePresent)
		} else {
			db.leafRefState.Store(leafRefStateUnknown)
		}
		return
	}
	if found {
		db.leafRefState.Store(leafRefStatePresent)
		return
	}
	db.leafRefState.Store(leafRefStateAbsent)
}

func collectValueLogRefCounts(ctx context.Context, db *DB, it iterator.UnsafeIterator, refs map[uint32]uint64) error {
	proj, _ := it.(iterator.PointerProjection)
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var (
			ptr   page.ValuePtr
			flags byte
		)
		if proj != nil {
			ptr, flags = proj.UnsafePointerProjection()
		} else {
			_, ptr, flags = it.UnsafeEntry()
		}
		if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
			refs[ptr.FileID]++
		}
		it.Next()
	}
	return it.Error()
}

func (db *DB) buildValueLogRefDelta(p *pager.Pager, rootID uint64, baseSeq uint64, entries []batchpkg.Entry) (*valueLogRefDelta, error) {
	if db == nil || db.valueLogRefTracker == nil || !db.valueLogRefTracker.canTrack(baseSeq) {
		return nil, nil
	}
	if db.indexOuterLeavesInValueLog {
		return nil, nil
	}
	delta := newValueLogRefDelta()
	if p == nil {
		return delta, nil
	}
	tr := tree.New(p, nil, rootID)
	for i := range entries {
		oldFileID, oldRef, err := lookupValueLogRefAtKey(tr, entries[i].Key)
		if err != nil {
			return nil, err
		}
		if oldRef {
			delta.add(oldFileID, -1)
		}
		if entries[i].Type == batchpkg.OpPut && entries[i].IsPtr && page.IsValueLogFileID(entries[i].ValuePtr.FileID) {
			delta.add(entries[i].ValuePtr.FileID, 1)
		}
	}
	return delta, nil
}

func lookupValueLogRefAtKey(tr *tree.Tree, key []byte) (uint32, bool, error) {
	if tr == nil {
		return 0, false, nil
	}
	entry, err := tr.GetEntry(key)
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		return 0, false, nil
	}
	return entry.ValuePtr.FileID, true, nil
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
	return atomicfile.Write(path, data, perm)
}
