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
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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
	inline  [16]valueLogRefDeltaEntry
	inlineN int
	changes map[uint32]int64
}

const valueLogRefDeltaPromotedMapInitCap = 128
const valueLogRefDeltaPoolMaxRetainedEntries = 512

var valueLogRefDeltaPool = sync.Pool{
	New: func() any {
		return &valueLogRefDelta{}
	},
}

type valueLogRefDeltaEntry struct {
	fileID uint32
	delta  int64
}

func newValueLogRefDelta() *valueLogRefDelta {
	d, _ := valueLogRefDeltaPool.Get().(*valueLogRefDelta)
	if d == nil {
		return &valueLogRefDelta{}
	}
	return d
}

func releaseValueLogRefDelta(d *valueLogRefDelta) {
	if d == nil {
		return
	}
	d.resetForReuse()
	valueLogRefDeltaPool.Put(d)
}

func (d *valueLogRefDelta) resetForReuse() {
	if d == nil {
		return
	}
	if d.inlineN > 0 {
		clear(d.inline[:d.inlineN])
		d.inlineN = 0
	}
	if d.changes == nil {
		return
	}
	// Keep small/typical maps warm for reuse; drop unusually large maps.
	if len(d.changes) > valueLogRefDeltaPoolMaxRetainedEntries {
		d.changes = nil
		return
	}
	clear(d.changes)
}

func (d *valueLogRefDelta) add(fileID uint32, delta int64) {
	if d == nil || delta == 0 {
		return
	}
	if d.changes == nil {
		for i := 0; i < d.inlineN; i++ {
			if d.inline[i].fileID != fileID {
				continue
			}
			next := d.inline[i].delta + delta
			if next == 0 {
				last := d.inlineN - 1
				d.inline[i] = d.inline[last]
				d.inline[last] = valueLogRefDeltaEntry{}
				d.inlineN = last
				return
			}
			d.inline[i].delta = next
			return
		}
		if d.inlineN < len(d.inline) {
			d.inline[d.inlineN] = valueLogRefDeltaEntry{fileID: fileID, delta: delta}
			d.inlineN++
			return
		}
		capHint := d.inlineN + 1
		if capHint < valueLogRefDeltaPromotedMapInitCap {
			capHint = valueLogRefDeltaPromotedMapInitCap
		}
		d.changes = make(map[uint32]int64, capHint)
		for i := 0; i < d.inlineN; i++ {
			entry := d.inline[i]
			if entry.delta == 0 {
				continue
			}
			d.changes[entry.fileID] = entry.delta
			d.inline[i] = valueLogRefDeltaEntry{}
		}
		d.inlineN = 0
	}
	next := d.changes[fileID] + delta
	if next == 0 {
		delete(d.changes, fileID)
		return
	}
	d.changes[fileID] = next
}

func (d *valueLogRefDelta) forEachChange(fn func(fileID uint32, change int64) error) error {
	if d == nil {
		return nil
	}
	if d.changes != nil {
		for fileID, change := range d.changes {
			if err := fn(fileID, change); err != nil {
				return err
			}
		}
		return nil
	}
	for i := 0; i < d.inlineN; i++ {
		entry := d.inline[i]
		if entry.delta == 0 {
			continue
		}
		if err := fn(entry.fileID, entry.delta); err != nil {
			return err
		}
	}
	return nil
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

	if err := delta.forEachChange(func(fileID uint32, change int64) error {
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
		return nil
	}); err != nil {
		return err
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

var referencedValueLogSegmentsLegacyHook struct {
	mu sync.Mutex
	fn func()
}

func registerReferencedValueLogSegmentsLegacyHook(hook func()) func() {
	referencedValueLogSegmentsLegacyHook.mu.Lock()
	prev := referencedValueLogSegmentsLegacyHook.fn
	referencedValueLogSegmentsLegacyHook.fn = hook
	referencedValueLogSegmentsLegacyHook.mu.Unlock()
	return func() {
		referencedValueLogSegmentsLegacyHook.mu.Lock()
		referencedValueLogSegmentsLegacyHook.fn = prev
		referencedValueLogSegmentsLegacyHook.mu.Unlock()
	}
}

func invokeReferencedValueLogSegmentsLegacyHook() {
	referencedValueLogSegmentsLegacyHook.mu.Lock()
	hook := referencedValueLogSegmentsLegacyHook.fn
	referencedValueLogSegmentsLegacyHook.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func refsFromValueLogLiveBytesBySegment(liveByID map[uint32]int64) map[uint32]struct{} {
	if len(liveByID) == 0 {
		return nil
	}
	refs := make(map[uint32]struct{}, len(liveByID))
	for fileID, live := range liveByID {
		if live <= 0 {
			continue
		}
		refs[fileID] = struct{}{}
	}
	return refs
}

func sameValueLogRefSets(a, b map[uint32]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for fileID := range a {
		if _, ok := b[fileID]; !ok {
			return false
		}
	}
	return true
}

func (db *DB) referencedValueLogSegments(ctx context.Context) (map[uint32]struct{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if valueLogDebtLedgerEnabled() && db.valueLogDebtLedger != nil {
		liveByID, ok, err := db.liveBytesBySegmentFromDebtLedger(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			refs := refsFromValueLogLiveBytesBySegment(liveByID)
			if valueLogDebtLedgerShadowCompareEnabled() {
				legacyRefs, err := db.referencedValueLogSegmentsLegacy(ctx)
				if err != nil {
					return nil, err
				}
				if !sameValueLogRefSets(refs, legacyRefs) {
					if rebuildErr := db.rebuildValueLogDebtLedger(ctx); rebuildErr != nil {
						db.reportError(rebuildErr)
					}
					return legacyRefs, nil
				}
			}
			return refs, nil
		}
	}
	return db.referencedValueLogSegmentsLegacy(ctx)
}

func (db *DB) referencedValueLogSegmentsLegacy(ctx context.Context) (map[uint32]struct{}, error) {
	invokeReferencedValueLogSegmentsLegacyHook()
	seq := db.currentCommitSeq()
	if db.valueLogRefTracker != nil {
		if refs, ok := db.valueLogRefTracker.referencedSet(seq); ok {
			if err := db.mergeLeafRefValueLogRefs(ctx, refs); err != nil {
				if errors.Is(err, valuelog.ErrFileNotFound) {
					if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
						return nil, refreshErr
					}
					if retryErr := db.mergeLeafRefValueLogRefs(ctx, refs); retryErr == nil {
						return refs, nil
					} else {
						return nil, retryErr
					}
				}
				return nil, err
			}
			return refs, nil
		}
	}
	counts, scannedSeq, err := db.scanValueLogRefCounts(ctx)
	if err != nil && errors.Is(err, valuelog.ErrFileNotFound) {
		if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
			return nil, refreshErr
		}
		counts, scannedSeq, err = db.scanValueLogRefCounts(ctx)
	}
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
		return collectNestedLeafPageValueLogRefCounts(ptr, reader, refs, &leafScratch)
	})
	return found, err
}

func collectNestedLeafPageValueLogRefCounts(ptr page.ValuePtr, reader tree.SlabReader, refs map[uint32]uint64, scratch *[]byte) error {
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
	if !n.VerifyChecksum() {
		return fmt.Errorf("treedb: checksum mismatch for value-log leaf page file=%d offset=%d", ptr.FileID, ptr.Offset)
	}
	// Nested leaf pages are a terminal reachability source here. We count the
	// payload pointers embedded in that page, but we do not recurse again through
	// those payloads as if they were more leaf pages.
	count := n.Count()
	for i := uint16(0); i < count; i++ {
		_, _, valPtr, flags, err := n.GetLeafEntryView(i)
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
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, ptr, flags := it.UnsafeEntry()
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
