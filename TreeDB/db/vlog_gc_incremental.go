package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

const (
	valueLogRefCountsFileName = "vlog_ref_counts.meta"
	// valueLogRefCountsVersion is the on-disk version for vlog_ref_counts.meta.
	//
	// Version 5 tracks value_vlog references reachable from the user tree,
	// system tree, and collection root trees. leaf_vlog reachability is owned by
	// leaf-generation GC and intentionally excluded here. Older metadata is
	// intentionally treated as stale/corrupt and rebuilt from a full scan.
	// TreeDB is still pre-alpha, so we do not preserve old
	// vlog_ref_counts.meta encodings yet.
	valueLogRefCountsVersion = uint32(5)
)

var (
	valueLogRefCountsMagic = [8]byte{'T', 'V', 'R', 'E', 'F', 'C', 'N', 'T'}
	errValueLogRefCorrupt  = errors.New("treedb: corrupt value-log ref counters metadata")
)

type valueLogRefResolutionSource string

const (
	valueLogRefResolutionSourceTracker        valueLogRefResolutionSource = "tracker"
	valueLogRefResolutionSourceFallbackScan   valueLogRefResolutionSource = "fallback_scan"
	valueLogRefResolutionSourceValidationScan valueLogRefResolutionSource = "validation_scan"
)

var scanValueLogRefCountsHook struct {
	mu sync.Mutex
	fn func()
}

var lookupValueLogRefAtKeyHook struct {
	mu sync.Mutex
	fn func()
}

func registerScanValueLogRefCountsHook(hook func()) func() {
	scanValueLogRefCountsHook.mu.Lock()
	prev := scanValueLogRefCountsHook.fn
	scanValueLogRefCountsHook.fn = hook
	scanValueLogRefCountsHook.mu.Unlock()
	return func() {
		scanValueLogRefCountsHook.mu.Lock()
		scanValueLogRefCountsHook.fn = prev
		scanValueLogRefCountsHook.mu.Unlock()
	}
}

func runScanValueLogRefCountsHook() {
	scanValueLogRefCountsHook.mu.Lock()
	hook := scanValueLogRefCountsHook.fn
	scanValueLogRefCountsHook.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func registerLookupValueLogRefAtKeyHook(hook func()) func() {
	lookupValueLogRefAtKeyHook.mu.Lock()
	prev := lookupValueLogRefAtKeyHook.fn
	lookupValueLogRefAtKeyHook.fn = hook
	lookupValueLogRefAtKeyHook.mu.Unlock()
	return func() {
		lookupValueLogRefAtKeyHook.mu.Lock()
		lookupValueLogRefAtKeyHook.fn = prev
		lookupValueLogRefAtKeyHook.mu.Unlock()
	}
}

func runLookupValueLogRefAtKeyHook() {
	lookupValueLogRefAtKeyHook.mu.Lock()
	hook := lookupValueLogRefAtKeyHook.fn
	lookupValueLogRefAtKeyHook.mu.Unlock()
	if hook != nil {
		hook()
	}
}

type valueLogRefDelta struct {
	inline                      [16]valueLogRefDeltaEntry
	inlineN                     int
	changes                     map[uint32]int64
	positives                   map[uint32]int64
	requiresCandidateProjection bool
	allowEmptyDependencyReuse   bool
	outerLeafDependencyReuse    bool
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
	d.requiresCandidateProjection = false
	d.allowEmptyDependencyReuse = false
	d.outerLeafDependencyReuse = false
	if d.changes != nil {
		// Keep small/typical maps warm for reuse; drop unusually large maps.
		if len(d.changes) > valueLogRefDeltaPoolMaxRetainedEntries {
			d.changes = nil
		} else {
			clear(d.changes)
		}
	}
	if d.positives != nil {
		if len(d.positives) > valueLogRefDeltaPoolMaxRetainedEntries {
			d.positives = nil
		} else {
			clear(d.positives)
		}
	}
}

func (d *valueLogRefDelta) add(fileID uint32, delta int64) {
	if d == nil || delta == 0 {
		return
	}
	if delta > 0 {
		d.addPositive(fileID, delta)
	}
	d.addChange(fileID, delta)
}

// addChange merges only the net reference-count change. Callers that combine
// already-accounted deltas use it together with addPositive so transient
// positive references retain their exact pending-append release accounting.
func (d *valueLogRefDelta) addChange(fileID uint32, delta int64) {
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

func (d *valueLogRefDelta) addPositive(fileID uint32, delta int64) {
	if d == nil || fileID == 0 || delta <= 0 {
		return
	}
	if d.positives == nil {
		d.positives = make(map[uint32]int64, valueLogRefDeltaPromotedMapInitCap)
	}
	d.positives[fileID] += delta
}

func (d *valueLogRefDelta) forEachPositive(fn func(fileID uint32, count int64) error) error {
	if d == nil || len(d.positives) == 0 {
		return nil
	}
	for fileID, count := range d.positives {
		if count <= 0 {
			continue
		}
		if err := fn(fileID, count); err != nil {
			return err
		}
	}
	return nil
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
	revision  uint64
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

func newValueLogRefTrackerForOptions(Options) *valueLogRefTracker {
	return newValueLogRefTracker()
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
	t.revision++
	t.mu.Unlock()
}

func (t *valueLogRefTracker) revisionSnapshot() uint64 {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	revision := t.revision
	t.mu.RUnlock()
	return revision
}

func (t *valueLogRefTracker) replaceUnlessAdvanced(counts map[uint32]uint64, commitSeq uint64, dirty bool, observedRevision uint64) bool {
	if t == nil {
		return false
	}
	next := make(map[uint32]uint64, len(counts))
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		next[fileID] = n
	}
	t.mu.Lock()
	// A GC fallback scan may finish after a concurrent commit advanced exact
	// counts. Preserve that advancement, but still allow ordinary stale-ahead
	// tracker corruption to be repaired when the tracker did not change during
	// the scan.
	if t.revision != observedRevision && t.commitSeq > commitSeq {
		t.mu.Unlock()
		return false
	}
	t.counts = next
	t.commitSeq = commitSeq
	t.valid = true
	t.dirty = dirty
	t.revision++
	t.mu.Unlock()
	return true
}

func (t *valueLogRefTracker) invalidate() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.valid = false
	t.revision++
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
		// Invalidation is the conservative fallback for commits without an exact
		// delta. Later deltas cannot repair missing history, so leave the optional
		// tracker invalid until a full rebuild instead of poisoning the DB.
		return nil
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
	t.revision++
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
		if t.dirty {
			t.dirty = false
			t.revision++
		}
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
	refs, _, err := db.referencedValueLogSegmentsWithSource(ctx)
	return refs, err
}

func (db *DB) referencedValueLogSegmentsWithSource(ctx context.Context) (map[uint32]struct{}, valueLogRefResolutionSource, error) {
	refs, source, _, err := db.referencedValueLogSegmentsWithSourceAtSeq(ctx)
	return refs, source, err
}

func (db *DB) referencedValueLogSegmentsWithSourceAtSeq(ctx context.Context) (map[uint32]struct{}, valueLogRefResolutionSource, uint64, error) {
	return db.referencedValueLogSegmentsWithSourceAtSeqPolicy(ctx, false)
}

func (db *DB) referencedValueLogSegmentsForGCAtSeq(ctx context.Context) (map[uint32]struct{}, valueLogRefResolutionSource, uint64, error) {
	return db.referencedValueLogSegmentsWithSourceAtSeqPolicy(ctx, true)
}

func (db *DB) referencedValueLogSegmentsWithSourceAtSeqPolicy(ctx context.Context, guardConcurrentAdvance bool) (map[uint32]struct{}, valueLogRefResolutionSource, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	seq := db.currentCommitSeq()
	var trackerRevisionBeforeScan uint64
	// Outer-leaf maintenance needs both logical ValuePtrs and raw leaf-log
	// segments. Publication only applies logical deltas, so GC/rewrite cannot
	// serve outer-leaf maintenance from this tracker even after a full rebuild.
	useTracker := db.valueLogRefTracker != nil && !db.indexOuterLeavesInValueLog
	if db.valueLogRefTracker != nil {
		trackerRevisionBeforeScan = db.valueLogRefTracker.revisionSnapshot()
	}
	if useTracker {
		if refs, ok := db.valueLogRefTracker.referencedSet(seq); ok {
			return refs, valueLogRefResolutionSourceTracker, seq, nil
		}
	}
	counts, scannedSeq, err := db.scanValueLogRefCounts(ctx)
	if err != nil && errors.Is(err, valuelog.ErrFileNotFound) {
		if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
			return nil, valueLogRefResolutionSourceFallbackScan, 0, refreshErr
		}
		counts, scannedSeq, err = db.scanValueLogRefCounts(ctx)
	}
	if err != nil {
		return nil, valueLogRefResolutionSourceFallbackScan, 0, err
	}
	if db.valueLogRefTracker != nil {
		if guardConcurrentAdvance {
			db.valueLogRefTracker.replaceUnlessAdvanced(counts, scannedSeq, true, trackerRevisionBeforeScan)
		} else {
			db.valueLogRefTracker.replace(counts, scannedSeq, true)
		}
	}
	refs := make(map[uint32]struct{}, len(counts))
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		refs[fileID] = struct{}{}
	}
	return refs, valueLogRefResolutionSourceFallbackScan, scannedSeq, nil
}

func (db *DB) scanValueLogRefCounts(ctx context.Context) (map[uint32]uint64, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runScanValueLogRefCountsHook()
	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return nil, 0, fmt.Errorf("missing db state")
	}
	commitSeq := snap.state.CommitSeq
	result, err := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{
		Collectors: maintenanceReachabilityValueLogRefCounts,
	})
	if err != nil {
		_ = snap.Close()
		return nil, 0, err
	}
	if err := snap.Close(); err != nil {
		return nil, 0, err
	}
	return result.valueLogRefCounts, commitSeq, nil
}

func (db *DB) referencedValueLogSegmentsForRecoverableRootSet(ctx context.Context, roots *RecoverableRootSet) (map[uint32]struct{}, error) {
	if roots == nil {
		return nil, ErrRecoverableRootSetStale
	}
	captured := roots.Roots()
	if len(captured) == 0 {
		return nil, ErrRecoverableRootSetStale
	}
	snap := roots.AcquireSnapshotForRoot(captured[0])
	if snap == nil {
		return nil, ErrRecoverableRootSetStale
	}
	protectedRootIDs := make([]uint64, 0, len(captured))
	protectedSystemRootIDs := make([]uint64, 0, len(captured))
	for _, root := range captured {
		protectedRootIDs = append(protectedRootIDs, root.UserRootPageID)
		protectedSystemRootIDs = append(protectedSystemRootIDs, root.SystemRootPageID)
	}
	result, err := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{
		Collectors:               maintenanceReachabilityValueLogRefCounts,
		ProtectedRootIDs:         protectedRootIDs,
		ProtectedSystemRootIDs:   protectedSystemRootIDs,
		ProjectProtectedValueLog: true,
	})
	closeErr := snap.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return result.valueLogReferencedSegments, nil
}

func scanValueLogRefCountRootIterator(snap *Snapshot, root maintenanceRoot) iterator.UnsafeIterator {
	opts := tree.IteratorOptions{Mode: tree.IteratorModePointerProjection}
	if snap != nil && root.kind == maintenanceRootUser && root.rootID == snap.treeRoot {
		return snap.tree.IteratorWithOptions(nil, nil, opts)
	}
	return tree.New(snap.idx.pager, &snap.reader, root.rootID).IteratorWithOptions(nil, nil, opts)
}

func (db *DB) shouldCollectValueLogRefDelta(baseSeq uint64) bool {
	if db == nil {
		return false
	}
	// Outer-leaf publication consumes this apply-local delta independently of
	// the persisted GC tracker. It proves the common additive/unchanged
	// transition without rescanning the candidate tree; subtractive transitions
	// still fall back to exact projection.
	if db.indexOuterLeavesInValueLog {
		return true
	}
	return db.valueLogRefTracker != nil && db.valueLogRefTracker.canTrack(baseSeq)
}

func (db *DB) buildValueLogRefDelta(p *pager.Pager, rootID uint64, baseSeq uint64, entries []batchpkg.Entry, ranges []batchpkg.DeleteRange, oldPointerRefs *zipper.PointerRefCounts, oldEntriesRemoved uint64, oldPointerRefsCollected bool) (*valueLogRefDelta, error) {
	delta, err := db.buildValueLogRefDeltaWithOptions(p, rootID, baseSeq, entries, ranges, oldPointerRefs, oldEntriesRemoved, oldPointerRefsCollected, false)
	if delta != nil {
		// The ordinary DB root follows the configured outer-leaf storage policy.
		// Mark that evidence explicitly so durable capture replaces, rather than
		// inherits, raw-leaf dependencies. Ordered multi-root callers override
		// this per root because their system roots remain pager-backed.
		delta.outerLeafDependencyReuse = db.indexOuterLeavesInValueLog
	}
	return delta, err
}

func (db *DB) buildValueLogRefDeltaWithOptions(p *pager.Pager, rootID uint64, baseSeq uint64, entries []batchpkg.Entry, ranges []batchpkg.DeleteRange, oldPointerRefs *zipper.PointerRefCounts, oldEntriesRemoved uint64, oldPointerRefsCollected bool, force bool) (*valueLogRefDelta, error) {
	if !force && !db.shouldCollectValueLogRefDelta(baseSeq) {
		return nil, nil
	}
	delta := newValueLogRefDelta()
	if db.indexOuterLeavesInValueLog {
		// TreeDB's initial logical root is a non-zero empty leaf page. Inspect
		// that one page rather than treating every dependency-empty rewrite as
		// a first publication; the latter can hide collection-root resources.
		switch {
		case rootID == 0:
			delta.allowEmptyDependencyReuse = true
		case p != nil:
			rootPage, err := p.Get(rootID)
			if err != nil {
				releaseValueLogRefDelta(delta)
				return nil, err
			}
			delta.allowEmptyDependencyReuse = node.NewNode(rootPage).Count() == 0
		}
	}
	if oldPointerRefsCollected {
		delta.requiresCandidateProjection = db.indexOuterLeavesInValueLog && oldEntriesRemoved > 0
		var countErr error
		oldPointerRefs.ForEach(func(fileID uint32, count uint64) bool {
			if !page.IsValueLogFileID(fileID) {
				return true
			}
			if count > uint64(^uint64(0)>>1) {
				countErr = fmt.Errorf("treedb: value-log ref decrement overflow for file %d: %d", fileID, count)
				return false
			}
			delta.add(fileID, -int64(count))
			return true
		})
		if countErr != nil {
			releaseValueLogRefDelta(delta)
			return nil, countErr
		}
		for i := range entries {
			if entries[i].Type == batchpkg.OpPut && entries[i].IsPtr && page.IsValueLogFileID(entries[i].ValuePtr.FileID) {
				delta.add(entries[i].ValuePtr.FileID, 1)
			}
		}
		return delta, nil
	}

	// Fail safe for any caller that requests tracking without apply-fed evidence.
	// This preserves correctness while making the normal foreground path measurable
	// through lookupValueLogRefAtKeyHook.
	if p == nil {
		return delta, nil
	}
	tr := tree.New(p, nil, rootID)
	for _, r := range ranges {
		it := tr.IteratorWithOptions(r.Start, r.End, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		for it.Valid() {
			_, ptr, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
				delta.add(ptr.FileID, -1)
			}
			it.Next()
		}
		if err := it.Error(); err != nil {
			_ = it.Close()
			return nil, err
		}
		if err := it.Close(); err != nil {
			return nil, err
		}
	}
	for i := range entries {
		if !batchpkg.DeleteRangesContainKey(ranges, entries[i].Key) {
			oldFileID, oldRef, err := lookupValueLogRefAtKey(tr, entries[i].Key)
			if err != nil {
				return nil, err
			}
			if oldRef {
				delta.add(oldFileID, -1)
			}
		}
		if entries[i].Type == batchpkg.OpPut && entries[i].IsPtr && page.IsValueLogFileID(entries[i].ValuePtr.FileID) {
			delta.add(entries[i].ValuePtr.FileID, 1)
		}
	}
	return delta, nil
}

func (db *DB) newNoopValueLogRefDeltaIfTrackable(baseSeq uint64) *valueLogRefDelta {
	if db == nil {
		return nil
	}
	if db.indexOuterLeavesInValueLog {
		return newValueLogRefDelta()
	}
	if db.valueLogRefTracker == nil || !db.valueLogRefTracker.canTrack(baseSeq) {
		return nil
	}
	return newValueLogRefDelta()
}

func lookupValueLogRefAtKey(tr *tree.Tree, key []byte) (uint32, bool, error) {
	runLookupValueLogRefAtKeyHook()
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
	checksum := crc.Checksum(buf)
	binary.LittleEndian.PutUint32(tmp4[:], checksum)
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
	if crc.Checksum(payload) != gotCRC {
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
