package caching

import (
	"errors"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func (db *DB) COWMode() bool { return db != nil && db.cow != nil }

// AcquireMVCCReadCut admits and pins the existing whole-publication cut. It is
// available only for the resolved COW engine; admission failures are preserved.
func (db *DB) AcquireMVCCReadCut() (*Snapshot, error) {
	if !db.COWMode() {
		return nil, ErrCOWUnsupported
	}
	return db.acquireCOWSnapshotWithError()
}

// SeekGEVersionRange reads owned bytes from this already-admitted physical cut.
// It never captures a newer database cut during seek or value materialization.
func (s *Snapshot) SeekGEVersionRange(start, end []byte) ([]byte, []byte, bool, error) {
	if s == nil {
		return nil, nil, false, backenddb.ErrClosed
	}
	// cowSeekGE's beginRead validates the snapshot before accessing its cut.
	return s.cowSeekGE(start, end)
}

// cowSeekGE uses only this snapshot's owned sources. An empty retained disk
// basis permits allocation-free cached lower bounds; disk records and physical
// tombstone advancement retain the general merge on the same snapshot.
func (s *Snapshot) cowSeekGE(start, end []byte) (key, value []byte, found bool, err error) {
	if err = s.beginRead(); err != nil {
		return nil, nil, false, err
	}
	defer s.endRead()
	if s.cowCut == nil {
		return nil, nil, false, ErrCOWUnsupported
	}
	empty, err := s.cowCut.basis.snapshot.OwnedUserRootEmpty()
	if err != nil {
		return nil, nil, false, err
	}
	if empty {
		var best pointSuccessorCandidate
		queue := s.rootIterator.immutables
		// A canonical exact-version range has the same shard affinity for
		// every valid or malformed timestamp suffix. Use that shard's owned
		// cut projection instead of seeking all unrelated sibling roots.
		if _, exact := mvcckey.ExactVersionRange(start, end); exact && s.db != nil &&
			!memtableViewHasRangeSpans(s.view) && len(s.rootPointShards) == len(s.cowCut.shards) {
			if shard := s.db.shardIndex(start); shard >= 0 && shard < len(s.rootPointShards) {
				queue = s.rootPointShards[shard].immutables
			}
		}
		for i := len(queue) - 1; i >= 0; i-- {
			if _, ok := queue[i].(memtable.SuccessorTable); !ok {
				empty = false // Unknown sources require the existing merge.
				break
			}
			candidate := seekPointSuccessorTable(queue[i], start, end, nil, 0, len(queue)-1-i)
			best = choosePointSuccessor(best, candidate)
		}
		if empty {
			if !best.found {
				return nil, nil, false, nil
			}
			if best.flags&node.FlagTombstone == 0 {
				// The original cut already owns this exact winning inline value.
				// Materialize it directly instead of resolving its physical key a
				// second time through the point-shard domains. Pointer values still
				// use the original workspace and dependency materialization path.
				if best.flags&node.FlagPointer == 0 {
					return append([]byte(nil), best.key...), append([]byte(nil), best.value...), true, nil
				}
				s.cowReadMu.Lock()
				borrowed, _, readErr := s.cowValueLocked(best.key)
				if readErr == nil {
					key = append([]byte(nil), best.key...)
					value = append([]byte(nil), borrowed...)
				}
				s.cowReadMu.Unlock()
				if readErr != nil {
					return nil, nil, false, readErr
				}
				return key, value, true, nil
			}
		}
	}
	if key, value, found, eligible, err := s.cowSeekGEOneAdmitted(start, end); eligible || err != nil {
		return key, value, found, err
	}
	return s.cowSeekGEMergeAdmitted(start, end)
}

// cowSeekGEMergeAdmitted owns a synchronous merge inside the caller's existing
// beginRead/endRead admission. A snapshot-bound iterator would recursively admit
// its construction and reads, deadlocking behind a queued DB Close writer.
func (s *Snapshot) cowSeekGEMergeAdmitted(start, end []byte) (key, value []byte, found bool, err error) {
	s.iteratorMu.Lock()
	if s.closed.Load() {
		s.iteratorMu.Unlock()
		return nil, nil, false, backenddb.ErrClosed
	}
	it, err := s.buildCOWIteratorLocked(start, end, false)
	s.iteratorMu.Unlock()
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	if !it.Valid() {
		return nil, nil, false, it.Error()
	}
	if err = it.Error(); err != nil {
		return nil, nil, false, err
	}
	key = append([]byte(nil), it.Key()...)
	value = append([]byte(nil), it.Value()...)
	if err = it.Error(); err != nil {
		return nil, nil, false, err
	}
	return key, value, true, nil
}

func (db *DB) cowGetMany(keys [][]byte) ([][]byte, error) {
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		return nil, err
	}
	defer s.Close() // original panic unwind; synchronous outcomes join below
	values := make([][]byte, len(keys))
	for i, key := range keys {
		v, err := s.Get(key)
		if err == tree.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, joinCOWCleanupError(err, s.Close())
		}
		values[i] = v
	}
	return values, s.Close()
}

type cowOwnedIterator struct {
	merging.Iterator
	snapshot *Snapshot
	lease    *memtable.COWExternalLease
	once     sync.Once
	err      error
}

func (db *DB) cowIterator(start, end []byte, reverse bool) (merging.Iterator, error) {
	lease, err := db.cow.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowOwnedIterator{}))))
	if err != nil {
		return nil, err
	}
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		lease.Close()
		return nil, err
	}
	it, err := s.bindNewIterator(func() (merging.Iterator, error) { return s.buildIteratorLocked(start, end, reverse) })
	if err != nil {
		err = joinCOWCleanupError(err, s.Close())
		lease.Close()
		return nil, err
	}
	return &cowOwnedIterator{Iterator: it, snapshot: s, lease: lease}, nil
}

func (it *cowOwnedIterator) Close() error {
	it.once.Do(func() {
		it.err = joinCOWCleanupError(it.Iterator.Close(), it.snapshot.Close())
		// The bound child excludes its active reads before returning. The
		// original basis owns any physical failure; this wrapper detaches its
		// consumed Snapshot alias before refunding its independent allocation.
		it.snapshot = nil
		it.lease.Close()
		it.lease = nil
	})
	return it.err
}

// cowMergeIterator owns construction backing independently of the Snapshot
// wrapper and source cursors. Source cursors retain their own admitted pins.
type cowMergeIterator struct {
	merging.Iterator
	lease     *memtable.COWExternalLease
	once      sync.Once
	err       error
	workspace *cowReadWorkspace
	snapshot  *Snapshot
	value     []byte
	cached    bool
}

// Physical cursors need an independent workspace even on a verified cache hit.
// Its fixed storage and inspection envelope share the construction allocation
// and lease; decoder/buffer growth retains its separate bounded admission.
// Memory-only cursors keep the smaller cowMergeIterator allocation.
type cowPhysicalMergeIterator struct {
	cowMergeIterator
	storage cowReadWorkspace
}

func (it *cowMergeIterator) Close() error {
	it.once.Do(func() {
		it.err = errors.Join(it.err, it.Iterator.Close())
		it.workspace.close()
		it.workspace = nil
		it.snapshot = nil
		it.Iterator = nil // only the closed bound wrapper exposes this private child
		it.value = nil
		it.lease.Close()
		it.lease = nil
	})
	return it.err
}

func (it *cowMergeIterator) Next() { it.Iterator.Next(); it.value = nil; it.cached = false }
func (it *cowMergeIterator) Seek(key []byte) {
	it.Iterator.Seek(key)
	it.value = nil
	it.cached = false
}
func (it *cowMergeIterator) SeekForward(key []byte) {
	seekForwardIterator(it.Iterator, key)
	it.value = nil
	it.cached = false
}

func seekForwardIterator(iter merging.Iterator, key []byte) {
	if forward, ok := iter.(merging.ForwardSeeker); ok {
		forward.SeekForward(key)
	} else {
		iter.Seek(key)
	}
}
func (it *cowMergeIterator) Value() []byte {
	if !it.cached && it.Iterator.Valid() {
		it.cached = true
		metadata, ok := it.Iterator.(iterator.RevisionUnsafeIterator)
		if !ok {
			it.err = ErrCOWUnsupported
			return nil
		}
		value, ptr, flags, _ := metadata.UnsafeEntryWithRevision()
		if flags&node.FlagPointer != 0 {
			w, err := it.readWorkspace()
			if err != nil {
				it.err = err
				return nil
			}
			value, it.err = w.read(ptr, false, nil)
		}
		it.value = value
	}
	return it.value
}
func (it *cowMergeIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], it.Value()...) }
func (it *cowMergeIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.Iterator.Error()
}
func (it *cowMergeIterator) readWorkspace() (*cowReadWorkspace, error) {
	if it.workspace == nil {
		w, err := newCOWReadWorkspace(it.snapshot)
		if err != nil {
			return nil, err
		}
		it.workspace = w
	}
	return it.workspace, nil
}
func (it *cowMergeIterator) readLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	// Bound iterator movement serializes its existing admitted workspace.
	// Its original cut retains the exact basis independently of shared scratch.
	w, err := it.readWorkspace()
	if err != nil {
		return nil, err
	}
	data, err := it.snapshot.cowCut.basis.readPhysicalLeafWithWorkspace(it.snapshot.cowCache, w, ptr, dst, false, true)
	if err != errCOWPhysicalDictionary {
		return data, err
	}
	// Dictionary definitions belong to the complete caller cut. Preserve its
	// original admitted decoder and resources for that fallback.
	return w.readLeaf(ptr, dst)
}

func (s *Snapshot) buildCOWIteratorLocked(start, end []byte, reverse bool) (merging.Iterator, error) {
	// Only this retained basis can prove the disk source empty; current backend
	// emptiness and legacy cache metadata cannot justify skipping it.
	empty, err := s.cowCut.basis.snapshot.OwnedUserRootEmpty()
	if err != nil {
		return nil, err
	}
	queue := s.rootIterator.immutables
	count := 0
	for _, source := range queue {
		// The cut owns immutable roots. Len includes tombstones: only exact
		// zero-entry roots can be omitted without changing precedence.
		if source.Len() != 0 {
			count++
		}
	}
	if !empty {
		count++
	}
	wrapper, heap := merging.ForwardAllocationSizes(count)
	if reverse {
		wrapper, heap = merging.ReverseAllocationSizes(count)
	}
	ownerSize := uint64(unsafe.Sizeof(cowMergeIterator{}))
	if !empty {
		ownerSize = uint64(unsafe.Sizeof(cowPhysicalMergeIterator{}))
	}
	bytes := memtable.COWAllocationCharge(ownerSize) +
		memtable.COWAllocationCharge(uint64(count)*uint64(unsafe.Sizeof(merging.IteratorSource{}))) +
		memtable.COWAllocationCharge(wrapper) + memtable.COWAllocationCharge(heap)
	if !empty {
		bytes += memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize()) + memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
		bytes += cowReadWorkspaceInspectionCharge()
	}
	lease, err := s.cowCache.budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	var it *cowMergeIterator
	if empty {
		it = &cowMergeIterator{snapshot: s, lease: lease}
	} else {
		owner := &cowPhysicalMergeIterator{
			cowMergeIterator: cowMergeIterator{snapshot: s, lease: lease},
			storage:          cowReadWorkspace{snapshot: s, budget: s.cowCache.budget, limits: s.cowCache.readLimits()},
		}
		it = &owner.cowMergeIterator
		it.workspace = &owner.storage
	}
	sources, err := s.cowIteratorSources(start, end, queue, count, empty, it, reverse)
	if err != nil {
		it.workspace.close()
		it.workspace = nil
		it.snapshot = nil
		lease.Close()
		return nil, err
	}
	for i := range sources {
		if err = sources[i].Iter.Error(); err != nil {
			for j := range sources {
				_ = sources[j].Iter.Close()
			}
			it.workspace.close()
			it.workspace = nil
			it.snapshot = nil
			lease.Close()
			return nil, err
		}
	}
	if reverse {
		it.Iterator = merging.NewReverseMergingIteratorWithFixedHeap(sources, start, end)
	} else {
		it.Iterator = merging.NewMergingIteratorWithFixedHeap(sources, start, end)
	}
	return it, nil
}

func (s *Snapshot) cowIteratorSources(start, end []byte, queue []memtable.Table, count int, empty bool, owner *cowMergeIterator, reverse bool) ([]merging.IteratorSource, error) {
	sources := make([]merging.IteratorSource, 0, count)
	for i := len(queue) - 1; i >= 0; i-- {
		if queue[i].Len() == 0 {
			continue
		}
		var source iterator.UnsafeIterator
		if reverse {
			source = queue[i].NewReverseIterator(start, end)
		} else {
			source = queue[i].NewIterator(start, end)
		}
		sources = append(sources, merging.IteratorSource{Iter: source, Priority: len(sources)})
	}
	if !empty {
		var disk iterator.UnsafeIterator
		var err error
		if reverse {
			disk, err = s.cowCut.basis.snapshot.OwnedReversePointerProjectionIterator(start, end, owner.readLeaf)
		} else {
			disk, err = s.cowCut.basis.snapshot.OwnedPointerProjectionIterator(start, end, owner.readLeaf)
		}
		if err != nil {
			for _, source := range sources {
				_ = source.Iter.Close()
			}
			return nil, err
		}
		sources = append(sources, merging.IteratorSource{Iter: disk, Priority: len(sources)})
	}
	return sources, nil
}

// cowSeekGEOneAdmitted compares one metadata candidate from every immutable
// source on the retained cut. Unknown sources and physical tombstone winners
// keep the existing full merge, including its suppression/advancement rules.
func (s *Snapshot) cowSeekGEOneAdmitted(start, end []byte) (key, value []byte, found, eligible bool, err error) {
	queue := s.rootIterator.immutables
	for _, source := range queue {
		if _, ok := source.(memtable.SuccessorTable); !ok {
			return nil, nil, false, false, nil
		}
	}
	s.cowReadMu.Lock()
	defer s.cowReadMu.Unlock()
	var inspected cowSuccessorInspection
	best, err := s.cowPhysicalSuccessorLocked(start, end, &inspected)
	if err == errCOWPhysicalDictionary {
		// Dictionary records retain the original caller-cut definition and decoder.
		w, workspaceErr := s.cowWorkspaceLocked()
		if workspaceErr != nil {
			return nil, nil, false, true, workspaceErr
		}
		point, scratchErr := w.pointScratch()
		if scratchErr != nil {
			return nil, nil, false, true, scratchErr
		}
		disk, ok, seekErr := s.cowCut.basis.snapshot.SeekGEEntryWithFixedScratch(start, end, point.key[:], point.leaf[:], &point.successor, w.readLeaf)
		if seekErr != nil {
			return nil, nil, false, true, seekErr
		}
		best = s.cowOwnedPointSuccessor(disk, ok, start, end)
		err = nil
	}
	if err != nil {
		return nil, nil, false, true, err
	}
	if !best.found {
		return nil, nil, false, true, nil
	}
	if best.flags&node.FlagTombstone != 0 {
		return nil, nil, false, false, nil
	}
	if best.flags&node.FlagPointer != 0 && !best.valueOwned {
		w, workspaceErr := s.cowWorkspaceLocked()
		if workspaceErr != nil {
			return nil, nil, false, true, workspaceErr
		}
		var borrowed []byte
		var readErr error
		if inspected.file != nil {
			borrowed, readErr = w.readWithInspection(inspected.file, best.ptr, inspected.shape, false, nil)
		} else {
			borrowed, readErr = w.read(best.ptr, false, nil)
		}
		if readErr != nil {
			return nil, nil, false, true, readErr
		}
		best.value = append([]byte(nil), borrowed...)
	}
	return best.key, best.value, true, true, nil
}

// Both physical and dictionary-fallback traversal use the same cut-local
// winner selection. Copy only the winner, before its physical scratch unlocks.
func (s *Snapshot) cowOwnedPointSuccessor(disk node.LeafEntry, found bool, start, end []byte) pointSuccessorCandidate {
	queue := s.rootIterator.immutables
	best := pointSuccessorCandidate{key: disk.Key, value: disk.Value, ptr: disk.ValuePtr, flags: disk.Flags, revision: disk.Revision, priority: len(queue), found: found}
	for i := len(queue) - 1; i >= 0; i-- {
		best = choosePointSuccessor(best, seekPointSuccessorTable(queue[i], start, end, nil, 0, len(queue)-1-i))
	}
	if !best.found || best.flags&node.FlagTombstone != 0 {
		best.key, best.value = nil, nil
		return best
	}
	best.key = append([]byte(nil), best.key...)
	if best.flags&node.FlagPointer == 0 {
		best.value = append([]byte(nil), best.value...)
	} else {
		best.value = nil
	}
	return best
}

func (s *Snapshot) cowPhysicalSuccessorLocked(start, end []byte, inspected *cowSuccessorInspection) (pointSuccessorCandidate, error) {
	// A retained reader may already own admitted traversal and decoder storage.
	// Keep using it rather than require a second, opportunistic admission.
	if s.cowReader != nil && s.cowReader.point != nil {
		return s.cowCutPhysicalSuccessorLocked(start, end)
	}
	basis := s.cowCut.basis
	// Reuse the original physical owner's admitted traversal storage when idle.
	// Never wait for another reader's scratch: retained readers keep their
	// independent cut-local path when the basis is already in use.
	if !basis.readMu.TryLock() {
		// A fixed number of independently admitted owners amortizes scratch
		// across short-lived parallel cuts. Never wait for a slot or grow the
		// pool; an admitted cut-local reader retains priority above this path.
		for i := range basis.readSlots {
			slot := &basis.readSlots[i]
			if !slot.mu.TryLock() {
				continue
			}
			fresh := slot.reader == nil
			w, err := basis.workspaceLocked(&slot.reader, s.cowCache.budget, s.cowCache.readLimits())
			var best pointSuccessorCandidate
			if err == nil {
				best, err = s.cowWorkspacePhysicalSuccessorLocked(w, start, end, inspected)
			}
			if fresh && (err != nil || (inspected.file != nil && !best.valueOwned)) && slot.reader != nil {
				// A fresh lane keeps only a self-contained result. Errors and
				// complete-cut value handoffs must release its admission before
				// the original local traversal/decoder can consume that capacity.
				slot.reader.close()
				slot.reader = nil
			}
			slot.mu.Unlock()
			if errors.Is(err, memtable.ErrCOWCapacity) {
				if inspected.file != nil {
					// Traversal and inspection completed. The cut can reuse that
					// metadata after a fresh slot refunds its refused value growth.
					return best, nil
				}
				continue
			}
			return best, err
		}
		return s.cowCutPhysicalSuccessorLocked(start, end)
	}
	fresh := basis.reader == nil
	best, err := s.cowBasisPhysicalSuccessorLocked(start, end, inspected)
	if fresh && (errors.Is(err, memtable.ErrCOWCapacity) || (inspected.file != nil && !best.valueOwned)) && basis.reader != nil {
		// A compressed-value inspection hands off without an error, but the
		// complete-cut decoder still needs its own admission. Refund fresh
		// traversal exactly as for a refused value grow before that handoff.
		basis.reader.close()
		basis.reader = nil
	}
	basis.readMu.Unlock()
	if errors.Is(err, memtable.ErrCOWCapacity) {
		if inspected.file != nil {
			return best, nil
		}
		// Shared scratch is an optimization. Its allocation or decoder growth
		// must not invalidate a read that the cut-local owner can still admit.
		return s.cowCutPhysicalSuccessorLocked(start, end)
	}
	return best, err
}

// The caller holds basis.readMu until winner bytes have become owned. Release
// that lock before invoking a cut-local fallback, which can perform its own I/O.
func (s *Snapshot) cowBasisPhysicalSuccessorLocked(start, end []byte, inspected *cowSuccessorInspection) (pointSuccessorCandidate, error) {
	basis := s.cowCut.basis
	w, err := basis.physicalWorkspaceLocked(s.cowCache.budget, s.cowCache.readLimits())
	if err != nil {
		return pointSuccessorCandidate{}, err
	}
	return s.cowWorkspacePhysicalSuccessorLocked(w, start, end, inspected)
}

// Caller owns this basis slot until all selected output bytes become owned.
func (s *Snapshot) cowWorkspacePhysicalSuccessorLocked(w *cowReadWorkspace, start, end []byte, inspected *cowSuccessorInspection) (pointSuccessorCandidate, error) {
	basis := s.cowCut.basis
	point, err := w.pointScratch()
	if err != nil {
		return pointSuccessorCandidate{}, err
	}
	readLeaf := func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
		// Keep the successor's original Snappy/LZ4 block-decoder policy.
		// Borrowing traversal must not introduce a new physicalMu wait.
		return basis.readPhysicalLeafWithWorkspace(s.cowCache, w, ptr, dst, false, true)
	}
	disk, found, err := basis.snapshot.SeekGEEntryWithFixedScratch(start, end, point.key[:], point.leaf[:], &point.successor, readLeaf)
	if err != nil {
		return pointSuccessorCandidate{}, err
	}
	best := s.cowOwnedPointSuccessor(disk, found, start, end)
	if best.found && best.flags&node.FlagTombstone == 0 && best.flags&node.FlagPointer != 0 && best.priority == len(s.rootIterator.immutables) {
		// Only the physical winner belongs to this basis. Queue pointers and
		// dictionary/codec fallback keep the original complete-cut resolver.
		best.value, best.valueOwned, err = w.readUncompressedOwned(best.ptr, inspected)
		if err != nil {
			return best, err
		}
	}
	return best, nil
}

// cowReadMu protects the original per-cut traversal/decoder workspace. This is
// also the nonblocking fallback when another reader owns the basis workspace.
func (s *Snapshot) cowCutPhysicalSuccessorLocked(start, end []byte) (pointSuccessorCandidate, error) {
	basis := s.cowCut.basis
	w, err := s.cowWorkspaceLocked()
	if err != nil {
		return pointSuccessorCandidate{}, err
	}
	point, err := w.pointScratch()
	if err != nil {
		return pointSuccessorCandidate{}, err
	}
	disk, found, err := basis.snapshot.SeekGEEntryWithFixedScratch(start, end, point.key[:], point.leaf[:], &point.successor, s.cowPhysicalLeafLocked)
	if err != nil {
		return pointSuccessorCandidate{}, err
	}
	return s.cowOwnedPointSuccessor(disk, found, start, end), nil
}
