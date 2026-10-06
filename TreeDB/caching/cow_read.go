package caching

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func (db *DB) COWMode() bool { return db != nil && db.cow != nil }

// cowSeekGE uses only this snapshot's owned sources. An empty retained disk
// basis permits allocation-free cached lower bounds; disk records and physical
// tombstone advancement retain the general merge on the same snapshot.
func (s *Snapshot) cowSeekGE(start, end []byte) (key, value []byte, found bool, err error) {
	if err = s.beginRead(); err != nil {
		return nil, nil, false, err
	}
	defer s.endRead()
	empty, err := s.cowCut.basis.snapshot.OwnedUserRootEmpty()
	if err != nil {
		return nil, nil, false, err
	}
	if empty {
		var best pointSuccessorCandidate
		queue := s.rootIterator.immutables
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
	// No read-workspace mutex is held while the iterator acquires its own pins
	// and decodes values. beginRead keeps this exact cut alive through Close.
	it, err := s.Iterator(start, end)
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
	defer s.Close()
	values := make([][]byte, len(keys))
	for i, key := range keys {
		v, err := s.Get(key)
		if err == tree.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		values[i] = v
	}
	return values, nil
}

type cowOwnedIterator struct {
	merging.Iterator
	snapshot *Snapshot
	lease    *memtable.COWExternalLease
	once     sync.Once
	err      error
}

func (db *DB) cowIterator(start, end []byte) (merging.Iterator, error) {
	lease, err := db.cow.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowOwnedIterator{}))))
	if err != nil {
		return nil, err
	}
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		lease.Close()
		return nil, err
	}
	it, err := s.bindNewIterator(func() (merging.Iterator, error) { return s.buildIteratorLocked(start, end, false) })
	if err != nil {
		_ = s.Close()
		lease.Close()
		return nil, err
	}
	return &cowOwnedIterator{Iterator: it, snapshot: s, lease: lease}, nil
}

func (it *cowOwnedIterator) Close() error {
	it.once.Do(func() { it.err = errors.Join(it.Iterator.Close(), it.snapshot.Close()); it.lease.Close() })
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

func (it *cowMergeIterator) Close() error {
	it.once.Do(func() {
		it.err = errors.Join(it.err, it.Iterator.Close())
		it.workspace.close()
		it.value = nil
		it.lease.Close()
	})
	return it.err
}

func (it *cowMergeIterator) Next() { it.Iterator.Next(); it.value = nil; it.cached = false }
func (it *cowMergeIterator) Seek(key []byte) {
	it.Iterator.Seek(key)
	it.value = nil
	it.cached = false
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
	w, err := it.readWorkspace()
	if err != nil {
		return nil, err
	}
	return w.readLeaf(ptr, dst)
}

func (s *Snapshot) buildCOWIteratorLocked(start, end []byte, reverse bool) (merging.Iterator, error) {
	if reverse {
		return nil, ErrCOWUnsupported
	}
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
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowMergeIterator{}))) +
		memtable.COWAllocationCharge(uint64(count)*uint64(unsafe.Sizeof(merging.IteratorSource{}))) +
		memtable.COWAllocationCharge(wrapper) + memtable.COWAllocationCharge(heap)
	if !empty {
		bytes += memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize()) + memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
	}
	lease, err := s.cowCache.budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	it := &cowMergeIterator{snapshot: s, lease: lease}
	sources, err := s.cowIteratorSources(start, end, queue, count, empty, it)
	if err != nil {
		it.workspace.close()
		lease.Close()
		return nil, err
	}
	for i := range sources {
		if err = sources[i].Iter.Error(); err != nil {
			for j := range sources {
				_ = sources[j].Iter.Close()
			}
			lease.Close()
			it.workspace.close()
			return nil, err
		}
	}
	it.Iterator = merging.NewMergingIteratorWithFixedHeap(sources, start, end)
	return it, nil
}

func (s *Snapshot) cowIteratorSources(start, end []byte, queue []memtable.Table, count int, empty bool, owner *cowMergeIterator) ([]merging.IteratorSource, error) {
	sources := make([]merging.IteratorSource, 0, count)
	for i := len(queue) - 1; i >= 0; i-- {
		if queue[i].Len() == 0 {
			continue
		}
		sources = append(sources, merging.IteratorSource{Iter: queue[i].NewIterator(start, end), Priority: len(sources)})
	}
	if !empty {
		disk, err := s.cowCut.basis.snapshot.OwnedPointerProjectionIterator(start, end, owner.readLeaf)
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
