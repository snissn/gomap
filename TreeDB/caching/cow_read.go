package caching

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

func (db *DB) COWMode() bool { return db != nil && db.cow != nil }

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
			value, it.err = it.workspace.read(ptr, false, nil)
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
func (s *Snapshot) buildCOWIteratorLocked(start, end []byte, reverse bool) (merging.Iterator, error) {
	if reverse {
		return nil, ErrCOWUnsupported
	}
	count := len(s.rootIterator.immutables) + 1
	wrapper, heap := merging.ForwardAllocationSizes(count)
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowMergeIterator{}))) +
		memtable.COWAllocationCharge(uint64(count)*uint64(unsafe.Sizeof(merging.IteratorSource{}))) +
		memtable.COWAllocationCharge(wrapper) + memtable.COWAllocationCharge(heap)
	bytes += memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize())
	lease, err := s.cowCache.budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	w, err := newCOWReadWorkspace(s)
	if err != nil {
		lease.Close()
		return nil, err
	}
	sources, err := s.cowIteratorSources(start, end, w)
	if err != nil {
		w.close()
		lease.Close()
		return nil, err
	}
	// Refused root cursors must not be wrapped into a successfully admitted
	// iterator; return the original refusal without allocating joined errors.
	for i := range sources {
		if err = sources[i].Iter.Error(); err != nil {
			for j := range sources {
				_ = sources[j].Iter.Close()
			}
			lease.Close()
			w.close()
			return nil, err
		}
	}
	return &cowMergeIterator{Iterator: merging.NewMergingIteratorWithFixedHeap(sources, start, end), workspace: w, lease: lease}, nil
}

func (s *Snapshot) cowIteratorSources(start, end []byte, w *cowReadWorkspace) ([]merging.IteratorSource, error) {
	queue := s.rootIterator.immutables
	sources := make([]merging.IteratorSource, 0, len(queue)+1)
	for i := len(queue) - 1; i >= 0; i-- {
		sources = append(sources, merging.IteratorSource{Iter: queue[i].NewIterator(start, end), Priority: len(sources)})
	}
	disk, err := s.cowCut.basis.snapshot.OwnedPointerProjectionIterator(start, end, w.readLeaf)
	if err != nil {
		for _, source := range sources {
			_ = source.Iter.Close()
		}
		return nil, err
	}
	sources = append(sources, merging.IteratorSource{Iter: disk, Priority: len(sources)})
	return sources, nil
}
