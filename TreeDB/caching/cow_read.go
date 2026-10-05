package caching

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
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
	lease *memtable.COWExternalLease
	once  sync.Once
	err   error
}

func (it *cowMergeIterator) Close() error {
	it.once.Do(func() { it.err = it.Iterator.Close(); it.lease.Close() })
	return it.err
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
	// Pointer source wrappers and their read closures are retained by the merger.
	if s.db.memtableValueLogPointers {
		bytes += uint64(count-1) * (memtable.COWAllocationCharge(uint64(unsafe.Sizeof(valueLogIterator{}))) + memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0)))))
	}
	lease, err := s.cowCache.budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	sources, err := s.iteratorSources(start, end, false)
	if err != nil {
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
			return nil, err
		}
	}
	return &cowMergeIterator{Iterator: merging.NewMergingIteratorWithFixedHeap(sources, start, end), lease: lease}, nil
}
