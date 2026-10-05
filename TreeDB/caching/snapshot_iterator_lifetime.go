package caching

import (
	"errors"
	"sync"
	"sync/atomic"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/merging"
)

type snapshotBoundIterator struct {
	cow    bool
	moveMu sync.Mutex
	owner  *Snapshot
	inner  merging.Iterator

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
	start     []byte
	end       []byte
}

func (s *Snapshot) bindNewIterator(create func() (merging.Iterator, error)) (merging.Iterator, error) {
	if s == nil || create == nil {
		return nil, backenddb.ErrClosed
	}
	return s.bindNewIteratorAtGeneration(s.generation.Load(), create)
}

func (s *Snapshot) bindNewIteratorAtGeneration(generation uint64, create func() (merging.Iterator, error)) (merging.Iterator, error) {
	if s == nil || create == nil {
		return nil, backenddb.ErrClosed
	}
	if s.cowCache != nil {
		if err := s.beginRead(); err != nil {
			return nil, err
		}
		defer s.endRead()
	}
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	if s.closed.Load() || generation != s.generation.Load() {
		return nil, backenddb.ErrClosed
	}
	inner, err := create()
	if err != nil {
		return nil, err
	}
	return s.bindIteratorLocked(inner)
}

// bindIteratorLocked registers inner while s.iteratorMu is held. Callers hold
// that lock across the closed check and all snapshot-backed source creation.
func (s *Snapshot) bindIteratorLocked(inner merging.Iterator) (merging.Iterator, error) {
	if inner == nil {
		return nil, backenddb.ErrClosed
	}
	start, end := inner.Domain()
	it := &snapshotBoundIterator{cow: s.cowCache != nil, owner: s, inner: inner, start: start, end: end}
	if s.iterators == nil {
		s.iterators = make(map[*snapshotBoundIterator]struct{})
	}
	s.iterators[it] = struct{}{}
	return it, nil
}

func (s *Snapshot) invalidateBoundIteratorsLocked() {
	for it := range s.iterators {
		it.closed.Store(true)
	}
}

func (it *snapshotBoundIterator) begin() bool {
	if it == nil {
		return false
	}
	if it.cow {
		it.moveMu.Lock()
	}
	valid := !it.closed.Load() && it.inner != nil
	if valid && it.cow {
		valid = it.owner != nil && it.owner.beginRead() == nil
	}
	if !valid && it.cow {
		it.moveMu.Unlock()
	}
	return valid
}
func (it *snapshotBoundIterator) endOperation() {
	if it.cow {
		it.owner.endRead()
		it.moveMu.Unlock()
	}
}

func (it *snapshotBoundIterator) Valid() bool {
	if !it.begin() {
		return false
	}
	defer it.endOperation()
	return it.inner.Valid()
}

func (it *snapshotBoundIterator) Next() {
	if !it.begin() {
		return
	}
	defer it.endOperation()
	it.inner.Next()
}

func (it *snapshotBoundIterator) Seek(key []byte) {
	if !it.begin() {
		return
	}
	defer it.endOperation()
	it.inner.Seek(key)
}

func (it *snapshotBoundIterator) Key() []byte {
	if !it.begin() {
		return nil
	}
	defer it.endOperation()
	return it.inner.Key()
}

func (it *snapshotBoundIterator) Value() []byte {
	if !it.begin() {
		return nil
	}
	defer it.endOperation()
	return it.inner.Value()
}

func (it *snapshotBoundIterator) KeyCopy(dst []byte) []byte {
	if !it.begin() {
		return dst[:0]
	}
	defer it.endOperation()
	return it.inner.KeyCopy(dst)
}

func (it *snapshotBoundIterator) ValueCopy(dst []byte) []byte {
	if !it.begin() {
		return dst[:0]
	}
	defer it.endOperation()
	return it.inner.ValueCopy(dst)
}

func (it *snapshotBoundIterator) Error() error {
	if !it.begin() {
		return backenddb.ErrClosed
	}
	defer it.endOperation()
	return it.inner.Error()
}

func (it *snapshotBoundIterator) Close() error {
	if it == nil {
		return nil
	}
	it.closed.Store(true)
	it.closeOnce.Do(func() {
		if it.cow {
			it.moveMu.Lock()
			defer it.moveMu.Unlock()
		}
		if it.inner != nil {
			it.closeErr = it.inner.Close()
		}
		owner := it.owner
		it.owner = nil
		if owner != nil {
			owner.iteratorMu.Lock()
			delete(owner.iterators, it)
			shouldFinalize := owner.closed.Load() && len(owner.iterators) == 0
			owner.iteratorMu.Unlock()
			if shouldFinalize {
				it.closeErr = errors.Join(it.closeErr, owner.finalizeCloseIfUnreferenced())
			}
		}
	})
	return it.closeErr
}

func (it *snapshotBoundIterator) Domain() ([]byte, []byte) { return it.start, it.end }
