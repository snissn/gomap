package db

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// snapshotBoundIterator prevents a Snapshot from releasing its pinned state
// until every iterator invalidated by Snapshot.Close is itself closed.
// Registration and closure use a mutex; the steady-state path uses one atomic
// closed check and does not allocate.
type snapshotBoundIterator struct {
	owner *Snapshot
	inner iterator.UnsafeIterator

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
	start     []byte
	end       []byte
}

func (s *Snapshot) bindNewIterator(create func() iterator.UnsafeIterator) (iterator.UnsafeIterator, error) {
	if s == nil || create == nil {
		return nil, ErrClosed
	}
	return s.bindNewIteratorAtGeneration(s.generation.Load(), create)
}

func (s *Snapshot) bindNewIteratorAtGeneration(generation uint64, create func() iterator.UnsafeIterator) (iterator.UnsafeIterator, error) {
	if s == nil || create == nil {
		return nil, ErrClosed
	}
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	if s.closed.Load() || generation != s.generation.Load() {
		return nil, ErrClosed
	}
	return s.bindIteratorLocked(create())
}

// bindIteratorLocked registers inner while s.iteratorMu is held. Iterator
// creation holds that lock from the closed check through construction and this
// registration, so Snapshot.Close cannot recycle s between those steps.
func (s *Snapshot) bindIteratorLocked(inner iterator.UnsafeIterator) (iterator.UnsafeIterator, error) {
	if inner == nil {
		return nil, ErrClosed
	}
	start, end := inner.Domain()
	it := &snapshotBoundIterator{owner: s, inner: inner, start: start, end: end}
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
	return it != nil && !it.closed.Load() && it.inner != nil
}

func (it *snapshotBoundIterator) Valid() bool {
	if !it.begin() {
		return false
	}
	return it.inner.Valid()
}

func (it *snapshotBoundIterator) Next() {
	if !it.begin() {
		return
	}
	it.inner.Next()
}

func (it *snapshotBoundIterator) Seek(key []byte) {
	if !it.begin() {
		return
	}
	it.inner.Seek(key)
}

func (it *snapshotBoundIterator) UnsafeKey() []byte {
	if !it.begin() {
		return nil
	}
	return it.inner.UnsafeKey()
}

func (it *snapshotBoundIterator) UnsafeValue() []byte {
	if !it.begin() {
		return nil
	}
	return it.inner.UnsafeValue()
}

func (it *snapshotBoundIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.begin() {
		return nil, page.ValuePtr{}, 0
	}
	return it.inner.UnsafeEntry()
}

func (it *snapshotBoundIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.begin() {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	return iterator.UnsafeEntryWithRevision(it.inner)
}

func (it *snapshotBoundIterator) Key() []byte {
	if !it.begin() {
		return nil
	}
	return it.inner.Key()
}

func (it *snapshotBoundIterator) Value() []byte {
	if !it.begin() {
		return nil
	}
	return it.inner.Value()
}

func (it *snapshotBoundIterator) KeyCopy(dst []byte) []byte {
	if !it.begin() {
		return dst[:0]
	}
	return it.inner.KeyCopy(dst)
}

func (it *snapshotBoundIterator) ValueCopy(dst []byte) []byte {
	if !it.begin() {
		return dst[:0]
	}
	return it.inner.ValueCopy(dst)
}

func (it *snapshotBoundIterator) IsDeleted() bool {
	if !it.begin() {
		return false
	}
	return it.inner.IsDeleted()
}

func (it *snapshotBoundIterator) Error() error {
	if it == nil || it.closed.Load() || !it.begin() {
		return ErrClosed
	}
	return it.inner.Error()
}

func (it *snapshotBoundIterator) Close() error {
	if it == nil {
		return nil
	}
	it.closed.Store(true)
	it.closeOnce.Do(func() {
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
