package caching

import (
	"errors"
	"sync"
	"sync/atomic"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/merging"
)

type snapshotBoundIterator struct {
	owner *Snapshot
	inner merging.Iterator

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
	start     []byte
	end       []byte
}

func (s *Snapshot) bindIterator(inner merging.Iterator) (merging.Iterator, error) {
	if inner == nil {
		return nil, backenddb.ErrClosed
	}
	start, end := inner.Domain()
	it := &snapshotBoundIterator{owner: s, inner: inner, start: start, end: end}
	s.iteratorMu.Lock()
	if s.closed.Load() {
		s.iteratorMu.Unlock()
		_ = inner.Close()
		return nil, backenddb.ErrClosed
	}
	if s.iterators == nil {
		s.iterators = make(map[*snapshotBoundIterator]struct{})
	}
	s.iterators[it] = struct{}{}
	s.iteratorMu.Unlock()
	return it, nil
}

func (s *Snapshot) invalidateBoundIterators() {
	s.iteratorMu.Lock()
	for it := range s.iterators {
		it.closed.Store(true)
	}
	s.iteratorMu.Unlock()
}

func (it *snapshotBoundIterator) begin() bool {
	return it != nil && !it.closed.Load() && it.inner != nil
}

func (it *snapshotBoundIterator) closeInner() {
	it.closeOnce.Do(func() { it.closeErr = it.inner.Close() })
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

func (it *snapshotBoundIterator) Error() error {
	if it == nil || it.closed.Load() || !it.begin() {
		return backenddb.ErrClosed
	}
	return it.inner.Error()
}

func (it *snapshotBoundIterator) Close() error {
	if it == nil {
		return nil
	}
	it.closed.Store(true)
	it.closeInner()
	if it.owner != nil {
		owner := it.owner
		owner.iteratorMu.Lock()
		delete(owner.iterators, it)
		shouldFinalize := owner.closed.Load() && len(owner.iterators) == 0
		owner.iteratorMu.Unlock()
		if shouldFinalize {
			it.closeErr = errors.Join(it.closeErr, owner.finalizeCloseIfUnreferenced())
		}
	}
	return it.closeErr
}

func (it *snapshotBoundIterator) Domain() ([]byte, []byte) { return it.start, it.end }
