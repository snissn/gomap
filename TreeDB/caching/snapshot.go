package caching

import (
	"errors"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Snapshot is a consistent point-in-time view of the cached+backend TreeDB state.
//
// Snapshot isolation in cached mode is implemented by rotating any pending writes
// out of the mutable memtables into the immutable queue, then reading from:
//   - the frozen queue (stored oldest-to-newest, scanned newest-to-oldest)
//   - the backend snapshot
//
// Mutable memtables are intentionally ignored so that writes after AcquireSnapshot
// are not visible through the snapshot.
type Snapshot struct {
	db      *DB
	view    *memtableView
	backend *backenddb.Snapshot

	closeOnce sync.Once
	closeErr  error
}

type backendSnapshotProvider interface {
	AcquireSnapshot() *backenddb.Snapshot
}

// AcquireSnapshot returns a cached snapshot that includes queued memtable writes.
func (db *DB) AcquireSnapshot() *Snapshot {
	if db == nil || db.backend == nil || db.closing.Load() {
		return nil
	}

	view := db.retainMemtableView()
	needsRotate := db.mutableBytes.Load() > 0
	if !needsRotate && view != nil {
		for _, mt := range view.mutables {
			if mt != nil && mt.Len() != 0 {
				needsRotate = true
				break
			}
		}
	}
	if view != nil && needsRotate {
		db.releaseMemtableView(view)
		view = nil

		db.mu.Lock()
		rotate := db.mutableBytes.Load() > 0
		if !rotate {
			for i := range db.mutableShards {
				mt := db.mutableShards[i].mem
				if mt != nil && mt.Len() != 0 {
					rotate = true
					break
				}
			}
		}
		if rotate {
			// Rotate to freeze the mutable memtables for snapshot isolation. Use the
			// iterator prealloc policy to keep the rotation cheap for read-heavy paths.
			if err := db.rotateMemtableLockedForIterator(minMemtablePrealloc); err != nil {
				db.mu.Unlock()
				if db.notifyError != nil {
					db.notifyError(err)
				}
				return nil
			}
		}
		view = db.retainMemtableView()
		db.mu.Unlock()
	}

	if view != nil && len(view.queue) == 0 {
		db.releaseMemtableView(view)
		view = nil
	}

	provider, ok := db.backend.(backendSnapshotProvider)
	if !ok {
		if view != nil {
			db.releaseMemtableView(view)
		}
		return nil
	}
	backendSnap := provider.AcquireSnapshot()
	if backendSnap == nil {
		if view != nil {
			db.releaseMemtableView(view)
		}
		return nil
	}

	return &Snapshot{db: db, view: view, backend: backendSnap}
}

func (s *Snapshot) Pager() *pager.Pager {
	if s == nil || s.backend == nil {
		return nil
	}
	return s.backend.Pager()
}

func (s *Snapshot) State() *backenddb.DBState {
	if s == nil || s.backend == nil {
		return nil
	}
	return s.backend.State()
}

func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		var errs []error
		if s.backend != nil {
			errs = append(errs, s.backend.Close())
			s.backend = nil
		}
		if s.view != nil && s.db != nil {
			s.db.releaseMemtableView(s.view)
			s.view = nil
		}
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

func (s *Snapshot) lookupQueueEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	if s == nil {
		return nil, page.ValuePtr{}, 0, false
	}
	snap := rootDomainSnapshotFromCachedSnapshot(s, key)
	return snap.getEntry(key)
}

func (s *Snapshot) iteratorSources(start, end []byte, reverse bool) ([]merging.IteratorSource, error) {
	if s == nil || s.backend == nil {
		return nil, backenddb.ErrClosed
	}
	queue := rootDomainSnapshotFromMemtableView(s.view, -1, false).immutables
	sources := make([]merging.IteratorSource, 0, len(queue)+1)
	prio := 0
	for idx := len(queue) - 1; idx >= 0; idx-- {
		var qIter iterator.UnsafeIterator
		if reverse {
			qIter = queue[idx].NewReverseIterator(start, end)
		} else {
			qIter = queue[idx].NewIterator(start, end)
		}
		if s.db != nil && s.db.memtableValueLogPointers {
			qIter = newValueLogIterator(qIter, func(key []byte, ptr page.ValuePtr) ([]byte, error) {
				return s.db.readValueLog(key, ptr)
			})
		}
		sources = append(sources, merging.IteratorSource{Iter: qIter, Priority: prio})
		prio++
	}
	var (
		diskIter iterator.UnsafeIterator
		err     error
	)
	if reverse {
		diskIter, err = s.backend.ReverseIterator(start, end)
	} else {
		diskIter, err = s.backend.Iterator(start, end)
	}
	if err != nil {
		for i := range sources {
			if sources[i].Iter != nil {
				_ = sources[i].Iter.Close()
			}
		}
		return nil, err
	}
	sources = append(sources, merging.IteratorSource{Iter: diskIter, Priority: prio})
	return sources, nil
}

// Iterator returns a stable iterator over the snapshot's queued memtables plus
// its backend snapshot.
func (s *Snapshot) Iterator(start, end []byte) (merging.Iterator, error) {
	sources, err := s.iteratorSources(start, end, false)
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return &emptyIterator{start: start, end: end}, nil
	}
	if len(sources) == 1 {
		return newSingleSourceIterator(sources[0].Iter, start, end), nil
	}
	return merging.NewMergingIterator(sources, start, end), nil
}

// ReverseIterator returns a stable reverse iterator over the snapshot's queued
// memtables plus its backend snapshot.
func (s *Snapshot) ReverseIterator(start, end []byte) (merging.Iterator, error) {
	sources, err := s.iteratorSources(start, end, true)
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return &emptyIterator{start: start, end: end}, nil
	}
	if len(sources) == 1 {
		return newSingleSourceIterator(sources[0].Iter, start, end), nil
	}
	return merging.NewReverseMergingIterator(sources, start, end), nil
}

func (s *Snapshot) GetAppend(key, dst []byte) ([]byte, error) {
	val, ptr, flags, found := s.lookupQueueEntry(key)
	if found {
		if flags&node.FlagTombstone != 0 {
			return dst, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer != 0 {
			if s.db == nil {
				return dst, errors.New("caching snapshot: value-log reader unavailable")
			}
			out, err := s.db.readValueLogAppend(key, ptr, dst)
			if err != nil {
				return dst, err
			}
			return out, nil
		}
		if val == nil {
			return dst, nil
		}
		return append(dst, val...), nil
	}

	if s == nil || s.backend == nil || s.db == nil {
		return dst, tree.ErrKeyNotFound
	}
	if err := s.db.flushDeferredValueLogForBackendRead(); err != nil {
		return dst, err
	}
	return s.backend.GetAppend(key, dst)
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.GetAppend(key, nil)
}

func (s *Snapshot) GetUnsafe(key []byte) ([]byte, error) {
	val, ptr, flags, found := s.lookupQueueEntry(key)
	if found {
		if flags&node.FlagTombstone != 0 {
			return nil, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer != 0 {
			if s.db == nil {
				return nil, errors.New("caching snapshot: value-log reader unavailable")
			}
			return s.db.readValueLog(key, ptr)
		}
		if val == nil {
			return []byte{}, nil
		}
		return val, nil
	}

	if s == nil || s.backend == nil || s.db == nil {
		return nil, tree.ErrKeyNotFound
	}
	if err := s.db.flushDeferredValueLogForBackendRead(); err != nil {
		return nil, err
	}
	return s.backend.GetUnsafe(key)
}

func (s *Snapshot) Has(key []byte) (bool, error) {
	_, _, flags, found := s.lookupQueueEntry(key)
	if found {
		return flags&node.FlagTombstone == 0, nil
	}
	if s == nil || s.backend == nil {
		return false, nil
	}
	return s.backend.Has(key)
}

func (s *Snapshot) GetEntry(key []byte) (node.LeafEntry, error) {
	val, ptr, flags, found := s.lookupQueueEntry(key)
	if found {
		keyCopy := append([]byte(nil), key...)
		return node.LeafEntry{
			Key:      keyCopy,
			Value:    val,
			ValuePtr: ptr,
			Flags:    flags,
		}, nil
	}

	if s == nil || s.backend == nil || s.db == nil {
		return node.LeafEntry{}, tree.ErrKeyNotFound
	}
	if err := s.db.flushDeferredValueLogForBackendRead(); err != nil {
		return node.LeafEntry{}, err
	}
	return s.backend.GetEntry(key)
}

func (s *Snapshot) GetEntryExact(key []byte) (node.LeafEntry, error) {
	val, ptr, flags, found := s.lookupQueueEntry(key)
	if found {
		keyCopy := append([]byte(nil), key...)
		return node.LeafEntry{
			Key:      keyCopy,
			Value:    val,
			ValuePtr: ptr,
			Flags:    flags,
		}, nil
	}

	if s == nil || s.backend == nil || s.db == nil {
		return node.LeafEntry{}, tree.ErrKeyNotFound
	}
	if err := s.db.flushDeferredValueLogForBackendRead(); err != nil {
		return node.LeafEntry{}, err
	}
	return s.backend.GetEntryExact(key)
}
