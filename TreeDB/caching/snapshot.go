package caching

import (
	"errors"
	"sync"
	"sync/atomic"

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
//
// Snapshot pointers are single-use: after Close returns, callers must discard the
// pointer and treat further use as invalid.
type Snapshot struct {
	db                  *DB
	view                *memtableView
	backend             *backenddb.Snapshot
	rootVersion         uint64
	rootPointShards     []rootDomainSnapshot // snapshot point roots; mutable runs are intentionally excluded
	rootIterator        rootDomainSnapshot
	rootPublished       rootDomainLookup
	rootPublishedRootID uint64

	closed atomic.Bool
}

type ownedReadScratch struct {
	buf []byte
}

var ownedReadScratchPool = sync.Pool{
	New: func() any {
		return &ownedReadScratch{buf: make([]byte, 0, page.PageSize)}
	},
}

func getOwnedReadScratch() *ownedReadScratch {
	scratch, _ := ownedReadScratchPool.Get().(*ownedReadScratch)
	if scratch == nil || cap(scratch.buf) != page.PageSize {
		return &ownedReadScratch{buf: make([]byte, 0, page.PageSize)}
	}
	scratch.buf = scratch.buf[:0]
	return scratch
}

func putOwnedReadScratch(scratch *ownedReadScratch) {
	if scratch == nil || cap(scratch.buf) != page.PageSize {
		return
	}
	scratch.buf = scratch.buf[:0]
	ownedReadScratchPool.Put(scratch)
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

	snap := &Snapshot{db: db, view: view, backend: backendSnap}
	if view != nil {
		snap.rootVersion = view.rootVersion
		snap.rootPointShards = view.rootSnapshotShards
		snap.rootIterator = view.rootIterator
	}
	return snap
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
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	var err error
	if s.backend != nil {
		err = s.backend.Close()
		s.backend = nil
	}
	if s.view != nil && s.db != nil {
		s.db.releaseMemtableView(s.view)
		s.view = nil
	}
	s.rootPointShards = nil
	s.rootIterator = rootDomainSnapshot{}
	s.rootPublished = nil
	s.rootPublishedRootID = 0
	s.rootVersion = 0
	s.db = nil
	return err
}

func (s *Snapshot) lookupQueueEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	val, ptr, flags, found, _ = s.lookupRootDomainEntry(key)
	return val, ptr, flags, found
}

func (s *Snapshot) lookupRootDomainEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	if s == nil {
		return nil, page.ValuePtr{}, 0, false, rootDomainEntrySourceNone
	}
	snap := rootDomainSnapshotFromCachedSnapshot(s, key)
	return snap.getEntryWithSource(key)
}

func recordSnapshotRootDomainRead(source rootDomainEntrySource, pointer bool, bytes int) {
	if source == rootDomainEntrySourcePublished {
		snapshotReadBackendHitsTotal.Add(1)
		if bytes > 0 {
			snapshotReadBackendBytesTotal.Add(uint64(bytes))
		}
		return
	}
	if pointer {
		snapshotReadQueuePointerHitsTotal.Add(1)
		if bytes > 0 {
			snapshotReadQueuePointerBytesTotal.Add(uint64(bytes))
		}
		return
	}
	snapshotReadQueueInlineHitsTotal.Add(1)
	if bytes > 0 {
		snapshotReadQueueInlineBytesTotal.Add(uint64(bytes))
	}
}

func (s *Snapshot) iteratorSources(start, end []byte, reverse bool) ([]merging.IteratorSource, error) {
	if s == nil || s.backend == nil {
		return nil, backenddb.ErrClosed
	}
	queue := s.rootIterator.immutables
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
		err      error
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
	val, ptr, flags, found, source := s.lookupRootDomainEntry(key)
	if found {
		if flags&node.FlagTombstone != 0 {
			return dst, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer != 0 {
			if s.db == nil {
				return dst, errors.New("caching snapshot: value-log reader unavailable")
			}
			oldLen := len(dst)
			out, err := s.db.readValueLogAppend(key, ptr, dst)
			if err != nil {
				return dst, err
			}
			recordSnapshotRootDomainRead(source, true, len(out)-oldLen)
			return out, nil
		}
		if val == nil {
			recordSnapshotRootDomainRead(source, false, 0)
			return dst, nil
		}
		recordSnapshotRootDomainRead(source, false, len(val))
		return append(dst, val...), nil
	}

	if s == nil || s.backend == nil || s.db == nil {
		return dst, tree.ErrKeyNotFound
	}
	if err := s.db.flushValueLogForBackendRead(); err != nil {
		return dst, err
	}
	oldLen := len(dst)
	out, err := s.backend.GetAppend(key, dst)
	if err != nil {
		return dst, err
	}
	snapshotReadBackendHitsTotal.Add(1)
	if n := len(out) - oldLen; n > 0 {
		snapshotReadBackendBytesTotal.Add(uint64(n))
	}
	return out, nil
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	val, ptr, flags, found, source := s.lookupRootDomainEntry(key)
	if found {
		if flags&node.FlagTombstone != 0 {
			return nil, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer != 0 {
			if s.db == nil {
				return nil, errors.New("caching snapshot: value-log reader unavailable")
			}
			scratch := getOwnedReadScratch()
			defer putOwnedReadScratch(scratch)

			out, err := s.db.readValueLogAppend(key, ptr, scratch.buf[:0])
			if err != nil {
				return nil, err
			}
			recordSnapshotRootDomainRead(source, true, len(out))
			if len(out) == 0 {
				return nil, nil
			}
			maybeRecordSnapshotGetCallerSample(len(out))
			return ownedReadResult(out, scratch), nil
		}
		recordSnapshotRootDomainRead(source, false, len(val))
		if len(val) == 0 {
			return nil, nil
		}
		maybeRecordSnapshotGetCallerSample(len(val))
		owned := make([]byte, len(val))
		copy(owned, val)
		return owned, nil
	}

	if s == nil || s.backend == nil || s.db == nil {
		return nil, tree.ErrKeyNotFound
	}
	if err := s.db.flushValueLogForBackendRead(); err != nil {
		return nil, err
	}
	out, err := s.backend.Get(key)
	if err != nil {
		return nil, err
	}
	snapshotReadBackendHitsTotal.Add(1)
	if len(out) == 0 {
		return nil, nil
	}
	snapshotReadBackendBytesTotal.Add(uint64(len(out)))
	maybeRecordSnapshotGetCallerSample(len(out))
	return out, nil
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
	if err := s.db.flushValueLogForBackendRead(); err != nil {
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
	if err := s.db.flushValueLogForBackendRead(); err != nil {
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
	if err := s.db.flushValueLogForBackendRead(); err != nil {
		return node.LeafEntry{}, err
	}
	return s.backend.GetEntryExact(key)
}
