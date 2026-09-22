package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	publiciterator "github.com/snissn/gomap/TreeDB/iterator"
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
	db              *DB
	view            *memtableView
	backend         *backenddb.Snapshot
	backendRootID   uint64
	backendFallback backendSnapshotLookup
	rootVersion     uint64
	rootPointShards []rootDomainSnapshot // snapshot point roots; mutable runs are intentionally excluded
	rootSystem      rootDomainSnapshot
	rootIterator    rootDomainSnapshot
	publishedRoots  *publishedRootSet

	closed     atomic.Bool
	generation atomic.Uint64
	finalized  atomic.Bool
	readState  atomic.Uint64
	iteratorMu sync.Mutex
	iterators  map[*snapshotBoundIterator]struct{}
}

type ownedReadScratch struct {
	buf []byte
}

var ownedReadScratchPool = sync.Pool{
	New: func() any {
		return &ownedReadScratch{buf: make([]byte, 0, page.PageSize)}
	},
}

func getSnapshot() *Snapshot {
	// Exported Snapshot handles must have unique identities. Reusing an address
	// would let a stale pointer retained after Close operate on the new snapshot;
	// no generation stored in the reused object can distinguish those aliases.
	snap := &Snapshot{}
	snap.iteratorMu.Lock()
	snap.generation.Add(1)
	snap.closed.Store(true)
	snap.readState.Store(snapshotReadClosedBit)
	snap.iteratorMu.Unlock()
	return snap
}

func activateSnapshot(snap *Snapshot) *Snapshot {
	if snap == nil {
		return nil
	}
	snap.iteratorMu.Lock()
	snap.closed.Store(false)
	snap.readState.Store(0)
	snap.iteratorMu.Unlock()
	return snap
}

func putSnapshot(snap *Snapshot) {
	if snap == nil {
		return
	}
	snap.db = nil
	snap.view = nil
	snap.backend = nil
	snap.backendRootID = 0
	snap.backendFallback = backendSnapshotLookup{}
	snap.rootVersion = 0
	snap.rootPointShards = nil
	snap.rootSystem = rootDomainSnapshot{}
	snap.rootIterator = rootDomainSnapshot{}
	snap.publishedRoots = nil
	snap.iteratorMu.Lock()
	clear(snap.iterators)
	snap.readState.Store(snapshotReadClosedBit)
	snap.closed.Store(true)
	snap.finalized.Store(false)
	snap.iteratorMu.Unlock()
	// Do not make the exported handle reusable. A stale alias must remain closed.
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

func snapshotRootDomainBlocksBackendFastPath(snap rootDomainSnapshot) bool {
	return rootDomainSnapshotHasInMemoryState(snap) || rootDomainSnapshotHasPublishedState(snap)
}

func snapshotRootDomainStaticBlocksBackendFastPath(snap rootDomainSnapshot) bool {
	return len(snap.immutables) != 0 || rootDomainSnapshotHasPublishedState(snap)
}

func memtableViewAllowsBackendSnapshotFastPath(view *memtableView) bool {
	if view == nil {
		return false
	}
	if len(view.queue) != 0 || view.publishedRoots != nil {
		return false
	}
	for _, mt := range view.mutables {
		if mt != nil && mt.Len() != 0 {
			return false
		}
	}
	pointSnapshots := view.rootSnapshotShards
	if len(pointSnapshots) == 0 {
		pointSnapshots = view.rootPointShards
	}
	for i := range pointSnapshots {
		if snapshotRootDomainStaticBlocksBackendFastPath(pointSnapshots[i]) {
			return false
		}
	}
	return !snapshotRootDomainBlocksBackendFastPath(view.rootSystem) &&
		!snapshotRootDomainBlocksBackendFastPath(view.rootIterator)
}

func (db *DB) backendReadValueLogCleanForSnapshotFastPath() bool {
	if db == nil || !db.valueLogEnabled() {
		return true
	}
	dirtySeq := db.backendReadVlogDirtySeq.Load()
	return dirtySeq == 0 || dirtySeq == db.backendReadVlogFlushedSeq.Load()
}

// AcquireBackendSnapshotFastPath returns a backend snapshot when the cached
// layer has no mutable, queued, root-domain, or pending value-log state that the
// cached Snapshot wrapper must preserve. The returned snapshot has the normal
// backend snapshot lifetime and must be closed by the caller.
func (db *DB) AcquireBackendSnapshotFastPath() *backenddb.Snapshot {
	if db == nil || db.backend == nil || db.closing.Load() {
		return nil
	}
	if !db.backendReadValueLogCleanForSnapshotFastPath() {
		return nil
	}
	view := db.retainMemtableViewUntracked()
	if !memtableViewAllowsBackendSnapshotFastPath(view) {
		db.releaseUntrackedMemtableView(view)
		return nil
	}
	db.releaseUntrackedMemtableView(view)

	provider, ok := db.backend.(backendSnapshotProvider)
	if !ok {
		return nil
	}
	backendSnap := provider.AcquireSnapshot()
	if backendSnap == nil {
		return nil
	}
	// A durability-none value-log flush can race between the clean check above
	// and the backend snapshot acquisition. If that happens, the raw backend
	// snapshot may see a newly-published pointer whose value-log tail still needs
	// the cached read barrier. Fail closed to the cached Snapshot wrapper.
	if !db.backendReadValueLogCleanForSnapshotFastPath() {
		_ = backendSnap.Close()
		return nil
	}
	return backendSnap
}

// AcquireSnapshot returns a cached snapshot that includes queued memtable writes.
func (db *DB) AcquireSnapshot() *Snapshot {
	if db == nil || db.backend == nil || db.closing.Load() {
		return nil
	}
	if backendSnap := db.AcquireBackendSnapshotFastPath(); backendSnap != nil {
		var backendRootID uint64
		if state, ok := backendSnap.StateToken(); ok {
			backendRootID = state.RootPageID
		}
		snap := getSnapshot()
		snap.db = db
		snap.backend = backendSnap
		snap.backendRootID = backendRootID
		snap.backendFallback = backendSnapshotLookup{db: db, snapshot: backendSnap, rootID: backendRootID}
		return activateSnapshot(snap)
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

	var (
		viewRootVersion     uint64
		viewRootPointShards []rootDomainSnapshot
		viewRootSystem      rootDomainSnapshot
		viewRootIterator    rootDomainSnapshot
		viewPublishedRoots  *publishedRootSet
	)
	if view != nil {
		viewRootVersion = view.rootVersion
		viewRootPointShards = view.rootSnapshotShards
		viewRootSystem = view.rootSystem
		viewRootIterator = view.rootIterator
		viewPublishedRoots = view.publishedRoots
		if len(view.queue) == 0 {
			if viewPublishedRoots == nil {
				viewRootVersion = 0
				viewRootPointShards = nil
				viewRootSystem = rootDomainSnapshot{}
				viewRootIterator = rootDomainSnapshot{}
			}
			db.releaseMemtableView(view)
			view = nil
		}
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

	var backendRootID uint64
	if state, ok := backendSnap.StateToken(); ok {
		backendRootID = state.RootPageID
	}
	snap := getSnapshot()
	snap.db = db
	snap.view = view
	snap.backend = backendSnap
	snap.backendRootID = backendRootID
	snap.backendFallback = backendSnapshotLookup{db: db, snapshot: backendSnap, rootID: backendRootID}
	snap.rootVersion = viewRootVersion
	snap.rootPointShards = viewRootPointShards
	snap.rootSystem = viewRootSystem
	snap.rootIterator = viewRootIterator
	snap.publishedRoots = viewPublishedRoots
	if snap.publishedRoots == nil {
		db.rootPublishStats.backendFallbacks.Add(1)
	}
	return activateSnapshot(snap)
}

func (s *Snapshot) Pager() *pager.Pager {
	if err := s.beginRead(); err != nil {
		return nil
	}
	defer s.endRead()
	if s.backend == nil {
		return nil
	}
	return s.backend.Pager()
}

func (s *Snapshot) State() *backenddb.DBState {
	if err := s.beginRead(); err != nil {
		return nil
	}
	defer s.endRead()
	if s.backend == nil {
		return nil
	}
	return s.backend.State()
}

func (s *Snapshot) StateToken() (backenddb.StateToken, bool) {
	if err := s.beginRead(); err != nil {
		return backenddb.StateToken{}, false
	}
	defer s.endRead()
	if s.backend == nil {
		return backenddb.StateToken{}, false
	}
	return s.backend.StateToken()
}

func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}
	s.iteratorMu.Lock()
	if !s.closed.CompareAndSwap(false, true) {
		s.iteratorMu.Unlock()
		return nil
	}
	s.readState.Or(snapshotReadClosedBit)
	s.invalidateBoundIteratorsLocked()
	s.iteratorMu.Unlock()
	return s.finalizeCloseIfUnreferenced()
}

func (s *Snapshot) finalizeCloseIfUnreferenced() error {
	s.iteratorMu.Lock()
	if len(s.iterators) != 0 || s.readState.Load() != snapshotReadClosedBit || !s.finalized.CompareAndSwap(false, true) {
		s.iteratorMu.Unlock()
		return nil
	}
	s.iteratorMu.Unlock()
	var err error
	if s.backend != nil {
		err = s.backend.Close()
	}
	if s.view != nil && s.db != nil {
		s.db.releaseMemtableView(s.view)
	}
	putSnapshot(s)
	return err
}

func (s *Snapshot) lookupQueueEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	val, ptr, flags, _, found, _ = s.lookupRootDomainEntryWithRevision(key)
	return val, ptr, flags, found
}

func (s *Snapshot) lookupQueueEntryWithRevision(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool) {
	val, ptr, flags, revision, found, _ = s.lookupRootDomainEntryWithRevision(key)
	return val, ptr, flags, revision, found
}

func (s *Snapshot) lookupRootDomainSnapshotEntry(key []byte) (snap rootDomainSnapshot, val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	snap, val, ptr, flags, _, found, source = s.lookupRootDomainSnapshotEntryWithRevision(key)
	return snap, val, ptr, flags, found, source
}

func (s *Snapshot) lookupRootDomainSnapshotEntryWithRevision(key []byte) (snap rootDomainSnapshot, val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool, source rootDomainEntrySource) {
	if s == nil {
		return rootDomainSnapshot{}, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, rootDomainEntrySourceNone
	}
	if memtableViewHasRangeSpans(s.view) {
		snap = rootDomainSnapshotFromCachedSnapshot(s, key)
		val, ptr, flags, revision, found, source = s.db.lookupViewEntryWithRangeSpansAndRootRevisionSource(s.view, key, false, snap, true)
		if found {
			return snap, val, ptr, flags, revision, true, source
		}
		return snap, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, rootDomainEntrySourceNone
	}
	snap = rootDomainSnapshotFromCachedSnapshot(s, key)
	val, ptr, flags, revision, found, source = snap.getEntryWithRevisionSource(key)
	return snap, val, ptr, flags, revision, found, source
}

func (s *Snapshot) lookupRootDomainEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	_, val, ptr, flags, _, found, source = s.lookupRootDomainSnapshotEntryWithRevision(key)
	return val, ptr, flags, found, source
}

func (s *Snapshot) lookupRootDomainEntryWithRevision(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool, source rootDomainEntrySource) {
	_, val, ptr, flags, revision, found, source = s.lookupRootDomainSnapshotEntryWithRevision(key)
	return val, ptr, flags, revision, found, source
}

func (s *Snapshot) lookupCachedRootDomainEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	val, ptr, flags, _, found = s.lookupCachedRootDomainEntryWithRevision(key)
	return val, ptr, flags, found
}

func (s *Snapshot) lookupCachedRootDomainEntryWithRevision(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool) {
	if s == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	if memtableViewHasRangeSpans(s.view) {
		val, ptr, flags, revision, found, _ = s.lookupEntryWithRangeSpansRevisionSource(key)
		return val, ptr, flags, revision, found
	}
	if len(s.rootPointShards) == 0 {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	shardIdx := 0
	if s.db != nil {
		shardIdx = s.db.shardIndex(key)
	}
	if shardIdx < 0 || shardIdx >= len(s.rootPointShards) {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	snap := s.rootPointShards[shardIdx]
	snap.published = nil
	snap.publishedRootID = 0
	return snap.getEntryWithRevision(key)
}

func recordSnapshotRootDomainRead(source rootDomainEntrySource, pointer bool, bytes int) {
	if !hotPathStatsEnabled {
		return
	}
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

type rootDomainPublishedValueLookup interface {
	GetValueAppend(key, dst []byte) ([]byte, error)
	GetValueUnsafe(key []byte) ([]byte, error)
}

type rootDomainPublishedBackendLookupMarker interface {
	rootDomainPublishedBackendLookupMarker()
}

// ErrSnapshotValueLogReaderUnavailable reports that a snapshot value-log
// pointer read could not proceed because the snapshot has no value-log reader.
var ErrSnapshotValueLogReaderUnavailable = errors.New("caching snapshot: value-log reader unavailable")

func rootDomainPublishedGetAppend(snap rootDomainSnapshot, key, dst []byte) ([]byte, bool, error) {
	if snap.published == nil {
		return dst, false, nil
	}
	lookup, ok := snap.published.(rootDomainPublishedValueLookup)
	if !ok {
		return dst, false, nil
	}
	out, err := lookup.GetValueAppend(key, dst)
	return out, true, err
}

func rootDomainPublishedGetUnsafe(snap rootDomainSnapshot, key []byte) ([]byte, bool, error) {
	if snap.published == nil {
		return nil, false, nil
	}
	lookup, ok := snap.published.(rootDomainPublishedValueLookup)
	if !ok {
		return nil, false, nil
	}
	out, err := lookup.GetValueUnsafe(key)
	return out, true, err
}

func rootDomainPublishedUsesBackendLookup(snap rootDomainSnapshot) bool {
	lookup, ok := snap.published.(rootDomainPublishedValueLookup)
	if !ok {
		return false
	}
	_, ok = lookup.(rootDomainPublishedBackendLookupMarker)
	return ok
}

func shouldShortCircuitPublishedAppendMiss(checkedPublishedEntry bool, publishedRoots *publishedRootSet, domainSnap rootDomainSnapshot) bool {
	if !checkedPublishedEntry || !rootDomainPublishedUsesBackendLookup(domainSnap) {
		return false
	}
	if domainSnap.publishedRootID != 0 {
		return true
	}
	return publishedRoots == nil
}

func (s *Snapshot) getAppendFromEntryWithSource(snap rootDomainSnapshot, key, dst []byte, oldLen int) ([]byte, bool, error) {
	val, ptr, flags, found, source := snap.getEntryWithSource(key)
	if !found {
		return dst, false, nil
	}
	if flags&node.FlagTombstone != 0 {
		return dst, true, tree.ErrKeyNotFound
	}
	if flags&node.FlagPointer != 0 {
		var valueLogDB *DB
		if s != nil {
			valueLogDB = s.db
		}
		if valueLogDB == nil {
			return dst, true, ErrSnapshotValueLogReaderUnavailable
		}
		out, err := valueLogDB.readValueLogAppend(key, ptr, dst)
		if err != nil {
			return dst, true, err
		}
		recordSnapshotRootDomainRead(source, true, len(out)-oldLen)
		return out, true, nil
	}
	if val == nil {
		recordSnapshotRootDomainRead(source, false, 0)
		return dst, true, nil
	}
	recordSnapshotRootDomainRead(source, false, len(val))
	return append(dst, val...), true, nil
}

func (s *Snapshot) appendVersionedEntryValue(key, dst []byte, oldLen int, val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, source rootDomainEntrySource) ([]byte, page.EntryRevision, error) {
	if flags&node.FlagTombstone != 0 {
		return dst, revision, tree.ErrKeyNotFound
	}
	if flags&node.FlagPointer != 0 {
		var valueLogDB *DB
		if s != nil {
			valueLogDB = s.db
		}
		if valueLogDB == nil {
			return dst, revision, ErrSnapshotValueLogReaderUnavailable
		}
		out, err := valueLogDB.readValueLogAppend(key, ptr, dst)
		if err != nil {
			return dst, revision, err
		}
		recordSnapshotRootDomainRead(source, true, len(out)-oldLen)
		return out, revision, nil
	}
	if val == nil {
		recordSnapshotRootDomainRead(source, false, 0)
		return dst, revision, nil
	}
	recordSnapshotRootDomainRead(source, false, len(val))
	return append(dst, val...), revision, nil
}

func (s *Snapshot) getVersionedAppendFromEntryWithSource(snap rootDomainSnapshot, key, dst []byte, oldLen int) ([]byte, page.EntryRevision, bool, error) {
	val, ptr, flags, revision, found, source := snap.getEntryWithRevisionSource(key)
	if !found {
		return dst, page.LegacyEntryRevision, false, nil
	}
	out, revision, err := s.appendVersionedEntryValue(key, dst, oldLen, val, ptr, flags, revision, source)
	return out, revision, true, err
}

func (s *Snapshot) iteratorSources(start, end []byte, reverse bool) ([]merging.IteratorSource, error) {
	if s == nil || s.backend == nil {
		return nil, backenddb.ErrClosed
	}
	rootSnap := rootDomainIteratorSnapshotFromCachedSnapshot(s)
	queue := rootSnap.immutables
	var queueRangeSpans [][]batch.DeleteRange
	if s.view != nil && len(s.view.queueRangeSpans) == len(queue) {
		queueRangeSpans = s.view.queueRangeSpans
	}
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
		qIter = newRangeSpanFilteringIterator(qIter, appendNewerRangeSpansForSource(nil, queueRangeSpans, idx), s.db)
		sources = append(sources, merging.IteratorSource{Iter: qIter, Priority: prio})
		prio++
	}
	var (
		diskIter iterator.UnsafeIterator
		err      error
		ok       bool
	)
	if reverse {
		diskIter, ok, err = rootDomainPublishedReverseIterator(rootSnap, start, end)
		if !ok {
			diskIter, err = s.backend.ReverseIteratorWithOptions(start, end, backenddb.IteratorOptions{})
			ok = true
		}
	} else {
		diskIter, ok, err = rootDomainPublishedIterator(rootSnap, start, end)
		if !ok {
			diskIter, err = s.backend.IteratorWithOptions(start, end, backenddb.IteratorOptions{})
			ok = true
		}
	}
	if err != nil {
		for i := range sources {
			if sources[i].Iter != nil {
				_ = sources[i].Iter.Close()
			}
		}
		return nil, err
	}
	if ok && diskIter != nil {
		diskIter = newRangeSpanFilteringIterator(diskIter, appendNewerRangeSpansForSource(nil, queueRangeSpans, -1), s.db)
		sources = append(sources, merging.IteratorSource{Iter: diskIter, Priority: prio})
	}
	return sources, nil
}

// Iterator returns a stable iterator over the snapshot's queued memtables plus
// its backend snapshot.
func (s *Snapshot) Iterator(start, end []byte) (publiciterator.Iterator, error) {
	return s.bindNewIterator(func() (merging.Iterator, error) {
		return s.buildIteratorLocked(start, end, false)
	})
}

// Iterate calls fn for each visible key/value pair in [start, end) using this
// snapshot's pinned memtable and backend view. Key and value are read-only views
// valid only until fn returns. Returning an error from fn stops iteration and
// returns that error.
func (s *Snapshot) Iterate(start, end []byte, fn func(key, value []byte) error) error {
	return s.iterate(start, end, false, fn)
}

// ReverseIterator returns a stable reverse iterator over the snapshot's queued
// memtables plus its backend snapshot.
func (s *Snapshot) ReverseIterator(start, end []byte) (publiciterator.Iterator, error) {
	return s.bindNewIterator(func() (merging.Iterator, error) {
		return s.buildIteratorLocked(start, end, true)
	})
}

// buildIteratorLocked accesses the snapshot's pinned view and backend and must
// run while iteratorMu is held by bindNewIterator.
func (s *Snapshot) buildIteratorLocked(start, end []byte, reverse bool) (merging.Iterator, error) {
	sources, err := s.iteratorSources(start, end, reverse)
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return &emptyIterator{start: start, end: end}, nil
	}
	if len(sources) == 1 {
		return newSingleSourceIterator(sources[0].Iter, start, end, reverse), nil
	}
	if reverse {
		return merging.NewReverseMergingIterator(sources, start, end), nil
	}
	return merging.NewMergingIterator(sources, start, end), nil
}

// ReverseIterate calls fn for each visible key/value pair in [start, end) in
// reverse order using this snapshot's pinned memtable and backend view. Key and
// value are read-only views valid only until fn returns. Returning an error from
// fn stops iteration and returns that error.
func (s *Snapshot) ReverseIterate(start, end []byte, fn func(key, value []byte) error) error {
	return s.iterate(start, end, true, fn)
}

func (s *Snapshot) iterate(start, end []byte, reverse bool, fn func(key, value []byte) error) (err error) {
	if err := s.beginRead(); err != nil {
		return err
	}
	s.endRead()
	if fn == nil {
		return errors.New("treedb: snapshot iterate nil callback")
	}
	var it publiciterator.Iterator
	if reverse {
		it, err = s.ReverseIterator(start, end)
	} else {
		it, err = s.Iterator(start, end)
	}
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, it.Close())
	}()
	var iterErr error
	for it.Valid() {
		key := it.Key()
		value := it.Value()
		if err := it.Error(); err != nil {
			iterErr = err
			break
		}
		if err := fn(key, value); err != nil {
			iterErr = err
			break
		}
		it.Next()
	}
	if iterErr == nil {
		iterErr = it.Error()
	}
	return iterErr
}

func (s *Snapshot) GetAppend(key, dst []byte) ([]byte, error) {
	if s == nil {
		return dst, tree.ErrKeyNotFound
	}
	if err := s.beginRead(); err != nil {
		return dst, err
	}
	defer s.endRead()
	key = normalizeRawKVPointKey(key)
	// Critical fast path for parallel point reads:
	// consult only mutable/immutable memtables first, then query published/backend
	// directly via append APIs. This avoids a published GetEntry pre-read that can
	// materialize leaf-log pages before the actual GetAppend pointer read.
	val, ptr, flags, found := s.lookupCachedRootDomainEntry(key)
	if found {
		if flags&node.FlagTombstone != 0 {
			return dst, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer != 0 {
			if s.db == nil {
				return dst, ErrSnapshotValueLogReaderUnavailable
			}
			oldLen := len(dst)
			out, err := s.db.readValueLogAppend(key, ptr, dst)
			if err != nil {
				return dst, err
			}
			recordSnapshotRootDomainRead(rootDomainEntrySourceCached, true, len(out)-oldLen)
			return out, nil
		}
		if val == nil {
			recordSnapshotRootDomainRead(rootDomainEntrySourceCached, false, 0)
			return dst, nil
		}
		recordSnapshotRootDomainRead(rootDomainEntrySourceCached, false, len(val))
		return append(dst, val...), nil
	}
	if memtableViewHasRangeSpans(s.view) {
		if s.backend == nil || s.db == nil {
			return dst, tree.ErrKeyNotFound
		}
		if err := s.db.flushValueLogForBackendRead(); err != nil {
			return dst, err
		}
		return s.backend.GetAppend(key, dst)
	}
	snap := rootDomainSnapshotFromCachedSnapshot(s, key)
	// backendSnapshotLookup (used when a published ref falls back to backend
	// snapshot lookup) flushes value-log state before backend snapshot reads.
	publishedRoots := s.publishedRoots
	origDst := dst
	oldLen := len(dst)
	out, ok, err := rootDomainPublishedGetAppend(snap, key, dst)
	checkedPublishedEntry := false
	if ok {
		if err != nil {
			// Published append lookups should not leak partial dst appends across
			// fallback paths when they report misses/errors.
			dst = origDst[:oldLen]
			if len(out) >= oldLen {
				dst = out[:oldLen]
			}
		}
		if err == nil {
			recordSnapshotRootDomainRead(rootDomainEntrySourcePublished, true, len(out)-oldLen)
			return out, nil
		}
		// Preserve historical miss semantics: published append misses should still
		// fall through to backend lookup when the key is absent, while true
		// tombstones remain not-found.
		if !errors.Is(err, tree.ErrKeyNotFound) {
			return dst, err
		}
		// Hot miss path: when this snapshot has no in-memory root-domain state,
		// a published not-found cannot be shadowed by queued tombstones.
		// Avoid an extra GetEntry probe that re-materializes leaf pages.
		if !rootDomainSnapshotHasInMemoryState(snap) && rootDomainPublishedUsesBackendLookup(snap) {
			return dst, tree.ErrKeyNotFound
		}
		checkedPublishedEntry = true
		if shouldShortCircuitPublishedAppendMiss(true, publishedRoots, snap) {
			// Published backend append miss already probed the relevant root.
			return dst, tree.ErrKeyNotFound
		}
		if out, found, err := s.getAppendFromEntryWithSource(snap, key, dst, oldLen); found {
			return out, err
		}
	}
	if !checkedPublishedEntry {
		if out, found, err := s.getAppendFromEntryWithSource(snap, key, dst, oldLen); found {
			return out, err
		}
	}
	if shouldShortCircuitPublishedAppendMiss(checkedPublishedEntry, publishedRoots, snap) {
		// Backend-published append misses already queried the target root:
		// - root-bound lookups (publishedRootID != 0) must not fall back to
		//   default-root GetAppend, which can cross root domains.
		// - when no published root set is pinned, backend fallback lookup already
		//   queried the default root (after flushValueLogForBackendRead inside
		//   backendSnapshotLookup.GetValueAppend), so avoid a duplicate backend miss
		//   probe.
		return dst, tree.ErrKeyNotFound
	}

	if s.backend == nil || s.db == nil {
		return dst, tree.ErrKeyNotFound
	}
	if err := s.db.flushValueLogForBackendRead(); err != nil {
		return dst, err
	}
	out, err = s.backend.GetAppend(key, dst)
	if err != nil {
		return dst, err
	}
	if hotPathStatsEnabled {
		snapshotReadBackendHitsTotal.Add(1)
		if n := len(out) - oldLen; n > 0 {
			snapshotReadBackendBytesTotal.Add(uint64(n))
		}
	}
	return out, nil
}

func (s *Snapshot) GetVersionedAppend(key, dst []byte) ([]byte, page.EntryRevision, error) {
	if s == nil {
		return dst, page.LegacyEntryRevision, tree.ErrKeyNotFound
	}
	if err := s.beginRead(); err != nil {
		return dst, page.LegacyEntryRevision, err
	}
	defer s.endRead()
	key = normalizeRawKVPointKey(key)
	oldLen := len(dst)
	val, ptr, flags, revision, found := s.lookupCachedRootDomainEntryWithRevision(key)
	if found {
		return s.appendVersionedEntryValue(key, dst, oldLen, val, ptr, flags, revision, rootDomainEntrySourceCached)
	}
	if memtableViewHasRangeSpans(s.view) {
		if s.backend == nil || s.db == nil {
			return dst, page.LegacyEntryRevision, tree.ErrKeyNotFound
		}
		if err := s.db.flushValueLogForBackendRead(); err != nil {
			return dst, page.LegacyEntryRevision, err
		}
		return s.backend.GetVersionedAppend(key, dst)
	}
	snap := rootDomainSnapshotFromCachedSnapshot(s, key)
	if out, entryRevision, found, err := s.getVersionedAppendFromEntryWithSource(snap, key, dst, oldLen); found {
		return out, entryRevision, err
	}
	if shouldShortCircuitPublishedAppendMiss(true, s.publishedRoots, snap) {
		return dst, page.LegacyEntryRevision, tree.ErrKeyNotFound
	}
	if s.backend == nil || s.db == nil {
		return dst, page.LegacyEntryRevision, tree.ErrKeyNotFound
	}
	if err := s.db.flushValueLogForBackendRead(); err != nil {
		return dst, page.LegacyEntryRevision, err
	}
	out, entryRevision, err := s.backend.GetVersionedAppend(key, dst)
	if err != nil {
		return dst, entryRevision, err
	}
	if hotPathStatsEnabled {
		snapshotReadBackendHitsTotal.Add(1)
		if n := len(out) - oldLen; n > 0 {
			snapshotReadBackendBytesTotal.Add(uint64(n))
		}
	}
	return out, entryRevision, nil
}

func (s *Snapshot) GetVersioned(key []byte) ([]byte, page.EntryRevision, error) {
	out, revision, err := s.GetVersionedAppend(key, nil)
	if err != nil {
		return nil, revision, err
	}
	if len(out) == 0 {
		return []byte{}, revision, nil
	}
	if cap(out) == len(out) {
		return out, revision, nil
	}
	owned := make([]byte, len(out))
	copy(owned, out)
	return owned, revision, nil
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	key = normalizeRawKVPointKey(key)
	snap, val, ptr, flags, found, source := s.lookupRootDomainSnapshotEntry(key)
	if found {
		if flags&node.FlagTombstone != 0 {
			return nil, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer != 0 {
			if source == rootDomainEntrySourcePublished {
				scratch := getOwnedReadScratch()
				defer putOwnedReadScratch(scratch)

				out, ok, err := rootDomainPublishedGetAppend(snap, key, scratch.buf[:0])
				if ok {
					if err != nil {
						return nil, err
					}
					recordSnapshotRootDomainRead(source, true, len(out))
					if len(out) == 0 {
						return []byte{}, nil
					}
					maybeRecordSnapshotGetCallerSample(len(out))
					return ownedReadResult(out, scratch), nil
				}
			}
			if s.db == nil {
				return nil, ErrSnapshotValueLogReaderUnavailable
			}
			scratch := getOwnedReadScratch()
			defer putOwnedReadScratch(scratch)

			out, err := s.db.readValueLogAppend(key, ptr, scratch.buf[:0])
			if err != nil {
				return nil, err
			}
			recordSnapshotRootDomainRead(source, true, len(out))
			if len(out) == 0 {
				return []byte{}, nil
			}
			maybeRecordSnapshotGetCallerSample(len(out))
			return ownedReadResult(out, scratch), nil
		}
		if len(val) == 0 {
			recordSnapshotRootDomainRead(source, false, len(val))
			return []byte{}, nil
		}
		recordSnapshotRootDomainRead(source, false, len(val))
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
	if hotPathStatsEnabled {
		snapshotReadBackendHitsTotal.Add(1)
	}
	if len(out) == 0 {
		return []byte{}, nil
	}
	if hotPathStatsEnabled {
		snapshotReadBackendBytesTotal.Add(uint64(len(out)))
	}
	maybeRecordSnapshotGetCallerSample(len(out))
	return out, nil
}

func (s *Snapshot) GetUnsafe(key []byte) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	return s.getUnsafeOpen(key)
}

func (s *Snapshot) getUnsafeOpen(key []byte) ([]byte, error) {
	key = normalizeRawKVPointKey(key)
	snap, val, ptr, flags, found, source := s.lookupRootDomainSnapshotEntry(key)
	if found {
		if flags&node.FlagTombstone != 0 {
			return nil, tree.ErrKeyNotFound
		}
		if flags&node.FlagPointer != 0 {
			if source == rootDomainEntrySourcePublished {
				out, ok, err := rootDomainPublishedGetUnsafe(snap, key)
				if ok {
					return out, err
				}
			}
			if s.db == nil {
				return nil, ErrSnapshotValueLogReaderUnavailable
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

// GetManyView calls fn once for each key with a read-only value view. Values
// are valid only until fn returns and must be copied before retaining.
func (s *Snapshot) GetManyView(keys [][]byte, fn tree.GetManyViewFunc) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if fn == nil {
		return errors.New("caching snapshot: GetManyView nil callback")
	}
	keys = normalizeRawKVPointKeys(keys)
	if len(keys) == 0 {
		return nil
	}
	if s.backend == nil {
		return backenddb.ErrClosed
	}
	for i, key := range keys {
		val, err := s.getUnsafeOpen(key)
		if err == tree.ErrKeyNotFound {
			if err := fn(i, key, nil, false); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if len(val) == 0 {
			val = rootDomainGetManyEmptyValue
		}
		if err := fn(i, key, val, true); err != nil {
			return err
		}
	}
	return nil
}

func (s *Snapshot) Has(key []byte) (bool, error) {
	if err := s.beginRead(); err != nil {
		return false, err
	}
	defer s.endRead()
	return s.hasOpen(key)
}

func (s *Snapshot) hasOpen(key []byte) (bool, error) {
	key = normalizeRawKVPointKey(key)
	_, _, flags, found := s.lookupCachedRootDomainEntry(key)
	if found {
		return flags&node.FlagTombstone == 0, nil
	}
	_, _, flags, found = s.lookupQueueEntry(key)
	if found {
		return flags&node.FlagTombstone == 0, nil
	}
	if s == nil || s.backend == nil {
		return false, nil
	}
	return s.backend.Has(key)
}

func (s *Snapshot) HasMany(keys [][]byte) ([]bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	keys = normalizeRawKVPointKeys(keys)
	out := make([]bool, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	if s == nil || s.backend == nil {
		return nil, backenddb.ErrClosed
	}
	if memtableViewHasRangeSpans(s.view) {
		for i, key := range keys {
			ok, err := s.hasOpen(key)
			if err != nil {
				return nil, err
			}
			out[i] = ok
		}
		return out, nil
	}

	refs := make([]getManyProbeRef, len(keys))
	shardCount := len(s.rootPointShards)
	for i, key := range keys {
		shard := 0
		if shardCount > 1 && s.db != nil {
			shard = s.db.shardIndex(key)
		}
		refs[i] = getManyProbeRef{key: key, idx: i, shard: shard}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].shard != refs[j].shard {
			return refs[i].shard < refs[j].shard
		}
		return bytes.Compare(refs[i].key, refs[j].key) < 0
	})

	unique := make([]getManyProbeRef, 0, len(refs))
	groupStarts := make([]int, 0, len(refs))
	for i, ref := range refs {
		if len(unique) == 0 || ref.shard != unique[len(unique)-1].shard || !bytes.Equal(ref.key, unique[len(unique)-1].key) {
			unique = append(unique, ref)
			groupStarts = append(groupStarts, i)
		}
	}
	if len(s.rootPointShards) > 0 {
		s.db.noteRootDomainSnapshotHasManyNative(len(keys), len(unique))
	}

	results := make([]rootDomainProbeResult, len(unique))
	if len(s.rootPointShards) > 0 {
		start := 0
		for start < len(unique) {
			end := start + 1
			shard := unique[start].shard
			for end < len(unique) && unique[end].shard == shard {
				end++
			}
			if shard >= 0 && shard < len(s.rootPointShards) {
				if err := s.rootPointShards[shard].getManySortedRefs(unique[start:end], results[start:end]); err != nil {
					return nil, err
				}
			}
			start = end
		}
	}

	backendIdx := make([]int, 0, len(unique))
	for i, res := range results {
		groupEnd := len(refs)
		if i+1 < len(groupStarts) {
			groupEnd = groupStarts[i+1]
		}
		switch {
		case !res.found:
			backendIdx = append(backendIdx, i)
		case res.flags&node.FlagTombstone == 0:
			for _, ref := range refs[groupStarts[i]:groupEnd] {
				out[ref.idx] = true
			}
		}
	}
	if len(s.rootPointShards) == 0 {
		s.db.noteRootDomainSnapshotHasManyBackendFallback(len(unique))
	} else {
		s.db.noteRootDomainSnapshotHasManyBackendFallback(len(backendIdx))
	}
	for _, uniqueIdx := range backendIdx {
		ok, err := s.backend.Has(unique[uniqueIdx].key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		groupEnd := len(refs)
		if uniqueIdx+1 < len(groupStarts) {
			groupEnd = groupStarts[uniqueIdx+1]
		}
		for _, ref := range refs[groupStarts[uniqueIdx]:groupEnd] {
			out[ref.idx] = true
		}
	}
	return out, nil
}

type prefixProbeRef struct {
	prefix []byte
	idx    int
}

func (s *Snapshot) HasPrefixes(prefixes [][]byte) ([]bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	out := make([]bool, len(prefixes))
	if len(prefixes) == 0 {
		return out, nil
	}
	if s == nil || s.backend == nil {
		return nil, backenddb.ErrClosed
	}
	if memtableViewHasRangeSpans(s.view) {
		for i, prefix := range prefixes {
			it, err := s.Iterator(prefix, rangeSpanPrefixEnd(prefix))
			if err != nil {
				return nil, err
			}
			out[i] = it.Valid()
			if err := it.Error(); err != nil {
				_ = it.Close()
				return nil, err
			}
			if err := it.Close(); err != nil {
				return nil, err
			}
		}
		return out, nil
	}

	refs := make([]prefixProbeRef, len(prefixes))
	for i, prefix := range prefixes {
		refs[i] = prefixProbeRef{prefix: prefix, idx: i}
	}
	sort.Slice(refs, func(i, j int) bool {
		return bytes.Compare(refs[i].prefix, refs[j].prefix) < 0
	})

	unique := make([][]byte, 0, len(refs))
	groupStarts := make([]int, 0, len(refs))
	for i, ref := range refs {
		if len(unique) == 0 || !bytes.Equal(ref.prefix, unique[len(unique)-1]) {
			unique = append(unique, ref.prefix)
			groupStarts = append(groupStarts, i)
		}
	}
	s.db.noteRootDomainSnapshotHasPrefixesNative(len(prefixes), len(unique))

	probe := make([]bool, len(unique))
	if err := rootDomainIteratorSnapshotFromCachedSnapshot(s).hasPrefixesSorted(unique, probe); err != nil {
		return nil, err
	}
	backendIdx := make([]int, 0, len(unique))
	backendPrefixes := make([][]byte, 0, len(unique))
	for i, ok := range probe {
		groupEnd := len(refs)
		if i+1 < len(groupStarts) {
			groupEnd = groupStarts[i+1]
		}
		if !ok {
			backendIdx = append(backendIdx, i)
			backendPrefixes = append(backendPrefixes, unique[i])
			continue
		}
		for _, ref := range refs[groupStarts[i]:groupEnd] {
			out[ref.idx] = true
		}
	}
	s.db.noteRootDomainSnapshotHasPrefixesFallback(len(backendPrefixes))
	if len(backendPrefixes) == 0 {
		return out, nil
	}
	backendOut, err := s.backend.HasPrefixes(backendPrefixes)
	if err != nil {
		return nil, err
	}
	if len(backendOut) != len(backendPrefixes) {
		return nil, fmt.Errorf("cachingdb: backend HasPrefixes returned %d values for %d prefixes", len(backendOut), len(backendPrefixes))
	}
	for i, ok := range backendOut {
		if !ok {
			continue
		}
		uniqueIdx := backendIdx[i]
		groupEnd := len(refs)
		if uniqueIdx+1 < len(groupStarts) {
			groupEnd = groupStarts[uniqueIdx+1]
		}
		for _, ref := range refs[groupStarts[uniqueIdx]:groupEnd] {
			out[ref.idx] = true
		}
	}
	return out, nil
}

func snapshotRawKVLeafEntry(key []byte, val []byte, ptr page.ValuePtr, flags byte) node.LeafEntry {
	return snapshotRawKVLeafEntryWithRevision(key, val, ptr, flags, page.LegacyEntryRevision)
}

func snapshotRawKVLeafEntryWithRevision(key []byte, val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) node.LeafEntry {
	if flags&(node.FlagPointer|node.FlagTombstone) == 0 {
		val = normalizeRawKVValue(val)
	}
	return node.LeafEntry{
		Key:      cloneRawKVPointKey(key),
		Value:    val,
		ValuePtr: ptr,
		Flags:    flags,
		Revision: revision,
	}
}

func (s *Snapshot) GetEntry(key []byte) (node.LeafEntry, error) {
	if err := s.beginRead(); err != nil {
		return node.LeafEntry{}, err
	}
	defer s.endRead()
	key = normalizeRawKVPointKey(key)
	val, ptr, flags, revision, found := s.lookupQueueEntryWithRevision(key)
	if found {
		return snapshotRawKVLeafEntryWithRevision(key, val, ptr, flags, revision), nil
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
	if err := s.beginRead(); err != nil {
		return node.LeafEntry{}, err
	}
	defer s.endRead()
	key = normalizeRawKVPointKey(key)
	val, ptr, flags, revision, found := s.lookupQueueEntryWithRevision(key)
	if found {
		return snapshotRawKVLeafEntryWithRevision(key, val, ptr, flags, revision), nil
	}

	if s == nil || s.backend == nil || s.db == nil {
		return node.LeafEntry{}, tree.ErrKeyNotFound
	}
	if err := s.db.flushValueLogForBackendRead(); err != nil {
		return node.LeafEntry{}, err
	}
	return s.backend.GetEntryExact(key)
}
