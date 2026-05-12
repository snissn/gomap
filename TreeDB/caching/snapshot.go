package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
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
	db              *DB
	view            *memtableView
	backend         *backenddb.Snapshot
	rootVersion     uint64
	rootPointShards []rootDomainSnapshot // snapshot point roots; mutable runs are intentionally excluded
	rootSystem      rootDomainSnapshot
	rootIterator    rootDomainSnapshot
	publishedRoots  *publishedRootSet

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
			} else {
				viewRootPointShards = append([]rootDomainSnapshot(nil), view.rootSnapshotShards...)
				viewPublishedRoots = clonePublishedRootSet(view.publishedRoots)
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

	snap := &Snapshot{
		db:      db,
		view:    view,
		backend: backendSnap,
	}
	snap.rootVersion = viewRootVersion
	snap.rootPointShards = viewRootPointShards
	snap.rootSystem = viewRootSystem
	snap.rootIterator = viewRootIterator
	snap.publishedRoots = viewPublishedRoots
	if snap.publishedRoots == nil {
		db.rootPublishStats.backendFallbacks.Add(1)
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
	s.rootSystem = rootDomainSnapshot{}
	s.rootIterator = rootDomainSnapshot{}
	s.publishedRoots = nil
	s.rootVersion = 0
	s.db = nil
	return err
}

func (s *Snapshot) lookupQueueEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	val, ptr, flags, found, _ = s.lookupRootDomainEntry(key)
	return val, ptr, flags, found
}

func (s *Snapshot) lookupRootDomainSnapshotEntry(key []byte) (snap rootDomainSnapshot, val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	if s == nil {
		return rootDomainSnapshot{}, nil, page.ValuePtr{}, 0, false, rootDomainEntrySourceNone
	}
	snap = rootDomainSnapshotFromCachedSnapshot(s, key)
	val, ptr, flags, found, source = snap.getEntryWithSource(key)
	return snap, val, ptr, flags, found, source
}

func (s *Snapshot) lookupRootDomainEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	_, val, ptr, flags, found, source = s.lookupRootDomainSnapshotEntry(key)
	return val, ptr, flags, found, source
}

func (s *Snapshot) lookupCachedRootDomainEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	if s == nil || len(s.rootPointShards) == 0 {
		return nil, page.ValuePtr{}, 0, false
	}
	shardIdx := 0
	if s.db != nil {
		shardIdx = s.db.shardIndex(key)
	}
	if shardIdx < 0 || shardIdx >= len(s.rootPointShards) {
		return nil, page.ValuePtr{}, 0, false
	}
	snap := s.rootPointShards[shardIdx]
	snap.published = nil
	snap.publishedRootID = 0
	return snap.getEntry(key)
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

type rootDomainPublishedValueLookup interface {
	GetValueAppend(key, dst []byte) ([]byte, error)
	GetValueUnsafe(key []byte) ([]byte, error)
}

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

func rootDomainPublishedUsesBackendLookup(published any) bool {
	switch published.(type) {
	case backendSnapshotLookup, *backendSnapshotLookup:
		return true
	default:
		return false
	}
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
		if s.db == nil {
			return dst, true, ErrSnapshotValueLogReaderUnavailable
		}
		out, err := s.db.readValueLogAppend(key, ptr, dst)
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

	snap := rootDomainSnapshotFromCachedSnapshot(s, key)
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
		checkedPublishedEntry = true
		if out, found, err := s.getAppendFromEntryWithSource(snap, key, dst, oldLen); found {
			return out, err
		}
	}
	if !checkedPublishedEntry {
		if out, found, err := s.getAppendFromEntryWithSource(snap, key, dst, oldLen); found {
			return out, err
		}
	}
	if checkedPublishedEntry {
		if rootDomainPublishedUsesBackendLookup(snap.published) {
			if snap.publishedRootID != 0 {
				// The published lookup already queried this specific root via GetAppendAtRoot.
				// Falling back to snapshot default-root GetAppend can cross root domains.
				return dst, tree.ErrKeyNotFound
			}
			if s.publishedRoots == nil {
				// rootDomainSnapshotFromCachedSnapshot() installs backendSnapshotLookup as
				// the published lookup when no published root set is pinned. A not-found
				// from that lookup already queried the backend snapshot, so avoid an extra
				// backend GetAppend miss probe here.
				return dst, tree.ErrKeyNotFound
			}
		}
	}

	if s == nil || s.backend == nil || s.db == nil {
		return dst, tree.ErrKeyNotFound
	}
	if err := s.db.flushValueLogForBackendRead(); err != nil {
		return dst, err
	}
	out, err = s.backend.GetAppend(key, dst)
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
						return nil, nil
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
				return nil, nil
			}
			maybeRecordSnapshotGetCallerSample(len(out))
			return ownedReadResult(out, scratch), nil
		}
		if len(val) == 0 {
			recordSnapshotRootDomainRead(source, false, len(val))
			return nil, nil
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
	snapshotReadBackendHitsTotal.Add(1)
	if len(out) == 0 {
		return nil, nil
	}
	snapshotReadBackendBytesTotal.Add(uint64(len(out)))
	maybeRecordSnapshotGetCallerSample(len(out))
	return out, nil
}

func (s *Snapshot) GetUnsafe(key []byte) ([]byte, error) {
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

func (s *Snapshot) Has(key []byte) (bool, error) {
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
	out := make([]bool, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	if s == nil || s.backend == nil {
		return nil, backenddb.ErrClosed
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
	out := make([]bool, len(prefixes))
	if len(prefixes) == 0 {
		return out, nil
	}
	if s == nil || s.backend == nil {
		return nil, backenddb.ErrClosed
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
