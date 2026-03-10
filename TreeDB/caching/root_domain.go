package caching

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type rootDomainLookup interface {
	GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool)
}

// rootDomainState is the native cached state shape for one logical root-domain.
// It is intentionally small and read-centric in R1: later phases will add
// flush scheduling and grouped publication, but the authoritative state model
// starts here.
type rootDomainState struct {
	publishedRootID uint64
	published       rootDomainLookup
	mutable         memtable.Table
	immutables      []memtable.Table // oldest-to-newest
}

type rootDomainSnapshot struct {
	publishedRootID uint64
	published       rootDomainLookup
	mutable         memtable.Table
	immutables      []memtable.Table // oldest-to-newest
}

type backendSnapshotLookup struct {
	snapshot *backenddb.Snapshot
}

func (l backendSnapshotLookup) GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	if l.snapshot == nil {
		return nil, page.ValuePtr{}, 0, false
	}
	entry, err := l.snapshot.GetEntry(key)
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, page.ValuePtr{}, 0, false
		}
		return nil, page.ValuePtr{}, 0, false
	}
	return entry.Value, entry.ValuePtr, entry.Flags, true
}

func rootDomainSnapshotFromMemtableView(view *memtableView, shardIdx int, includeMutable bool) rootDomainSnapshot {
	if view == nil {
		return rootDomainSnapshot{}
	}
	if shardIdx < 0 {
		snap := view.rootIterator
		if snap.immutables != nil || snap.mutable != nil || snap.published != nil || snap.publishedRootID != 0 {
			return snap
		}
	}
	if shardIdx >= 0 && shardIdx < len(view.rootPointShards) {
		if includeMutable {
			return view.rootPointShards[shardIdx]
		}
		if shardIdx < len(view.rootSnapshotShards) {
			return view.rootSnapshotShards[shardIdx]
		}
		snap := view.rootPointShards[shardIdx]
		snap.mutable = nil
		return snap
	}
	snap := rootDomainSnapshot{}
	if includeMutable && shardIdx >= 0 && shardIdx < len(view.mutables) {
		snap.mutable = view.mutables[shardIdx]
	}
	if len(view.queue) == 0 {
		return snap
	}
	if shardIdx < 0 {
		snap.immutables = view.queue
		return snap
	}
	snap.immutables = make([]memtable.Table, 0, len(view.queue))
	for idx, mt := range view.queue {
		if mt == nil {
			continue
		}
		if shardIdx >= 0 && len(view.queueShardIDs) > idx && int(view.queueShardIDs[idx]) != shardIdx {
			continue
		}
		snap.immutables = append(snap.immutables, mt)
	}
	return snap
}

func rootDomainSnapshotHasInMemoryState(snap rootDomainSnapshot) bool {
	return (snap.mutable != nil && snap.mutable.Len() != 0) || len(snap.immutables) != 0
}

func livePointRootDomainSnapshot(view *memtableView, db *DB, key []byte) (rootDomainSnapshot, bool) {
	if view == nil {
		return rootDomainSnapshot{}, false
	}
	shardCount := len(view.rootPointShards)
	if shardCount == 0 {
		shardCount = len(view.mutables)
	}
	if shardCount == 0 {
		return rootDomainSnapshot{}, false
	}
	shardIdx := 0
	if shardCount > 1 && db != nil {
		shardIdx = db.shardIndex(key)
	}
	if shardIdx >= 0 && shardIdx < len(view.rootPointShards) {
		return view.rootPointShards[shardIdx], true
	}
	return rootDomainSnapshotFromMemtableView(view, shardIdx, true), true
}

func liveIteratorRootDomainSnapshot(view *memtableView) (rootDomainSnapshot, bool) {
	if view == nil {
		return rootDomainSnapshot{}, false
	}
	if rootDomainSnapshotHasInMemoryState(view.rootIterator) {
		return view.rootIterator, true
	}
	if len(view.queue) == 0 {
		return rootDomainSnapshot{}, false
	}
	return rootDomainSnapshotFromMemtableView(view, -1, false), true
}

func (db *DB) rawMemtableEntryFallback(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	if db == nil {
		return nil, page.ValuePtr{}, 0, false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	shardIdx := 0
	if len(db.mutableShards) > 1 {
		shardIdx = db.shardIndex(key)
	}
	if shardIdx >= 0 && shardIdx < len(db.mutableShards) {
		if mt := db.mutableShards[shardIdx].mem; mt != nil {
			if val, ptr, flags, found = mt.GetEntry(key); found {
				return val, ptr, flags, true
			}
		}
	}
	for idx := len(db.queue) - 1; idx >= 0; idx-- {
		mt := db.queue[idx]
		if mt == nil {
			continue
		}
		if len(db.queueShardIDs) > idx && int(db.queueShardIDs[idx]) != shardIdx {
			continue
		}
		if val, ptr, flags, found = mt.GetEntry(key); found {
			return val, ptr, flags, true
		}
	}
	return nil, page.ValuePtr{}, 0, false
}

func populateRootDomainSnapshots(view *memtableView) {
	if view == nil {
		return
	}
	if len(view.queue) > 0 {
		view.rootIterator = rootDomainSnapshot{immutables: view.queue}
	} else {
		view.rootIterator = rootDomainSnapshot{}
	}
	if len(view.mutables) == 0 {
		view.rootPointShards = nil
		view.rootSnapshotShards = nil
		return
	}
	points := make([]rootDomainSnapshot, len(view.mutables))
	snapshots := make([]rootDomainSnapshot, len(view.mutables))
	for idx := range view.mutables {
		points[idx].mutable = view.mutables[idx]
	}
	if len(view.queue) == 0 {
		view.rootPointShards = points
		view.rootSnapshotShards = snapshots
		return
	}
	if len(view.queueShardIDs) != len(view.queue) {
		for idx := range points {
			points[idx].immutables = view.queue
			snapshots[idx].immutables = view.queue
		}
		view.rootPointShards = points
		view.rootSnapshotShards = snapshots
		return
	}
	counts := make([]int, len(points))
	for idx, mt := range view.queue {
		if mt == nil {
			continue
		}
		shardIdx := int(view.queueShardIDs[idx])
		if shardIdx < 0 || shardIdx >= len(counts) {
			continue
		}
		counts[shardIdx]++
	}
	runs := make([][]memtable.Table, len(points))
	for idx, count := range counts {
		if count > 0 {
			runs[idx] = make([]memtable.Table, 0, count)
		}
	}
	for idx, mt := range view.queue {
		if mt == nil {
			continue
		}
		shardIdx := int(view.queueShardIDs[idx])
		if shardIdx < 0 || shardIdx >= len(runs) {
			continue
		}
		runs[shardIdx] = append(runs[shardIdx], mt)
	}
	for idx := range points {
		points[idx].immutables = runs[idx]
		snapshots[idx].immutables = runs[idx]
	}
	view.rootPointShards = points
	view.rootSnapshotShards = snapshots
}

func rootDomainSnapshotFromCachedSnapshot(s *Snapshot, key []byte) rootDomainSnapshot {
	if s == nil {
		return rootDomainSnapshot{}
	}
	shardIdx := 0
	if s.db != nil {
		shardIdx = s.db.shardIndex(key)
	}
	var snap rootDomainSnapshot
	if shardIdx >= 0 && shardIdx < len(s.rootPointShards) {
		snap = s.rootPointShards[shardIdx]
	} else {
		snap = rootDomainSnapshotFromMemtableView(s.view, shardIdx, false)
	}
	if s.rootPublished != nil {
		snap.published = s.rootPublished
		snap.publishedRootID = s.rootPublishedRootID
	} else if s.backend != nil {
		snap.published = backendSnapshotLookup{snapshot: s.backend}
		if state := s.backend.State(); state != nil {
			snap.publishedRootID = state.RootPageID
		}
	}
	return snap
}

func (s *rootDomainState) snapshot() rootDomainSnapshot {
	if s == nil {
		return rootDomainSnapshot{}
	}
	snap := rootDomainSnapshot{
		publishedRootID: s.publishedRootID,
		published:       s.published,
		mutable:         s.mutable,
	}
	if len(s.immutables) > 0 {
		snap.immutables = append(make([]memtable.Table, 0, len(s.immutables)), s.immutables...)
	}
	return snap
}

func (s *rootDomainState) sealMutable(next memtable.Table) {
	if s == nil {
		return
	}
	if s.mutable != nil {
		s.mutable.Freeze()
		s.immutables = append(s.immutables, s.mutable)
	}
	s.mutable = next
}

func (s rootDomainSnapshot) getEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	if s.mutable != nil {
		if val, ptr, flags, found = s.mutable.GetEntry(key); found {
			return val, ptr, flags, true
		}
	}
	for idx := len(s.immutables) - 1; idx >= 0; idx-- {
		if val, ptr, flags, found = s.immutables[idx].GetEntry(key); found {
			return val, ptr, flags, true
		}
	}
	if s.published != nil {
		return s.published.GetEntry(key)
	}
	return nil, page.ValuePtr{}, 0, false
}

func (s rootDomainSnapshot) visibleValue(key []byte) ([]byte, bool) {
	val, _, flags, found := s.getEntry(key)
	if !found || flags&node.FlagTombstone != 0 {
		return nil, false
	}
	return val, true
}

func (s rootDomainSnapshot) hasVisibleKey(key []byte) bool {
	_, ok := s.visibleValue(key)
	return ok
}
