package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
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

type rootDomainEntrySource uint8

const (
	rootDomainEntrySourceNone rootDomainEntrySource = iota
	rootDomainEntrySourceCached
	rootDomainEntrySourcePublished
)

func (db *DB) ensureRootDomainStatesLocked() {
	if db == nil {
		return
	}
	if len(db.rootPointStates) != len(db.mutableShards) {
		db.rootPointStates = make([]rootDomainState, len(db.mutableShards))
		db.resyncRootDomainQueuedRunsLocked()
		return
	}
	for i := range db.mutableShards {
		db.rootPointStates[i].mutable = db.mutableShards[i].mem
	}
}

func (db *DB) resetRootDomainStatesLocked() {
	if db == nil {
		return
	}
	db.rootPointStates = make([]rootDomainState, len(db.mutableShards))
	for i := range db.mutableShards {
		db.rootPointStates[i].mutable = db.mutableShards[i].mem
	}
	db.rootIteratorState = rootDomainState{}
}

func (db *DB) resyncRootDomainQueuedRunsLocked() {
	if db == nil {
		return
	}
	if len(db.rootPointStates) != len(db.mutableShards) {
		db.rootPointStates = make([]rootDomainState, len(db.mutableShards))
	}
	for i := range db.mutableShards {
		db.rootPointStates[i].mutable = db.mutableShards[i].mem
		if cap(db.rootPointStates[i].immutables) >= len(db.queue) {
			db.rootPointStates[i].immutables = db.rootPointStates[i].immutables[:0]
		} else {
			db.rootPointStates[i].immutables = make([]memtable.Table, 0, len(db.queue))
		}
	}
	if cap(db.rootIteratorState.immutables) >= len(db.queue) {
		db.rootIteratorState.immutables = db.rootIteratorState.immutables[:0]
	} else {
		db.rootIteratorState.immutables = make([]memtable.Table, 0, len(db.queue))
	}
	db.rootIteratorState.mutable = nil
	if len(db.queue) == 0 {
		return
	}
	if len(db.queueShardIDs) != len(db.queue) {
		for _, mt := range db.queue {
			if mt == nil {
				continue
			}
			for i := range db.rootPointStates {
				db.rootPointStates[i].immutables = append(db.rootPointStates[i].immutables, mt)
			}
			db.rootIteratorState.immutables = append(db.rootIteratorState.immutables, mt)
		}
		return
	}
	for idx, mt := range db.queue {
		if mt == nil {
			continue
		}
		db.rootIteratorState.immutables = append(db.rootIteratorState.immutables, mt)
		shardIdx := int(db.queueShardIDs[idx])
		if shardIdx < 0 || shardIdx >= len(db.rootPointStates) {
			continue
		}
		db.rootPointStates[shardIdx].immutables = append(db.rootPointStates[shardIdx].immutables, mt)
	}
}

func (db *DB) publishRootDomainSnapshotsLocked(view *memtableView) {
	if db == nil || view == nil {
		return
	}
	db.ensureRootDomainStatesLocked()
	if len(db.rootPointStates) == 0 {
		view.rootPointShards = nil
		view.rootSnapshotShards = nil
		view.rootIterator = rootDomainSnapshot{}
		view.rootIteratorRanges = nil
		return
	}
	points := make([]rootDomainSnapshot, len(db.rootPointStates))
	snapshots := make([]rootDomainSnapshot, len(db.rootPointStates))
	for i := range db.rootPointStates {
		points[i] = db.rootPointStates[i].snapshot()
		snapshots[i] = points[i]
		snapshots[i].mutable = nil
	}
	view.rootPointShards = points
	view.rootSnapshotShards = snapshots
	view.rootIterator = db.rootIteratorState.snapshot()
	view.rootIterator.mutable = nil
	view.rootIteratorRanges = rootDomainQueueRangesForTables(view.queue, view.queueRanges)
}

func (db *DB) promoteRootDomainMutableLocked(shardIdx int, sealed, next memtable.Table) {
	if db == nil {
		return
	}
	db.ensureRootDomainStatesLocked()
	if shardIdx < 0 || shardIdx >= len(db.rootPointStates) {
		return
	}
	if sealed != nil {
		db.rootPointStates[shardIdx].immutables = append(db.rootPointStates[shardIdx].immutables, sealed)
		db.rootIteratorState.immutables = append(db.rootIteratorState.immutables, sealed)
	}
	db.rootPointStates[shardIdx].mutable = next
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
		if len(view.queueShardIDs) > idx && int(view.queueShardIDs[idx]) != shardIdx {
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
		return rootDomainSnapshot{}, false
	}
	shardIdx := 0
	if shardCount > 1 && db != nil {
		shardIdx = db.shardIndex(key)
	}
	if shardIdx >= 0 && shardIdx < len(view.rootPointShards) {
		return view.rootPointShards[shardIdx], true
	}
	return rootDomainSnapshot{}, false
}

func liveIteratorRootDomainSnapshot(view *memtableView) (rootDomainSnapshot, bool) {
	if view == nil {
		return rootDomainSnapshot{}, false
	}
	if rootDomainSnapshotHasInMemoryState(view.rootIterator) {
		return view.rootIterator, true
	}
	return rootDomainSnapshot{}, false
}

func rootDomainQueueRangesForTables(queue []memtable.Table, ranges []keyRange) []keyRange {
	if len(queue) == 0 || len(ranges) == 0 {
		return nil
	}
	out := make([]keyRange, 0, len(queue))
	for idx, mt := range queue {
		if mt == nil {
			continue
		}
		if idx >= len(ranges) {
			return nil
		}
		out = append(out, ranges[idx])
	}
	return out
}

// rawPointRootDomainEntry is the low-level stale-window fallback for point
// reads when no published memtable view is available. It intentionally resolves
// newest-wins precedence under db.mu without constructing a synthetic snapshot,
// keeping the fallback zero-allocation on queued-shard hits.
func (db *DB) rawPointRootDomainEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
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

func (db *DB) rawIteratorRootDomainSnapshot() (rootDomainSnapshot, []keyRange) {
	if db == nil {
		return rootDomainSnapshot{}, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	snap := rootDomainSnapshot{}
	if len(db.queue) > 0 {
		snap.immutables = append([]memtable.Table(nil), db.queue...)
	}
	var ranges []keyRange
	if len(db.queueRanges) > 0 {
		ranges = append([]keyRange(nil), db.queueRanges...)
	}
	return snap, ranges
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
	}
	if s.rootPublished != nil {
		snap.published = s.rootPublished
		snap.publishedRootID = s.rootPublishedRootID
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
	val, ptr, flags, found, _ = s.getEntryWithSource(key)
	return val, ptr, flags, found
}

func (s rootDomainSnapshot) getEntryWithSource(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	if s.mutable != nil {
		if val, ptr, flags, found = s.mutable.GetEntry(key); found {
			return val, ptr, flags, true, rootDomainEntrySourceCached
		}
	}
	for idx := len(s.immutables) - 1; idx >= 0; idx-- {
		if val, ptr, flags, found = s.immutables[idx].GetEntry(key); found {
			return val, ptr, flags, true, rootDomainEntrySourceCached
		}
	}
	if s.published != nil {
		val, ptr, flags, found = s.published.GetEntry(key)
		if found {
			return val, ptr, flags, true, rootDomainEntrySourcePublished
		}
	}
	return nil, page.ValuePtr{}, 0, false, rootDomainEntrySourceNone
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
