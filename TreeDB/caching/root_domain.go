package caching

import (
	"bytes"
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/merging"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type rootDomainLookup interface {
	GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool)
}

type rootDomainIteratorFactory interface {
	Iterator(start, end []byte) (iterator.UnsafeIterator, error)
}

type rootDomainUnsafeIteratorFactory interface {
	NewIterator(start, end []byte) iterator.UnsafeIterator
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

type rootDomainProbeResult struct {
	val   []byte
	ptr   page.ValuePtr
	flags byte
	found bool
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
		for i := range db.rootPointStates {
			db.rootPointStates[i].immutables = append(db.rootPointStates[i].immutables, db.queue...)
		}
		db.rootIteratorState.immutables = append(db.rootIteratorState.immutables, db.queue...)
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
	view.rootIteratorRanges = view.queueRanges
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

func (l backendSnapshotLookup) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	if l.snapshot == nil {
		return nil, backenddb.ErrClosed
	}
	return l.snapshot.Iterator(start, end)
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

func populateRootDomainSnapshots(view *memtableView) {
	if view == nil {
		return
	}
	if len(view.queue) > 0 {
		view.rootIterator = rootDomainSnapshot{immutables: view.queue}
		view.rootIteratorRanges = view.queueRanges
	} else {
		view.rootIterator = rootDomainSnapshot{}
		view.rootIteratorRanges = nil
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

func rootDomainIteratorSnapshotFromCachedSnapshot(s *Snapshot) rootDomainSnapshot {
	if s == nil {
		return rootDomainSnapshot{}
	}
	snap := s.rootIterator
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

func probeRootDomainTableSorted(mt memtable.Table, keys [][]byte, out []rootDomainProbeResult) error {
	if mt == nil || len(keys) == 0 {
		return nil
	}
	it := mt.NewIterator(keys[0], nil)
	defer func() { _ = it.Close() }()

	keyIdx := 0
	for keyIdx < len(keys) && it.Valid() {
		for keyIdx < len(keys) && out[keyIdx].found {
			keyIdx++
		}
		if keyIdx >= len(keys) {
			break
		}
		cmp := bytes.Compare(it.UnsafeKey(), keys[keyIdx])
		switch {
		case cmp < 0:
			it.Next()
		case cmp > 0:
			keyIdx++
		default:
			val, ptr, flags := it.UnsafeEntry()
			out[keyIdx] = rootDomainProbeResult{
				val:   val,
				ptr:   ptr,
				flags: flags,
				found: true,
			}
			keyIdx++
			it.Next()
		}
	}
	return it.Error()
}

func probeRootDomainTableSortedRefs(mt memtable.Table, refs []getManyProbeRef, out []rootDomainProbeResult) error {
	if mt == nil || len(refs) == 0 {
		return nil
	}
	it := mt.NewIterator(refs[0].key, nil)
	defer func() { _ = it.Close() }()

	refIdx := 0
	for refIdx < len(refs) && it.Valid() {
		for refIdx < len(refs) && out[refIdx].found {
			refIdx++
		}
		if refIdx >= len(refs) {
			break
		}
		cmp := bytes.Compare(it.UnsafeKey(), refs[refIdx].key)
		switch {
		case cmp < 0:
			it.Next()
		case cmp > 0:
			refIdx++
		default:
			val, ptr, flags := it.UnsafeEntry()
			out[refIdx] = rootDomainProbeResult{
				val:   val,
				ptr:   ptr,
				flags: flags,
				found: true,
			}
			refIdx++
			it.Next()
		}
	}
	return it.Error()
}

func (s rootDomainSnapshot) getManySorted(keys [][]byte, out []rootDomainProbeResult) error {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) != len(out) {
		return errors.New("cachingdb: root-domain sorted probe input/output length mismatch")
	}
	if err := probeRootDomainTableSorted(s.mutable, keys, out); err != nil {
		return err
	}
	for idx := len(s.immutables) - 1; idx >= 0; idx-- {
		if err := probeRootDomainTableSorted(s.immutables[idx], keys, out); err != nil {
			return err
		}
	}
	return nil
}

func (s rootDomainSnapshot) getManySortedRefs(refs []getManyProbeRef, out []rootDomainProbeResult) error {
	if len(refs) == 0 {
		return nil
	}
	if len(refs) != len(out) {
		return errors.New("cachingdb: root-domain sorted ref probe input/output length mismatch")
	}
	if err := probeRootDomainTableSortedRefs(s.mutable, refs, out); err != nil {
		return err
	}
	for idx := len(s.immutables) - 1; idx >= 0; idx-- {
		if err := probeRootDomainTableSortedRefs(s.immutables[idx], refs, out); err != nil {
			return err
		}
	}
	return nil
}

func (s rootDomainSnapshot) prefixIteratorSources(start []byte) ([]merging.IteratorSource, error) {
	sources := make([]merging.IteratorSource, 0, 1+len(s.immutables)+1)
	prio := 0
	if s.mutable != nil {
		sources = append(sources, merging.IteratorSource{
			Iter:     s.mutable.NewIterator(start, nil),
			Priority: prio,
		})
		prio++
	}
	for idx := len(s.immutables) - 1; idx >= 0; idx-- {
		if s.immutables[idx] == nil {
			continue
		}
		sources = append(sources, merging.IteratorSource{
			Iter:     s.immutables[idx].NewIterator(start, nil),
			Priority: prio,
		})
		prio++
	}
	switch published := s.published.(type) {
	case rootDomainUnsafeIteratorFactory:
		sources = append(sources, merging.IteratorSource{
			Iter:     published.NewIterator(start, nil),
			Priority: prio,
		})
	case rootDomainIteratorFactory:
		iter, err := published.Iterator(start, nil)
		if err != nil {
			for i := range sources {
				_ = sources[i].Iter.Close()
			}
			return nil, err
		}
		sources = append(sources, merging.IteratorSource{
			Iter:     iter,
			Priority: prio,
		})
	}
	return sources, nil
}

func (s rootDomainSnapshot) hasPrefixesSorted(prefixes [][]byte, out []bool) error {
	if len(prefixes) == 0 {
		return nil
	}
	if len(prefixes) != len(out) {
		return errors.New("cachingdb: root-domain sorted prefix probe input/output length mismatch")
	}
	sources, err := s.prefixIteratorSources(prefixes[0])
	if err != nil {
		return err
	}
	if len(sources) == 0 {
		return nil
	}

	var it merging.Iterator
	if len(sources) == 1 {
		it = newSingleSourceIterator(sources[0].Iter, prefixes[0], nil)
	} else {
		it = merging.NewMergingIterator(sources, prefixes[0], nil)
	}
	defer func() { _ = it.Close() }()

	prefixIdx := 0
	for prefixIdx < len(prefixes) && it.Valid() {
		key := it.Key()
		for prefixIdx < len(prefixes) {
			prefix := prefixes[prefixIdx]
			if bytes.HasPrefix(key, prefix) {
				out[prefixIdx] = true
				prefixIdx++
				continue
			}
			if bytes.Compare(key, prefix) > 0 {
				prefixIdx++
				continue
			}
			break
		}
		if prefixIdx >= len(prefixes) {
			break
		}
		if bytes.Compare(key, prefixes[prefixIdx]) < 0 {
			it.Next()
		}
	}
	return it.Error()
}
