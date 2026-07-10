package caching

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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

type rootDomainRevisionLookup interface {
	GetEntryWithRevision(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool)
}

type rootDomainIteratorFactory interface {
	Iterator(start, end []byte) (iterator.UnsafeIterator, error)
}

type rootDomainReverseIteratorFactory interface {
	ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error)
}

type rootDomainUnsafeIteratorFactory interface {
	NewIterator(start, end []byte) iterator.UnsafeIterator
}

type rootDomainUnsafeReverseIteratorFactory interface {
	NewReverseIterator(start, end []byte) iterator.UnsafeIterator
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

type publishedRootRef struct {
	lookup rootDomainLookup
	rootID uint64
}

type publishedRootSet struct {
	generation  uint64
	pointShards []publishedRootRef
	system      publishedRootRef
	iterator    publishedRootRef
}

type rootPublishGroup struct {
	generation       uint64
	systemRootPageID uint64
	pointShards      []rootDomainSnapshot
	system           rootDomainSnapshot
	iterator         rootDomainSnapshot
	published        *publishedRootSet
}

type rootDomainPublishTelemetry struct {
	installs              atomic.Uint64
	clears                atomic.Uint64
	staleRejects          atomic.Uint64
	backendFallbacks      atomic.Uint64
	publishFailures       atomic.Uint64
	retrySuccesses        atomic.Uint64
	nativeSystemPublishes atomic.Uint64
	batchReplayFallbacks  atomic.Uint64
}

type rootDomainPublishStats struct {
	installs              uint64
	clears                uint64
	staleRejects          uint64
	backendFallbacks      uint64
	publishFailures       uint64
	retrySuccesses        uint64
	nativeSystemPublishes uint64
	batchReplayFallbacks  uint64
}

func rootDomainLookupEntryWithRevision(src rootDomainLookup, key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool) {
	if src == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	if revisions, ok := src.(rootDomainRevisionLookup); ok {
		return revisions.GetEntryWithRevision(key)
	}
	val, ptr, flags, found = src.GetEntry(key)
	return val, ptr, flags, page.LegacyEntryRevision, found
}

func memtableEntryWithRevision(mt memtable.Table, key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool) {
	if mt == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	if revisions, ok := mt.(memtable.RevisionTable); ok {
		return revisions.GetEntryWithRevision(key)
	}
	val, ptr, flags, found = mt.GetEntry(key)
	return val, ptr, flags, page.LegacyEntryRevision, found
}

var (
	errStalePublishedRootGeneration = errors.New("caching: stale published root generation")
	errInvalidPublishedRootSet      = errors.New("caching: invalid published root set")
)

type rootDomainProbeResult struct {
	val   []byte
	ptr   page.ValuePtr
	flags byte
	found bool
}

type backendStateProvider interface {
	State() *backenddb.DBState
}

type backendSystemRootPublisher interface {
	PublishSystemRootIterator(iter iterator.UnsafeIterator) (uint64, error)
}

type backendOrderedRootPublisher interface {
	PublishOrderedRootIterator(baseRoot uint64, iter iterator.UnsafeIterator) (uint64, error)
}

type backendGroupedOrderedRootPublisher interface {
	PublishOrderedRootGroup(systemIter iterator.UnsafeIterator, ordered []backenddb.OrderedRootPublishInput) (uint64, []uint64, error)
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
	db.rootSystemState = rootDomainState{}
	db.rootIteratorState = rootDomainState{}
	db.rootPublishedSet = nil
	db.dirtyRootPublishGroupID = 0
	db.dirtyRootPublishGroupPending = false
	db.rootPublishRetryPending = false
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
		view.publishedRoots = nil
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
	view.rootSystem = db.rootSystemState.snapshot()
	view.rootSystem.mutable = nil
	view.rootIterator = db.rootIteratorState.snapshot()
	view.rootIterator.mutable = nil
	view.rootIteratorRanges = rootDomainQueueRangesForTables(view.queue, view.queueRanges)
	if db.rootPublishedSet != nil {
		view.publishedRoots = clonePublishedRootSet(db.rootPublishedSet)
	} else {
		view.publishedRoots = publishedRootSetFromMemtableView(view)
	}
}

func clonePublishedRootSet(set *publishedRootSet) *publishedRootSet {
	if set == nil {
		return nil
	}
	cloned := &publishedRootSet{
		generation: set.generation,
		system:     set.system,
		iterator:   set.iterator,
	}
	if len(set.pointShards) > 0 {
		cloned.pointShards = append(make([]publishedRootRef, 0, len(set.pointShards)), set.pointShards...)
	}
	return cloned
}

func (db *DB) rootDomainPublishStatsSnapshot() rootDomainPublishStats {
	if db == nil {
		return rootDomainPublishStats{}
	}
	return rootDomainPublishStats{
		installs:              db.rootPublishStats.installs.Load(),
		clears:                db.rootPublishStats.clears.Load(),
		staleRejects:          db.rootPublishStats.staleRejects.Load(),
		backendFallbacks:      db.rootPublishStats.backendFallbacks.Load(),
		publishFailures:       db.rootPublishStats.publishFailures.Load(),
		retrySuccesses:        db.rootPublishStats.retrySuccesses.Load(),
		nativeSystemPublishes: db.rootPublishStats.nativeSystemPublishes.Load(),
		batchReplayFallbacks:  db.rootPublishStats.batchReplayFallbacks.Load(),
	}
}

func (db *DB) hasDirtyRootPublishGroupLocked() bool {
	return db != nil && db.dirtyRootPublishGroupPending && db.rootPublishedSet != nil
}

func (db *DB) markDirtyRootPublishGroupLocked(set *publishedRootSet) {
	if db == nil {
		return
	}
	if set == nil {
		db.dirtyRootPublishGroupID = 0
		db.dirtyRootPublishGroupPending = false
		return
	}
	db.dirtyRootPublishGroupID = set.generation
	db.dirtyRootPublishGroupPending = true
}

func (db *DB) clearDirtyRootPublishGroupLocked() {
	if db == nil {
		return
	}
	db.dirtyRootPublishGroupID = 0
	db.dirtyRootPublishGroupPending = false
}

func (db *DB) buildRootPublishGroupLocked(set *publishedRootSet) *rootPublishGroup {
	if db == nil {
		return nil
	}
	db.ensureRootDomainStatesLocked()
	group := &rootPublishGroup{
		published: clonePublishedRootSet(set),
	}
	if set != nil {
		group.generation = set.generation
	}
	if stateDB, ok := db.backend.(backendStateTokenReader); ok && stateDB != nil {
		if state, available := stateDB.StateToken(); available {
			group.systemRootPageID = state.SystemRootPageID
		}
	} else if stateDB, ok := db.backend.(backendStateProvider); ok && stateDB != nil {
		if state := stateDB.State(); state != nil {
			group.systemRootPageID = state.SystemRootPageID
		}
	}
	if len(db.rootPointStates) > 0 {
		group.pointShards = make([]rootDomainSnapshot, len(db.rootPointStates))
		for i := range db.rootPointStates {
			snap := db.rootPointStates[i].snapshot()
			snap.mutable = nil
			group.pointShards[i] = snap
		}
	}
	group.system = db.rootSystemState.snapshot()
	group.system.mutable = nil
	group.iterator = db.rootIteratorState.snapshot()
	group.iterator.mutable = nil
	return group
}

func (db *DB) validatePublishedRootSetLocked(set *publishedRootSet) error {
	if db == nil {
		return nil
	}
	db.ensureRootDomainStatesLocked()
	if set != nil && db.rootPublishedSet != nil &&
		set.generation != 0 && db.rootPublishedSet.generation != 0 &&
		set.generation < db.rootPublishedSet.generation {
		db.rootPublishStats.staleRejects.Add(1)
		return errStalePublishedRootGeneration
	}
	if set != nil {
		switch len(set.pointShards) {
		case 0, 1, len(db.rootPointStates):
		default:
			return errInvalidPublishedRootSet
		}
	}
	return nil
}

func (db *DB) applyPublishedRootSetLocked(cloned *publishedRootSet) {
	if db == nil {
		return
	}
	db.ensureRootDomainStatesLocked()
	for i := range db.rootPointStates {
		db.rootPointStates[i].published = nil
		db.rootPointStates[i].publishedRootID = 0
	}
	db.rootSystemState.published = nil
	db.rootSystemState.publishedRootID = 0
	db.rootIteratorState.published = nil
	db.rootIteratorState.publishedRootID = 0
	db.rootPublishedSet = cloned

	if cloned != nil {
		switch len(cloned.pointShards) {
		case 0:
		case 1:
			ref := cloned.pointShards[0]
			for i := range db.rootPointStates {
				db.rootPointStates[i].published = ref.lookup
				db.rootPointStates[i].publishedRootID = ref.rootID
			}
		default:
			limit := len(cloned.pointShards)
			if limit > len(db.rootPointStates) {
				limit = len(db.rootPointStates)
			}
			for i := 0; i < limit; i++ {
				ref := cloned.pointShards[i]
				db.rootPointStates[i].published = ref.lookup
				db.rootPointStates[i].publishedRootID = ref.rootID
			}
		}
		db.rootSystemState.published = cloned.system.lookup
		db.rootSystemState.publishedRootID = cloned.system.rootID
		db.rootIteratorState.published = cloned.iterator.lookup
		db.rootIteratorState.publishedRootID = cloned.iterator.rootID
		if want := cloned.generation; want != 0 {
			for {
				cur := db.rootDomainVersion.Load()
				if cur >= want {
					break
				}
				if db.rootDomainVersion.CompareAndSwap(cur, want-1) {
					break
				}
			}
		}
		db.rootPublishStats.installs.Add(1)
	} else {
		db.rootPublishStats.clears.Add(1)
	}

	db.publishMemtablesLocked()
}

func (db *DB) publishInstalledRootSetLocked(set *publishedRootSet) error {
	if db == nil {
		return nil
	}
	// Keep the locked-call contract for tests/internal callers, but run hooks and
	// backend publication through the out-of-lock path used by production flushes.
	db.mu.Unlock()
	err := db.publishInstalledRootSet(set)
	db.mu.Lock()
	return err
}

func (db *DB) publishInstalledRootSet(set *publishedRootSet) error {
	if db == nil {
		return nil
	}

	db.mu.Lock()
	cloned := clonePublishedRootSet(set)
	if err := db.validatePublishedRootSetLocked(cloned); err != nil {
		db.mu.Unlock()
		return err
	}
	group := db.buildRootPublishGroupLocked(cloned)
	hook := db.rootPublishHook
	systemPublisher, hasSystemPublisher := db.backend.(backendSystemRootPublisher)
	db.mu.Unlock()

	if hook != nil {
		if err := hook(group); err != nil {
			db.mu.Lock()
			db.rootPublishStats.publishFailures.Add(1)
			db.rootPublishRetryPending = true
			db.mu.Unlock()
			return err
		}
	}
	if publisher, ok := db.backend.(backendGroupedOrderedRootPublisher); ok && rootDomainSnapshotNeedsPublish(group.system) && rootPublishGroupNeedsNonSystemPublish(group) {
		if err := publishGroupedMixedRootsUnlocked(publisher, group, cloned); err != nil {
			db.mu.Lock()
			db.rootPublishStats.publishFailures.Add(1)
			db.rootPublishRetryPending = true
			db.mu.Unlock()
			return err
		}
		db.mu.Lock()
		db.rootPublishStats.nativeSystemPublishes.Add(1)
		db.mu.Unlock()
	} else if hasSystemPublisher && rootDomainSnapshotNeedsPublish(group.system) {
		iter, err := group.system.publishIterator(nil, nil)
		if err != nil {
			db.mu.Lock()
			db.rootPublishStats.publishFailures.Add(1)
			db.rootPublishRetryPending = true
			db.mu.Unlock()
			return err
		}
		newSystemRootID, err := systemPublisher.PublishSystemRootIterator(iter)
		if err != nil {
			db.mu.Lock()
			db.rootPublishStats.publishFailures.Add(1)
			db.rootPublishRetryPending = true
			db.mu.Unlock()
			return err
		}
		db.mu.Lock()
		db.rootPublishStats.nativeSystemPublishes.Add(1)
		db.mu.Unlock()
		group.systemRootPageID = newSystemRootID
		cloned.system.rootID = newSystemRootID
		cloned.system.lookup = nil
		group.system = rootDomainSnapshot{
			publishedRootID: newSystemRootID,
		}
	}
	if publisher, ok := db.backend.(backendOrderedRootPublisher); ok && !rootDomainSnapshotNeedsPublish(group.system) {
		if err := publishGroupedNonSystemRootsUnlocked(publisher, group, cloned); err != nil {
			db.mu.Lock()
			db.rootPublishStats.publishFailures.Add(1)
			db.rootPublishRetryPending = true
			db.mu.Unlock()
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if cloned != nil && db.rootPublishedSet != nil &&
		cloned.generation != 0 && db.rootPublishedSet.generation != 0 &&
		db.rootPublishedSet.generation > cloned.generation {
		db.rootPublishRetryPending = false
		return nil
	}
	if err := db.validatePublishedRootSetLocked(cloned); err != nil {
		return err
	}
	db.applyPublishedRootSetLocked(cloned)
	db.clearDirtyRootPublishGroupLocked()
	if db.rootPublishRetryPending {
		db.rootPublishStats.retrySuccesses.Add(1)
		db.rootPublishRetryPending = false
	}
	return nil
}

func rootPublishGroupNeedsNonSystemPublish(group *rootPublishGroup) bool {
	if group == nil {
		return false
	}
	for i := range group.pointShards {
		if rootDomainSnapshotNeedsPublish(group.pointShards[i]) {
			return true
		}
	}
	return rootDomainSnapshotNeedsPublish(group.iterator)
}

func publishGroupedMixedRootsLocked(publisher backendGroupedOrderedRootPublisher, group *rootPublishGroup, cloned *publishedRootSet) error {
	return publishGroupedMixedRoots(publisher, group, cloned)
}

func publishGroupedMixedRootsUnlocked(publisher backendGroupedOrderedRootPublisher, group *rootPublishGroup, cloned *publishedRootSet) error {
	return publishGroupedMixedRoots(publisher, group, cloned)
}

func publishGroupedMixedRoots(publisher backendGroupedOrderedRootPublisher, group *rootPublishGroup, cloned *publishedRootSet) error {
	if publisher == nil || group == nil || cloned == nil {
		return nil
	}
	systemIter, err := group.system.publishIterator(nil, nil)
	if err != nil {
		return err
	}
	ordered := make([]backenddb.OrderedRootPublishInput, 0, len(group.pointShards)+1)
	backendOwnsIterators := false
	defer func() {
		if backendOwnsIterators {
			return
		}
		_ = systemIter.Close()
		for idx := range ordered {
			if ordered[idx].Iter != nil {
				_ = ordered[idx].Iter.Close()
			}
		}
	}()
	pointIdxs := make([]int, 0, len(group.pointShards))
	includeIterator := false
	for i := range group.pointShards {
		if !rootDomainSnapshotNeedsPublish(group.pointShards[i]) {
			continue
		}
		iter, err := group.pointShards[i].publishIterator(nil, nil)
		if err != nil {
			return err
		}
		ordered = append(ordered, backenddb.OrderedRootPublishInput{
			BaseRoot: group.pointShards[i].publishedRootID,
			Iter:     iter,
		})
		pointIdxs = append(pointIdxs, i)
	}
	if rootDomainSnapshotNeedsPublish(group.iterator) {
		iter, err := group.iterator.publishIterator(nil, nil)
		if err != nil {
			return err
		}
		ordered = append(ordered, backenddb.OrderedRootPublishInput{
			BaseRoot: group.iterator.publishedRootID,
			Iter:     iter,
		})
		includeIterator = true
	}
	backendOwnsIterators = true
	newSystemRootID, rootIDs, err := publisher.PublishOrderedRootGroup(systemIter, ordered)
	if err != nil {
		return err
	}
	if len(rootIDs) != len(ordered) {
		return fmt.Errorf("grouped ordered root publish returned %d root IDs for %d ordered roots", len(rootIDs), len(ordered))
	}
	cloned.system.rootID = newSystemRootID
	cursor := 0
	for _, pointIdx := range pointIdxs {
		if pointIdx < len(cloned.pointShards) {
			cloned.pointShards[pointIdx].rootID = rootIDs[cursor]
		}
		cursor++
	}
	if includeIterator && cursor < len(rootIDs) {
		cloned.iterator.rootID = rootIDs[cursor]
	}
	return nil
}

func publishGroupedNonSystemRootsLocked(publisher backendOrderedRootPublisher, group *rootPublishGroup, cloned *publishedRootSet) error {
	return publishGroupedNonSystemRoots(publisher, group, cloned)
}

func publishGroupedNonSystemRootsUnlocked(publisher backendOrderedRootPublisher, group *rootPublishGroup, cloned *publishedRootSet) error {
	return publishGroupedNonSystemRoots(publisher, group, cloned)
}

func publishGroupedNonSystemRoots(publisher backendOrderedRootPublisher, group *rootPublishGroup, cloned *publishedRootSet) error {
	if publisher == nil || group == nil || cloned == nil {
		return nil
	}
	for i := range group.pointShards {
		if !rootDomainSnapshotNeedsPublish(group.pointShards[i]) {
			continue
		}
		iter, err := group.pointShards[i].publishIterator(nil, nil)
		if err != nil {
			return err
		}
		newRootID, err := publisher.PublishOrderedRootIterator(group.pointShards[i].publishedRootID, iter)
		if err != nil {
			return err
		}
		if i < len(cloned.pointShards) {
			cloned.pointShards[i].rootID = newRootID
		}
	}
	if rootDomainSnapshotNeedsPublish(group.iterator) {
		iter, err := group.iterator.publishIterator(nil, nil)
		if err != nil {
			return err
		}
		newIteratorRootID, err := publisher.PublishOrderedRootIterator(group.iterator.publishedRootID, iter)
		if err != nil {
			return err
		}
		cloned.iterator.rootID = newIteratorRootID
	}
	return nil
}

func (db *DB) installPublishedRootSetLocked(set *publishedRootSet) bool {
	if db == nil {
		return false
	}
	cloned := clonePublishedRootSet(set)
	if err := db.validatePublishedRootSetLocked(cloned); err != nil {
		return false
	}
	db.applyPublishedRootSetLocked(cloned)
	db.markDirtyRootPublishGroupLocked(cloned)
	return true
}

func (db *DB) attemptDirtyRootPublish() (attempted, ok bool) {
	if db == nil {
		return false, false
	}
	db.mu.Lock()
	if !db.hasDirtyRootPublishGroupLocked() {
		db.mu.Unlock()
		return false, false
	}
	set := clonePublishedRootSet(db.rootPublishedSet)
	db.mu.Unlock()
	err := db.publishInstalledRootSet(set)
	if err != nil {
		return true, false
	}
	return true, true
}

func (db *DB) promoteRootDomainMutableLocked(shardIdx int, sealed, next memtable.Table) {
	if db == nil {
		return
	}
	db.ensureRootDomainStatesLocked()
	if shardIdx < 0 || shardIdx >= len(db.rootPointStates) {
		return
	}
	if sealed != nil && sealed.Len() != 0 {
		db.rootPointStates[shardIdx].immutables = append(db.rootPointStates[shardIdx].immutables, sealed)
		db.rootIteratorState.immutables = append(db.rootIteratorState.immutables, sealed)
	}
	db.rootPointStates[shardIdx].mutable = next
}

type backendSnapshotLookup struct {
	db       *DB
	snapshot *backenddb.Snapshot
	rootID   uint64
}

func (backendSnapshotLookup) rootDomainPublishedBackendLookupMarker() {}

func (l backendSnapshotLookup) GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	val, ptr, flags, _, found = l.GetEntryWithRevision(key)
	return val, ptr, flags, found
}

func (l backendSnapshotLookup) GetEntryWithRevision(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool) {
	if l.snapshot == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	if l.db != nil {
		if err := l.db.flushValueLogForBackendRead(); err != nil {
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
		}
	}
	var (
		entry node.LeafEntry
		err   error
	)
	if l.rootID != 0 {
		entry, err = l.snapshot.GetEntryAtRoot(l.rootID, key)
	} else {
		entry, err = l.snapshot.GetEntry(key)
	}
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
		}
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	return entry.Value, entry.ValuePtr, entry.Flags, entry.Revision, true
}

func (l backendSnapshotLookup) GetValueAppend(key, dst []byte) ([]byte, error) {
	if l.snapshot == nil {
		return dst, backenddb.ErrClosed
	}
	if l.db != nil {
		if err := l.db.flushValueLogForBackendRead(); err != nil {
			return dst, err
		}
	}
	if l.rootID != 0 {
		return l.snapshot.GetAppendAtRoot(l.rootID, key, dst)
	}
	return l.snapshot.GetAppend(key, dst)
}

func (l backendSnapshotLookup) GetValueUnsafe(key []byte) ([]byte, error) {
	if l.snapshot == nil {
		return nil, backenddb.ErrClosed
	}
	if l.db != nil {
		if err := l.db.flushValueLogForBackendRead(); err != nil {
			return nil, err
		}
	}
	if l.rootID != 0 {
		return l.snapshot.GetUnsafeAtRoot(l.rootID, key)
	}
	return l.snapshot.GetUnsafe(key)
}

func (l backendSnapshotLookup) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	if l.snapshot == nil {
		return nil, backenddb.ErrClosed
	}
	if l.db != nil {
		if err := l.db.flushValueLogForBackendRead(); err != nil {
			return nil, err
		}
	}
	if l.rootID != 0 {
		return l.snapshot.IteratorAtRoot(l.rootID, start, end)
	}
	return l.snapshot.IteratorWithOptions(start, end, backenddb.IteratorOptions{})
}

func (l backendSnapshotLookup) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	if l.snapshot == nil {
		return nil, backenddb.ErrClosed
	}
	if l.db != nil {
		if err := l.db.flushValueLogForBackendRead(); err != nil {
			return nil, err
		}
	}
	if l.rootID != 0 {
		return l.snapshot.ReverseIteratorAtRoot(l.rootID, start, end)
	}
	return l.snapshot.ReverseIteratorWithOptions(start, end, backenddb.IteratorOptions{})
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

func rootDomainSnapshotHasPublishedState(snap rootDomainSnapshot) bool {
	return snap.published != nil || snap.publishedRootID != 0
}

func publishedRootSetFromMemtableView(view *memtableView) *publishedRootSet {
	if view == nil {
		return nil
	}
	if view.publishedRoots != nil {
		return view.publishedRoots
	}
	pointSource := view.rootSnapshotShards
	if len(pointSource) == 0 {
		pointSource = view.rootPointShards
	}
	pointShards := make([]publishedRootRef, len(pointSource))
	hasPublished := false
	for i := range pointSource {
		if !rootDomainSnapshotHasPublishedState(pointSource[i]) {
			continue
		}
		hasPublished = true
		pointShards[i] = publishedRootRef{
			lookup: pointSource[i].published,
			rootID: pointSource[i].publishedRootID,
		}
	}
	iterRef := publishedRootRef{}
	systemRef := publishedRootRef{}
	if rootDomainSnapshotHasPublishedState(view.rootIterator) {
		hasPublished = true
		iterRef = publishedRootRef{
			lookup: view.rootIterator.published,
			rootID: view.rootIterator.publishedRootID,
		}
	}
	if rootDomainSnapshotHasPublishedState(view.rootSystem) {
		hasPublished = true
		systemRef = publishedRootRef{
			lookup: view.rootSystem.published,
			rootID: view.rootSystem.publishedRootID,
		}
	}
	if !hasPublished {
		return nil
	}
	return &publishedRootSet{
		generation:  view.rootVersion,
		pointShards: pointShards,
		system:      systemRef,
		iterator:    iterRef,
	}
}

func rootDomainSystemSnapshotFromCachedSnapshot(s *Snapshot) rootDomainSnapshot {
	if s == nil {
		return rootDomainSnapshot{}
	}
	snap := s.rootSystem
	if s.publishedRoots != nil {
		ref := s.publishedRoots.system
		if ref.rootID != 0 {
			snap.publishedRootID = ref.rootID
		}
		if ref.lookup != nil {
			snap.published = ref.lookup
			return snap
		}
	}
	if s.backend != nil {
		rootID := snap.publishedRootID
		if state, ok := s.backend.StateToken(); ok && rootID == 0 {
			rootID = state.SystemRootPageID
			snap.publishedRootID = rootID
		}
		if rootID != 0 {
			snap.published = backendSnapshotLookup{db: s.db, snapshot: s.backend, rootID: rootID}
		}
	}
	return snap
}

func rootDomainSnapshotBackendRootID(s *Snapshot, fallback uint64) uint64 {
	if fallback != 0 {
		return fallback
	}
	if s == nil || s.backend == nil {
		return 0
	}
	if state, ok := s.backend.StateToken(); ok {
		return state.RootPageID
	}
	return 0
}

func rootDomainPointPublishedRef(set *publishedRootSet, shardIdx int) publishedRootRef {
	if set == nil {
		return publishedRootRef{}
	}
	switch {
	case shardIdx >= 0 && shardIdx < len(set.pointShards):
		return set.pointShards[shardIdx]
	case len(set.pointShards) == 1:
		return set.pointShards[0]
	default:
		return publishedRootRef{}
	}
}

func rootDomainApplyPublishedRef(s *Snapshot, snap *rootDomainSnapshot, ref publishedRootRef) bool {
	if snap == nil {
		return false
	}
	if ref.rootID != 0 {
		snap.publishedRootID = ref.rootID
	}
	if ref.lookup != nil {
		snap.published = ref.lookup
		return true
	}
	if s != nil && s.backend != nil && snap.publishedRootID != 0 {
		snap.published = backendSnapshotLookup{db: s.db, snapshot: s.backend, rootID: snap.publishedRootID}
		return true
	}
	return false
}

func rootDomainApplyBackendFallback(s *Snapshot, snap *rootDomainSnapshot) {
	if snap == nil || s == nil || s.backend == nil {
		return
	}
	rootID := rootDomainSnapshotBackendRootID(s, snap.publishedRootID)
	if rootID != 0 {
		snap.publishedRootID = rootID
	}
	if rootID == s.backendRootID {
		snap.published = &s.backendFallback
		return
	}
	snap.published = backendSnapshotLookup{db: s.db, snapshot: s.backend, rootID: rootID}
}

func rootDomainIteratorPublishedRef(set *publishedRootSet) publishedRootRef {
	if set == nil {
		return publishedRootRef{}
	}
	return set.iterator
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
	if rootDomainApplyPublishedRef(s, &snap, rootDomainPointPublishedRef(s.publishedRoots, shardIdx)) {
		return snap
	}
	rootDomainApplyBackendFallback(s, &snap)
	return snap
}

func rootDomainIteratorSnapshotFromCachedSnapshot(s *Snapshot) rootDomainSnapshot {
	if s == nil {
		return rootDomainSnapshot{}
	}
	snap := s.rootIterator
	if rootDomainApplyPublishedRef(s, &snap, rootDomainIteratorPublishedRef(s.publishedRoots)) {
		return snap
	}
	rootDomainApplyBackendFallback(s, &snap)
	return snap
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
	if rootDomainSnapshotHasInMemoryState(view.rootIterator) || rootDomainSnapshotHasPublishedState(view.rootIterator) {
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
	val, ptr, flags, _, found = db.rawPointRootDomainEntryWithRevision(key)
	return val, ptr, flags, found
}

func (db *DB) rawPointRootDomainEntryWithRevision(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool) {
	if db == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	shardIdx := 0
	if len(db.mutableShards) > 1 {
		shardIdx = db.shardIndex(key)
	}
	if shardIdx >= 0 && shardIdx < len(db.mutableShards) {
		if mt := db.mutableShards[shardIdx].mem; mt != nil {
			if val, ptr, flags, revision, found = memtableEntryWithRevision(mt, key); found {
				return val, ptr, flags, revision, true
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
		if val, ptr, flags, revision, found = memtableEntryWithRevision(mt, key); found {
			return val, ptr, flags, revision, true
		}
	}
	return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
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
	val, ptr, flags, _, found, _ = s.getEntryWithRevisionSource(key)
	return val, ptr, flags, found
}

func (s rootDomainSnapshot) getEntryWithSource(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	val, ptr, flags, _, found, source = s.getEntryWithRevisionSource(key)
	return val, ptr, flags, found, source
}

func (s rootDomainSnapshot) getEntryWithRevision(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool) {
	val, ptr, flags, revision, found, _ = s.getEntryWithRevisionSource(key)
	return val, ptr, flags, revision, found
}

func (s rootDomainSnapshot) getEntryWithRevisionSource(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool, source rootDomainEntrySource) {
	if s.mutable != nil {
		if val, ptr, flags, revision, found = memtableEntryWithRevision(s.mutable, key); found {
			return val, ptr, flags, revision, true, rootDomainEntrySourceCached
		}
	}
	for idx := len(s.immutables) - 1; idx >= 0; idx-- {
		if val, ptr, flags, revision, found = memtableEntryWithRevision(s.immutables[idx], key); found {
			return val, ptr, flags, revision, true, rootDomainEntrySourceCached
		}
	}
	if s.published != nil {
		val, ptr, flags, revision, found = rootDomainLookupEntryWithRevision(s.published, key)
		if found {
			return val, ptr, flags, revision, true, rootDomainEntrySourcePublished
		}
	}
	return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, rootDomainEntrySourceNone
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

type sortedPointProbePreference interface {
	PreferSortedPointProbes(first, last []byte, refCount int) bool
}

func probeRootDomainTablePointRefs(mt memtable.Table, refs []getManyProbeRef, out []rootDomainProbeResult) {
	for refIdx := 0; refIdx < len(refs); refIdx++ {
		if out[refIdx].found {
			continue
		}
		val, ptr, flags, found := mt.GetEntry(refs[refIdx].key)
		if !found {
			continue
		}
		out[refIdx] = rootDomainProbeResult{
			val:   val,
			ptr:   ptr,
			flags: flags,
			found: true,
		}
	}
}

func probeRootDomainTableSortedRefs(mt memtable.Table, refs []getManyProbeRef, out []rootDomainProbeResult) error {
	if mt == nil || len(refs) == 0 {
		return nil
	}
	if pref, ok := mt.(sortedPointProbePreference); ok && pref.PreferSortedPointProbes(refs[0].key, refs[len(refs)-1].key, len(refs)) {
		probeRootDomainTablePointRefs(mt, refs, out)
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

func rootDomainSnapshotNeedsPublish(s rootDomainSnapshot) bool {
	return s.mutable != nil || len(s.immutables) > 0
}

func rootDomainPublishedIterator(s rootDomainSnapshot, start, end []byte) (iterator.UnsafeIterator, bool, error) {
	if s.published == nil {
		return nil, s.publishedRootID != 0, nil
	}
	switch published := s.published.(type) {
	case rootDomainUnsafeIteratorFactory:
		return published.NewIterator(start, end), true, nil
	case rootDomainIteratorFactory:
		iter, err := published.Iterator(start, end)
		return iter, true, err
	default:
		return nil, true, nil
	}
}

func rootDomainPublishedReverseIterator(s rootDomainSnapshot, start, end []byte) (iterator.UnsafeIterator, bool, error) {
	if s.published == nil {
		return nil, s.publishedRootID != 0, nil
	}
	switch published := s.published.(type) {
	case rootDomainUnsafeReverseIteratorFactory:
		return published.NewReverseIterator(start, end), true, nil
	case rootDomainReverseIteratorFactory:
		iter, err := published.ReverseIterator(start, end)
		return iter, true, err
	default:
		return nil, true, nil
	}
}

type leasedUnsafeIterator struct {
	iterator.UnsafeIterator
	closeOnce sync.Once
	closeErr  error
	release   func()
}

func (it *leasedUnsafeIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if it == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	return iterator.UnsafeEntryWithRevision(it.UnsafeIterator)
}

func (it *leasedUnsafeIterator) Close() error {
	if it == nil {
		return nil
	}
	it.closeOnce.Do(func() {
		if it.UnsafeIterator != nil {
			it.closeErr = it.UnsafeIterator.Close()
		}
		if it.release != nil {
			it.release()
		}
	})
	return it.closeErr
}

func (db *DB) resolveLivePublishedRootSnapshot(s rootDomainSnapshot) (rootDomainSnapshot, func()) {
	if db == nil || s.published != nil || s.publishedRootID == 0 {
		return s, nil
	}
	provider, ok := db.backend.(backendSnapshotProvider)
	if !ok {
		return s, nil
	}
	snap := provider.AcquireSnapshot()
	if snap == nil {
		return s, nil
	}
	s.published = backendSnapshotLookup{db: db, snapshot: snap, rootID: s.publishedRootID}
	return s, func() { _ = snap.Close() }
}

func (db *DB) livePublishedRootIterator(s rootDomainSnapshot, start, end []byte, reverse bool) (iterator.UnsafeIterator, bool, error) {
	s, release := db.resolveLivePublishedRootSnapshot(s)
	var (
		it  iterator.UnsafeIterator
		ok  bool
		err error
	)
	if reverse {
		it, ok, err = rootDomainPublishedReverseIterator(s, start, end)
	} else {
		it, ok, err = rootDomainPublishedIterator(s, start, end)
	}
	if err != nil || !ok || it == nil {
		if release != nil {
			release()
		}
		return it, ok, err
	}
	if release != nil {
		it = &leasedUnsafeIterator{UnsafeIterator: it, release: release}
	}
	return it, ok, nil
}

func (s rootDomainSnapshot) iteratorSources(start, end []byte) ([]merging.IteratorSource, error) {
	sources := make([]merging.IteratorSource, 0, 1+len(s.immutables)+1)
	prio := 0
	if s.mutable != nil {
		sources = append(sources, merging.IteratorSource{
			Iter:     s.mutable.NewIterator(start, end),
			Priority: prio,
		})
		prio++
	}
	for idx := len(s.immutables) - 1; idx >= 0; idx-- {
		if s.immutables[idx] == nil {
			continue
		}
		sources = append(sources, merging.IteratorSource{
			Iter:     s.immutables[idx].NewIterator(start, end),
			Priority: prio,
		})
		prio++
	}
	switch published := s.published.(type) {
	case rootDomainUnsafeIteratorFactory:
		sources = append(sources, merging.IteratorSource{
			Iter:     published.NewIterator(start, end),
			Priority: prio,
		})
	case rootDomainIteratorFactory:
		iter, err := published.Iterator(start, end)
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

type rootDomainUnsafeMergeItem struct {
	iter     iterator.UnsafeIterator
	priority int
	key      []byte
}

type rootDomainUnsafeMergeHeap []rootDomainUnsafeMergeItem

func (h rootDomainUnsafeMergeHeap) Len() int { return len(h) }
func (h rootDomainUnsafeMergeHeap) Less(i, j int) bool {
	if cmp := bytes.Compare(h[i].key, h[j].key); cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}
func (h rootDomainUnsafeMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *rootDomainUnsafeMergeHeap) push(x rootDomainUnsafeMergeItem) {
	*h = append(*h, x)
	for j := len(*h) - 1; j > 0; {
		i := (j - 1) / 2
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *rootDomainUnsafeMergeHeap) pop() rootDomainUnsafeMergeItem {
	old := *h
	n := len(old)
	if n == 0 {
		return rootDomainUnsafeMergeItem{}
	}
	old.Swap(0, n-1)
	*h = old[:n-1]
	for i := 0; ; {
		j1 := 2*i + 1
		if j1 >= len(*h) {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < len(*h) && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return old[n-1]
}

func (h rootDomainUnsafeMergeHeap) peek() *rootDomainUnsafeMergeItem {
	if len(h) == 0 {
		return nil
	}
	return &h[0]
}

type rootDomainUnsafeIterator struct {
	all        []iterator.UnsafeIterator
	priorities []int
	h          rootDomainUnsafeMergeHeap
	cur        rootDomainUnsafeMergeItem
	hasCur     bool
	valid      bool
	err        error
	start      []byte
	end        []byte
}

func newRootDomainUnsafeIterator(sources []merging.IteratorSource, start, end []byte) iterator.UnsafeIterator {
	if len(sources) == 0 {
		return &rootDomainEmptyUnsafeIterator{start: start, end: end}
	}
	if len(sources) == 1 {
		return sources[0].Iter
	}
	it := &rootDomainUnsafeIterator{
		all:        make([]iterator.UnsafeIterator, 0, len(sources)),
		priorities: make([]int, 0, len(sources)),
		h:          make(rootDomainUnsafeMergeHeap, 0, len(sources)),
		start:      start,
		end:        end,
	}
	for _, src := range sources {
		it.all = append(it.all, src.Iter)
		it.priorities = append(it.priorities, src.Priority)
		if src.Iter != nil && src.Iter.Valid() {
			it.h = append(it.h, rootDomainUnsafeMergeItem{
				iter:     src.Iter,
				priority: src.Priority,
				key:      src.Iter.UnsafeKey(),
			})
		}
	}
	for i := len(it.h)/2 - 1; i >= 0; i-- {
		for cur := i; ; {
			j1 := 2*cur + 1
			if j1 >= len(it.h) {
				break
			}
			j := j1
			if j2 := j1 + 1; j2 < len(it.h) && it.h.Less(j2, j1) {
				j = j2
			}
			if !it.h.Less(j, cur) {
				break
			}
			it.h.Swap(cur, j)
			cur = j
		}
	}
	it.advance()
	return it
}

func (it *rootDomainUnsafeIterator) advance() {
	it.valid = false
	it.hasCur = false
	for len(it.h) > 0 {
		top := it.h.pop()
		currentKey := top.key
		if it.end != nil && bytes.Compare(currentKey, it.end) >= 0 {
			it.h.push(top)
			return
		}
		for len(it.h) > 0 {
			next := it.h.peek()
			if next == nil || !bytes.Equal(next.key, currentKey) {
				break
			}
			shadowed := it.h.pop()
			shadowed.iter.Next()
			if shadowed.iter.Valid() {
				shadowed.key = shadowed.iter.UnsafeKey()
				it.h.push(shadowed)
			}
		}
		if top.iter.IsDeleted() {
			top.iter.Next()
			if top.iter.Valid() {
				top.key = top.iter.UnsafeKey()
				it.h.push(top)
			}
			continue
		}
		it.cur = top
		it.hasCur = true
		it.valid = true
		return
	}
}

func (it *rootDomainUnsafeIterator) Valid() bool { return it.valid }

func (it *rootDomainUnsafeIterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
	if it.hasCur {
		it.cur.iter.Next()
		if it.cur.iter.Valid() {
			it.cur.key = it.cur.iter.UnsafeKey()
			it.h.push(it.cur)
		}
		it.hasCur = false
	}
	it.advance()
}

func (it *rootDomainUnsafeIterator) Seek(key []byte) {
	it.h = it.h[:0]
	it.cur = rootDomainUnsafeMergeItem{}
	it.hasCur = false
	it.valid = false
	for idx, src := range it.all {
		if src == nil {
			continue
		}
		src.Seek(key)
		if src.Valid() {
			priority := idx
			if idx < len(it.priorities) {
				priority = it.priorities[idx]
			}
			it.h.push(rootDomainUnsafeMergeItem{
				iter:     src,
				priority: priority,
				key:      src.UnsafeKey(),
			})
		}
	}
	it.advance()
}

func (it *rootDomainUnsafeIterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	return it.cur.iter.UnsafeKey()
}

func (it *rootDomainUnsafeIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	return it.cur.iter.UnsafeValue()
}

func (it *rootDomainUnsafeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.valid {
		return nil, page.ValuePtr{}, 0
	}
	return it.cur.iter.UnsafeEntry()
}

func (it *rootDomainUnsafeIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.valid {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	return iterator.UnsafeEntryWithRevision(it.cur.iter)
}

func (it *rootDomainUnsafeIterator) Key() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.iter.Key()
}

func (it *rootDomainUnsafeIterator) Value() []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.iter.Value()
}

func (it *rootDomainUnsafeIterator) KeyCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.iter.KeyCopy(dst)
}

func (it *rootDomainUnsafeIterator) ValueCopy(dst []byte) []byte {
	if !it.valid {
		panic("iterator invalid")
	}
	return it.cur.iter.ValueCopy(dst)
}

func (it *rootDomainUnsafeIterator) IsDeleted() bool { return false }

func (it *rootDomainUnsafeIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	for _, src := range it.all {
		if src != nil && src.Error() != nil {
			return src.Error()
		}
	}
	return nil
}

func (it *rootDomainUnsafeIterator) Close() error {
	var firstErr error
	for _, src := range it.all {
		if src == nil {
			continue
		}
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (it *rootDomainUnsafeIterator) Domain() (start, end []byte) { return it.start, it.end }

type rootDomainEmptyUnsafeIterator struct {
	start []byte
	end   []byte
}

func (it *rootDomainEmptyUnsafeIterator) Next()               { panic("iterator invalid") }
func (it *rootDomainEmptyUnsafeIterator) Seek([]byte)         {}
func (it *rootDomainEmptyUnsafeIterator) Valid() bool         { return false }
func (it *rootDomainEmptyUnsafeIterator) UnsafeKey() []byte   { return nil }
func (it *rootDomainEmptyUnsafeIterator) UnsafeValue() []byte { return nil }
func (it *rootDomainEmptyUnsafeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return nil, page.ValuePtr{}, 0
}

func (it *rootDomainEmptyUnsafeIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
}
func (it *rootDomainEmptyUnsafeIterator) Key() []byte                 { panic("iterator invalid") }
func (it *rootDomainEmptyUnsafeIterator) Value() []byte               { panic("iterator invalid") }
func (it *rootDomainEmptyUnsafeIterator) KeyCopy([]byte) []byte       { panic("iterator invalid") }
func (it *rootDomainEmptyUnsafeIterator) ValueCopy([]byte) []byte     { panic("iterator invalid") }
func (it *rootDomainEmptyUnsafeIterator) IsDeleted() bool             { return false }
func (it *rootDomainEmptyUnsafeIterator) Error() error                { return nil }
func (it *rootDomainEmptyUnsafeIterator) Close() error                { return nil }
func (it *rootDomainEmptyUnsafeIterator) Domain() (start, end []byte) { return it.start, it.end }

func (s rootDomainSnapshot) publishIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	sources, err := s.iteratorSources(start, end)
	if err != nil {
		return nil, err
	}
	return newRootDomainUnsafeIterator(sources, start, end), nil
}

func (s rootDomainSnapshot) hasPrefixesSorted(prefixes [][]byte, out []bool) error {
	if len(prefixes) == 0 {
		return nil
	}
	if len(prefixes) != len(out) {
		return errors.New("cachingdb: root-domain sorted prefix probe input/output length mismatch")
	}
	sources, err := s.iteratorSources(prefixes[0], nil)
	if err != nil {
		return err
	}
	if len(sources) == 0 {
		return nil
	}

	var it merging.Iterator
	if len(sources) == 1 {
		it = newSingleSourceIterator(sources[0].Iter, prefixes[0], nil, false)
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
