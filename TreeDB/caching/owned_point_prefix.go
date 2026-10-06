package caching

import (
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
)

// tryFlushOwnedPointPrefix claims a finite, already queued point prefix. All
// lane drain locks precede db.mu and the backend build group: a shared checkpoint
// drain or assist cannot mutate this prefix while the private group is open.
// TryLock avoids waiting for another drain while holding a partial claim. A busy
// lane or unsupported route falls back to the existing coordinator.
func (db *DB) tryFlushOwnedPointPrefix(syncFlush, reqSync bool, frontier *checkpointFrontier, sharedBackground bool) (attempted, progress bool) {
	if db.cow != nil || len(db.flushLaneMu) < 2 || len(db.flushLaneMu) != len(db.lanes) || db.flushSpanRunTargetPlanning {
		return false, false
	}
	mode := db.flushCollectionMode(flushCollectionBackground)
	if mode != flushCollectionBackground && mode != flushCollectionCheckpoint {
		return false, false
	}
	if _, ok := db.backend.(backendRootPublicationBuildGrouper); !ok {
		return false, false
	}
	claimed := 0
	for i := range db.flushLaneMu {
		if !db.flushLaneMu[i].TryLock() {
			for j := claimed - 1; j >= 0; j-- {
				db.flushLaneMu[j].Unlock()
			}
			return false, false
		}
		claimed++
	}
	defer func() {
		for i := claimed - 1; i >= 0; i-- {
			db.flushLaneMu[i].Unlock()
		}
	}()
	db.mu.Lock()
	mode = db.flushCollectionMode(flushCollectionBackground)
	if mode != flushCollectionBackground && mode != flushCollectionCheckpoint {
		db.mu.Unlock()
		return false, false
	}
	if db.shouldPreemptBackgroundFlushForCheckpoint(reqSync) {
		db.mu.Unlock()
		db.observeCheckpointBackgroundFlushPreempted()
		return true, false
	}
	if frontier == nil && mode == flushCollectionCheckpoint {
		if active, ok := db.activeCheckpointFrontier(); ok {
			frontier = &active
			sharedBackground = true
		}
	}
	if len(db.queue) == 0 {
		db.mu.Unlock()
		return false, false
	}
	// Select using the original head lane, then conservatively retain the existing
	// base collector ceiling across lanes. No future unit can join after this cut.
	headLane := 0
	if len(db.queueLaneIDs) > 0 {
		headLane = int(db.queueLaneIDs[0])
	}
	maxUnits, targetBytes, maxOps := db.selectFlushUnitBudgetLocked(headLane, mode)
	baseUnits, baseBytes := db.baseFlushUnitBudget()
	if maxUnits > baseUnits {
		maxUnits = baseUnits
	}
	if baseBytes > 0 && (targetBytes <= 0 || targetBytes > baseBytes) {
		targetBytes = baseBytes
	}
	if sharedBackground && frontier != nil {
		// The old shared pass could claim at most one unit per active lane. Preserve
		// that aggregate bound and still return after this single cooperative pass.
		active := 0
		seen := make([]bool, len(db.lanes))
		for _, id := range db.queueIDs {
			if unit, ok := frontier.ids[id]; ok && int(unit.laneID) < len(seen) && !seen[unit.laneID] {
				seen[unit.laneID] = true
				active++
			}
		}
		if maxUnits > active {
			maxUnits = active
		}
	}
	if maxUnits < 2 {
		db.mu.Unlock()
		return false, false
	}
	planStart := time.Now()
	units, ids, totalBytes, totalLen := db.collectFlushUnitsWithOpsLocked(-1, maxUnits, targetBytes, maxOps)
	if frontier != nil {
		units, ids, totalBytes, totalLen = filterFlushUnitsToCheckpointFrontier(units, ids, *frontier)
	}
	eligible := len(units) > 1 && totalLen > 0 && db.canStreamCanonicalPointUnitsFromStableIterators(units, flushUnitSpanCount(units))
	differentLane := false
	for _, unit := range units {
		if unit.id == 0 || unit.laneID < 0 || unit.laneID >= len(db.lanes) {
			eligible = false
		}
		if unit.laneID != headLane {
			differentLane = true
		}
	}
	if !eligible || !differentLane {
		db.mu.Unlock()
		return false, false
	}
	if mode == flushCollectionCheckpoint {
		db.observeCheckpointFlushBacklogCoalescingDrain(units, totalBytes, totalLen)
	}
	planningDur := time.Since(planStart)
	db.mu.Unlock()
	// Check the actual batch bridge before opening a group or mutating storage.
	probe := db.newBackendBatchWithSize(0)
	if probe == nil {
		return false, false
	}
	_, supported := probe.(backendBatchRootPublicationBuildGroupSetter)
	closeErr := probe.Close()
	if closeErr != nil {
		db.reportError(closeErr)
		return true, false
	}
	if !supported {
		return false, false
	}
	db.beginFlushCoordinatorWork(totalBytes)
	defer func() { db.finishFlushCoordinatorWork(totalBytes, progress) }()
	db.observeFlushApplyPlan(len(units), totalLen, totalBytes, planningDur)
	db.observeFlushSpanRunSource(len(units), totalLen, 0, 0)
	progress = db.flushCanonicalPointUnitsStableIteratorStreamed(syncFlush, -1, nil, units, ids, totalBytes, totalLen, 0, mode)
	if progress && mode == flushCollectionCheckpoint && frontier != nil {
		db.observeCheckpointSharedDrainWork(sharedBackground, len(units), totalLen, totalBytes)
	}
	return true, progress
}

// The ordinary single-lane stream keeps its existing allocation path. A claimed
// mixed stream scatters deferred pointers back into canonical key order after
// writing each entry through its original source lane. There is no key routing
// or destination-lane authority substitution.
func (db *DB) materializeCanonicalOpsForSourceLane(ops []batch.Entry, sources []int, syncFlush bool, lane int) (bool, error) {
	if len(sources) == 0 {
		return db.materializeCanonicalOpsDeferredValueLogPointers(ops, syncFlush, lane)
	}
	if !db.deferredValueLogEnabled() {
		return false, nil
	}
	if len(sources) != len(ops) {
		return false, fmt.Errorf("cachingdb: missing canonical source lanes")
	}
	selected := getEntrySlice(len(ops))
	defer func() { putEntrySlice(selected) }()
	indexes := make([]int, 0, len(ops))
	for i, source := range sources {
		if source == lane {
			selected = append(selected, ops[i])
			indexes = append(indexes, i)
		}
	}
	wrote, err := db.materializeCanonicalOpsDeferredValueLogPointers(selected, syncFlush, lane)
	if err != nil {
		return false, err
	}
	for i, index := range indexes {
		ops[index] = selected[i]
	}
	return wrote, nil
}
