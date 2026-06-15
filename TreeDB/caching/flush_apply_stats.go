package caching

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

func addCacheDurationNs(dst interface{ Add(uint64) uint64 }, d time.Duration) {
	if d <= 0 {
		return
	}
	dst.Add(uint64(d.Nanoseconds()))
}

func addCacheInt64(dst interface{ Add(uint64) uint64 }, v int64) {
	if v <= 0 {
		return
	}
	dst.Add(uint64(v))
}

func addAtomicInt64FloorZero(dst *atomic.Int64, delta int64) {
	if dst == nil || delta == 0 {
		return
	}
	for {
		cur := dst.Load()
		next := cur + delta
		if next < 0 {
			next = 0
		}
		if dst.CompareAndSwap(cur, next) {
			return
		}
	}
}

func saturatingSubUint64(after, before uint64) uint64 {
	if after <= before {
		return 0
	}
	return after - before
}

func parseCacheStatsUint64(stats map[string]string, key string) uint64 {
	if len(stats) == 0 || key == "" {
		return 0
	}
	v, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func (db *DB) backendFlushApplyReducerPublishNs() uint64 {
	if db == nil || db.backend == nil {
		return 0
	}
	stats := db.backend.Stats()
	return parseCacheStatsUint64(stats, "treedb.flush_apply.root_reduce.ns_total") + parseCacheStatsUint64(stats, "treedb.flush_apply.guarded_publish.ns_total")
}

func (db *DB) observeFlushApplyPlan(units, entries int, bytes int64, planning time.Duration) {
	if db == nil || units <= 0 {
		return
	}
	db.flushApplyBatches.Add(1)
	db.flushApplyUnits.Add(uint64(units))
	if entries > 0 {
		db.flushApplyEntries.Add(uint64(entries))
	}
	addCacheInt64(&db.flushApplyBytes, bytes)
	addCacheDurationNs(&db.flushApplyPlanningNs, planning)
}

func (db *DB) observeFlushApplyBuild(build time.Duration) {
	if db == nil {
		return
	}
	addCacheDurationNs(&db.flushApplyBuildNs, build)
}

func (db *DB) observeFlushApplyBackendWrite(write time.Duration) {
	if db == nil {
		return
	}
	addCacheDurationNs(&db.flushApplyBackendWriteNs, write)
}

func (db *DB) observeFlushApplyLeafLogEncodeCompress(d time.Duration) {
	if db == nil {
		return
	}
	addCacheDurationNs(&db.flushApplyLeafLogEncodeCompressNs, d)
}

func (db *DB) observeFlushApplyLeafLogAppend(wait, wall time.Duration, bytes int64, frames, records int) {
	if db == nil {
		return
	}
	addCacheDurationNs(&db.flushApplyLeafLogAppendWaitNs, wait)
	addCacheDurationNs(&db.flushApplyLeafLogAppendNs, wall)
	addCacheInt64(&db.flushApplyLeafLogAppendBytes, bytes)
	if frames > 0 {
		db.flushApplyLeafLogAppendFrames.Add(uint64(frames))
	}
	if records > 0 {
		db.flushApplyLeafLogAppendRecords.Add(uint64(records))
	}
}

func (db *DB) observeForegroundFlushAssist(wait time.Duration, flushed int) {
	if db == nil {
		return
	}
	db.flushApplyForegroundAssistCalls.Add(1)
	addCacheDurationNs(&db.flushApplyForegroundAssistWaitNs, wait)
	if flushed > 0 {
		db.flushApplyForegroundAssistFlushes.Add(uint64(flushed))
	}
}

func (db *DB) observeFlushSpanRunSource(sourceMemtables, sourcePointOps, rangeDeleteOps int, rangeBarriers int) {
	if db == nil || sourceMemtables <= 0 {
		return
	}
	db.flushSpanRunRuns.Add(1)
	db.flushSpanRunSourceMemtables.Add(uint64(sourceMemtables))
	updateAtomicMaxUint64(&db.flushSpanRunSourceMemtablesMax, uint64(sourceMemtables))
	if sourcePointOps > 0 {
		db.flushSpanRunSourcePointOps.Add(uint64(sourcePointOps))
	}
	if rangeDeleteOps > 0 {
		db.flushSpanRunRangeDeleteOps.Add(uint64(rangeDeleteOps))
	}
	if rangeBarriers > 0 {
		db.flushSpanRunRangeBarriers.Add(uint64(rangeBarriers))
	}
}

func (db *DB) observeFlushSpanRunPlannedOps(plannedPointOps, rangeDeleteOps int) {
	if db == nil {
		return
	}
	plannedOps := plannedPointOps + rangeDeleteOps
	if plannedOps > 0 {
		db.flushSpanRunPlannedOps.Add(uint64(plannedOps))
	}
	if plannedPointOps > 0 {
		db.flushSpanRunPlannedPointOps.Add(uint64(plannedPointOps))
	}
}

func (db *DB) observeFlushSpanRunShadowedOps(shadowedOps int) {
	if db == nil || shadowedOps <= 0 {
		return
	}
	db.flushSpanRunShadowedOps.Add(uint64(shadowedOps))
}

func (db *DB) observeFlushSpanRunBackendChunks(chunks int) {
	if db == nil || chunks <= 0 {
		return
	}
	db.flushSpanRunBackendChunks.Add(uint64(chunks))
}

func (db *DB) observeCheckpointActiveBackgroundFlushWait(wait time.Duration) {
	if db == nil || wait <= 0 {
		return
	}
	ns := uint64(wait.Nanoseconds())
	db.checkpointActiveBackgroundFlushWaitNs.Add(ns)
	db.checkpointActiveBackgroundFlushWaitSamples.Add(1)
	updateAtomicMaxUint64(&db.checkpointActiveBackgroundFlushWaitMaxNs, ns)
}

const flushCoordinatorProgressWait = 10 * time.Millisecond

func (db *DB) beginFlushCoordinatorPass() {
	if db == nil {
		return
	}
	db.flushCoordinatorActive.Add(1)
	db.signalFlushCoordinatorWaiters()
}

func (db *DB) endFlushCoordinatorPass() {
	if db == nil {
		return
	}
	addAtomicInt64FloorZero(&db.flushCoordinatorActive, -1)
	db.signalFlushCoordinatorWaiters()
}

func (db *DB) beginFlushCoordinatorWork(bytes int64) {
	if db == nil {
		return
	}
	if bytes > 0 {
		db.flushCoordinatorInFlightBytes.Add(bytes)
	}
	db.signalFlushCoordinatorWaiters()
}

func (db *DB) finishFlushCoordinatorWork(bytes int64, success bool) {
	if db == nil {
		return
	}
	if bytes > 0 {
		addAtomicInt64FloorZero(&db.flushCoordinatorInFlightBytes, -bytes)
	}
	if !success {
		db.flushCoordinatorErrors.Add(1)
	}
	db.signalFlushCoordinatorWaiters()
}

func (db *DB) recordFlushCoordinatorProgress() {
	if db == nil {
		return
	}
	db.flushCoordinatorProgress.Add(1)
	db.signalFlushCoordinatorWaiters()
}

func (db *DB) signalFlushCoordinatorWaiters() {
	if db == nil {
		return
	}
	db.flushCoordinatorWaitMu.Lock()
	if db.flushCoordinatorWaitCh == nil {
		db.flushCoordinatorWaitCh = make(chan struct{})
	}
	close(db.flushCoordinatorWaitCh)
	db.flushCoordinatorWaitCh = make(chan struct{})
	db.flushCoordinatorWaitMu.Unlock()
}

func (db *DB) flushCoordinatorWaitSnapshot() (uint64, <-chan struct{}) {
	if db == nil {
		return 0, nil
	}
	db.flushCoordinatorWaitMu.Lock()
	if db.flushCoordinatorWaitCh == nil {
		db.flushCoordinatorWaitCh = make(chan struct{})
	}
	progress := db.flushCoordinatorProgress.Load()
	ch := db.flushCoordinatorWaitCh
	db.flushCoordinatorWaitMu.Unlock()
	return progress, ch
}

func (db *DB) waitForActiveFlushProgress(beforeBacklog, targetBacklog int64, timeout time.Duration) bool {
	if db == nil || db.flushCoordinatorActive.Load() <= 0 || db.flushCoordinatorInFlightBytes.Load() <= 0 {
		return false
	}
	if timeout <= 0 {
		timeout = flushCoordinatorProgressWait
	}
	progress, ch := db.flushCoordinatorWaitSnapshot()
	if ch == nil {
		return false
	}
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ch:
		db.flushApplyCoordinatorProgressWaits.Add(1)
		addCacheDurationNs(&db.flushApplyCoordinatorProgressWaitNs, time.Since(start))
		nowBacklog := db.queueBacklogBytes.Load()
		return nowBacklog < targetBacklog || nowBacklog < beforeBacklog || db.flushCoordinatorProgress.Load() != progress
	case <-timer.C:
		db.flushApplyCoordinatorProgressWaits.Add(1)
		addCacheDurationNs(&db.flushApplyCoordinatorProgressWaitNs, time.Since(start))
		db.flushApplyCoordinatorStallWaits.Add(1)
		return false
	}
}

func (db *DB) flushCoordinatorForegroundYieldEnabled() bool {
	return db != nil && db.flushApplyConcurrency > 1
}

func (db *DB) activeFlushEffectiveBacklog(backlog int64) (int64, bool) {
	if db == nil || !db.flushCoordinatorForegroundYieldEnabled() || db.flushCoordinatorActive.Load() <= 0 {
		return backlog, false
	}
	inFlight := db.flushCoordinatorInFlightBytes.Load()
	if inFlight <= 0 {
		return backlog, false
	}
	effectiveBacklog := backlog - inFlight
	if effectiveBacklog < 0 {
		effectiveBacklog = 0
	}
	return effectiveBacklog, true
}

func (db *DB) foregroundAssistYieldToActiveFlush(backlog, stopBytes int64) bool {
	if db == nil || stopBytes <= 0 || backlog < stopBytes {
		return false
	}
	effectiveBacklog, ok := db.activeFlushEffectiveBacklog(backlog)
	if !ok {
		return false
	}
	if effectiveBacklog >= stopBytes {
		db.flushApplyCoordinatorHardOverloadFallbacks.Add(1)
		return false
	}
	db.flushApplyCoordinatorActiveAssistSkips.Add(1)
	return true
}

func (db *DB) appendCacheFlushApplyStats(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	stats["treedb.cache.flush_apply.batches_total"] = fmt.Sprintf("%d", db.flushApplyBatches.Load())
	stats["treedb.cache.flush_apply.units_total"] = fmt.Sprintf("%d", db.flushApplyUnits.Load())
	stats["treedb.cache.flush_apply.entries_total"] = fmt.Sprintf("%d", db.flushApplyEntries.Load())
	stats["treedb.cache.flush_apply.bytes_total"] = fmt.Sprintf("%d", db.flushApplyBytes.Load())
	stats["treedb.cache.flush_apply.planning_ns_total"] = fmt.Sprintf("%d", db.flushApplyPlanningNs.Load())
	stats["treedb.cache.flush_apply.build_ns_total"] = fmt.Sprintf("%d", db.flushApplyBuildNs.Load())
	stats["treedb.cache.flush_apply.backend_write_ns_total"] = fmt.Sprintf("%d", db.flushApplyBackendWriteNs.Load())
	stats["treedb.cache.flush_apply.leaf_log_encode_compress_ns_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogEncodeCompressNs.Load())
	stats["treedb.cache.flush_apply.leaf_log_append_wait_ns_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogAppendWaitNs.Load())
	stats["treedb.cache.flush_apply.leaf_log_append_ns_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogAppendNs.Load())
	stats["treedb.cache.flush_apply.leaf_log_append_bytes_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogAppendBytes.Load())
	leafLogAppendFrames := db.flushApplyLeafLogAppendFrames.Load()
	stats["treedb.cache.flush_apply.leaf_log_append_frames_total"] = fmt.Sprintf("%d", leafLogAppendFrames)
	stats["treedb.cache.flush_apply.leaf_log_append_records_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogAppendRecords.Load())
	if entries := db.flushApplyEntries.Load(); entries > 0 {
		stats["treedb.cache.flush_apply.leaf_log_append_frames_per_op"] = fmt.Sprintf("%.6f", float64(leafLogAppendFrames)/float64(entries))
	}
	stats["treedb.cache.flush_apply.foreground_assist_calls_total"] = fmt.Sprintf("%d", db.flushApplyForegroundAssistCalls.Load())
	stats["treedb.cache.flush_apply.foreground_assist_wait_ns_total"] = fmt.Sprintf("%d", db.flushApplyForegroundAssistWaitNs.Load())
	stats["treedb.cache.flush_apply.foreground_assist_flushes_total"] = fmt.Sprintf("%d", db.flushApplyForegroundAssistFlushes.Load())
	stats["treedb.cache.flush_span_run.runs_total"] = fmt.Sprintf("%d", db.flushSpanRunRuns.Load())
	stats["treedb.cache.flush_span_run.source_point_ops_total"] = fmt.Sprintf("%d", db.flushSpanRunSourcePointOps.Load())
	stats["treedb.cache.flush_span_run.planned_ops_total"] = fmt.Sprintf("%d", db.flushSpanRunPlannedOps.Load())
	stats["treedb.cache.flush_span_run.planned_point_ops_total"] = fmt.Sprintf("%d", db.flushSpanRunPlannedPointOps.Load())
	stats["treedb.cache.flush_span_run.source_memtables_total"] = fmt.Sprintf("%d", db.flushSpanRunSourceMemtables.Load())
	stats["treedb.cache.flush_span_run.source_memtables_max"] = fmt.Sprintf("%d", db.flushSpanRunSourceMemtablesMax.Load())
	stats["treedb.cache.flush_span_run.shadowed_ops_total"] = fmt.Sprintf("%d", db.flushSpanRunShadowedOps.Load())
	stats["treedb.cache.flush_span_run.range_barriers_total"] = fmt.Sprintf("%d", db.flushSpanRunRangeBarriers.Load())
	stats["treedb.cache.flush_span_run.range_delete_ops_total"] = fmt.Sprintf("%d", db.flushSpanRunRangeDeleteOps.Load())
	stats["treedb.cache.flush_span_run.backend_chunks_total"] = fmt.Sprintf("%d", db.flushSpanRunBackendChunks.Load())
	if runs := db.flushSpanRunRuns.Load(); runs > 0 {
		stats["treedb.cache.flush_span_run.ops_per_run"] = fmt.Sprintf("%.6f", float64(db.flushSpanRunPlannedOps.Load())/float64(runs))
	}
	stats["treedb.cache.flush_apply.coordinator.active"] = fmt.Sprintf("%d", db.flushCoordinatorActive.Load())
	stats["treedb.cache.flush_apply.coordinator.in_flight_bytes"] = fmt.Sprintf("%d", db.flushCoordinatorInFlightBytes.Load())
	stats["treedb.cache.flush_apply.coordinator.progress_total"] = fmt.Sprintf("%d", db.flushCoordinatorProgress.Load())
	stats["treedb.cache.flush_apply.coordinator.errors_total"] = fmt.Sprintf("%d", db.flushCoordinatorErrors.Load())
	stats["treedb.cache.flush_apply.coordinator.active_assist_skips_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorActiveAssistSkips.Load())
	stats["treedb.cache.flush_apply.coordinator.progress_waits_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorProgressWaits.Load())
	stats["treedb.cache.flush_apply.coordinator.progress_wait_ns_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorProgressWaitNs.Load())
	stats["treedb.cache.flush_apply.coordinator.stall_waits_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorStallWaits.Load())
	stats["treedb.cache.flush_apply.coordinator.blocking_fallbacks_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorBlockingFallbacks.Load())
	stats["treedb.cache.flush_apply.coordinator.hard_overload_fallbacks_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorHardOverloadFallbacks.Load())
}
