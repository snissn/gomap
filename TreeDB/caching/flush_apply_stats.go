package caching

import (
	"fmt"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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

type backendFlushApplyReducerPublishNser interface {
	FlushApplyReducerPublishNs() uint64
}

func (db *DB) backendFlushApplyReducerPublishNs() uint64 {
	if db == nil || db.backend == nil {
		return 0
	}
	if provider, ok := db.backend.(backendFlushApplyReducerPublishNser); ok {
		return provider.FlushApplyReducerPublishNs()
	}
	return 0
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

func (db *DB) observeFlushApplyBackendBatchWrite(write time.Duration) {
	if db == nil {
		return
	}
	addCacheDurationNs(&db.flushApplyBackendBatchWriteNs, write)
}

func (db *DB) observeFlushApplyDeferredVLogPointerMaterialize(d time.Duration) {
	if db == nil {
		return
	}
	addCacheDurationNs(&db.flushApplyDeferredVLogPointerMaterializeNs, d)
}

func (db *DB) observeFlushApplyVLogFlush(d time.Duration) {
	if db == nil {
		return
	}
	addCacheDurationNs(&db.flushApplyVLogFlushNs, d)
}

func (db *DB) observeFlushApplyVLogSync(d time.Duration) {
	if db == nil {
		return
	}
	addCacheDurationNs(&db.flushApplyVLogSyncNs, d)
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

func (db *DB) observeLeafLogLaneAppend(l *lane, wait, hold time.Duration, bytes int64, pages int, err error) {
	if db == nil || l == nil {
		return
	}
	l.leafLogAppendCalls.Add(1)
	if pages > 0 {
		l.leafLogAppendPages.Add(uint64(pages))
	}
	if bytes > 0 {
		l.leafLogAppendBytes.Add(uint64(bytes))
	}
	addCacheDurationNs(&l.leafLogAppendLockWaitNs, wait)
	addCacheDurationNs(&l.leafLogAppendLockHoldNs, hold)
	if err != nil {
		l.leafLogAppendErrors.Add(1)
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

func (db *DB) observeFlushSpanRunTargetLeafSpans(spans []backenddb.FlushSpanRunTargetLeafSpan, splitSummary backenddb.FlushSpanRunChunkSplitSummary) {
	if db == nil {
		return
	}
	targetLeafSpans := len(spans)
	singleOpSpans := 0
	spanOps := 0
	spanBytes := 0
	for i := range spans {
		span := spans[i]
		spanOps += span.OpCount
		spanBytes += span.ByteCount
		if span.OpCount == 1 {
			singleOpSpans++
		}
	}
	db.observeFlushSpanRunTargetLeafSpanSummary(targetLeafSpans, singleOpSpans, spanOps, spanBytes, splitSummary)
}

func (db *DB) observeFlushSpanRunTargetLeafSpanSummary(targetLeafSpans, singleOpSpans, spanOps, spanBytes int, splitSummary backenddb.FlushSpanRunChunkSplitSummary) {
	if db == nil {
		return
	}
	if targetLeafSpans > 0 {
		db.flushSpanRunTargetLeafSpans.Add(uint64(targetLeafSpans))
	}
	if singleOpSpans > 0 {
		db.flushSpanRunSingleOpSpans.Add(uint64(singleOpSpans))
	}
	if spanOps > 0 {
		db.flushSpanRunSpanOps.Add(uint64(spanOps))
	}
	if spanBytes > 0 {
		db.flushSpanRunSpanBytes.Add(uint64(spanBytes))
	}
	if splitSummary.TargetLeavesSplitAcrossChunks > 0 {
		db.flushSpanRunTargetLeavesSplitAcrossChunks.Add(uint64(splitSummary.TargetLeavesSplitAcrossChunks))
	}
	if splitSummary.MaxChunksPerTargetLeaf > 0 {
		updateAtomicMaxUint64(&db.flushSpanRunMaxChunksPerTargetLeaf, uint64(splitSummary.MaxChunksPerTargetLeaf))
	}
}

func (db *DB) observeCheckpointActiveBackgroundFlushWait(wait time.Duration) {
	if db == nil {
		return
	}
	if wait <= 0 {
		db.checkpointActiveBackgroundFlushWaitLastNs.Store(0)
		return
	}
	ns := uint64(wait.Nanoseconds())
	db.checkpointActiveBackgroundFlushWaitNs.Add(ns)
	db.checkpointActiveBackgroundFlushWaitLastNs.Store(ns)
	db.checkpointActiveBackgroundFlushWaitSamples.Add(1)
	updateAtomicMaxUint64(&db.checkpointActiveBackgroundFlushWaitMaxNs, ns)
}

type writeWaitReason uint8

const (
	writeWaitReasonNone writeWaitReason = iota
	writeWaitReasonFrontierCutover
	writeWaitReasonCheckpointDrain
	writeWaitReasonMaintenance
)

type writeWaitReasonStats struct {
	totalNs atomic.Uint64
	maxNs   atomic.Uint64
	lastNs  atomic.Uint64
	samples atomic.Uint64
	buckets [10]atomic.Uint64
}

var writeWaitLatencyBuckets = [...]struct {
	upperNs uint64
	label   string
}{
	{upperNs: uint64((10 * time.Microsecond).Nanoseconds()), label: "10us"},
	{upperNs: uint64((100 * time.Microsecond).Nanoseconds()), label: "100us"},
	{upperNs: uint64(time.Millisecond.Nanoseconds()), label: "1ms"},
	{upperNs: uint64((10 * time.Millisecond).Nanoseconds()), label: "10ms"},
	{upperNs: uint64((50 * time.Millisecond).Nanoseconds()), label: "50ms"},
	{upperNs: uint64((100 * time.Millisecond).Nanoseconds()), label: "100ms"},
	{upperNs: uint64((250 * time.Millisecond).Nanoseconds()), label: "250ms"},
	{upperNs: uint64(time.Second.Nanoseconds()), label: "1s"},
	{upperNs: uint64((5 * time.Second).Nanoseconds()), label: "5s"},
	{upperNs: ^uint64(0), label: "inf"},
}

func (s *writeWaitReasonStats) observe(wait time.Duration) {
	if s == nil {
		return
	}
	ns := uint64(1)
	if wait > 0 && wait.Nanoseconds() > 0 {
		ns = uint64(wait.Nanoseconds())
	}
	s.totalNs.Add(ns)
	s.lastNs.Store(ns)
	updateAtomicMaxUint64(&s.maxNs, ns)
	for i := range writeWaitLatencyBuckets {
		if ns <= writeWaitLatencyBuckets[i].upperNs {
			s.buckets[i].Add(1)
			break
		}
	}
	// Publish the sample only after its bucket is visible. Stats readers use the
	// sample count as the quantile target, so the reverse order can transiently
	// produce an incomplete cumulative histogram and an artificial +Inf result.
	s.samples.Add(1)
}

func writeWaitQuantileUpperNs(s *writeWaitReasonStats, percentile uint64) uint64 {
	if s == nil || percentile == 0 {
		return 0
	}
	total := s.samples.Load()
	if total == 0 {
		return 0
	}
	target := (total*percentile + 99) / 100
	var cumulative uint64
	for i := range writeWaitLatencyBuckets {
		cumulative += s.buckets[i].Load()
		if cumulative >= target {
			return writeWaitLatencyBuckets[i].upperNs
		}
	}
	return ^uint64(0)
}

func appendWriteWaitReasonStats(stats map[string]string, reason string, s *writeWaitReasonStats) {
	if stats == nil || reason == "" || s == nil {
		return
	}
	prefix := "treedb.cache.write.wait." + reason
	stats[prefix+".ns_total"] = fmt.Sprintf("%d", s.totalNs.Load())
	stats[prefix+".ns_max"] = fmt.Sprintf("%d", s.maxNs.Load())
	stats[prefix+".ns_last"] = fmt.Sprintf("%d", s.lastNs.Load())
	stats[prefix+".count_total"] = fmt.Sprintf("%d", s.samples.Load())
	var cumulative uint64
	for i := range writeWaitLatencyBuckets {
		cumulative += s.buckets[i].Load()
		stats[prefix+".bucket_le_"+writeWaitLatencyBuckets[i].label+".count_total"] = fmt.Sprintf("%d", cumulative)
	}
	stats[prefix+".p50_upper_ns"] = fmt.Sprintf("%d", writeWaitQuantileUpperNs(s, 50))
	stats[prefix+".p95_upper_ns"] = fmt.Sprintf("%d", writeWaitQuantileUpperNs(s, 95))
	stats[prefix+".p99_upper_ns"] = fmt.Sprintf("%d", writeWaitQuantileUpperNs(s, 99))
}

func (db *DB) observeWriteWaitReason(reason writeWaitReason, wait time.Duration) {
	if db == nil || reason == writeWaitReasonNone {
		return
	}
	switch reason {
	case writeWaitReasonFrontierCutover:
		db.writeWaitFrontierCutover.observe(wait)
	case writeWaitReasonCheckpointDrain:
		db.writeWaitCheckpointDrain.observe(wait)
	case writeWaitReasonMaintenance:
		db.writeWaitMaintenance.observe(wait)
	}
}

func (db *DB) observeWriteWaitForCheckpoint(wait time.Duration) {
	if db == nil {
		return
	}
	// Reaching this observer means a writer saw an active checkpoint and took
	// the checkpoint wait path. Preserve that sample even when a platform's
	// monotonic clock rounds the short wait to zero (notably on Windows).
	ns := uint64(1)
	if wait > 0 && wait.Nanoseconds() > 0 {
		ns = uint64(wait.Nanoseconds())
	}
	db.writeWaitForCheckpointNs.Add(ns)
	db.writeWaitForCheckpointLastNs.Store(ns)
	db.writeWaitForCheckpointSamples.Add(1)
	updateAtomicMaxUint64(&db.writeWaitForCheckpointMaxNs, ns)
}

func (db *DB) appendWriteWaitForCheckpointStats(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	stats["treedb.cache.write.wait_for_checkpoint.ns_total"] = fmt.Sprintf("%d", db.writeWaitForCheckpointNs.Load())
	stats["treedb.cache.write.wait_for_checkpoint.ns_max"] = fmt.Sprintf("%d", db.writeWaitForCheckpointMaxNs.Load())
	stats["treedb.cache.write.wait_for_checkpoint.ns_last"] = fmt.Sprintf("%d", db.writeWaitForCheckpointLastNs.Load())
	stats["treedb.cache.write.wait_for_checkpoint.count_total"] = fmt.Sprintf("%d", db.writeWaitForCheckpointSamples.Load())
	stats["treedb.cache.write.wait_for_checkpoint.active"] = fmt.Sprintf("%d", db.writeWaitForCheckpointActive.Load())
	appendWriteWaitReasonStats(stats, "frontier_cutover", &db.writeWaitFrontierCutover)
	appendWriteWaitReasonStats(stats, "checkpoint_drain", &db.writeWaitCheckpointDrain)
	appendWriteWaitReasonStats(stats, "maintenance", &db.writeWaitMaintenance)
	stats["treedb.cache.write.post_frontier_admission.count_total"] = fmt.Sprintf("%d", db.writePostFrontierAdmissions.Load())
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
	db.flushCoordinatorActiveWorkers.Add(1)
	if bytes > 0 {
		db.flushCoordinatorInFlightBytes.Add(bytes)
	}
	db.signalFlushCoordinatorWaiters()
}

func (db *DB) finishFlushCoordinatorWork(bytes int64, success bool) {
	if db == nil {
		return
	}
	addAtomicInt64FloorZero(&db.flushCoordinatorActiveWorkers, -1)
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

func (db *DB) appendCacheLeafLogLaneStats(stats map[string]string) {
	if db == nil || stats == nil || !db.indexOuterLeavesInValueLog {
		return
	}
	lanes := db.leafLogAppendLanesSnapshot()
	stats["treedb.cache.leaf_log_lanes.configured"] = fmt.Sprintf("%d", len(lanes))
	activeLanes := 0
	usedLanes := 0
	var totalCalls, totalPages, totalBytes, totalWaitNs, totalHoldNs, totalErrors, totalRotations, totalIdleRotations uint64
	for i, l := range lanes {
		if l == nil {
			continue
		}
		calls := l.leafLogAppendCalls.Load()
		pages := l.leafLogAppendPages.Load()
		bytes := l.leafLogAppendBytes.Load()
		waitNs := l.leafLogAppendLockWaitNs.Load()
		holdNs := l.leafLogAppendLockHoldNs.Load()
		errors := l.leafLogAppendErrors.Load()
		rotations := l.vlogRotateTotal.Load()
		idleRotations := l.vlogRotateIdleTotal.Load()
		activeSegments := 0
		closedSegments := 0
		l.vlogMu.Lock()
		if l.vlogPath != "" && l.vlogSeq > 0 {
			activeSegments = 1
		}
		closedSegments = len(l.vlogClosedSizes)
		l.vlogMu.Unlock()
		if activeSegments > 0 || closedSegments > 0 || calls > 0 || rotations > 0 {
			activeLanes++
		}
		if calls > 0 || pages > 0 {
			usedLanes++
		}
		totalCalls += calls
		totalPages += pages
		totalBytes += bytes
		totalWaitNs += waitNs
		totalHoldNs += holdNs
		totalErrors += errors
		totalRotations += rotations
		totalIdleRotations += idleRotations
		prefix := fmt.Sprintf("treedb.cache.leaf_log_lanes.lane.%02d", i)
		stats[prefix+".append_calls_total"] = fmt.Sprintf("%d", calls)
		stats[prefix+".append_pages_total"] = fmt.Sprintf("%d", pages)
		stats[prefix+".append_bytes_total"] = fmt.Sprintf("%d", bytes)
		stats[prefix+".append_lock_wait_ns_total"] = fmt.Sprintf("%d", waitNs)
		stats[prefix+".append_lock_hold_ns_total"] = fmt.Sprintf("%d", holdNs)
		stats[prefix+".append_errors_total"] = fmt.Sprintf("%d", errors)
		stats[prefix+".segment_rotations_total"] = fmt.Sprintf("%d", rotations)
		stats[prefix+".segment_rotations_idle_total"] = fmt.Sprintf("%d", idleRotations)
		stats[prefix+".segments_active"] = fmt.Sprintf("%d", activeSegments)
		stats[prefix+".segments_closed"] = fmt.Sprintf("%d", closedSegments)
	}
	stats["treedb.cache.leaf_log_lanes.active"] = fmt.Sprintf("%d", activeLanes)
	stats["treedb.cache.leaf_log_lanes.append_lanes_used"] = fmt.Sprintf("%d", usedLanes)
	stats["treedb.cache.leaf_log_lanes.append_calls_total"] = fmt.Sprintf("%d", totalCalls)
	stats["treedb.cache.leaf_log_lanes.append_pages_total"] = fmt.Sprintf("%d", totalPages)
	stats["treedb.cache.leaf_log_lanes.append_bytes_total"] = fmt.Sprintf("%d", totalBytes)
	stats["treedb.cache.leaf_log_lanes.append_lock_wait_ns_total"] = fmt.Sprintf("%d", totalWaitNs)
	stats["treedb.cache.leaf_log_lanes.append_lock_hold_ns_total"] = fmt.Sprintf("%d", totalHoldNs)
	stats["treedb.cache.leaf_log_lanes.append_errors_total"] = fmt.Sprintf("%d", totalErrors)
	stats["treedb.cache.leaf_log_lanes.segment_rotations_total"] = fmt.Sprintf("%d", totalRotations)
	stats["treedb.cache.leaf_log_lanes.segment_rotations_idle_total"] = fmt.Sprintf("%d", totalIdleRotations)
}

func (db *DB) appendCacheFlushApplyStats(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	db.appendCacheLeafLogLaneStats(stats)
	stats["treedb.cache.flush_apply.concurrency"] = fmt.Sprintf("%d", db.flushApplyConcurrency)
	stats["treedb.cache.flush_apply.span_native"] = fmt.Sprintf("%t", db.flushApplySpanNative)
	stats["treedb.cache.flush_apply.batches_total"] = fmt.Sprintf("%d", db.flushApplyBatches.Load())
	stats["treedb.cache.flush_apply.units_total"] = fmt.Sprintf("%d", db.flushApplyUnits.Load())
	stats["treedb.cache.flush_apply.entries_total"] = fmt.Sprintf("%d", db.flushApplyEntries.Load())
	stats["treedb.cache.flush_apply.bytes_total"] = fmt.Sprintf("%d", db.flushApplyBytes.Load())
	stats["treedb.cache.flush_apply.planning_ns_total"] = fmt.Sprintf("%d", db.flushApplyPlanningNs.Load())
	stats["treedb.cache.flush_apply.build_ns_total"] = fmt.Sprintf("%d", db.flushApplyBuildNs.Load())
	stats["treedb.cache.flush_apply.backend_write_ns_total"] = fmt.Sprintf("%d", db.flushApplyBackendWriteNs.Load())
	stats["treedb.cache.flush_apply.backend_batch_write_ns_total"] = fmt.Sprintf("%d", db.flushApplyBackendBatchWriteNs.Load())
	stats["treedb.cache.flush_apply.deferred_vlog_pointer_materialize_ns_total"] = fmt.Sprintf("%d", db.flushApplyDeferredVLogPointerMaterializeNs.Load())
	stats["treedb.cache.flush_apply.vlog_flush_ns_total"] = fmt.Sprintf("%d", db.flushApplyVLogFlushNs.Load())
	stats["treedb.cache.flush_apply.vlog_sync_ns_total"] = fmt.Sprintf("%d", db.flushApplyVLogSyncNs.Load())
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
	targetLeafSpans := db.flushSpanRunTargetLeafSpans.Load()
	singleOpSpans := db.flushSpanRunSingleOpSpans.Load()
	spanOps := db.flushSpanRunSpanOps.Load()
	spanBytes := db.flushSpanRunSpanBytes.Load()
	stats["treedb.cache.flush_span_run.target_leaf_spans_total"] = fmt.Sprintf("%d", targetLeafSpans)
	stats["treedb.cache.flush_span_run.single_op_spans_total"] = fmt.Sprintf("%d", singleOpSpans)
	stats["treedb.cache.flush_span_run.span_ops_total"] = fmt.Sprintf("%d", spanOps)
	stats["treedb.cache.flush_span_run.span_bytes_total"] = fmt.Sprintf("%d", spanBytes)
	stats["treedb.cache.flush_span_run.target_leaves_split_across_chunks_total"] = fmt.Sprintf("%d", db.flushSpanRunTargetLeavesSplitAcrossChunks.Load())
	stats["treedb.cache.flush_span_run.max_chunks_per_target_leaf"] = fmt.Sprintf("%d", db.flushSpanRunMaxChunksPerTargetLeaf.Load())
	if targetLeafSpans > 0 {
		stats["treedb.cache.flush_span_run.ops_per_span"] = fmt.Sprintf("%.6f", float64(spanOps)/float64(targetLeafSpans))
		stats["treedb.cache.flush_span_run.bytes_per_span"] = fmt.Sprintf("%.6f", float64(spanBytes)/float64(targetLeafSpans))
		stats["treedb.cache.flush_span_run.single_op_span_ratio"] = fmt.Sprintf("%.6f", float64(singleOpSpans)/float64(targetLeafSpans))
	}
	if runs := db.flushSpanRunRuns.Load(); runs > 0 {
		stats["treedb.cache.flush_span_run.ops_per_run"] = fmt.Sprintf("%.6f", float64(db.flushSpanRunPlannedOps.Load())/float64(runs))
	}
	stats["treedb.cache.flush_backlog_coalescing.enabled"] = fmt.Sprintf("%t", db.flushBacklogCoalescing)
	stats["treedb.cache.flush_backlog_coalescing.decisions_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingDecisions.Load())
	stats["treedb.cache.flush_backlog_coalescing.admitted_runs_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingAdmittedRuns.Load())
	stats["treedb.cache.flush_backlog_coalescing.admitted_extra_memtables_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingAdmittedExtraMemtables.Load())
	stats["treedb.cache.flush_backlog_coalescing.admitted_extra_bytes_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingAdmittedExtraBytes.Load())
	stats["treedb.cache.flush_backlog_coalescing.admitted_extra_ops_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingAdmittedExtraOps.Load())
	stats["treedb.cache.flush_backlog_coalescing.selected_memtables_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingSelectedMemtables.Load())
	stats["treedb.cache.flush_backlog_coalescing.selected_memtables_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingSelectedMemtablesMax.Load())
	stats["treedb.cache.flush_backlog_coalescing.selected_bytes_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingSelectedBytes.Load())
	stats["treedb.cache.flush_backlog_coalescing.selected_bytes_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingSelectedBytesMax.Load())
	stats["treedb.cache.flush_backlog_coalescing.selected_ops_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingSelectedOps.Load())
	stats["treedb.cache.flush_backlog_coalescing.selected_ops_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingSelectedOpsMax.Load())
	stats["treedb.cache.flush_backlog_coalescing.checkpoint.admitted_runs_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingCheckpointAdmittedRuns.Load())
	stats["treedb.cache.flush_backlog_coalescing.checkpoint.selected_memtables_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingCheckpointSelectedMemtables.Load())
	stats["treedb.cache.flush_backlog_coalescing.checkpoint.selected_memtables_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingCheckpointSelectedMemtablesMax.Load())
	stats["treedb.cache.flush_backlog_coalescing.checkpoint.selected_bytes_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingCheckpointSelectedBytes.Load())
	stats["treedb.cache.flush_backlog_coalescing.checkpoint.selected_bytes_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingCheckpointSelectedBytesMax.Load())
	stats["treedb.cache.flush_backlog_coalescing.checkpoint.selected_ops_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingCheckpointSelectedOps.Load())
	stats["treedb.cache.flush_backlog_coalescing.checkpoint.selected_ops_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingCheckpointSelectedOpsMax.Load())
	stats["treedb.cache.flush_backlog_coalescing.checkpoint.base_budget_covered_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingCheckpointBaseBudgetCovered.Load())
	stats["treedb.cache.flush_backlog_coalescing.queued_memtables_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingQueuedMemtablesMax.Load())
	stats["treedb.cache.flush_backlog_coalescing.queued_bytes_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingQueuedBytesMax.Load())
	stats["treedb.cache.flush_backlog_coalescing.queued_age_ns_max"] = fmt.Sprintf("%d", db.flushBacklogCoalescingQueuedAgeNsMax.Load())
	lastSinglePPM := db.flushBacklogCoalescingLastSingleOpSpanRatioPPM.Load()
	lastOpsPerSpanPPM := db.flushBacklogCoalescingLastOpsPerSpanPPM.Load()
	lastOldLeafBytesPerOpPPM := db.flushBacklogCoalescingLastOldLeafBytesPerOpPPM.Load()
	stats["treedb.cache.flush_backlog_coalescing.last_single_op_span_ratio"] = fmt.Sprintf("%.6f", float64(lastSinglePPM)/float64(flushBacklogCoalescingPPM))
	stats["treedb.cache.flush_backlog_coalescing.last_ops_per_span"] = fmt.Sprintf("%.6f", float64(lastOpsPerSpanPPM)/float64(flushBacklogCoalescingPPM))
	stats["treedb.cache.flush_backlog_coalescing.last_old_leaf_bytes_per_op"] = fmt.Sprintf("%.6f", float64(lastOldLeafBytesPerOpPPM)/float64(flushBacklogCoalescingPPM))
	for reason := flushBacklogCoalescingSkipReason(0); reason < flushBacklogCoalescingSkipReasonCount; reason++ {
		stats["treedb.cache.flush_backlog_coalescing.skip.reason."+reason.String()+"_total"] = fmt.Sprintf("%d", db.flushBacklogCoalescingSkipReasons[reason].Load())
	}
	stats["treedb.cache.flush_apply.coordinator.active"] = fmt.Sprintf("%d", db.flushCoordinatorActive.Load())
	stats["treedb.cache.flush_apply.coordinator.active_workers"] = fmt.Sprintf("%d", db.flushCoordinatorActiveWorkers.Load())
	stats["treedb.cache.flush_apply.coordinator.in_flight_bytes"] = fmt.Sprintf("%d", db.flushCoordinatorInFlightBytes.Load())
	stats["treedb.cache.flush_apply.coordinator.progress_total"] = fmt.Sprintf("%d", db.flushCoordinatorProgress.Load())
	stats["treedb.cache.flush_apply.coordinator.errors_total"] = fmt.Sprintf("%d", db.flushCoordinatorErrors.Load())
	stats["treedb.cache.flush_apply.coordinator.active_assist_skips_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorActiveAssistSkips.Load())
	stats["treedb.cache.flush_apply.coordinator.progress_waits_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorProgressWaits.Load())
	stats["treedb.cache.flush_apply.coordinator.progress_wait_ns_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorProgressWaitNs.Load())
	stats["treedb.cache.flush_apply.coordinator.stall_waits_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorStallWaits.Load())
	stats["treedb.cache.flush_apply.coordinator.checkpoint_preemptions_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorCheckpointPreemptions.Load())
	stats["treedb.cache.flush_apply.coordinator.blocking_fallbacks_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorBlockingFallbacks.Load())
	stats["treedb.cache.flush_apply.coordinator.hard_overload_fallbacks_total"] = fmt.Sprintf("%d", db.flushApplyCoordinatorHardOverloadFallbacks.Load())
}
