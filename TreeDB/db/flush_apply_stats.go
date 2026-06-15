package db

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/zipper"
)

func addDurationNs(dst interface{ Add(uint64) uint64 }, d time.Duration) {
	if d <= 0 {
		return
	}
	dst.Add(uint64(d.Nanoseconds()))
}

func addIntMetric(dst interface{ Add(uint64) uint64 }, v int) {
	if v <= 0 {
		return
	}
	dst.Add(uint64(v))
}

func addInt64Metric(dst interface{ Add(uint64) uint64 }, v int64) {
	if v <= 0 {
		return
	}
	dst.Add(uint64(v))
}

func storeUint64Max(dst *atomic.Uint64, value uint64) {
	if dst == nil || value == 0 {
		return
	}
	for {
		cur := dst.Load()
		if value <= cur || dst.CompareAndSwap(cur, value) {
			return
		}
	}
}

func (db *DB) observeFlushApplyReadOnlyPrepare(summary zipper.ReadOnlyLeafSpanSummary, workerSummary zipper.ReadOnlyLeafSpanWorkerRangeSummary, prepareNs uint64, err error, validationFailure bool) {
	if db == nil {
		return
	}
	db.flushApplyReadOnlyPrepareCalls.Add(1)
	if err != nil {
		db.flushApplyReadOnlyPrepareErrors.Add(1)
	}
	if validationFailure {
		db.flushApplyReadOnlyPrepareValidationFail.Add(1)
	}
	db.flushApplyReadOnlyPrepareNs.Add(prepareNs)
	if workerSummary.TargetWorkers > 0 {
		requested := uint64(workerSummary.TargetWorkers)
		db.flushApplyReadOnlyPrepareRequested.Add(requested)
		storeUint64Max(&db.flushApplyReadOnlyPrepareRequestedMax, requested)
	}
	if summary.Spans > 0 {
		spans := uint64(summary.Spans)
		db.flushApplyReadOnlyPrepareSpans.Add(spans)
		storeUint64Max(&db.flushApplyReadOnlyPrepareSpansMax, spans)
	}
	if summary.SpanOps > 0 {
		spanOps := uint64(summary.SpanOps)
		db.flushApplyReadOnlyPrepareSpanOps.Add(spanOps)
		storeUint64Max(&db.flushApplyReadOnlyPrepareSpanOpsMax, spanOps)
	}
	if summary.SpanBytes > 0 {
		spanBytes := uint64(summary.SpanBytes)
		db.flushApplyReadOnlyPrepareSpanBytes.Add(spanBytes)
		storeUint64Max(&db.flushApplyReadOnlyPrepareSpanBytesMax, spanBytes)
	}
	if summary.SingleOpSpans > 0 {
		singleOpSpans := uint64(summary.SingleOpSpans)
		db.flushApplyReadOnlyPrepareSingleOpSpans.Add(singleOpSpans)
		storeUint64Max(&db.flushApplyReadOnlyPrepareSingleOpSpansMax, singleOpSpans)
	}
	if workerSummary.Ranges > 0 {
		ranges := uint64(workerSummary.Ranges)
		db.flushApplyReadOnlyPrepareWorkerRanges.Add(ranges)
		storeUint64Max(&db.flushApplyReadOnlyPrepareWorkerRangesMax, ranges)
	}
}

func (db *DB) observeFlushApplySpanNativeFallback(summary zipper.ReadOnlyLeafSpanSummary, reason FlushSpanRunFallbackReason) {
	if db == nil {
		return
	}
	if !reason.Valid() {
		reason = FlushSpanRunFallbackUnknown
	}
	ops := summary.Ops
	if ops <= 0 {
		ops = summary.SpanOps
	}
	spans := summary.Spans
	if ops > 0 {
		db.flushApplySpanNativeCandidateOps.Add(uint64(ops))
		db.flushApplySpanNativeIneligibleOps.Add(uint64(ops))
		db.flushApplySpanNativeFallbackOps[reason].Add(uint64(ops))
	}
	if spans > 0 {
		db.flushApplySpanNativeCandidateSpans.Add(uint64(spans))
		db.flushApplySpanNativeIneligibleSpans.Add(uint64(spans))
		db.flushApplySpanNativeFallbackSpans[reason].Add(uint64(spans))
	}
	db.flushApplySpanNativeFallbacks.Add(1)
}

func classifyFlushApplySpanNativeFallback(summary zipper.ReadOnlyLeafSpanSummary, err error, validationFailure bool) FlushSpanRunFallbackReason {
	switch {
	case validationFailure:
		return FlushSpanRunFallbackValidationFailed
	case err != nil:
		return FlushSpanRunFallbackPrepareError
	case summary.ColdBuild:
		return FlushSpanRunFallbackColdBuild
	case summary.Maintenance:
		return FlushSpanRunFallbackMaintenance
	case !summary.ExactLeafSpans:
		return FlushSpanRunFallbackInexactLeafSpans
	case summary.DeleteRanges > 0:
		return FlushSpanRunFallbackRangeDeleteBarrier
	default:
		return FlushSpanRunFallbackSpanNativeNotImplemented
	}
}

func (db *DB) observeFlushApplyMetrics(metrics adaptive.Metrics, applyWall time.Duration, err error) {
	if db == nil {
		return
	}
	db.flushApplyCalls.Add(1)
	if err != nil {
		db.flushApplyErrors.Add(1)
	}
	addDurationNs(&db.flushApplyNs, applyWall)
	addIntMetric(&db.flushApplyOps, metrics.ZipperApplyOps)
	addIntMetric(&db.flushApplyOldNodeLoads, metrics.ZipperNodeLoads)
	addIntMetric(&db.flushApplyOldPagerNodeLoads, metrics.ZipperPagerNodeLoads)
	addIntMetric(&db.flushApplyOldLeafLogNodeLoads, metrics.ZipperLeafLogNodeLoads)
	addIntMetric(&db.flushApplyOldLeafLogCacheHits, metrics.ZipperLeafLogCacheHits)
	addIntMetric(&db.flushApplyOldLeafLogReaderCalls, metrics.ZipperLeafLogReaderCalls)
	addIntMetric(&db.flushApplyOldLeafLogViewReads, metrics.ZipperLeafLogViewReads)
	addIntMetric(&db.flushApplyOldLeafLogScratchReads, metrics.ZipperLeafLogScratchReads)
	addIntMetric(&db.flushApplyOldPagerNodeBytesRead, metrics.ZipperPagerNodeBytesRead)
	addIntMetric(&db.flushApplyOldLeafLogNodeBytesRead, metrics.ZipperLeafLogNodeBytesRead)
	addIntMetric(&db.flushApplyOldLeafLogRecordHintBytesRead, metrics.ZipperLeafLogRecordHintBytesRead)
	addIntMetric(&db.flushApplyLeafMerges, metrics.ZipperLeafMerges)
	addIntMetric(&db.flushApplyInternalMerges, metrics.ZipperInternalMerges)
	addIntMetric(&db.flushApplyInternalParallelMerges, metrics.ZipperInternalParallelMerges)
	addIntMetric(&db.flushApplyInternalParallelChildren, metrics.ZipperInternalParallelChildren)
	addIntMetric(&db.flushApplyInternalParallelWorkers, metrics.ZipperInternalParallelWorkers)
	addIntMetric(&db.flushApplyInternalParallelOps, metrics.ZipperInternalParallelOps)
	addIntMetric(&db.flushApplyLeafPagesWritten, metrics.ZipperLeafPagesWritten)
	addIntMetric(&db.flushApplyPagerLeafPagesWritten, metrics.ZipperPagerLeafPagesWritten)
	addIntMetric(&db.flushApplyLeafLogPagesWritten, metrics.ZipperLeafLogPagesWritten)
	addIntMetric(&db.flushApplyLeafPageBytesWritten, metrics.ZipperLeafPageBytesWritten)
	addIntMetric(&db.flushApplyPagerLeafPageBytesWritten, metrics.ZipperPagerLeafPageBytesWritten)
	addIntMetric(&db.flushApplyLeafLogPageBytesWritten, metrics.ZipperLeafLogPageBytesWritten)
	addIntMetric(&db.flushApplyLeafLogRecordHintBytesWritten, metrics.ZipperLeafLogRecordHintBytesWritten)
	addIntMetric(&db.flushApplyInternalPagesWritten, metrics.ZipperInternalPagesWritten)
	addIntMetric(&db.flushApplyInternalPageBytesWritten, metrics.ZipperInternalPageBytesWritten)
	addIntMetric(&db.flushApplyInternalChildRefs, metrics.ZipperInternalChildRefs)
	addIntMetric(&db.flushApplyRootSplitLevels, metrics.ZipperRootSplitLevels)
	addInt64Metric(&db.flushApplyRootReduceNs, metrics.ZipperRootReduceNs)
}

func (db *DB) observeFlushApplyPreparedOutput(metrics adaptive.Metrics, retiredPages int) {
	if db == nil {
		return
	}
	addIntMetric(&db.flushApplyPreparedOutputLeafLogPagesPrepared, metrics.ZipperLeafLogPagesWritten)
	addIntMetric(&db.flushApplyPreparedOutputLeafLogBytesPrepared, metrics.ZipperLeafLogPageBytesWritten)
	addIntMetric(&db.flushApplyPreparedOutputRetiredPagesPrepared, retiredPages)
}

func (db *DB) observeFlushApplyInstalledOutput(metrics adaptive.Metrics, retiredPages int) {
	if db == nil {
		return
	}
	addIntMetric(&db.flushApplyPreparedOutputLeafLogPagesInstalled, metrics.ZipperLeafLogPagesWritten)
	addIntMetric(&db.flushApplyPreparedOutputLeafLogBytesInstalled, metrics.ZipperLeafLogPageBytesWritten)
	addIntMetric(&db.flushApplyPreparedOutputRetiredPagesInstalled, retiredPages)
}

func (db *DB) observeFlushApplyAbandonedOutput(metrics adaptive.Metrics, retiredPages int) {
	if db == nil {
		return
	}
	addIntMetric(&db.flushApplyPreparedOutputLeafLogPagesAbandoned, metrics.ZipperLeafLogPagesWritten)
	addIntMetric(&db.flushApplyPreparedOutputLeafLogBytesAbandoned, metrics.ZipperLeafLogPageBytesWritten)
	addIntMetric(&db.flushApplyPreparedOutputRetiredPagesAbandoned, retiredPages)
}

func (db *DB) observeFlushApplyCommitWait(wait time.Duration) {
	if db == nil {
		return
	}
	addDurationNs(&db.flushApplyCommitWaitNs, wait)
}

func (db *DB) observeFlushApplyGuardedPublish(hold time.Duration) {
	if db == nil {
		return
	}
	db.flushApplyGuardedPublishCalls.Add(1)
	addDurationNs(&db.flushApplyGuardedPublishNs, hold)
}

func (db *DB) observeFlushApplyRetry() {
	if db == nil {
		return
	}
	db.flushApplyRetries.Add(1)
}

func (db *DB) observeFlushApplyMismatch() {
	if db == nil {
		return
	}
	db.flushApplyMismatches.Add(1)
}

func (db *DB) appendFlushApplyStats(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	applyOps := db.flushApplyOps.Load()
	stats["treedb.flush_apply.apply_calls_total"] = fmt.Sprintf("%d", db.flushApplyCalls.Load())
	stats["treedb.flush_apply.apply_errors_total"] = fmt.Sprintf("%d", db.flushApplyErrors.Load())
	stats["treedb.flush_apply.apply_ops_total"] = fmt.Sprintf("%d", applyOps)
	stats["treedb.flush_apply.apply_ns_total"] = fmt.Sprintf("%d", db.flushApplyNs.Load())
	stats["treedb.flush_apply.old_leaf_read_decode.node_loads_total"] = fmt.Sprintf("%d", db.flushApplyOldNodeLoads.Load())
	stats["treedb.flush_apply.old_leaf_read_decode.pager_node_loads_total"] = fmt.Sprintf("%d", db.flushApplyOldPagerNodeLoads.Load())
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_node_loads_total"] = fmt.Sprintf("%d", db.flushApplyOldLeafLogNodeLoads.Load())
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_cache_hits_total"] = fmt.Sprintf("%d", db.flushApplyOldLeafLogCacheHits.Load())
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_reader_calls_total"] = fmt.Sprintf("%d", db.flushApplyOldLeafLogReaderCalls.Load())
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_view_reads_total"] = fmt.Sprintf("%d", db.flushApplyOldLeafLogViewReads.Load())
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_scratch_reads_total"] = fmt.Sprintf("%d", db.flushApplyOldLeafLogScratchReads.Load())
	pagerBytes := db.flushApplyOldPagerNodeBytesRead.Load()
	leafLogBytes := db.flushApplyOldLeafLogNodeBytesRead.Load()
	oldLeafDecodeBytes := pagerBytes + leafLogBytes
	stats["treedb.flush_apply.old_leaf_read_decode.bytes_total"] = fmt.Sprintf("%d", oldLeafDecodeBytes)
	if applyOps > 0 {
		stats["treedb.flush_apply.old_leaf_read_decode.bytes_per_op"] = fmt.Sprintf("%.6f", float64(oldLeafDecodeBytes)/float64(applyOps))
	}
	stats["treedb.flush_apply.old_leaf_read_decode.pager_bytes_total"] = fmt.Sprintf("%d", pagerBytes)
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_bytes_total"] = fmt.Sprintf("%d", leafLogBytes)
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_record_hint_bytes_total"] = fmt.Sprintf("%d", db.flushApplyOldLeafLogRecordHintBytesRead.Load())
	leafMerges := db.flushApplyLeafMerges.Load()
	stats["treedb.flush_apply.merge_build.leaf_merges_total"] = fmt.Sprintf("%d", leafMerges)
	if applyOps > 0 {
		stats["treedb.flush_apply.merge_build.leaf_merges_per_op"] = fmt.Sprintf("%.6f", float64(leafMerges)/float64(applyOps))
	}
	stats["treedb.flush_apply.merge_build.internal_merges_total"] = fmt.Sprintf("%d", db.flushApplyInternalMerges.Load())
	stats["treedb.flush_apply.merge_build.internal_parallel_merges_total"] = fmt.Sprintf("%d", db.flushApplyInternalParallelMerges.Load())
	stats["treedb.flush_apply.merge_build.internal_parallel_children_total"] = fmt.Sprintf("%d", db.flushApplyInternalParallelChildren.Load())
	stats["treedb.flush_apply.merge_build.internal_parallel_workers_total"] = fmt.Sprintf("%d", db.flushApplyInternalParallelWorkers.Load())
	stats["treedb.flush_apply.merge_build.internal_parallel_ops_total"] = fmt.Sprintf("%d", db.flushApplyInternalParallelOps.Load())
	replacementLeafPages := db.flushApplyLeafPagesWritten.Load()
	stats["treedb.flush_apply.merge_build.leaf_pages_written_total"] = fmt.Sprintf("%d", replacementLeafPages)
	stats["treedb.flush_apply.merge_build.replacement_leaf_pages_total"] = fmt.Sprintf("%d", replacementLeafPages)
	if applyOps > 0 {
		stats["treedb.flush_apply.merge_build.replacement_leaf_pages_per_op"] = fmt.Sprintf("%.6f", float64(replacementLeafPages)/float64(applyOps))
	}
	stats["treedb.flush_apply.merge_build.pager_leaf_pages_written_total"] = fmt.Sprintf("%d", db.flushApplyPagerLeafPagesWritten.Load())
	stats["treedb.flush_apply.merge_build.leaf_log_pages_written_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogPagesWritten.Load())
	stats["treedb.flush_apply.merge_build.leaf_page_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyLeafPageBytesWritten.Load())
	stats["treedb.flush_apply.merge_build.pager_leaf_page_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyPagerLeafPageBytesWritten.Load())
	stats["treedb.flush_apply.merge_build.leaf_log_page_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogPageBytesWritten.Load())
	stats["treedb.flush_apply.merge_build.leaf_log_record_hint_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogRecordHintBytesWritten.Load())
	stats["treedb.flush_apply.prepared_output.leaf_log_pages_prepared_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputLeafLogPagesPrepared.Load())
	stats["treedb.flush_apply.prepared_output.leaf_log_bytes_prepared_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputLeafLogBytesPrepared.Load())
	stats["treedb.flush_apply.prepared_output.leaf_log_pages_installed_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputLeafLogPagesInstalled.Load())
	stats["treedb.flush_apply.prepared_output.leaf_log_bytes_installed_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputLeafLogBytesInstalled.Load())
	stats["treedb.flush_apply.prepared_output.leaf_log_pages_abandoned_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputLeafLogPagesAbandoned.Load())
	stats["treedb.flush_apply.prepared_output.leaf_log_bytes_abandoned_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputLeafLogBytesAbandoned.Load())
	stats["treedb.flush_apply.prepared_output.retired_pages_prepared_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputRetiredPagesPrepared.Load())
	stats["treedb.flush_apply.prepared_output.retired_pages_installed_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputRetiredPagesInstalled.Load())
	stats["treedb.flush_apply.prepared_output.retired_pages_abandoned_total"] = fmt.Sprintf("%d", db.flushApplyPreparedOutputRetiredPagesAbandoned.Load())
	stats["treedb.flush_apply.merge_build.internal_pages_written_total"] = fmt.Sprintf("%d", db.flushApplyInternalPagesWritten.Load())
	stats["treedb.flush_apply.merge_build.internal_page_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyInternalPageBytesWritten.Load())
	stats["treedb.flush_apply.merge_build.internal_child_refs_total"] = fmt.Sprintf("%d", db.flushApplyInternalChildRefs.Load())
	rootReduceNs := db.flushApplyRootReduceNs.Load()
	stats["treedb.flush_apply.root_reduce.ns_total"] = fmt.Sprintf("%d", rootReduceNs)
	if applyOps > 0 {
		stats["treedb.flush_apply.root_reduce.ns_per_op"] = fmt.Sprintf("%.6f", float64(rootReduceNs)/float64(applyOps))
	}
	stats["treedb.flush_apply.root_reduce.split_levels_total"] = fmt.Sprintf("%d", db.flushApplyRootSplitLevels.Load())
	stats["treedb.flush_apply.read_only_prepare.calls_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareCalls.Load())
	stats["treedb.flush_apply.read_only_prepare.errors_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareErrors.Load())
	stats["treedb.flush_apply.read_only_prepare.validation_failures_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareValidationFail.Load())
	stats["treedb.flush_apply.read_only_prepare.ns_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareNs.Load())
	stats["treedb.flush_apply.read_only_prepare.requested_workers_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareRequested.Load())
	stats["treedb.flush_apply.read_only_prepare.requested_workers_max"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareRequestedMax.Load())
	stats["treedb.flush_apply.read_only_prepare.spans_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareSpans.Load())
	stats["treedb.flush_apply.read_only_prepare.spans_max"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareSpansMax.Load())
	stats["treedb.flush_apply.read_only_prepare.span_ops_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareSpanOps.Load())
	stats["treedb.flush_apply.read_only_prepare.span_ops_max"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareSpanOpsMax.Load())
	readOnlySpans := db.flushApplyReadOnlyPrepareSpans.Load()
	readOnlySpanOps := db.flushApplyReadOnlyPrepareSpanOps.Load()
	readOnlySpanBytes := db.flushApplyReadOnlyPrepareSpanBytes.Load()
	stats["treedb.flush_apply.read_only_prepare.span_bytes_total"] = fmt.Sprintf("%d", readOnlySpanBytes)
	stats["treedb.flush_apply.read_only_prepare.span_bytes_max"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareSpanBytesMax.Load())
	stats["treedb.flush_apply.read_only_prepare.single_op_spans_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareSingleOpSpans.Load())
	stats["treedb.flush_apply.read_only_prepare.single_op_spans_max"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareSingleOpSpansMax.Load())
	stats["treedb.flush_apply.span_run.target_leaf_spans_total"] = fmt.Sprintf("%d", readOnlySpans)
	stats["treedb.flush_apply.span_run.single_op_spans_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareSingleOpSpans.Load())
	stats["treedb.flush_apply.span_run.span_ops_total"] = fmt.Sprintf("%d", readOnlySpanOps)
	stats["treedb.flush_apply.span_run.span_bytes_total"] = fmt.Sprintf("%d", readOnlySpanBytes)
	if readOnlySpans > 0 {
		stats["treedb.flush_apply.span_run.ops_per_span"] = fmt.Sprintf("%.6f", float64(readOnlySpanOps)/float64(readOnlySpans))
		stats["treedb.flush_apply.span_run.bytes_per_span"] = fmt.Sprintf("%.6f", float64(readOnlySpanBytes)/float64(readOnlySpans))
	}
	stats["treedb.flush_apply.read_only_prepare.worker_ranges_total"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareWorkerRanges.Load())
	stats["treedb.flush_apply.read_only_prepare.worker_ranges_max"] = fmt.Sprintf("%d", db.flushApplyReadOnlyPrepareWorkerRangesMax.Load())
	stats["treedb.flush_apply.commit_wait_ns_total"] = fmt.Sprintf("%d", db.flushApplyCommitWaitNs.Load())
	stats["treedb.flush_apply.guarded_publish.calls_total"] = fmt.Sprintf("%d", db.flushApplyGuardedPublishCalls.Load())
	guardedPublishNs := db.flushApplyGuardedPublishNs.Load()
	stats["treedb.flush_apply.guarded_publish.ns_total"] = fmt.Sprintf("%d", guardedPublishNs)
	if applyOps > 0 {
		stats["treedb.flush_apply.guarded_publish.ns_per_op"] = fmt.Sprintf("%.6f", float64(guardedPublishNs)/float64(applyOps))
	}
	stats["treedb.flush_apply.span_native.candidate_ops_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeCandidateOps.Load())
	stats["treedb.flush_apply.span_native.candidate_spans_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeCandidateSpans.Load())
	stats["treedb.flush_apply.span_native.eligible_ops_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeEligibleOps.Load())
	stats["treedb.flush_apply.span_native.eligible_spans_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeEligibleSpans.Load())
	stats["treedb.flush_apply.span_native.ineligible_ops_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeIneligibleOps.Load())
	stats["treedb.flush_apply.span_native.ineligible_spans_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeIneligibleSpans.Load())
	stats["treedb.flush_apply.span_native.fallbacks_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeFallbacks.Load())
	for _, reason := range FlushSpanRunFallbackReasons() {
		name := reason.String()
		stats["treedb.flush_apply.span_native.fallback.reason."+name+".ops_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeFallbackOps[reason].Load())
		stats["treedb.flush_apply.span_native.fallback.reason."+name+".spans_total"] = fmt.Sprintf("%d", db.flushApplySpanNativeFallbackSpans[reason].Load())
	}
	stats["treedb.flush_apply.retry_total"] = fmt.Sprintf("%d", db.flushApplyRetries.Load())
	stats["treedb.flush_apply.mismatch_total"] = fmt.Sprintf("%d", db.flushApplyMismatches.Load())
}
