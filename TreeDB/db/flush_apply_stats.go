package db

import (
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
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
	addIntMetric(&db.flushApplyLeafPagesWritten, metrics.ZipperLeafPagesWritten)
	addIntMetric(&db.flushApplyPagerLeafPagesWritten, metrics.ZipperPagerLeafPagesWritten)
	addIntMetric(&db.flushApplyLeafLogPagesWritten, metrics.ZipperLeafLogPagesWritten)
	addIntMetric(&db.flushApplyLeafPageBytesWritten, metrics.ZipperLeafPageBytesWritten)
	addIntMetric(&db.flushApplyPagerLeafPageBytesWritten, metrics.ZipperPagerLeafPageBytesWritten)
	addIntMetric(&db.flushApplyLeafLogPageBytesWritten, metrics.ZipperLeafLogPageBytesWritten)
	addIntMetric(&db.flushApplyInternalPagesWritten, metrics.ZipperInternalPagesWritten)
	addIntMetric(&db.flushApplyInternalPageBytesWritten, metrics.ZipperInternalPageBytesWritten)
	addIntMetric(&db.flushApplyInternalChildRefs, metrics.ZipperInternalChildRefs)
	addIntMetric(&db.flushApplyRootSplitLevels, metrics.ZipperRootSplitLevels)
	addInt64Metric(&db.flushApplyRootReduceNs, metrics.ZipperRootReduceNs)
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
	stats["treedb.flush_apply.apply_calls_total"] = fmt.Sprintf("%d", db.flushApplyCalls.Load())
	stats["treedb.flush_apply.apply_errors_total"] = fmt.Sprintf("%d", db.flushApplyErrors.Load())
	stats["treedb.flush_apply.apply_ops_total"] = fmt.Sprintf("%d", db.flushApplyOps.Load())
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
	stats["treedb.flush_apply.old_leaf_read_decode.bytes_total"] = fmt.Sprintf("%d", pagerBytes+leafLogBytes)
	stats["treedb.flush_apply.old_leaf_read_decode.pager_bytes_total"] = fmt.Sprintf("%d", pagerBytes)
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_bytes_total"] = fmt.Sprintf("%d", leafLogBytes)
	stats["treedb.flush_apply.old_leaf_read_decode.leaf_log_record_hint_bytes_total"] = fmt.Sprintf("%d", db.flushApplyOldLeafLogRecordHintBytesRead.Load())
	stats["treedb.flush_apply.merge_build.leaf_merges_total"] = fmt.Sprintf("%d", db.flushApplyLeafMerges.Load())
	stats["treedb.flush_apply.merge_build.internal_merges_total"] = fmt.Sprintf("%d", db.flushApplyInternalMerges.Load())
	stats["treedb.flush_apply.merge_build.leaf_pages_written_total"] = fmt.Sprintf("%d", db.flushApplyLeafPagesWritten.Load())
	stats["treedb.flush_apply.merge_build.pager_leaf_pages_written_total"] = fmt.Sprintf("%d", db.flushApplyPagerLeafPagesWritten.Load())
	stats["treedb.flush_apply.merge_build.leaf_log_pages_written_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogPagesWritten.Load())
	stats["treedb.flush_apply.merge_build.leaf_page_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyLeafPageBytesWritten.Load())
	stats["treedb.flush_apply.merge_build.pager_leaf_page_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyPagerLeafPageBytesWritten.Load())
	stats["treedb.flush_apply.merge_build.leaf_log_page_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogPageBytesWritten.Load())
	stats["treedb.flush_apply.merge_build.internal_pages_written_total"] = fmt.Sprintf("%d", db.flushApplyInternalPagesWritten.Load())
	stats["treedb.flush_apply.merge_build.internal_page_bytes_written_total"] = fmt.Sprintf("%d", db.flushApplyInternalPageBytesWritten.Load())
	stats["treedb.flush_apply.merge_build.internal_child_refs_total"] = fmt.Sprintf("%d", db.flushApplyInternalChildRefs.Load())
	stats["treedb.flush_apply.root_reduce.ns_total"] = fmt.Sprintf("%d", db.flushApplyRootReduceNs.Load())
	stats["treedb.flush_apply.root_reduce.split_levels_total"] = fmt.Sprintf("%d", db.flushApplyRootSplitLevels.Load())
	stats["treedb.flush_apply.commit_wait_ns_total"] = fmt.Sprintf("%d", db.flushApplyCommitWaitNs.Load())
	stats["treedb.flush_apply.guarded_publish.calls_total"] = fmt.Sprintf("%d", db.flushApplyGuardedPublishCalls.Load())
	stats["treedb.flush_apply.guarded_publish.ns_total"] = fmt.Sprintf("%d", db.flushApplyGuardedPublishNs.Load())
	stats["treedb.flush_apply.retry_total"] = fmt.Sprintf("%d", db.flushApplyRetries.Load())
	stats["treedb.flush_apply.mismatch_total"] = fmt.Sprintf("%d", db.flushApplyMismatches.Load())
}
