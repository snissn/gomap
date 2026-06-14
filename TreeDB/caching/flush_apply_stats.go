package caching

import (
	"fmt"
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
	stats["treedb.cache.flush_apply.leaf_log_append_frames_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogAppendFrames.Load())
	stats["treedb.cache.flush_apply.leaf_log_append_records_total"] = fmt.Sprintf("%d", db.flushApplyLeafLogAppendRecords.Load())
	stats["treedb.cache.flush_apply.foreground_assist_calls_total"] = fmt.Sprintf("%d", db.flushApplyForegroundAssistCalls.Load())
	stats["treedb.cache.flush_apply.foreground_assist_wait_ns_total"] = fmt.Sprintf("%d", db.flushApplyForegroundAssistWaitNs.Load())
	stats["treedb.cache.flush_apply.foreground_assist_flushes_total"] = fmt.Sprintf("%d", db.flushApplyForegroundAssistFlushes.Load())
}
