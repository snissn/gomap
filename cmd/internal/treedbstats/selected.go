package treedbstats

import "strings"

// Selected returns the TreeDB stats that benchmark/reporting tools preserve in
// JSON artifacts. Keeping the allowlist in one place avoids drift between
// fixture producers and report consumers.
func Selected(stats map[string]string) map[string]string {
	if len(stats) == 0 {
		return nil
	}
	out := make(map[string]string)
	for key, value := range stats {
		if isSelectedKey(key) {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func isSelectedKey(key string) bool {
	switch {
	case key == "treedb.commit_seq":
		return true
	case key == "treedb.applied_command_lsn":
		return true
	case strings.HasPrefix(key, "treedb.command_wal."):
		return true
	case strings.HasPrefix(key, "treedb.cache.command_wal."):
		return true
	case isSelectedWriteKey(key):
		return true
	case isSelectedAutoCheckpointKey(key):
		return true
	case key == "treedb.cache.backpressure_mode":
		return true
	case key == "treedb.cache.queue_laneid_misses":
		return true
	case strings.HasPrefix(key, "treedb.cache.stats."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_mmap."):
		return true
	case isSelectedTotalKey(key, "treedb.cache.vlog_read."):
		return true
	case isSelectedTotalKey(key, "treedb.cache.vlog_template."):
		return true
	case isSelectedValueLogTemplateDefCacheKey(key, "treedb.cache.vlog_template_def_cache."):
		return true
	case isSelectedCachedValueLogWriteIOKey(key):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_decode_buffer_grow."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_decode_scratch."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_writer_append_buf."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_auto."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_write_mode."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_payload_kind."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_payload_split."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_outer_leaf_codec."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_block."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_dict."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_autotune."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_leaf_scan."):
		return true
	case strings.HasPrefix(key, "treedb.vlog.mmap"):
		return true
	case isSelectedTotalKey(key, "treedb.vlog.read."):
		return true
	case isSelectedValueLogTemplateDefCacheKey(key, "treedb.vlog.template_def_cache."):
		return true
	case strings.HasPrefix(key, "treedb.vlog.decode_buffer_grow."):
		return true
	case strings.HasPrefix(key, "treedb.vlog.decode_scratch."):
		return true
	case strings.HasPrefix(key, "treedb.vlog.writer_append_buf."):
		return true
	case strings.HasPrefix(key, "treedb.vlog.grouped_frame_cache."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_grouped_frame_cache."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_zombie."):
		return true
	case strings.HasPrefix(key, "treedb.process.memory."):
		return true
	case strings.HasPrefix(key, "treedb.cache.memtable_residency."):
		return true
	case strings.HasPrefix(key, "treedb.process.memtable_residency."):
		return true
	case strings.HasPrefix(key, "treedb.cache.append_only."):
		return true
	case strings.HasPrefix(key, "treedb.process.append_only."):
		return true
	case strings.HasPrefix(key, "treedb.vlog.outer_leaf_block_cache."):
		return true
	case strings.HasPrefix(key, "treedb.process.read_path.backend_tree."):
		return true
	case strings.HasPrefix(key, "treedb.process.read_path.outer_leaf."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_generation."):
		return true
	case strings.HasPrefix(key, "treedb.cache.flush_apply."):
		return true
	case strings.HasPrefix(key, "treedb.cache.leaf_log_lanes."):
		return true
	case strings.HasPrefix(key, "treedb.cache.flush_span_run."):
		return true
	case strings.HasPrefix(key, "treedb.cache.checkpoint."):
		return true
	case strings.HasPrefix(key, "treedb.flush_admission."):
		return true
	case strings.HasPrefix(key, "treedb.flush_apply."):
		return true
	case strings.HasPrefix(key, "treedb.raw.span_native."):
		return true
	case strings.HasPrefix(key, "treedb.leaf_generation."):
		return true
	case strings.HasPrefix(key, "treedb.cache.flush_backlog_coalescing."):
		return true
	case key == "treedb.cache.queue_len":
		return true
	case key == "treedb.cache.queue_backlog_bytes":
		return true
	case key == "treedb.cache.vlog_retained_segments":
		return true
	case key == "treedb.cache.vlog_retained_bytes_estimate":
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_retained_prune."):
		return true
	case strings.HasPrefix(key, "treedb.publish.ordered_root_delta_group."):
		return isSelectedOrderedRootDeltaGroupKey(key)
	case strings.HasPrefix(key, "treedb.collections.write_domain."):
		return true
	case strings.HasPrefix(key, "treedb.publish.watermark."):
		return true
	default:
		return false
	}
}

func isSelectedAutoCheckpointKey(key string) bool {
	switch key {
	case "treedb.cache.auto_checkpoint.count",
		"treedb.cache.auto_checkpoint.last_reason":
		return true
	default:
		return false
	}
}

func isSelectedWriteKey(key string) bool {
	switch key {
	case "treedb.cache.write.wait_for_checkpoint.ns_total",
		"treedb.cache.write.wait_for_checkpoint.count_total":
		return true
	default:
		return false
	}
}

func isSelectedTotalKey(key, prefix string) bool {
	return strings.HasPrefix(key, prefix) && strings.HasSuffix(key, "_total")
}

func isSelectedValueLogTemplateDefCacheKey(key, prefix string) bool {
	suffix, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return false
	}
	switch suffix {
	case "hits", "misses":
		return true
	default:
		return false
	}
}

func isSelectedCachedValueLogWriteIOKey(key string) bool {
	switch key {
	case "treedb.cache.vlog_writev.bytes",
		"treedb.cache.vlog_writev.syscalls",
		"treedb.cache.vlog_writev.iovecs",
		"treedb.cache.vlog_writev.flushes",
		"treedb.cache.vlog_write.bytes",
		"treedb.cache.vlog_write.syscalls",
		"treedb.cache.vlog_write.calls",
		"treedb.cache.vlog_io.bytes",
		"treedb.cache.vlog_io.syscalls":
		return true
	default:
		return false
	}
}

func isSelectedOrderedRootDeltaGroupKey(key string) bool {
	if !strings.HasPrefix(key, "treedb.publish.ordered_root_delta_group.span_native.triage.route.") {
		return true
	}
	return !strings.HasSuffix(key, ".context") && !strings.HasSuffix(key, ".detail")
}
