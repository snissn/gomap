package treedbstats

import "testing"

func TestSelectedKeepsSharedTreeDBStats(t *testing.T) {
	stats := map[string]string{
		"treedb.commit_seq":          "7",
		"treedb.applied_command_lsn": "9",
		"treedb.command_wal.enabled": "true",
		"treedb.command_wal.frames":  "3",
		"treedb.process.read_path.backend_tree.get_append_pointer_hits_total": "5",
		"treedb.process.read_path.outer_leaf.cache.hits":                      "11",
		"treedb.vlog.mmap_read.fallback_readat":                               "13",
		"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes":                       "1073741824",
		"treedb.cache.vlog_mmap.max_mapped_leaf_sealed_segments":              "32",
		"treedb.publish.ordered_root_delta_group.calls_total":                 "19",
		"treedb.publish.watermark.latency_p99_ms":                             "23",
		"treedb.collections.write_domain.indexed_flush.calls_total":           "29",
		"treedb.unrelated_stat_that_should_not_leave_the_helper":              "17",
	}
	got := Selected(stats)
	for _, key := range []string{
		"treedb.commit_seq",
		"treedb.applied_command_lsn",
		"treedb.command_wal.enabled",
		"treedb.command_wal.frames",
		"treedb.process.read_path.backend_tree.get_append_pointer_hits_total",
		"treedb.process.read_path.outer_leaf.cache.hits",
		"treedb.vlog.mmap_read.fallback_readat",
		"treedb.vlog.mmap_max_mapped_leaf_sealed_bytes",
		"treedb.cache.vlog_mmap.max_mapped_leaf_sealed_segments",
		"treedb.publish.ordered_root_delta_group.calls_total",
		"treedb.publish.watermark.latency_p99_ms",
		"treedb.collections.write_domain.indexed_flush.calls_total",
	} {
		if got[key] == "" {
			t.Fatalf("Selected missing %s from %#v", key, got)
		}
	}
	if _, ok := got["treedb.unrelated_stat_that_should_not_leave_the_helper"]; ok {
		t.Fatalf("Selected kept unrelated stat: %#v", got)
	}
}
