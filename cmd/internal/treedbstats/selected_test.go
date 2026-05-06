package treedbstats

import "testing"

func TestSelectedKeepsSharedTreeDBStats(t *testing.T) {
	stats := map[string]string{
		"treedb.commit_seq": "7",
		"treedb.process.read_path.backend_tree.get_append_pointer_hits_total":            "5",
		"treedb.process.read_path.outer_leaf.cache.hits":                                 "11",
		"treedb.vlog.mmap_read.fallback_readat":                                          "13",
		"treedb.publish.ordered_root_delta_group.calls_total":                            "19",
		"treedb.publish.install_guard.calls_total":                                       "20",
		"treedb.publish.install_guard.failures_total":                                    "21",
		"treedb.publish.install_guard.ns_total":                                          "22",
		"treedb.publish.watermark.latency_p99_ms":                                        "23",
		"treedb.collections.write_domain.indexed_flush.calls_total":                      "29",
		"treedb.collections.write_domain.root_delta_plan.raw_unit.primary.entries_total": "31",
		"treedb.collections.write_domain.root_delta_plan.final.secondary.bytes_total":    "37",
		"treedb.collections.write_domain.root_delta_plan.squashed_entries_total":         "41",
		"treedb.collections.write_domain.coalesced_flush_batch.net_zero_batches_total":   "43",
		"treedb.collections.write_domain.primary_only.duplicate_ids_coalesced_total":     "53",
		"treedb.collections.write_domain.primary_only.drains_total":                      "59",
		"treedb.unrelated_stat_that_should_not_leave_the_helper":                         "17",
	}
	got := Selected(stats)
	for _, key := range []string{
		"treedb.commit_seq",
		"treedb.process.read_path.backend_tree.get_append_pointer_hits_total",
		"treedb.process.read_path.outer_leaf.cache.hits",
		"treedb.vlog.mmap_read.fallback_readat",
		"treedb.publish.ordered_root_delta_group.calls_total",
		"treedb.publish.install_guard.calls_total",
		"treedb.publish.install_guard.failures_total",
		"treedb.publish.install_guard.ns_total",
		"treedb.publish.watermark.latency_p99_ms",
		"treedb.collections.write_domain.indexed_flush.calls_total",
		"treedb.collections.write_domain.root_delta_plan.raw_unit.primary.entries_total",
		"treedb.collections.write_domain.root_delta_plan.final.secondary.bytes_total",
		"treedb.collections.write_domain.root_delta_plan.squashed_entries_total",
		"treedb.collections.write_domain.coalesced_flush_batch.net_zero_batches_total",
		"treedb.collections.write_domain.primary_only.duplicate_ids_coalesced_total",
		"treedb.collections.write_domain.primary_only.drains_total",
	} {
		if got[key] == "" {
			t.Fatalf("Selected missing %s from %#v", key, got)
		}
	}
	if _, ok := got["treedb.unrelated_stat_that_should_not_leave_the_helper"]; ok {
		t.Fatalf("Selected kept unrelated stat: %#v", got)
	}
}
