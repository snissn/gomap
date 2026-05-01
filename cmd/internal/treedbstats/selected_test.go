package treedbstats

import "testing"

func TestSelectedKeepsSharedTreeDBStats(t *testing.T) {
	stats := map[string]string{
		"treedb.commit_seq": "7",
		"treedb.process.read_path.outer_leaf.cache.hits":         "11",
		"treedb.vlog.mmap_read.fallback_readat":                  "13",
		"treedb.unrelated_stat_that_should_not_leave_the_helper": "17",
	}
	got := Selected(stats)
	for _, key := range []string{
		"treedb.commit_seq",
		"treedb.process.read_path.outer_leaf.cache.hits",
		"treedb.vlog.mmap_read.fallback_readat",
	} {
		if got[key] == "" {
			t.Fatalf("Selected missing %s from %#v", key, got)
		}
	}
	if _, ok := got["treedb.unrelated_stat_that_should_not_leave_the_helper"]; ok {
		t.Fatalf("Selected kept unrelated stat: %#v", got)
	}
}
