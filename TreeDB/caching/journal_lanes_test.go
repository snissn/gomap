package caching

import (
	"strconv"
	"testing"
)

func TestDefaultJournalLaneCountIsCoalescingSafe(t *testing.T) {
	cases := []struct {
		procs int
		want  int
	}{
		{procs: 1, want: 3},
		{procs: 2, want: 3},
		{procs: 4, want: 3},
		{procs: 12, want: 3},
		{procs: 16, want: 3},
		{procs: 24, want: 3},
	}

	for _, tc := range cases {
		tc := tc
		t.Run("procs="+strconv.Itoa(tc.procs), func(t *testing.T) {
			if got := defaultJournalLaneCount(tc.procs); got != tc.want {
				t.Fatalf("defaultJournalLaneCount(%d)=%d want %d", tc.procs, got, tc.want)
			}
		})
	}
}

func TestOpenStatsExposeResolvedDefaultJournalLanes(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.cache.journal_lanes.configured"]; got != "0" {
		t.Fatalf("configured journal lanes=%q want 0", got)
	}
	if got := stats["treedb.cache.journal_lanes.defaulted"]; got != "true" {
		t.Fatalf("defaulted journal lanes=%q want true", got)
	}
	if got := stats["treedb.cache.journal_lanes.effective"]; got != "3" {
		t.Fatalf("effective journal lanes=%q want 3", got)
	}
	if got := stats["treedb.cache.journal_lanes.hot"]; got != "1" {
		t.Fatalf("hot journal lanes=%q want 1", got)
	}
	if got := stats["treedb.cache.journal_lanes.warm"]; got != "1" {
		t.Fatalf("warm journal lanes=%q want 1", got)
	}
	if got := stats["treedb.cache.journal_lanes.cold"]; got != "1" {
		t.Fatalf("cold journal lanes=%q want 1", got)
	}
	if got := stats["treedb.cache.memtable_shards"]; got == "" {
		t.Fatalf("missing memtable shard stat")
	}
}

func TestOpenStatsExposeExplicitJournalLaneOverride(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{FlushThreshold: 1 << 20, JournalLanes: 6})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.cache.journal_lanes.configured"]; got != "6" {
		t.Fatalf("configured journal lanes=%q want 6", got)
	}
	if got := stats["treedb.cache.journal_lanes.defaulted"]; got != "false" {
		t.Fatalf("defaulted journal lanes=%q want false", got)
	}
	if got := stats["treedb.cache.journal_lanes.effective"]; got != "6" {
		t.Fatalf("effective journal lanes=%q want 6", got)
	}
	if got := stats["treedb.cache.journal_lanes.hot"]; got != "4" {
		t.Fatalf("hot journal lanes=%q want 4", got)
	}
	if got := stats["treedb.cache.journal_lanes.warm"]; got != "1" {
		t.Fatalf("warm journal lanes=%q want 1", got)
	}
	if got := stats["treedb.cache.journal_lanes.cold"]; got != "1" {
		t.Fatalf("cold journal lanes=%q want 1", got)
	}
}
