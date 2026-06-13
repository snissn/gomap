package caching

import (
	"strconv"
	"testing"
)

func deleteRangeStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("stats missing %s", key)
	}
	got, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("stats[%s]=%q parse: %v", key, raw, err)
	}
	return got
}

func TestCachingDBDeleteRangeStatsCountsVisitedTombstones(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{FlushThreshold: 1 << 20, JournalLanes: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, key := range []string{"a", "b", "c", "z"} {
		if err := db.Set([]byte(key), []byte("v")); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}
	if err := db.DeleteRange([]byte("a"), []byte("d")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	stats := db.Stats()
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.calls_total"); got != 1 {
		t.Fatalf("calls_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.input_ranges_total"); got != 1 {
		t.Fatalf("input_ranges_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.effective_ranges_total"); got != 1 {
		t.Fatalf("effective_ranges_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.visited_keys_total"); got != 3 {
		t.Fatalf("visited_keys_total=%d want 3", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.tombstone_keys_total"); got != 3 {
		t.Fatalf("tombstone_keys_total=%d want 3", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.iterators_total"); got != 1 {
		t.Fatalf("iterators_total=%d want 1", got)
	}
}

func TestCachingBatchDeleteRangeStatsCoalescingAndMaterialization(t *testing.T) {
	backend := NewMockBackend()
	for _, key := range []string{"a", "b", "c", "d"} {
		backend.Set([]byte(key), []byte("v"))
	}
	db, err := Open(t.TempDir(), backend, Options{FlushThreshold: 1 << 20, JournalLanes: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	defer b.Close()
	for _, bounds := range []struct{ start, end string }{
		{"a", "c"}, // [a,c)
		{"c", "e"}, // adjacent, union [a,e)
		{"b", "d"}, // overlap inside union
		{"a", "c"}, // duplicate range
	} {
		if err := b.DeleteRange([]byte(bounds.start), []byte(bounds.end)); err != nil {
			t.Fatalf("DeleteRange(%s,%s): %v", bounds.start, bounds.end, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	stats := db.Stats()
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.batch_calls_total"); got != 4 {
		t.Fatalf("batch_calls_total=%d want 4", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.batch_writes_total"); got != 1 {
		t.Fatalf("batch_writes_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.input_ranges_total"); got != 4 {
		t.Fatalf("input_ranges_total=%d want 4", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.effective_ranges_total"); got != 1 {
		t.Fatalf("effective_ranges_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.coalesced_ranges_total"); got != 3 {
		t.Fatalf("coalesced_ranges_total=%d want 3", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.visited_keys_total"); got != 4 {
		t.Fatalf("visited_keys_total=%d want 4", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.materialized_keys_total"); got != 4 {
		t.Fatalf("materialized_keys_total=%d want 4", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.materialized_key_bytes_total"); got != 4 {
		t.Fatalf("materialized_key_bytes_total=%d want 4", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.tombstone_keys_total"); got != 4 {
		t.Fatalf("tombstone_keys_total=%d want 4", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.snapshot_iterators_total"); got != 1 {
		t.Fatalf("snapshot_iterators_total=%d want 1", got)
	}
	if got := deleteRangeStatUint64(t, stats, "treedb.cache.delete_range.iterators_total"); got != 1 {
		t.Fatalf("iterators_total=%d want 1", got)
	}
}
