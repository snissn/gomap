package treedb

import (
	"strconv"
	"testing"
)

func issue2704StatUint64(t *testing.T, stats map[string]string, key string) uint64 {
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

func TestDeleteRangeIssue2704BatchBoundsCoalescingValueLogPointersReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	seed := map[string]string{
		"":  "empty-pointer-value",
		"a": "left-pointer-value",
		"b": "delete-b-pointer-value",
		"c": "delete-c-pointer-value",
		"d": "replace-d-pointer-value",
		"e": "delete-e-pointer-value",
		"z": "right-pointer-value",
	}
	for key, value := range seed {
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			_ = db.Close()
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	b := db.NewBatch()
	if err := b.DeleteRange(nil, []byte{}); err != nil { // empty lower-unbounded range: no-op
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("empty DeleteRange: %v", err)
	}
	for _, bounds := range []struct{ start, end string }{
		{"b", "d"}, // initial range
		{"d", "e"}, // adjacent to the previous range
		{"c", "z"}, // overlapping extension
		{"b", "d"}, // duplicate inside the union
	} {
		if err := b.DeleteRange([]byte(bounds.start), []byte(bounds.end)); err != nil {
			_ = b.Close()
			_ = db.Close()
			t.Fatalf("DeleteRange(%s,%s): %v", bounds.start, bounds.end, err)
		}
	}
	if err := b.Set([]byte("d"), []byte("after-range-pointer-value")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("Set(d after range): %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	_ = b.Close()

	stats := db.Stats()
	if got := issue2704StatUint64(t, stats, "treedb.cache.delete_range.batch_calls_total"); got != 4 {
		_ = db.Close()
		t.Fatalf("batch_calls_total=%d want 4", got)
	}
	if got := issue2704StatUint64(t, stats, "treedb.cache.delete_range.input_ranges_total"); got != 4 {
		_ = db.Close()
		t.Fatalf("input_ranges_total=%d want 4", got)
	}
	if got := issue2704StatUint64(t, stats, "treedb.cache.delete_range.effective_ranges_total"); got != 1 {
		_ = db.Close()
		t.Fatalf("effective_ranges_total=%d want 1", got)
	}
	if got := issue2704StatUint64(t, stats, "treedb.cache.delete_range.coalesced_ranges_total"); got != 3 {
		_ = db.Close()
		t.Fatalf("coalesced_ranges_total=%d want 3", got)
	}

	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()

	requireRawKVValue(t, reopen, []byte{}, []byte("empty-pointer-value"))
	requireRawKVValue(t, reopen, []byte("a"), []byte("left-pointer-value"))
	requireRawKVValue(t, reopen, []byte("d"), []byte("after-range-pointer-value"))
	requireRawKVValue(t, reopen, []byte("z"), []byte("right-pointer-value"))
	for _, key := range []string{"b", "c", "e"} {
		has, err := reopen.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil after reopen", key, has, err)
		}
	}
}
