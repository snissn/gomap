package caching_test

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Regression: route mode must preserve visibility parity through repeated
// separate WriteSync delete+reinsert churn on overlapping key ranges.
func TestRegression_RouteMode_DeleteReinsertChurnParityAfterReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.IndexOuterLeafMode = treedb.IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.OuterLeafBlockTargetBytes = 512
	if opts.MemtableMode == "" || opts.MemtableMode == "adaptive" {
		opts.MemtableMode = "adaptive:hash_sorted"
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	flush := func(b treedb.Batch, label string) {
		t.Helper()
		if err := b.WriteSync(); err != nil {
			_ = b.Close()
			t.Fatalf("%s writesync: %v", label, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("%s close: %v", label, err)
		}
	}

	expected := make(map[string][]byte, 2800)

	seed := db.NewBatch()
	for i := 0; i < 2400; i++ {
		k := routeParityWideKey(i)
		v := routeParityWideVal('s', i)
		if err := seed.Set([]byte(k), v); err != nil {
			_ = seed.Close()
			t.Fatalf("seed set %q: %v", k, err)
		}
		expected[k] = append([]byte(nil), v...)
	}
	flush(seed, "seed")

	const cycles = 24
	const window = 128
	for cycle := 0; cycle < cycles; cycle++ {
		start := (cycle * 47) % (2400 - window)
		delBatch := db.NewBatch()
		for i := start; i < start+window; i++ {
			k := routeParityWideKey(i)
			if err := delBatch.Delete([]byte(k)); err != nil {
				_ = delBatch.Close()
				t.Fatalf("cycle=%d delete %q: %v", cycle, k, err)
			}
			delete(expected, k)
		}
		flush(delBatch, "delete-cycle")

		setBatch := db.NewBatch()
		for i := start; i < start+window; i++ {
			k := routeParityWideKey(i)
			v := routeParityWideVal(byte('a'+(cycle%20)), i)
			if err := setBatch.Set([]byte(k), v); err != nil {
				_ = setBatch.Close()
				t.Fatalf("cycle=%d set %q: %v", cycle, k, err)
			}
			expected[k] = append([]byte(nil), v...)
		}
		flush(setBatch, "set-cycle")

		if cycle%6 == 5 {
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("cycle=%d checkpoint: %v", cycle, err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("cycle=%d close before reopen: %v", cycle, err)
			}
			db, err = treedb.Open(opts)
			if err != nil {
				t.Fatalf("cycle=%d reopen: %v", cycle, err)
			}
			assertRouteParityState(t, db, expected)
		}
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("final checkpoint: %v", err)
	}
	assertRouteParityState(t, db, expected)

	if err := db.Close(); err != nil {
		t.Fatalf("final close before reopen: %v", err)
	}
	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}

	assertRouteParityState(t, db, expected)
}
