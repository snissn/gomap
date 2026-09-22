package treedb

import (
	"bytes"
	"sort"
	"testing"
)

func rotateCachedWritesIntoQueue(t *testing.T, db *DB) {
	t.Helper()
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}
}

func collectIterator(t *testing.T, it Iterator, reverse bool) map[string][]byte {
	t.Helper()
	defer func() { _ = it.Close() }()
	got := make(map[string][]byte)
	var prev []byte
	for it.Valid() {
		k := append([]byte(nil), it.Key()...)
		v := append([]byte(nil), it.Value()...)
		if reverse {
			if prev != nil && bytes.Compare(prev, k) <= 0 {
				t.Fatalf("reverse iterator out of order: prev=%q cur=%q", string(prev), string(k))
			}
		} else {
			if prev != nil && bytes.Compare(prev, k) >= 0 {
				t.Fatalf("forward iterator out of order: prev=%q cur=%q", string(prev), string(k))
			}
		}
		prev = k
		got[string(k)] = v
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return got
}

func assertModel(t *testing.T, db *DB, model map[string][]byte) {
	t.Helper()
	// Point reads.
	for k, want := range model {
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) mismatch: got=%q want=%q", k, string(got), string(want))
		}
	}

	// Forward iterator.
	fwd, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	gotFwd := collectIterator(t, fwd, false)
	if len(gotFwd) != len(model) {
		t.Fatalf("forward iterator count=%d want=%d", len(gotFwd), len(model))
	}
	for k, want := range model {
		got, ok := gotFwd[k]
		if !ok {
			t.Fatalf("forward iterator missing key=%q", k)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("forward iterator value mismatch key=%q got=%q want=%q", k, string(got), string(want))
		}
	}

	// Reverse iterator.
	rev, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	gotRev := collectIterator(t, rev, true)
	if len(gotRev) != len(model) {
		t.Fatalf("reverse iterator count=%d want=%d", len(gotRev), len(model))
	}
	for k, want := range model {
		got, ok := gotRev[k]
		if !ok {
			t.Fatalf("reverse iterator missing key=%q", k)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reverse iterator value mismatch key=%q got=%q want=%q", k, string(got), string(want))
		}
	}
}

func TestCachedMerge_MultipleFrozenMemtables_NewestWins(t *testing.T) {
	modes := []string{"skiplist", "hash_sorted", "btree", "append_only", "adaptive"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			opts := OptionsFor(ProfileFast, dir)
			opts.FlushThreshold = 256 << 20
			opts.MemtableMode = mode
			opts.MemtableShards = 4
			opts.DisableBackgroundPrune = true
			opts.BackgroundIndexVacuumInterval = -1
			opts.ValueLog.Generational.Policy = ValueLogGenerationOff

			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer db.Close()

			model := make(map[string][]byte)

			// memtable #1
			for _, k := range []string{"a", "b", "c"} {
				v := []byte("v1-" + k)
				if err := db.Set([]byte(k), v); err != nil {
					t.Fatalf("Set(%q): %v", k, err)
				}
				model[k] = append([]byte(nil), v...)
			}
			rotateCachedWritesIntoQueue(t, db)

			// memtable #2 overwrites and deletes.
			if err := db.Set([]byte("b"), []byte("v2-b")); err != nil {
				t.Fatalf("Set(b): %v", err)
			}
			model["b"] = []byte("v2-b")
			if err := db.Delete([]byte("c")); err != nil {
				t.Fatalf("Delete(c): %v", err)
			}
			delete(model, "c")
			if err := db.Set([]byte("d"), []byte("v2-d")); err != nil {
				t.Fatalf("Set(d): %v", err)
			}
			model["d"] = []byte("v2-d")
			rotateCachedWritesIntoQueue(t, db)

			// memtable #3 overwrites again.
			if err := db.Set([]byte("a"), []byte("v3-a")); err != nil {
				t.Fatalf("Set(a): %v", err)
			}
			model["a"] = []byte("v3-a")
			if err := db.Set([]byte("e"), []byte("v3-e")); err != nil {
				t.Fatalf("Set(e): %v", err)
			}
			model["e"] = []byte("v3-e")
			rotateCachedWritesIntoQueue(t, db)

			// Ensure reads/iterators merge queue correctly (no backend yet).
			assertModel(t, db, model)

			// Flush to backend, then create more queued state to ensure
			// queue+backend merge remains correct.
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}

			// memtable #4 overrides backend.
			if err := db.Set([]byte("b"), []byte("v4-b")); err != nil {
				t.Fatalf("Set(b): %v", err)
			}
			model["b"] = []byte("v4-b")
			if err := db.Delete([]byte("d")); err != nil {
				t.Fatalf("Delete(d): %v", err)
			}
			delete(model, "d")
			rotateCachedWritesIntoQueue(t, db)

			assertModel(t, db, model)

			// Reopen should match as well.
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			db2, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer db2.Close()
			assertModel(t, db2, model)

			// Extra sanity: full key set in iterator matches sorted model keys.
			keys := make([]string, 0, len(model))
			for k := range model {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			it, err := db2.Iterator(nil, nil)
			if err != nil {
				t.Fatalf("Iterator: %v", err)
			}
			var gotKeys []string
			for it.Valid() {
				gotKeys = append(gotKeys, string(it.Key()))
				it.Next()
			}
			_ = it.Close()
			if err := it.Error(); err != nil {
				t.Fatalf("Iterator error: %v", err)
			}
			if len(gotKeys) != len(keys) {
				t.Fatalf("keys count=%d want=%d gotKeys=%v wantKeys=%v", len(gotKeys), len(keys), gotKeys, keys)
			}
			for i := range keys {
				if gotKeys[i] != keys[i] {
					t.Fatalf("keys[%d]=%q want %q (got=%v want=%v)", i, gotKeys[i], keys[i], gotKeys, keys)
				}
			}
		})
	}
}
