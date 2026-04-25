package treedb

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func bulkRestoreValue(phase byte, i int) []byte {
	// Produce deterministic, non-empty values with small length variation to
	// exercise both leaf packing and value-log pointer plumbing.
	n := 64 + (i % 37)
	out := make([]byte, n)
	for j := range out {
		out[j] = byte(i>>uint(j%8)) ^ phase ^ byte(j)
	}
	return out
}

func bulkRestoreKey(i int) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(i))
	return k
}

func runBulkRestoreMaintainsKeyValueParity(t *testing.T, profile Profile) {
	t.Helper()

	opts := OptionsFor(profile, t.TempDir())
	opts.FlushThreshold = 1 << 20 // 1MiB to force frequent rotations/flushes.
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	opts.BackgroundIndexVacuumInterval = -1
	// Keep pruning deterministic for tests (no background worker).
	opts.DisableBackgroundPrune = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	const (
		keys      = 50_000
		batchSize = 2_000
	)

	live := make([][]byte, keys)
	var b Batch
	defer func() {
		if b != nil {
			_ = b.Close()
		}
	}()

	newBatch := func() {
		t.Helper()
		if b != nil {
			_ = b.Close()
			b = nil
		}
		b = db.NewBatch()
		if b == nil {
			t.Fatalf("NewBatch returned nil")
		}
	}

	commitBatch := func(createNew bool) {
		t.Helper()
		if b == nil {
			return
		}
		if err := b.Write(); err != nil {
			t.Fatalf("batch write: %v", err)
		}
		_ = b.Close()
		b = nil
		if createNew {
			newBatch()
		}
	}

	// Phase 1: restore-like strictly increasing keys.
	newBatch()
	for i := 0; i < keys; i++ {
		key := bulkRestoreKey(i)
		val := bulkRestoreValue(1, i)
		live[i] = val
		if err := b.Set(key, val); err != nil {
			t.Fatalf("set i=%d: %v", i, err)
		}
		if (i+1)%batchSize == 0 {
			commitBatch(i+1 < keys)
		}
	}
	commitBatch(false)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after restore: %v", err)
	}

	// Phase 2: overwrite/delete churn similar to IAVL updates after restore.
	newBatch()
	for i := 0; i < keys; i++ {
		key := bulkRestoreKey(i)
		switch {
		case i%7 == 0:
			if err := b.Delete(key); err != nil {
				t.Fatalf("delete i=%d: %v", i, err)
			}
			live[i] = nil
		case i%3 == 0:
			val := bulkRestoreValue(2, i)
			if err := b.Set(key, val); err != nil {
				t.Fatalf("overwrite i=%d: %v", i, err)
			}
			live[i] = val
		}
		if (i+1)%batchSize == 0 {
			commitBatch(i+1 < keys)
		}
	}
	commitBatch(false)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after churn: %v", err)
	}

	// Verify point reads.
	for i := 0; i < keys; i++ {
		got, gerr := db.Get(bulkRestoreKey(i))
		if gerr != nil {
			t.Fatalf("get i=%d: %v", i, gerr)
		}
		want := live[i]
		if want == nil {
			if got != nil {
				t.Fatalf("get i=%d: got value len=%d, want missing", i, len(got))
			}
			continue
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get i=%d: value mismatch got_len=%d want_len=%d", i, len(got), len(want))
		}
	}

	// Verify full forward scan order and content.
	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	idx := 0
	for it.Valid() {
		key := it.KeyCopy(nil)
		if len(key) != 8 {
			t.Fatalf("iterator key len=%d, want 8", len(key))
		}
		i := int(binary.BigEndian.Uint64(key))
		for idx < keys && live[idx] == nil {
			idx++
		}
		if idx >= keys {
			t.Fatalf("iterator returned extra key=%d", i)
		}
		if i != idx {
			t.Fatalf("iterator key mismatch got=%d want=%d", i, idx)
		}
		want := live[i]
		if want == nil {
			t.Fatalf("iterator returned deleted key=%d", i)
		}
		if got := it.Value(); !bytes.Equal(got, want) {
			t.Fatalf("iterator value mismatch key=%d got_len=%d want_len=%d", i, len(got), len(want))
		}
		idx++
		it.Next()
	}
	if ierr := it.Error(); ierr != nil {
		_ = it.Close()
		t.Fatalf("iterator error: %v", ierr)
	}
	if cerr := it.Close(); cerr != nil {
		t.Fatalf("iterator close: %v", cerr)
	}
	for idx < keys && live[idx] == nil {
		idx++
	}
	if idx != keys {
		t.Fatalf("iterator ended early at idx=%d of %d", idx, keys)
	}

	// Verify full reverse scan order and content.
	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	idx = keys - 1
	for rit.Valid() {
		key := rit.KeyCopy(nil)
		if len(key) != 8 {
			t.Fatalf("reverse iterator key len=%d, want 8", len(key))
		}
		i := int(binary.BigEndian.Uint64(key))
		for idx >= 0 && live[idx] == nil {
			idx--
		}
		if idx < 0 {
			t.Fatalf("reverse iterator returned extra key=%d", i)
		}
		if i != idx {
			t.Fatalf("reverse iterator key mismatch got=%d want=%d", i, idx)
		}
		want := live[i]
		if want == nil {
			t.Fatalf("reverse iterator returned deleted key=%d", i)
		}
		if got := rit.Value(); !bytes.Equal(got, want) {
			t.Fatalf("reverse iterator value mismatch key=%d got_len=%d want_len=%d", i, len(got), len(want))
		}
		idx--
		rit.Next()
	}
	if ierr := rit.Error(); ierr != nil {
		_ = rit.Close()
		t.Fatalf("reverse iterator error: %v", ierr)
	}
	if cerr := rit.Close(); cerr != nil {
		t.Fatalf("reverse iterator close: %v", cerr)
	}
	for idx >= 0 && live[idx] == nil {
		idx--
	}
	if idx != -1 {
		t.Fatalf("reverse iterator ended early at idx=%d", idx)
	}

	// Reopen parity: all values must remain readable after close/open.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db = nil
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	for i := 0; i < keys; i++ {
		got, gerr := db.Get(bulkRestoreKey(i))
		if gerr != nil {
			t.Fatalf("reopen get i=%d: %v", i, gerr)
		}
		want := live[i]
		if want == nil {
			if got != nil {
				t.Fatalf("reopen get i=%d: got value len=%d, want missing", i, len(got))
			}
			continue
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen get i=%d: value mismatch got_len=%d want_len=%d", i, len(got), len(want))
		}
	}
}

func TestProfileFast_BulkRestoreMaintainsKeyValueParity(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileWALOnFast} {
		profile := profile
		t.Run(string(profile), func(t *testing.T) {
			runBulkRestoreMaintainsKeyValueParity(t, profile)
		})
	}
}

func TestProfileFast_BulkRestoreRemainsReadableBeforeCheckpointBarrier(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileWALOnFast} {
		profile := profile
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.FlushThreshold = 1 << 20
			opts.ValueLog.Generational.Policy = ValueLogGenerationOff
			opts.BackgroundIndexVacuumInterval = -1
			opts.DisableBackgroundPrune = true

			db, err := Open(opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()

			const (
				keys      = 50_000
				batchSize = 2_000
			)

			live := make([][]byte, keys)
			b := db.NewBatch()
			if b == nil {
				t.Fatal("NewBatch returned nil")
			}
			defer func() {
				if b != nil {
					_ = b.Close()
				}
			}()

			commitBatch := func(createNew bool) {
				t.Helper()
				if b == nil {
					return
				}
				if err := b.Write(); err != nil {
					t.Fatalf("batch write: %v", err)
				}
				_ = b.Close()
				b = nil
				if createNew {
					b = db.NewBatch()
					if b == nil {
						t.Fatal("NewBatch returned nil")
					}
				}
			}

			for i := 0; i < keys; i++ {
				key := bulkRestoreKey(i)
				val := bulkRestoreValue(1, i)
				live[i] = val
				if err := b.Set(key, val); err != nil {
					t.Fatalf("set i=%d: %v", i, err)
				}
				if (i+1)%batchSize == 0 {
					commitBatch(i+1 < keys)
				}
			}
			commitBatch(false)

			for i := 0; i < keys; i += 257 {
				got, err := db.Get(bulkRestoreKey(i))
				if err != nil {
					t.Fatalf("post-restore immediate get i=%d: %v", i, err)
				}
				if !bytes.Equal(got, live[i]) {
					t.Fatalf("post-restore immediate get i=%d mismatch got_len=%d want_len=%d", i, len(got), len(live[i]))
				}
			}

			churn := db.NewBatch()
			if churn == nil {
				t.Fatal("NewBatch returned nil for churn")
			}
			defer func() {
				if churn != nil {
					_ = churn.Close()
				}
			}()
			for i := 0; i < keys; i++ {
				key := bulkRestoreKey(i)
				switch {
				case i%7 == 0:
					if err := churn.Delete(key); err != nil {
						t.Fatalf("delete i=%d: %v", i, err)
					}
					live[i] = nil
				case i%3 == 0:
					val := bulkRestoreValue(2, i)
					if err := churn.Set(key, val); err != nil {
						t.Fatalf("overwrite i=%d: %v", i, err)
					}
					live[i] = val
				}
				if (i+1)%batchSize == 0 {
					if err := churn.Write(); err != nil {
						t.Fatalf("churn write i=%d: %v", i, err)
					}
					_ = churn.Close()
					churn = nil
					if i+1 < keys {
						churn = db.NewBatch()
						if churn == nil {
							t.Fatal("NewBatch returned nil for churn rotation")
						}
					}
				}
			}
			if churn != nil {
				if err := churn.Write(); err != nil {
					t.Fatalf("final churn write: %v", err)
				}
				_ = churn.Close()
				churn = nil
			}

			for i := 0; i < keys; i += 257 {
				got, err := db.Get(bulkRestoreKey(i))
				if err != nil {
					t.Fatalf("post-churn pre-checkpoint get i=%d: %v", i, err)
				}
				want := live[i]
				if want == nil {
					if got != nil {
						t.Fatalf("post-churn pre-checkpoint get i=%d got value len=%d want missing", i, len(got))
					}
					continue
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("post-churn pre-checkpoint get i=%d mismatch got_len=%d want_len=%d", i, len(got), len(want))
				}
			}
		})
	}
}
