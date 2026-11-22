package treedb

import (
	"errors"
	"runtime"
	"testing"
)

func cowTxnBeginWriterOpen(t *testing.T, profile Profile, mode, directory string) *DB {
	t.Helper()
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := OptionsFor(profile, directory)
	opts.MemtableMode = mode
	opts.MemtableShards = 2
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("open %s/%s: %v", profile, mode, err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func TestPublicCOWTxnBeginWriterAllocations(t *testing.T) {
	// Track the exact Linux/amd64 Go1.26.11 ABI used to remove the
	// allocation. The owner/ACK/reopen test below runs on every platform.
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || runtime.Version() != "go1.26.11" {
		t.Skip("allocation ceilings are scoped to Go 1.26.11 linux/amd64")
	}
	for _, tc := range []struct {
		name     string
		profile  Profile
		mode     string
		ceilings [3]float64
	}{
		{"durable-cow-btree", ProfileCommandWALDurable, "cow_btree", [3]float64{9, 8, 8}},
		{"durable-cow-ordered", ProfileCommandWALDurable, "cow_ordered", [3]float64{9, 8, 8}},
		{"no-wal-cow-btree", ProfileNoWALFast, "cow_btree", [3]float64{9, 8, 8}},
		{"no-wal-adaptive", ProfileNoWALFast, "adaptive", [3]float64{2, 1, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := cowTxnBeginWriterOpen(t, tc.profile, tc.mode, t.TempDir())
			var reused ConditionalTxn
			closeTxn := func(tx *ConditionalTxn, err error) {
				if err != nil {
					t.Fatalf("begin: %v", err)
				}
				if err := tx.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
			}
			for i, shape := range []struct {
				name string
				run  func()
			}{
				{"fresh", func() { closeTxn(db.NewConditionalTxn()) }},
				{"reused", func() { closeTxn(&reused, db.InitConditionalTxn(&reused)) }},
				{"reused-with-snapshot", func() { closeTxn(&reused, db.InitConditionalTxnWithSnapshot(&reused)) }},
			} {
				t.Run(shape.name, func(t *testing.T) {
					ceiling := tc.ceilings[i]
					for repeat := 0; repeat < 3; repeat++ {
						allocs := testing.AllocsPerRun(128, shape.run)
						t.Logf("public warm Begin/Close: %.0f allocations per cycle", allocs)
						if allocs > ceiling {
							t.Errorf("public warm Begin/Close allocated %.0f per cycle, want at most %.0f", allocs, ceiling)
						}
					}
				})
			}
		})
	}
}

func TestPublicCOWTxnBeginWriterOwnerAndReopen(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile Profile
		mode    string
	}{
		{"durable-cow-btree", ProfileCommandWALDurable, "cow_btree"},
		{"durable-cow-ordered", ProfileCommandWALDurable, "cow_ordered"},
		{"no-wal-cow-btree", ProfileNoWALFast, "cow_btree"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leftDir, rightDir := t.TempDir(), t.TempDir()
			left := cowTxnBeginWriterOpen(t, tc.profile, tc.mode, leftDir)
			right := cowTxnBeginWriterOpen(t, tc.profile, tc.mode, rightDir)
			key := []byte("origin")
			for _, row := range []struct {
				db   *DB
				seed string
			}{{left, "left"}, {right, "right"}} {
				if err := row.db.SetSync(key, []byte(row.seed)); err != nil {
					t.Fatalf("seed %q: %v", row.seed, err)
				}
			}

			// Intentionally reuse caller storage across different DB owners.
			var tx ConditionalTxn
			if err := left.InitConditionalTxn(&tx); err != nil {
				t.Fatal(err)
			}
			old := tx.Snapshot()
			if old == nil {
				t.Fatal("ordinary public COW Init lost its existing snapshot")
			}
			oldRevision := cowPublicContractSnapshotValue(t, old, "origin", []byte("left"), true)
			if err := tx.Set([]byte("left-only"), []byte("left-value")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("ordinary left Commit: %v", err)
			}
			if err := right.InitConditionalTxnWithSnapshot(&tx); err != nil {
				t.Fatal(err)
			}
			if _, _, err := old.GetVersioned(key); !errors.Is(err, ErrConditionalTxnClosed) {
				t.Fatalf("reusing storage revived left view: %v", err)
			}
			snap := tx.Snapshot()
			if snap == nil {
				t.Fatal("right WithSnapshot lost its opening view")
			}
			cowPublicContractSnapshotValue(t, snap, "origin", []byte("right"), true)
			if err := tx.Set([]byte("right-only"), []byte("right-value")); err != nil {
				t.Fatal(err)
			}
			if err := tx.CommitSync(); err != nil {
				t.Fatalf("right CommitSync: %v", err)
			}

			for _, row := range []struct {
				database          *DB
				dir, own, missing string
			}{{left, leftDir, "left", "right"}, {right, rightDir, "right", "left"}} {
				if err := row.database.Close(); err != nil {
					t.Fatalf("close %s: %v", row.own, err)
				}
				if err := row.database.InitConditionalTxn(&tx); !errors.Is(err, ErrClosed) {
					t.Fatalf("closed %s accepted transaction: %v", row.own, err)
				}
				reopened := cowTxnBeginWriterOpen(t, tc.profile, tc.mode, row.dir)
				view := reopened.AcquireSnapshot()
				if view == nil {
					t.Fatal("fresh reopen snapshot missing")
				}
				if rev := cowPublicContractSnapshotValue(t, view, "origin", []byte(row.own), true); row.own == "left" && rev != oldRevision {
					t.Fatalf("reopen lost original revision %d vs %d", rev, oldRevision)
				}
				cowPublicContractSnapshotValue(t, view, row.own+"-only", []byte(row.own+"-value"), true)
				cowPublicContractSnapshotValue(t, view, row.missing+"-only", nil, false)
				if err := view.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
