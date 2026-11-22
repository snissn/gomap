package main

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

type txnPublicStats struct {
	sequence uint64
	nextLSN  uint64
	durable  uint64
	active   uint64
}

func observeTxnPublicStats(t *testing.T, db *treedb.DB) txnPublicStats {
	t.Helper()
	stats := db.Stats()
	parse := func(key string) uint64 {
		raw, ok := stats[key]
		value, err := strconv.ParseUint(raw, 10, 64)
		if !ok || err != nil {
			t.Fatalf("required public stat %q=%q (present=%v): %v", key, raw, ok, err)
		}
		return value
	}
	return txnPublicStats{
		sequence: parse("treedb.cache.cow.txn.logical_seq"),
		nextLSN:  parse("treedb.command_wal.next_lsn"),
		durable:  parse("treedb.command_wal.durable_wal_lsn"),
		active:   parse("treedb.cache.conditional_txn.active"),
	}
}

func requireOneDurableAssignment(t *testing.T, before, after txnPublicStats, active uint64) {
	t.Helper()
	if after.sequence != before.sequence+1 ||
		after.nextLSN != before.nextLSN+1 ||
		after.durable != before.nextLSN ||
		after.durable <= before.durable || after.active != active {
		t.Fatalf("ordinary Commit must publish and durably assign exactly one frame (active=%d): before=%+v after=%+v", active, before, after)
	}
}

func TestTransferConflictAndAudit(t *testing.T) {
	dir := t.TempDir()
	opts := profileOptions(dir)
	// Isolate exact assignment counters from unrelated automatic WAL maintenance
	// in this forced-order control only; sustained runs retain normal settings.
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Errorf("close test DB: %v", err)
			}
		}
	})
	if err := seed(db, 1); err != nil {
		t.Fatal(err)
	}
	held := db.AcquireSnapshot()
	if held == nil {
		t.Fatal("no independent opening snapshot")
	}
	t.Cleanup(func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Errorf("close held snapshot: %v", err)
			}
		}
	})
	debit, credit := pairKeys(0)
	keys := [][]byte{debit, credit}
	initial := make([][]byte, len(keys))
	for i, key := range keys {
		initial[i], err = held.Get(key)
		if err != nil || len(initial[i]) == 0 {
			t.Fatalf("read held seed %q: %x, %v", key, initial[i], err)
		}
	}
	if err := audit(db, []uint64{0}); err != nil {
		t.Fatalf("audit seed: %v", err)
	}

	newTransfer := func() *treedb.ConditionalTxn {
		t.Helper()
		tx, err := db.NewConditionalTxn()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := tx.Close(); err != nil {
				t.Errorf("close test transaction: %v", err)
			}
		})
		if err := stageTransfer(tx, debit, credit); err != nil {
			t.Fatal(err)
		}
		return tx
	}

	winner, stale := newTransfer(), newTransfer()
	before := observeTxnPublicStats(t, db)
	if before.nextLSN == 0 || before.active != 2 {
		t.Fatalf("expected two live transactions with WAL active: %+v", before)
	}
	if err := winner.Commit(); err != nil {
		t.Fatalf("winner Commit: %v", err)
	}
	afterWinner := observeTxnPublicStats(t, db)
	requireOneDurableAssignment(t, before, afterWinner, 1)
	if err := stale.Commit(); err != treedb.ErrConcurrentModification {
		t.Fatalf("stale Commit must be a clean pre-assignment conflict: %v", err)
	}
	afterConflict := observeTxnPublicStats(t, db)
	wantAfterConflict := afterWinner
	wantAfterConflict.active = 0
	if afterConflict != wantAfterConflict {
		t.Fatalf("conflict changed state or leaked handle: want=%+v after=%+v", wantAfterConflict, afterConflict)
	}
	if err := audit(db, []uint64{1}); err != nil {
		t.Fatalf("only winner's transfer should be visible: %v", err)
	}
	if err := audit(db, []uint64{2}); err == nil {
		t.Fatal("audit accepted a receipt that counted the lost transfer")
	}

	retry := newTransfer()
	if err := retry.Commit(); err != nil {
		t.Fatalf("retry from a fresh view: %v", err)
	}
	requireOneDurableAssignment(t, afterConflict, observeTxnPublicStats(t, db), 0)
	if err := audit(db, []uint64{2}); err != nil {
		t.Fatalf("audit after retry: %v", err)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint while original cut is held: %v", err)
	}
	for i, key := range keys {
		got, err := held.Get(key)
		if err != nil || !bytes.Equal(got, initial[i]) {
			t.Fatalf("original cut changed after both commits and checkpoint for %q: want %x got %x, %v", key, initial[i], got, err)
		}
		got, err = db.Get(key)
		if err != nil || bytes.Equal(got, initial[i]) {
			t.Fatalf("current cut did not change for %q: old %x got %x, %v", key, initial[i], got, err)
		}
	}
	if err := held.Close(); err != nil {
		t.Fatalf("release held cut: %v", err)
	}
	held = nil
	if err := db.Close(); err != nil {
		t.Fatalf("checked close: %v", err)
	}
	db = nil
	if err := reopenAudit(dir, []uint64{2}); err != nil {
		t.Fatalf("fresh reopen audit: %v", err)
	}
	if err := reopenAudit(dir, []uint64{1}); err == nil {
		t.Fatal("fresh reopen audit accepted a missing transfer")
	}
}

func TestTransferSustainedFidelity(t *testing.T) {
	const ops = 3
	for _, c := range []struct {
		name    string
		layout  string
		clients int
		pairs   int
	}{
		{name: "one-disjoint", layout: "disjoint", clients: 1, pairs: 1},
		{name: "sixteen-disjoint", layout: "disjoint", clients: 16, pairs: 16},
		{name: "sixteen-hot", layout: "hot", clients: 16, pairs: 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dir := filepath.Join(t.TempDir(), "db")
			result, err := run(ctx, config{
				Dir: dir, Clients: c.clients, PairsPerClient: 1,
				OpsPerClient: ops, MaxAttempts: 256, Layout: c.layout, HoldReader: true,
			})
			if err != nil {
				t.Fatalf("public transfer workload: %v", err)
			}
			expected := make([]uint64, c.pairs)
			if c.layout == "hot" {
				expected[0] = uint64(c.clients * ops)
			} else {
				for i := range expected {
					expected[i] = ops
				}
			}
			if result.Completed != uint64(c.clients*ops) || !slices.Equal(result.Counts, expected) {
				t.Fatalf("wrong complete logical operation receipt: completed=%d counts=%v want=%v", result.Completed, result.Counts, expected)
			}
			if result.CheckpointNS <= 0 || !result.HeldVerified {
				t.Fatalf("missing checked checkpoint/held-reader evidence: checkpoint_ns=%d held_verified=%v", result.CheckpointNS, result.HeldVerified)
			}
			if err := reopenAudit(dir, expected); err != nil {
				t.Fatalf("audit persisted workload: %v", err)
			}
			// Conservation alone would miss a receipt with one spurious success.
			expected[0]++
			if err := reopenAudit(dir, expected); err == nil {
				t.Fatal("reopen audit accepted an extra logical success")
			}
		})
	}
}
