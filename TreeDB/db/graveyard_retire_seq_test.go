package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestPruneReclaimsRetiredPagesAfterKeepRecentWindow(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                    dir,
		Durability:             DurabilityWALOffRelaxed,
		KeepRecent:             1,
		DisableBackgroundPrune: true, // deterministic: prune on commit path
		PreferAppendAlloc:      true, // keep freed pages visible in the freelist
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	// Insert enough keys to ensure multiple pages are created/retired across commits.
	val1 := bytes.Repeat([]byte("a"), 32)
	val2 := bytes.Repeat([]byte("b"), 32)

	const keys = 5000

	commit := func(val []byte) {
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			var k [8]byte
			k[0] = byte(i >> 24)
			k[1] = byte(i >> 16)
			k[2] = byte(i >> 8)
			k[3] = byte(i)
			if err := b.Set(k[:], val); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		// Keep the two durable meta slots advancing deterministically. Relaxed
		// writes may otherwise coalesce seq 1 and seq 2 into a single publication,
		// legitimately leaving the redundant recovery slot at seq 0.
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	commit(val1) // seq 1

	commit(val2) // seq 2; retires pages last reachable at seq 1

	rep2, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(seq2): %v", err)
	}
	freeIDs2 := uint64(0)
	if v := rep2["treedb.freelist.free_ids"]; v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("parse freelist.free_ids(seq2): %v", err)
		}
		freeIDs2 = parsed
	}

	// A third commit advances the safe history window. With KeepRecent=1 and no
	// readers, pages retired at seq 1 should become eligible for pruning.
	{
		b := d.NewBatch().(*Batch)
		var k [8]byte
		if err := b.Set(k[:], val1); err != nil {
			t.Fatalf("set seq3: %v", err)
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write seq3: %v", err)
		}
		_ = b.Close()
	}
	// Visibility can lead the redundant-meta durability frontier. Reclamation
	// remains fenced until the new root is durable.
	d.Prune()
	visibleOnly, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(visible seq3): %v", err)
	}
	visibleFreeIDs, err := strconv.ParseUint(visibleOnly["treedb.freelist.free_ids"], 10, 64)
	if err != nil {
		t.Fatalf("parse freelist.free_ids(visible seq3): %v", err)
	}
	if visibleFreeIDs != freeIDs2 {
		t.Fatalf("visible-only seq3 reclaimed pages before redundant-meta coverage: before=%d after=%d", freeIDs2, visibleFreeIDs)
	}

	// Once the new root is durable, request pruning explicitly for this
	// deterministic background-prune-disabled test.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seq3: %v", err)
	}
	d.Prune()

	rep3, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(seq3): %v", err)
	}
	if rep3["treedb.freelist.head"] == "" {
		t.Fatalf("missing treedb.freelist.head; rep=%v", rep3)
	}
	if rep3["treedb.freelist.head"] == "0" {
		t.Fatalf("expected freelist head to be non-zero after seq3; rep=%v", rep3)
	}
	freeIDsStr := rep3["treedb.freelist.free_ids"]
	if freeIDsStr == "" {
		t.Fatalf("missing treedb.freelist.free_ids; rep=%v", rep3)
	}
	freeIDs3, err := strconv.ParseUint(freeIDsStr, 10, 64)
	if err != nil {
		t.Fatalf("parse freelist.free_ids: %v", err)
	}
	if freeIDs3 <= freeIDs2 {
		t.Fatalf("expected freelist free_ids to increase after seq3 (seq1-retired pages should become reclaimable); before=%d after=%d rep2=%v rep3=%v", freeIDs2, freeIDs3, rep2, rep3)
	}
}
