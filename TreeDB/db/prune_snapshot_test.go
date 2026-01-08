package db

import (
	"testing"
)

func TestPrune_RespectsOpenSnapshots(t *testing.T) {
	// This test verifies that background pruning correctly identifies pages
	// that are still referenced by open snapshots and delays their reclamation.
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.KeepRecent = 1
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Initial write
	key := []byte("k")
	if err := db.SetSync(key, []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// 2. Open snapshot pinned to Seq 1
	snap := db.AcquireSnapshot()
	if snap.state.CommitSeq != 1 {
		t.Fatalf("expected snap seq 1, got %d", snap.state.CommitSeq)
	}

	// 3. Overwrite key to retire pages from Seq 1
	if err := db.SetSync(key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	// Seq 2 committed. Pages from Seq 1 are now in the graveyard.

	// 4. Trigger explicit prune
	db.Prune()

	// 5. Verify pages from Seq 1 were NOT reclaimed because of the snapshot
	rep, _ := db.FragmentationReport()
	if head := rep["treedb.freelist.head"]; head != "" && head != "0" {
		t.Errorf("Expected 0 freelist head while snapshot open, got %s", head)
	}

	// 6. Close snapshot
	snap.Close()

	// 6.5. Perform another write to advance currentSeq
	if err := db.SetSync([]byte("k2"), []byte("v3")); err != nil {
		t.Fatal(err)
	}
	// currentSeq is now 3. Seq 1 is now < (3 - 1) = 2. Safe to free.

	// 7. Trigger prune again
	db.Prune()

	// 8. Verify pages WERE reclaimed (moved from graveyard to freelist)
	rep2, _ := db.FragmentationReport()
	if head := rep2["treedb.freelist.head"]; head == "" || head == "0" {
		t.Errorf("Expected non-zero freelist head after prune, got %s", head)
	}
}
