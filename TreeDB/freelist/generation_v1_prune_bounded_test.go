package freelist

import "testing"

func TestBoundedPrune512FreshCapabilityAndWholeWork(t *testing.T) {
	retired := make(map[uint64]uint64, 512)
	for id := uint64(2); id < 514; id++ {
		retired[id] = 7
	}
	base, err := NewFreelistGenerationV1(10, 1024, nil, retired)
	if err != nil {
		t.Fatal(err)
	}
	txn, err := BeginCandidateV1(base, base.GenerationRef(), NewReservationLedger())
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := NewReuseCapability(10, 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	work := txn.PruneWithCapabilityBounded(pinned)
	if work.PromotedPages != 0 || txn.root.freeCount() != 0 {
		t.Fatal("reused held pages")
	}
	cap, err := NewReuseCapability(10, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var examined, promoted, visits uint64
	steps := 0
	for txn.root.retiredCount() != 0 {
		steps++
		if steps > 128 {
			t.Fatal("cursor made no bounded progress")
		}
		w := txn.PruneWithCapabilityBounded(cap)
		if w.NodeVisits > BoundedPruneMaxNodeVisits || w.EntriesExamined > BoundedPruneMaxEntries || w.PromotedPages > BoundedPruneMaxPromotions || w.MutationPaths > 1 || w.MutationItems > BoundedPruneMaxPromotions || w.PageCredits > BoundedPruneMaxPageCredits || w.ByteCredits > BoundedPruneMaxByteCredits {
			t.Fatalf("unbounded step %+v", w)
		}
		examined += w.EntriesExamined
		promoted += w.PromotedPages
		visits += w.NodeVisits
	}
	if promoted != 512 || examined > 768 || visits > uint64(steps)*BoundedPruneMaxNodeVisits {
		t.Fatalf("nonlinear work steps=%d examined=%d promoted=%d visits=%d", steps, examined, promoted, visits)
	}
	if txn.root.freeCount() != 512 {
		t.Fatalf("free=%d", txn.root.freeCount())
	}
	// A stale hint may revisit but cannot grant stale reuse authority.
	txn.pruneCursor = 0
	heldAgain, _ := NewReuseCapability(10, 1, 0)
	if w := txn.PruneWithCapabilityBounded(heldAgain); w.PromotedPages != 0 {
		t.Fatal("cursor overrode capability")
	}
}
