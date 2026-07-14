package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocatorCOWCandidateOwnsAllocationsAndAuxiliaryPages(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	allocator := New(p, 0)
	base := MustNewFreelistGenerationV1(1, 4, nil, nil)
	if err := allocator.EnableCOWV1(base, NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	dataPageID, err := allocator.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	if dataPageID != 4 || p.PageCount() != 5 {
		t.Fatalf("allocated/page-count=(%d,%d), want (4,5)", dataPageID, p.PageCount())
	}
	if err := allocator.RetireCOWV1([]uint64{dataPageID}, 1); err != nil {
		t.Fatal(err)
	}
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryPageStoreV1()
	var candidateID CandidateIDV1
	candidateID[0] = 2
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateID, capability, 2, store)
	if err != nil {
		t.Fatal(err)
	}
	aux := prepared.AuxiliaryPageIDs()
	if len(aux) != 2 || aux[0] != 5 || aux[1] != 6 {
		t.Fatalf("auxiliary pages=%v, want [5 6]", aux)
	}
	if prepared.Candidate().Generation().RetiredCount() != 1 {
		t.Fatalf("retired=%d want 1", prepared.Candidate().Generation().RetiredCount())
	}
	if prepared.Candidate().Generation().HighWater() <= aux[1] {
		t.Fatalf("high-water=%d does not cover auxiliary page %d", prepared.Candidate().Generation().HighWater(), aux[1])
	}
}

func TestAllocatorCOWDoesNotReuseUntilTwoSlotHorizonAdvances(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	allocator := New(p, 0)
	if err := allocator.EnableCOWV1(MustNewFreelistGenerationV1(1, 4, nil, nil), NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	retiredID, err := allocator.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.RetireCOWV1([]uint64{retiredID}, 2); err != nil {
		t.Fatal(err)
	}

	finish := func(commit, oldest uint64) {
		t.Helper()
		capability, err := NewReuseCapability(oldest, oldest, 0)
		if err != nil {
			t.Fatal(err)
		}
		var candidateID CandidateIDV1
		candidateID[0] = byte(commit)
		prepared, err := allocator.PrepareCOWCandidateV1(commit, commit, candidateID, capability, 0, NewMemoryPageStoreV1())
		if err != nil {
			t.Fatal(err)
		}
		if err := allocator.PublishCOWCandidateV1(prepared, capability); err != nil {
			t.Fatal(err)
		}
	}
	finish(2, 1)
	first, err := allocator.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	if first == retiredID {
		t.Fatalf("page %d reused while retained by older durable slot", retiredID)
	}
	finish(3, 2)
	second, err := allocator.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	if second == retiredID {
		t.Fatalf("page %d reused at equal retirement/horizon sequence", retiredID)
	}
	finish(4, 3)
	if !allocator.COWGenerationV1().Allocatable(retiredID) {
		t.Fatalf("page %d did not become allocatable after horizon advance", retiredID)
	}
	beforeHighWater := allocator.COWGenerationV1().HighWater()
	reused, err := allocator.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	if reused >= beforeHighWater {
		t.Fatalf("allocated page=%d at/above high-water %d, want safe old-page reuse", reused, beforeHighWater)
	}
}
