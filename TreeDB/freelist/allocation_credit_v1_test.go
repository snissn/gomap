package freelist

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type loanCredit5105 struct{}

func (*loanCredit5105) ReserveAllocation(uint64) error { return nil }
func (*loanCredit5105) RetainAllocationCredit() error  { return nil }
func (*loanCredit5105) ReleaseAllocationCredit()       {}

type loanWriter5105 func(uint64, CandidatePageViewV1) error

func (writer loanWriter5105) WriteCandidatePageV1(id uint64, view CandidatePageViewV1) error {
	return writer(id, view)
}

func loanCandidate5105() *FreelistCandidateV1 {
	return &FreelistCandidateV1{
		generation: MustNewFreelistGenerationV1(1, 4, nil, nil),
		creator:    &allocationCreditLeaseV1{resident: &loanCredit5105{}, refs: 1},
		pages: []candidatePageV1{
			{PageID: 2, view: CandidatePageViewV1{data: bytes.Repeat([]byte{17}, page.PageSize)}},
			{PageID: 3, view: CandidatePageViewV1{data: bytes.Repeat([]byte{31}, page.PageSize)}},
		},
	}
}

func TestCandidateFiniteGenericWriterRefusedBeforeCallback5105(t *testing.T) {
	candidate := loanCandidate5105()
	calls := 0
	err := candidate.WritePagesToV1(loanWriter5105(func(_ uint64, _ CandidatePageViewV1) error { calls++; panic("finite callback escaped") }))
	if !errors.Is(err, ErrFiniteAllocationExportV1) || calls != 0 {
		t.Fatalf("error=%v calls=%d", err, calls)
	}
	if candidate.Pages() != nil || candidate.Generation() != nil || candidate.DirtyPageIDs() != nil {
		t.Fatal("finite raw output escaped")
	}
}

func TestCandidateFiniteDirectPagerAndOrdinaryRetainingWriter5105(t *testing.T) {
	candidate := loanCandidate5105()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err = p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	if err = candidate.WritePagesToPagerV1(p); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{2, 3} {
		got, err := p.ReadPage(id)
		if err != nil {
			t.Fatal(err)
		}
		want := byte(17)
		if id == 3 {
			want = 31
		}
		if len(got) != page.PageSize || got[0] != want || got[len(got)-1] != want {
			t.Fatalf("page %d bytes changed", id)
		}
	}
	candidate.creator = nil
	var retained CandidatePageViewV1
	err = candidate.WritePagesToV1(loanWriter5105(func(_ uint64, view CandidatePageViewV1) error { retained = view; return errors.New("retaining writer") }))
	if err == nil {
		t.Fatal("writer failure discarded")
	}
	dst := make([]byte, page.PageSize)
	if retained.Len() != page.PageSize || retained.CopyTo(dst) != nil || dst[0] != 17 {
		t.Fatal("ordinary retaining writer lost immutable backing")
	}
}

func TestPublishedGenerationLeaseInstanceCensus5105(t *testing.T) {
	g := MustNewFreelistGenerationV1(1, 4, nil, nil)
	g.ref = GenerationRefV1{GenerationID: 1, HeaderPageID: 2, HighWater: 4}
	a := &Allocator{cow: &allocatorCOWStateV1{generation: g, txn: &FreelistTxn{}}}
	first, err := a.AcquirePublishedGenerationLeaseV1(g.ref)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.AcquirePublishedGenerationLeaseV1(g.ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := a.ResidentGenerationProfileV1(); got.PhysicalCutLeases != 2 || got.DistinctPhysicalCutGenerations != 1 {
		t.Fatalf("same root census=%+v", got)
	}
	newer := MustNewFreelistGenerationV1(2, 4, nil, nil)
	newer.ref = GenerationRefV1{GenerationID: 2, HeaderPageID: 2, HighWater: 4}
	a.cow.generation = newer
	third, err := a.AcquirePublishedGenerationLeaseV1(newer.ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := a.ResidentGenerationProfileV1(); got.DistinctPhysicalCutGenerations != 2 || got.RetainedHighWaterSum != 8 {
		t.Fatalf("same page ID merged backing=%+v", got)
	}
	first.Close()
	first.Close()
	second.Close()
	if _, err := first.SnapshotPageUnusedV1(2, 1); !errors.Is(err, ErrFiniteAllocationExportV1) {
		t.Fatal("closed lease readable")
	}
	if got := a.ResidentGenerationProfileV1(); got.PhysicalCutLeases != 1 || got.DistinctPhysicalCutGenerations != 1 {
		t.Fatalf("close census=%+v", got)
	}
	third.Close()
	if got := a.ResidentGenerationProfileV1(); got.PhysicalCutLeases != 0 {
		t.Fatalf("final census=%+v", got)
	}
}

func TestFiniteTerminalBackingAndAmbiguousRetention5105(t *testing.T) {
	candidate := loanCandidate5105()
	prepared := &PreparedCOWCandidateV1{candidate: candidate, auxiliary: []uint64{2}, activated: true}
	if !errors.Is(prepared.ClearTerminalBackingV1(), ErrCandidateConsumed) || len(candidate.pages) != 2 {
		t.Fatal("visible/ambiguous backing released")
	}
	prepared.published = true
	if err := prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	if prepared.candidate != nil || prepared.auxiliary != nil || candidate.generation != nil || candidate.pages != nil || candidate.dirtyIDs != nil || candidate.creator != nil {
		t.Fatal("terminal backing retained")
	}
}

func TestCandidateGettersSynchronizeTerminalClear5105(t *testing.T) {
	candidate := loanCandidate5105()
	prepared := &PreparedCOWCandidateV1{candidate: candidate, published: true}
	done := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		defer close(done)
		for i := 0; i < 1000; i++ {
			candidate.Generation()
			candidate.GenerationRef()
			candidate.DirtyPageIDs()
			candidate.ReservationRecord()
			candidate.Pages()
			candidate.PageCount()
		}
	}()
	<-started
	if err := prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	<-done
	if candidate.Generation() != nil || candidate.GenerationRef() != (GenerationRefV1{}) || candidate.PageCount() != 0 {
		t.Fatal("released candidate did not return safe scalar state")
	}
}
