package freelist

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/pager"
)

func materializeAllocatorCOWCandidateForTest(t *testing.T, p *pager.Pager, prepared *PreparedCOWCandidateV1) {
	t.Helper()
	if prepared == nil || prepared.Candidate() == nil || prepared.Candidate().Generation() == nil {
		t.Fatal("missing prepared COW candidate")
	}
	if target := prepared.Candidate().Generation().HighWater(); p.PageCount() < target {
		if err := p.Truncate(target); err != nil {
			t.Fatal(err)
		}
	}
	for _, image := range prepared.Candidate().Pages() {
		if err := p.Write(image.PageID, image.Data); err != nil {
			t.Fatalf("write candidate page %d: %v", image.PageID, err)
		}
	}
}

func TestAllocatorFailedCOWCandidateWakesBlockedAllocation(t *testing.T) {
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
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	var candidateID CandidateIDV1
	candidateID[0] = 2
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateID, capability, 0, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}

	waiting := make(chan struct{})
	var waitingOnce sync.Once
	TestHookCOWWaitBeforeSleep = func() { waitingOnce.Do(func() { close(waiting) }) }
	defer func() { TestHookCOWWaitBeforeSleep = nil }()
	allocDone := make(chan error, 1)
	go func() {
		_, err := allocator.Alloc(0)
		allocDone <- err
	}()
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("allocation did not wait behind the prepared COW candidate")
	}

	want := errors.New("durable-root publication failed")
	if err := allocator.FailCOWCandidateV1(prepared, want); err != nil {
		t.Fatal(err)
	}
	if err := allocator.FailCOWCandidateV1(prepared, want); err != nil {
		t.Fatalf("idempotent candidate failure: %v", err)
	}
	select {
	case err := <-allocDone:
		if !errors.Is(err, want) {
			t.Fatalf("blocked allocation error=%v, want %v", err, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed COW candidate did not wake blocked allocation")
	}
	if err := allocator.PublishCOWCandidateV1(prepared, capability); !errors.Is(err, want) {
		t.Fatalf("publish failed candidate error=%v, want retained failure %v", err, want)
	}
}

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

func TestAllocatorCOWCandidateMustBeMaterializedBeforePublish(t *testing.T) {
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
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateIDFromString("materialize-before-publish"), capability, 1, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.PublishCOWCandidateV1(prepared, capability); !errors.Is(err, ErrGenerationFormat) {
		t.Fatalf("publish unmaterialized candidate error=%v, want ErrGenerationFormat", err)
	}
	materializeAllocatorCOWCandidateForTest(t, p, prepared)
	if err := allocator.PublishCOWCandidateV1(prepared, capability); err != nil {
		t.Fatalf("publish materialized candidate: %v", err)
	}
}

func TestAllocatorActivatedCOWGenerationsBuildAheadOfDurablePublication(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	ledger := NewReservationLedger()
	allocator := New(p, 0)
	if err := allocator.EnableCOWV1(MustNewFreelistGenerationV1(1, 4, nil, nil), ledger); err != nil {
		t.Fatal(err)
	}
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}

	firstData, err := allocator.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	first, err := allocator.PrepareCOWCandidateV1(2, 2, candidateIDFromString("visible-first"), capability, 0, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.ActivateCOWCandidateV1(first); err != nil {
		t.Fatalf("activate first: %v", err)
	}

	secondData, err := allocator.Alloc(0)
	if err != nil {
		t.Fatalf("allocation behind visible undurable generation: %v", err)
	}
	if secondData <= firstData {
		t.Fatalf("second data page=%d want above first=%d", secondData, firstData)
	}
	second, err := allocator.PrepareCOWCandidateV1(3, 3, candidateIDFromString("visible-second"), capability, 0, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.ActivateCOWCandidateV1(second); err != nil {
		t.Fatalf("activate second: %v", err)
	}

	materializeAllocatorCOWCandidateForTest(t, p, first)
	materializeAllocatorCOWCandidateForTest(t, p, second)
	for name, prefix := range map[string][]*PreparedCOWCandidateV1{
		"missing first": {second},
		"reordered":     {second, first},
		"extra":         {first, second, &PreparedCOWCandidateV1{}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := allocator.PublishActivatedCOWPrefixV1(prefix, capability); err == nil {
				t.Fatal("malformed activated prefix unexpectedly published")
			}
			if first.published || second.published || len(allocator.cow.activated) != 2 {
				t.Fatalf("malformed prefix mutated allocator: first=%v second=%v debt=%d", first.published, second.published, len(allocator.cow.activated))
			}
		})
	}
	// The second materialized generation is a legitimate physical tail beyond
	// the first prefix because the live transaction owns its higher high-water.
	firstHighWater := first.Candidate().Generation().HighWater()
	physicalPages := p.PageCount()
	liveHighWater := allocator.cow.txn.highWater
	if !(firstHighWater < physicalPages && physicalPages <= liveHighWater) {
		t.Fatalf("build-ahead tail invariant: first high-water=%d physical pages=%d live high-water=%d", firstHighWater, physicalPages, liveHighWater)
	}
	if err := allocator.PublishActivatedCOWPrefixV1([]*PreparedCOWCandidateV1{first}, capability); err != nil {
		t.Fatalf("publish first activated prefix with tracked newer tail: %v", err)
	}
	if !first.published || second.published || len(allocator.cow.activated) != 1 {
		t.Fatalf("first prefix publication state: first=%v second=%v debt=%d", first.published, second.published, len(allocator.cow.activated))
	}
	if err := allocator.PublishActivatedCOWPrefixV1([]*PreparedCOWCandidateV1{second}, capability); err != nil {
		t.Fatalf("publish second activated prefix: %v", err)
	}
	if !first.published || !second.published {
		t.Fatalf("published flags first=%v second=%v", first.published, second.published)
	}
	if got := len(allocator.cow.activated); got != 0 {
		t.Fatalf("activated debt=%d want 0", got)
	}
}

func TestAllocatorActivatedCOWPrefixRejectsUntrackedPhysicalTail(t *testing.T) {
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
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := allocator.PrepareCOWCandidateV1(
		2, 2, candidateIDFromString("untracked-physical-tail"), capability, 1, NewMemoryPageStoreV1(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.ActivateCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}
	materializeAllocatorCOWCandidateForTest(t, p, prepared)
	if err := p.Truncate(p.PageCount() + 1); err != nil {
		t.Fatal(err)
	}
	if err := allocator.PublishActivatedCOWPrefixV1([]*PreparedCOWCandidateV1{prepared}, capability); !errors.Is(err, ErrGenerationFormat) {
		t.Fatalf("publish with untracked physical tail error=%v, want ErrGenerationFormat", err)
	}
	if prepared.published || len(allocator.cow.activated) != 1 {
		t.Fatalf("untracked-tail rejection mutated allocator: published=%v debt=%d", prepared.published, len(allocator.cow.activated))
	}
}

func TestAllocatorActivatedCOWFailurePoisonsAndWakesAllocation(t *testing.T) {
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
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateIDFromString("visible-failure"), capability, 0, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.ActivateCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}
	want := errors.New("visible durable-root publication failed")
	if err := allocator.FailCOWCandidateV1(prepared, want); err != nil {
		t.Fatal(err)
	}
	if _, err := allocator.Alloc(0); !errors.Is(err, want) {
		t.Fatalf("allocation error=%v want %v", err, want)
	}
}

func TestReservationLedgerPublishBatchValidatesBeforeMutation(t *testing.T) {
	ledger := NewReservationLedger()
	first := candidateIDFromString("batch-first")
	second := candidateIDFromString("batch-second")
	missing := candidateIDFromString("batch-missing")
	if err := ledger.reserve(first, []uint64{10}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.reserve(second, []uint64{11}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.PublishBatch([]CandidateIDV1{first, missing, second}); err == nil {
		t.Fatal("PublishBatch unexpectedly accepted an unknown middle candidate")
	}
	if !ledger.Reserved(10) || !ledger.Reserved(11) {
		t.Fatal("failed batch validation partially released reservations")
	}
	if err := ledger.PublishBatch([]CandidateIDV1{first, second}); err != nil {
		t.Fatal(err)
	}
	if ledger.Reserved(10) || ledger.Reserved(11) {
		t.Fatal("successful batch publication retained reservations")
	}
}

func TestAllocatorCOWCandidateRetirementsRollbackBeforePublish(t *testing.T) {
	newAllocator := func(t *testing.T) (*Allocator, *pager.Pager, ReuseCapability) {
		t.Helper()
		p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Alloc(8); err != nil {
			_ = p.Close()
			t.Fatal(err)
		}
		allocator := New(p, 0)
		if err := allocator.EnableCOWV1(MustNewFreelistGenerationV1(1, 8, nil, nil), NewReservationLedger()); err != nil {
			_ = p.Close()
			t.Fatal(err)
		}
		capability, err := NewReuseCapability(1, 1, 0)
		if err != nil {
			_ = p.Close()
			t.Fatal(err)
		}
		return allocator, p, capability
	}

	t.Run("prepare failure", func(t *testing.T) {
		allocator, p, capability := newAllocator(t)
		defer p.Close()
		before := allocator.Counters()
		if _, err := allocator.PrepareCOWCandidateRetiringV1(
			2, 2, candidateIDFromString("failed-retirement-stage"), capability,
			[]COWRetirementV1{{PageIDs: []uint64{4, 5, 6}, LastReachableCommitSeq: 1}},
			1, failingPageSinkV1{},
		); err == nil {
			t.Fatal("candidate preparation unexpectedly succeeded")
		}
		if got := p.PageCount(); got != 8 {
			t.Fatalf("pager pages after failed prepare=%d want 8", got)
		}
		if got := allocator.Counters(); got != before {
			t.Fatalf("allocator counters after rollback=%+v want %+v", got, before)
		}
		prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateIDFromString("retry-without-retirement"), capability, 1, NewMemoryPageStoreV1())
		if err != nil {
			t.Fatalf("prepare after rollback: %v", err)
		}
		if got := prepared.Candidate().Generation().RetiredCount(); got != 0 {
			t.Fatalf("retry inherited %d stale retirements", got)
		}
	})

	t.Run("caller abort", func(t *testing.T) {
		allocator, p, capability := newAllocator(t)
		defer p.Close()
		before := allocator.Counters()
		prepared, err := allocator.PrepareCOWCandidateRetiringV1(
			2, 2, candidateIDFromString("aborted-retirement-stage"), capability,
			[]COWRetirementV1{{PageIDs: []uint64{4, 5, 6}, LastReachableCommitSeq: 1}},
			1, NewMemoryPageStoreV1(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if got := prepared.Candidate().Generation().RetiredCount(); got != 3 {
			t.Fatalf("prepared retired=%d want 3", got)
		}
		if got := p.PageCount(); got != 8 {
			t.Fatalf("pager pages after prepare=%d want 8", got)
		}
		waiting := make(chan struct{})
		var waitingOnce sync.Once
		TestHookCOWWaitBeforeSleep = func() { waitingOnce.Do(func() { close(waiting) }) }
		defer func() { TestHookCOWWaitBeforeSleep = nil }()
		allocationDone := make(chan struct {
			id  uint64
			err error
		}, 1)
		go func() {
			id, err := allocator.Alloc(0)
			allocationDone <- struct {
				id  uint64
				err error
			}{id: id, err: err}
		}()
		select {
		case <-waiting:
		case <-time.After(2 * time.Second):
			t.Fatal("allocation did not wait behind prepared candidate")
		}
		if err := allocator.AbortCOWCandidateV1(prepared); err != nil {
			t.Fatalf("abort candidate: %v", err)
		}
		select {
		case result := <-allocationDone:
			if result.err != nil {
				t.Fatalf("allocation after abort: %v", result.err)
			}
			if result.id != 8 {
				t.Fatalf("allocation after abort=%d want append page 8, not retired live page 4", result.id)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("abort did not wake blocked allocation")
		}
		// The one admitted allocation after abort is expected; retirement
		// counters must still have rolled back to their prior values.
		if got := allocator.Counters(); got.FreePages != before.FreePages {
			t.Fatalf("allocator free counters after abort=%+v want FreePages=%d", got, before.FreePages)
		}
		retry, err := allocator.PrepareCOWCandidateV1(2, 2, candidateIDFromString("prepare-after-abort"), capability, 1, NewMemoryPageStoreV1())
		if err != nil {
			t.Fatalf("prepare after abort: %v", err)
		}
		if got := retry.Candidate().Generation().RetiredCount(); got != 0 {
			t.Fatalf("post-abort candidate inherited %d stale retirements", got)
		}
	})
}

func TestAllocatorCOWAuxiliaryPagesStayContiguousAboveReusableHoles(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(12); err != nil {
		t.Fatal(err)
	}
	allocator := New(p, 0)
	base := MustNewFreelistGenerationV1(1, 12, []uint64{3, 7, 9}, nil)
	if err := allocator.EnableCOWV1(base, NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	var candidateID CandidateIDV1
	candidateID[0] = 9
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateID, capability, 3, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared.AuxiliaryPageIDs(); len(got) != 3 || got[0] != 12 || got[1] != 13 || got[2] != 14 {
		t.Fatalf("auxiliary pages=%v, want contiguous [12 13 14]", got)
	}
	var other CandidateIDV1
	other[0] = 10
	if _, err := allocator.PrepareCOWCandidateV1(3, 3, other, capability, 3, NewMemoryPageStoreV1()); !errors.Is(err, ErrCOWCandidatePrepared) {
		t.Fatalf("mismatched prepare error=%v, want ErrCOWCandidatePrepared", err)
	}
}

func TestAllocatorCOWPreferAppendBypassesReusablePages(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(12); err != nil {
		t.Fatal(err)
	}
	allocator := New(p, 0)
	base := MustNewFreelistGenerationV1(1, 12, []uint64{3, 7}, nil)
	if err := allocator.EnableCOWV1(base, NewReservationLedger()); err != nil {
		t.Fatal(err)
	}

	allocator.SetPreferAppend(true)
	appended, err := allocator.Alloc(3)
	if err != nil {
		t.Fatal(err)
	}
	if appended != 12 || p.PageCount() != 13 {
		t.Fatalf("prefer-append allocation/page-count=(%d,%d), want (12,13)", appended, p.PageCount())
	}
	stats := allocator.Counters()
	if stats.AppendAllocPages != 1 || stats.ReuseAllocPages != 0 {
		t.Fatalf("prefer-append counters=%+v, want one append and zero reuse", stats)
	}

	allocator.SetPreferAppend(false)
	reused, err := allocator.Alloc(3)
	if err != nil {
		t.Fatal(err)
	}
	if reused != 3 && reused != 7 {
		t.Fatalf("normal allocation=%d, want one of reusable pages [3 7]", reused)
	}
}

func TestAllocatorCOWAllocAppendIsScopedAndAdvancesCandidateHighWater(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(12); err != nil {
		t.Fatal(err)
	}
	allocator := New(p, 0)
	base := MustNewFreelistGenerationV1(1, 12, []uint64{5}, nil)
	if err := allocator.EnableCOWV1(base, NewReservationLedger()); err != nil {
		t.Fatalf("EnableCOWV1: %v", err)
	}

	appended, err := allocator.AllocAppend()
	if err != nil {
		t.Fatalf("AllocAppend: %v", err)
	}
	if appended != 12 {
		t.Fatalf("AllocAppend=%d want high-water page 12", appended)
	}
	reused, err := allocator.Alloc(5)
	if err != nil {
		t.Fatalf("Alloc reusable: %v", err)
	}
	if reused != 5 {
		t.Fatalf("Alloc after AllocAppend=%d want reusable page 5", reused)
	}

	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatalf("NewReuseCapability: %v", err)
	}
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateIDFromString("alloc-append"), capability, 1, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatalf("PrepareCOWCandidateV1: %v", err)
	}
	if got := prepared.AuxiliaryPageIDs(); len(got) != 1 || got[0] <= appended {
		t.Fatalf("auxiliary pages=%v overlap appended page %d", got, appended)
	}
}

func TestAllocatorCOWAllocAppendSkipsActivatedVirtualTail(t *testing.T) {
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
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := allocator.PrepareCOWCandidateV1(
		2, 2, candidateIDFromString("activated-virtual-tail"), capability, 2, NewMemoryPageStoreV1(),
	)
	if err != nil {
		t.Fatal(err)
	}
	virtualHighWater := prepared.Candidate().Generation().HighWater()
	if virtualHighWater <= p.PageCount() {
		t.Fatalf("candidate high-water=%d want above pager pages=%d", virtualHighWater, p.PageCount())
	}
	if err := allocator.ActivateCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}

	pageID, err := allocator.AllocAppend()
	if err != nil {
		t.Fatalf("AllocAppend behind activated virtual tail: %v", err)
	}
	if pageID != virtualHighWater || p.PageCount() != virtualHighWater+1 {
		t.Fatalf("append/page-count=(%d,%d), want (%d,%d)", pageID, p.PageCount(), virtualHighWater, virtualHighWater+1)
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
		materializeAllocatorCOWCandidateForTest(t, p, prepared)
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
	beforeHighWater := allocator.COWGenerationV1().HighWater()
	reused, err := allocator.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	if reused >= beforeHighWater {
		t.Fatalf("allocated page=%d at/above high-water %d, want safe old-page reuse", reused, beforeHighWater)
	}
}

func TestAllocatorCOWCandidateEncodesSealedReuseCapability(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(5); err != nil {
		t.Fatal(err)
	}
	allocator := New(p, 0)
	base := MustNewFreelistGenerationV1(1, 5, nil, map[uint64]uint64{4: 1})
	if err := allocator.EnableCOWV1(base, NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	capability, err := NewReuseCapability(2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	var candidateID CandidateIDV1
	candidateID[0] = 2
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateID, capability, 0, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	generation := prepared.Candidate().Generation()
	if generation.RetiredCount() != 0 || !generation.Allocatable(4) {
		t.Fatalf("candidate retired=%d allocatable(4)=%v, want capability-pruned durable state", generation.RetiredCount(), generation.Allocatable(4))
	}
}
