package freelist

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/pager"
	"math"
	"path/filepath"
	"testing"
)

type overwritingMetadataSink4627 struct {
	store     *MemoryPageStoreV1
	remaining int
}

type pagerFailureSink4627 struct {
	p      *pager.Pager
	writes int
}

func (s *pagerFailureSink4627) WritePage(id uint64, data []byte) error {
	if s.writes == 1 {
		return errors.New("injected failure after physical tail write")
	}
	if id >= s.p.PageCount() {
		if err := s.p.Truncate(id + 1); err != nil {
			return err
		}
	}
	if err := s.p.Write(id, data); err != nil {
		return err
	}
	s.writes++
	return nil
}

func TestFreelistMetadataReuseAfterPhysicalTailFailure4627(t *testing.T) {
	for _, gap := range []bool{false, true} {
		name := "contiguous"
		if gap {
			name = "abandoned-earlier-reservation"
		}
		t.Run(name, func(t *testing.T) { testMetadataPhysicalTailFailure4627(t, gap) })
	}
}

func testMetadataPhysicalTailFailure4627(t *testing.T, gap bool) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := pager.Open(path, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(512); err != nil {
		t.Fatal(err)
	}
	var free []uint64
	for id := uint64(2); id < 200; id++ {
		free = append(free, id)
	}
	ledger := NewReservationLedger()
	earlier := candidateIDFromString("earlier-unwritten-tail")
	if gap {
		if _, _, err := ledger.reserveTail(earlier, 512, 100, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	blocker := candidateIDFromString("occupy-holes")
	if err := ledger.reserve(blocker, free); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	if err := a.EnableCOWV1(MustNewFreelistGenerationV1(1, 512, free, nil), ledger); err != nil {
		t.Fatal(err)
	}
	capability, err := NewReuseCapability(1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sink := &pagerFailureSink4627{p: p}
	if _, err := a.PrepareCOWCandidateV1(2, 2, candidateIDFromString("tail-fails"), capability, 0, sink); err == nil {
		t.Fatal("expected physical failure")
	}
	if sink.writes != 1 || p.PageCount() <= 512 {
		t.Fatal("fixture did not extend physical pager")
	}
	if err := ledger.Abandon(blocker); err != nil {
		t.Fatal(err)
	}
	if gap {
		if err := ledger.Abandon(earlier); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := a.PrepareCOWCandidateV1(3, 3, candidateIDFromString("reuse-only"), capability, 0, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	g := prepared.Candidate().Generation()
	failedPhysicalEnd := p.PageCount()
	t.Logf("physical pages=%d logical high-water=%d header=%d burned intervals=%d", p.PageCount(), g.HighWater(), g.GenerationRef().HeaderPageID, len(ledger.burnedTails))
	// Exact existing compatibility publication seam in durable_root_runtime.go.
	if err := p.Truncate(g.HighWater()); err != nil {
		t.Fatalf("reuse-only publication after physical rollback: %v", err)
	}
	materializeAllocatorCOWCandidateForTest(t, p, prepared)
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := a.PublishCOWCandidateV1(prepared, capability); err != nil {
		t.Fatal(err)
	}
	if len(ledger.burnedTails) != 0 {
		t.Fatal("publication stranded burned tail")
	}
	ref := prepared.Candidate().GenerationRef()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = pager.Open(path, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	loaded, err := LoadGenerationV1(p, ref)
	if err != nil {
		t.Fatal(err)
	}
	covered := false
	for _, extent := range loaded.record.Extents {
		if extent.Kind == ReservationAbandonedAppend && extent.StartPageID <= 512 && extent.StartPageID+uint64(extent.Count) >= failedPhysicalEnd {
			covered = true
		}
	}
	if !covered {
		t.Fatal("reopened generation lost failed physical output coverage")
	}
	next := NewFreelistTxn(loaded, NewReservationLedger())
	candidate, err := next.MaterializeCandidate(4, 4, candidateIDFromString("reuse-resumes"), NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Generation().HighWater() != loaded.HighWater() || candidate.GenerationRef().HeaderPageID >= 512 {
		t.Fatal("eligible reuse did not resume")
	}
}

func (s *overwritingMetadataSink4627) WritePage(id uint64, data []byte) error {
	if s.remaining == 0 {
		return errors.New("injected physical write failure")
	}
	if s.remaining > 0 {
		s.remaining--
	}
	s.store.Pages[id] = append([]byte(nil), data...)
	return nil
}

// This extends the original tail-only codec's storage contract. Safe capacity
// is produced by allocation, retirement and a strict recovery capability, not
// by injecting a free bitmap. Real DB fallback/process cuts remain separate.
func TestFreelistGenerationV1_MetadataReusesRetiredCapacity4627(t *testing.T) {
	store := NewMemoryPageStoreV1()
	ledger := NewReservationLedger()
	base := MustNewFreelistGenerationV1(1, 2, nil, nil)
	seed := NewFreelistTxn(base, ledger)
	for i := 0; i < 512; i++ {
		id, err := seed.Allocate(0)
		if err != nil {
			t.Fatal(err)
		}
		seed.Retire(id, 1)
	}
	seedID := candidateIDFromString("metadata-reuse-seed")
	seedCandidate, err := seed.MaterializeCandidate(2, 2, seedID, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.MarkVisible(seedID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Publish(seedID); err != nil {
		t.Fatal(err)
	}
	base = seedCandidate.Generation()
	start := base.HighWater()
	for cycle := uint64(0); cycle < 8; cycle++ {
		txn := NewFreelistTxn(base, ledger)
		if cycle == 0 {
			equal, err := NewReuseCapability(1, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			txn.PruneWithCapability(equal)
			if txn.root.freeCount != 0 {
				t.Fatal("equality prematurely grants reuse")
			}
		}
		capability, err := NewReuseCapability(base.CommitSeq(), base.CommitSeq(), 0)
		if err != nil {
			t.Fatal(err)
		}
		txn.PruneWithCapability(capability)
		if txn.root.freeCount < 256 {
			t.Fatalf("insufficient safe capacity: %d", txn.root.freeCount)
		}
		candidateID := candidateIDFromString(benchmarkSizeName(int(cycle + 100)))
		candidate, err := txn.MaterializeCandidate(base.GenerationID()+1, base.CommitSeq()+1, candidateID, store)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := LoadGenerationV1(store, candidate.GenerationRef())
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range candidate.DirtyPageIDs() {
			if decoded.Allocatable(id) {
				t.Fatalf("metadata page %d is free in its own generation", id)
			}
		}
		if _, err := LoadGenerationV1(store, base.GenerationRef()); err != nil {
			t.Fatalf("protected prior generation: %v", err)
		}
		if err := ledger.MarkVisible(candidateID); err != nil {
			t.Fatal(err)
		}
		if err := ledger.Publish(candidateID); err != nil {
			t.Fatal(err)
		}
		base = decoded
		t.Logf("cycle=%d high_water=%d safe_free=%d metadata_pages=%d", cycle, base.HighWater(), base.FreeCount(), candidate.PageCount())
	}
	if base.HighWater() > start {
		t.Fatalf("metadata-only publications extended high-water %d -> %d despite ample recovery-safe capacity", start, base.HighWater())
	}
}

func TestFreelistMetadataReusePlacement4627(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sparse    int
		aux       int
		wantReuse bool
	}{
		{"full_chunk", 0, 0, true}, {"aux_prefix", 0, 32, true},
		{"aux_consumes_first_chunk", 0, 254, true},
		{"skip_fragmented_chunks", 3, 0, true}, {"bounded_starvation", 4, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var free []uint64
			for i := 0; i < tc.sparse; i++ {
				free = append(free, uint64(i*256+2))
			}
			for id := uint64(tc.sparse*256 + 2); id < uint64((tc.sparse+2)*256); id++ {
				free = append(free, id)
			}
			base := MustNewFreelistGenerationV1(1, 2048, free, nil)
			txn := NewFreelistTxn(base, NewReservationLedger())
			aux, err := txn.allocateContiguousRange(tc.aux)
			if err != nil {
				t.Fatal(err)
			}
			for i, id := range aux {
				if id >= base.HighWater() || i > 0 && id != aux[i-1]+1 {
					t.Fatalf("non-reused/noncontiguous aux=%v", aux)
				}
			}
			store := NewMemoryPageStoreV1()
			candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString(tc.name), store)
			if err != nil {
				t.Fatal(err)
			}
			if got := candidate.Generation().HighWater() == base.HighWater(); got != tc.wantReuse {
				t.Fatalf("reused=%t want%t", got, tc.wantReuse)
			}
			decoded, err := LoadGenerationV1(store, candidate.GenerationRef())
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range append(candidate.DirtyPageIDs(), aux...) {
				if decoded.Allocatable(id) {
					t.Fatalf("owned page%d free", id)
				}
			}
		})
	}
}

func TestFreelistMetadataReuseArbitrarySinkRollback4627(t *testing.T) {
	free := make([]uint64, 128)
	for i := range free {
		free[i] = uint64(i + 2)
	}
	base := MustNewFreelistGenerationV1(1, 512, free, nil)
	ledger := NewReservationLedger()
	failed := candidateIDFromString("reuse-failed")
	txn := NewFreelistTxn(base, ledger)
	store := NewMemoryPageStoreV1()
	if _, err := txn.MaterializeCandidate(2, 2, failed, &overwritingMetadataSink4627{store: store, remaining: 1}); err == nil {
		t.Fatal("missing physical sink failure")
	}
	if len(store.Pages) != 1 {
		t.Fatal("physical partial write missing")
	}
	if err := ledger.Abandon(failed); err == nil {
		t.Fatal("write-attempted abandon allowed")
	}
	if err := ledger.RollbackPreVisible(failed); err != nil {
		t.Fatal(err)
	}
	if ledger.Reservations() != 0 || len(ledger.burnedTails) != 0 {
		t.Fatal("safe free holes burned or leaked")
	}
	next := NewFreelistTxn(base, ledger)
	id := candidateIDFromString("reuse-retry")
	candidate, err := next.MaterializeCandidate(3, 3, id, &overwritingMetadataSink4627{store: store, remaining: -1})
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Generation().HighWater() != base.HighWater() {
		t.Fatal("retry did not reuse")
	}
	if err := ledger.MarkVisible(id); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail(id); err == nil {
		t.Fatal("released visible metadata")
	}
	if err := ledger.Publish(id); err != nil {
		t.Fatal(err)
	}
}

func TestFreelistMetadataReuseMaximumHighWater4627(t *testing.T) {
	var free []uint64
	for id := uint64(2); id < 128; id++ {
		free = append(free, id)
	}
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, math.MaxUint64, free, nil), NewReservationLedger())
	store := NewMemoryPageStoreV1()
	candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("max-water"), store)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.GenerationRef().HighWater != math.MaxUint64 || candidate.Generation().HighWater() != math.MaxUint64 {
		t.Fatal("lost logical extent")
	}
	if _, err := LoadGenerationV1(store, candidate.GenerationRef()); err != nil {
		t.Fatal(err)
	}
}

func TestFreelistMetadataReuseAtomicClaim4627(t *testing.T) {
	ledger := NewReservationLedger()
	one, two := candidateIDFromString("one"), candidateIDFromString("two")
	if ledger.claimReusedMetadata(one, 20, 4, 512, []allocatedPage{{21, ReservationReusedData}}, nil) {
		t.Fatal("own data overlaps metadata")
	}
	if !ledger.claimReusedMetadata(one, 20, 4, 512, nil, nil) {
		t.Fatal("first claim")
	}
	if ledger.claimReusedMetadata(two, 22, 4, 512, nil, nil) {
		t.Fatal("overlapping candidate")
	}
	if err := ledger.RollbackPreVisible(one); err != nil {
		t.Fatal(err)
	}
	if !ledger.claimReusedMetadata(two, 22, 4, 512, nil, nil) {
		t.Fatal("released interval unavailable")
	}
}

func TestFreelistMetadataReuseStagedAllocatorRollback4627(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(512); err != nil {
		t.Fatal(err)
	}
	var free []uint64
	for id := uint64(2); id < 200; id++ {
		free = append(free, id)
	}
	base := MustNewFreelistGenerationV1(1, 512, free, nil)
	ledger := NewReservationLedger()
	a := New(p, 0)
	if err := a.EnableCOWV1(base, ledger); err != nil {
		t.Fatal(err)
	}
	capability, err := NewReuseCapability(1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	before := a.Counters()
	store := NewMemoryPageStoreV1()
	_, err = a.PrepareCOWCandidateV1(2, 2, candidateIDFromString("staged-fail"), capability, 3, &overwritingMetadataSink4627{store: store, remaining: 1})
	if err == nil {
		t.Fatal("missing staged sink failure")
	}
	if got := a.Counters(); got != before {
		t.Fatalf("rollback stats=%+v want%+v", got, before)
	}
	if ledger.Reservations() != 0 {
		t.Fatal("staged rollback leaked reservation")
	}
	prepared, err := a.PrepareCOWCandidateV1(3, 3, candidateIDFromString("staged-retry"), capability, 3, &overwritingMetadataSink4627{store: store, remaining: -1})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Candidate().Generation().HighWater() != 512 {
		t.Fatal("staged retry appended")
	}
	materializeAllocatorCOWCandidateForTest(t, p, prepared)
}

func TestFreelistMetadataReuseAuxiliaryClaimConflict4627(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(512); err != nil {
		t.Fatal(err)
	}
	var free []uint64
	for id := uint64(2); id < 200; id++ {
		free = append(free, id)
	}
	ledger := NewReservationLedger()
	a := New(p, 0)
	if err := a.EnableCOWV1(MustNewFreelistGenerationV1(1, 512, free, nil), ledger); err != nil {
		t.Fatal(err)
	}
	// Observe and stage the actual contiguous auxiliary selection before its
	// eventual combined claim, then let another candidate win those pages.
	aux, err := a.cow.txn.allocateContiguousRange(3)
	if err != nil {
		t.Fatal(err)
	}
	other := candidateIDFromString("aux-winner")
	if err := ledger.reserve(other, aux); err != nil {
		t.Fatal(err)
	}
	beforeTxn, beforeStats := a.cow.txn, a.Counters()
	capability, err := NewReuseCapability(1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sink := NewMemoryPageStoreV1()
	_, err = a.PrepareCOWCandidateV1(2, 2, candidateIDFromString("aux-loser"), capability, 0, sink)
	if !errors.Is(err, ErrPageReserved) {
		t.Fatalf("claim conflict=%v", err)
	}
	if len(sink.Pages) != 0 {
		t.Fatal("conflicting candidate wrote before rejection")
	}
	if a.cow.txn != beforeTxn || a.Counters() != beforeStats || a.cow.prepared != nil {
		t.Fatal("allocator rollback lost original state")
	}
	for _, id := range aux {
		if ledger.owners[id] != other {
			t.Fatal("loser rollback released winner")
		}
	}
}

func TestFreelistMetadataTailPlacementAfterBurn4627(t *testing.T) {
	t.Run("no-burn-unchanged", func(t *testing.T) {
		ledger := NewReservationLedger()
		start, count, err := ledger.reserveTail(candidateIDFromString("plain"), 512, 16, nil, nil)
		if err != nil || start != 512 || count != 18 {
			t.Fatalf("placement=%d+%d err=%v", start, count, err)
		}
	})
	t.Run("live-tail-remains-owned", func(t *testing.T) {
		ledger := NewReservationLedger()
		first := candidateIDFromString("live")
		start, count, err := ledger.reserveTail(first, 512, 16, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		second := candidateIDFromString("later")
		next, _, err := ledger.reserveTail(second, 512, 16, nil, nil)
		if err != nil || next != start+count {
			t.Fatalf("conflict placement=%d err=%v", next, err)
		}
		if err := ledger.RollbackPreVisible(second); err != nil {
			t.Fatal(err)
		}
		if !ledger.Reserved(start) || ledger.candidates[first] == nil {
			t.Fatal("lost live ownership")
		}
	})
	t.Run("overflow-rejected-without-claim", func(t *testing.T) {
		ledger := NewReservationLedger()
		ledger.burnedTails = []reservationInterval{{start: math.MaxUint64 - 1, count: 3}}
		id := candidateIDFromString("overflow")
		if _, _, err := ledger.reserveTail(id, 512, 16, nil, nil); !errors.Is(err, ErrNoAllocatablePage) {
			t.Fatalf("overflow=%v", err)
		}
		if ledger.candidates[id] != nil {
			t.Fatal("invalid interval claimed candidate")
		}
	})
}

func TestFreelistMetadataReuseMixedDirtySiblingSizing4627(t *testing.T) {
	var free []uint64
	for id := uint64(2); id < 200; id++ {
		free = append(free, id)
	}
	for id := uint64(512); id < 700; id++ {
		free = append(free, id)
	}
	store := NewMemoryPageStoreV1()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 1024, free, nil), 2, store)
	txn := NewFreelistTxn(base, NewReservationLedger())
	if err := txn.ReservePage(600); err != nil {
		t.Fatal(err)
	}
	var visits uint64
	selected := findFreeGE(txn.root, 0, 0, &visits)
	if selected == nil || selected.chunkNo != 0 || selected.pageID == 0 {
		t.Fatal("fixture needs materialized selected chunk beside dirty sibling")
	}
	candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("mixed-sibling"), store)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.GenerationRef().HeaderPageID >= 256 {
		t.Fatal("metadata did not use selected materialized chunk")
	}
	decoded, err := LoadGenerationV1(store, candidate.GenerationRef())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Allocatable(600) {
		t.Fatal("dirty sibling allocation lost")
	}
	var emitted uint64
	for _, extent := range candidate.ReservationRecord().Entries() {
		if extent.Kind == ReservationTargetMetadata {
			emitted += uint64(extent.Count)
		}
	}
	if emitted != uint64(candidate.PageCount()) {
		t.Fatalf("planned/emitted metadata=%d/%d", emitted, candidate.PageCount())
	}
}

func TestFreelistMetadataReusePublishesBurnedCoverage4627(t *testing.T) {
	ledger := NewReservationLedger()
	failed := candidateIDFromString("old-tail")
	start, count, err := ledger.reserveTail(failed, 500, 16, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.markTailWriteAttempted(failed); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail(failed); err != nil {
		t.Fatal(err)
	}
	var free []uint64
	for id := uint64(2); id < 128; id++ {
		free = append(free, id)
	}
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 500, free, nil), ledger)
	// Ordinary append already records skipped burned space before metadata.
	if _, err := txn.AllocateAppend(); err != nil {
		t.Fatal(err)
	}
	id := candidateIDFromString("covered-reuse")
	candidate, err := txn.MaterializeCandidate(2, 2, id, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if candidate.GenerationRef().HeaderPageID >= 500 {
		t.Fatal("did not reuse metadata")
	}
	covered := false
	for _, extent := range candidate.ReservationRecord().Entries() {
		if extent.Kind == ReservationAbandonedAppend && extent.StartPageID == start && uint64(extent.Count) == count {
			covered = true
		}
	}
	if !covered {
		t.Fatal("missing persistent abandoned coverage")
	}
	if err := ledger.MarkVisible(id); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Publish(id); err != nil {
		t.Fatal(err)
	}
	if ledger.Reservations() != 0 || len(ledger.burnedTails) != 0 {
		t.Fatal("persisted coverage failed to release burned tail")
	}
}

func TestFreelistMetadataReuseReservationChain4627(t *testing.T) {
	var free []uint64
	for id := uint64(2); id < 200; id++ {
		free = append(free, id)
	}
	// Fragmented data reservations cross the single-page record boundary.
	for i := 0; i < reservationEntriesPerPage+2; i++ {
		free = append(free, 1024+uint64(i*2))
	}
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 4096, free, nil), NewReservationLedger())
	for i := 0; i < reservationEntriesPerPage+2; i++ {
		if err := txn.ReservePage(1024 + uint64(i*2)); err != nil {
			t.Fatal(err)
		}
	}
	store := NewMemoryPageStoreV1()
	candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("large-record"), store)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.GenerationRef().HeaderPageID >= 4096 || len(candidate.ReservationRecord().pageIDs) < 2 {
		t.Fatal("missing reused multipage reservation chain")
	}
	if _, err := LoadGenerationV1(store, candidate.GenerationRef()); err != nil {
		t.Fatal(err)
	}
}
