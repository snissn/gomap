package freelist

import (
	"errors"
	"testing"
)

func TestFreelistGenerationV1_OlderGenerationAndPagesRemainImmutable(t *testing.T) {
	store := NewMemoryPageStoreV1()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(7, 100, []uint64{4, 8, 12}, nil), 8, store)
	before := make(map[uint64]string, len(store.Pages))
	for id, data := range store.Pages {
		before[id] = string(data)
	}
	txn := NewFreelistTxn(base, NewReservationLedger())
	if _, err := txn.Allocate(0); err != nil {
		t.Fatal(err)
	}
	next, err := txn.MaterializeCandidate(9, 9, candidateIDFromString("next"), store)
	if err != nil {
		t.Fatal(err)
	}
	if !base.Allocatable(12) || next.Generation().Allocatable(12) {
		t.Fatal("generation view mutated")
	}
	for id, want := range before {
		if got := string(store.Pages[id]); got != want {
			t.Fatalf("page %d changed", id)
		}
	}
}

func TestFreelistGenerationV1_MaterializeDoesNotAssignIDsIntoBase(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 64, []uint64{2, 9, 33}, nil)
	if base.root.pageID != 0 {
		t.Fatal("new model unexpectedly materialized")
	}
	txn := NewFreelistTxn(base, nil)
	if _, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("detached"), NewMemoryPageStoreV1()); err != nil {
		t.Fatal(err)
	}
	if base.root.pageID != 0 {
		t.Fatalf("base root assigned page %d", base.root.pageID)
	}
	if err := walkState(base.root, 0, func(chunk *stateChunk) error {
		if chunk.pageID != 0 {
			t.Fatalf("base chunk assigned page %d", chunk.pageID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFreelistGenerationV1_HorizonPreventsPrematureReuse(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 10, nil, map[uint64]uint64{5: 9})
	txn := NewFreelistTxn(base, NewReservationLedger())
	txn.Prune(RecoveryHorizon{OldestRecoverableCommitSeq: 9, MinPinnedSnapshotCommitSeq: ^uint64(0), HistoryFloorCommitSeq: ^uint64(0)})
	if id, err := txn.Allocate(0); err != nil || id != 10 {
		t.Fatalf("got (%d,%v), want append 10", id, err)
	}
	txn = NewFreelistTxn(base, NewReservationLedger())
	txn.Prune(RecoveryHorizon{OldestRecoverableCommitSeq: 10, MinPinnedSnapshotCommitSeq: ^uint64(0), HistoryFloorCommitSeq: ^uint64(0)})
	if id, err := txn.Allocate(0); err != nil || id != 5 {
		t.Fatalf("got (%d,%v), want reuse 5", id, err)
	}
}

func TestFreelistTxn_CandidateReservationsSurviveSupersedeAndFailure(t *testing.T) {
	ledger := NewReservationLedger()
	one, two := candidateIDFromString("one"), candidateIDFromString("two")
	base := MustNewFreelistGenerationV1(1, 20, []uint64{3}, nil)
	first := NewFreelistTxn(base, ledger)
	if _, err := first.Allocate(0); err != nil {
		t.Fatal(err)
	}
	if err := first.Reserve(one); err != nil {
		t.Fatal(err)
	}
	second := NewFreelistTxn(base, ledger)
	if err := second.ReservePage(3); !errors.Is(err, ErrPageReserved) {
		t.Fatalf("double reservation: %v", err)
	}
	if err := ledger.Supersede(one, two); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail(two); err != nil {
		t.Fatal(err)
	}
	if id, err := NewFreelistTxn(base, ledger).Allocate(0); err != nil || id != 3 {
		t.Fatalf("released reservation: %d %v", id, err)
	}
}

func TestReservationLedger_VisibleAndAmbiguousStatesRetainOwnership(t *testing.T) {
	ledger := NewReservationLedger()
	candidateID := candidateIDFromString("candidate")
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 9, []uint64{2}, nil), ledger)
	if _, err := txn.Allocate(0); err != nil {
		t.Fatal(err)
	}
	if err := txn.Reserve(candidateID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.MarkVisible(candidateID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail(candidateID); err == nil {
		t.Fatal("visible failure released ownership")
	}
	if err := ledger.Retry(candidateID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Poison(candidateID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Shutdown(candidateID); err == nil {
		t.Fatal("poisoned transition should be terminal")
	}
	if !ledger.Reserved(2) {
		t.Fatal("ambiguous state lost reservation")
	}
	if err := ledger.Publish(candidateID); err != nil {
		t.Fatal(err)
	}
	if ledger.Reserved(2) {
		t.Fatal("confirmed publication retained process ownership")
	}
}

func TestFreelistGenerationV1_PageCodecRoundTripAndCorruption(t *testing.T) {
	store := NewMemoryPageStoreV1()
	g := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 99, []uint64{2, 5, 8}, map[uint64]uint64{11: 7}), 2, store)
	loaded, err := LoadGenerationV1(store, g.GenerationRef())
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Allocatable(8) || loaded.HighWater() != g.HighWater() {
		t.Fatal("round trip lost state")
	}
	corrupt := NewMemoryPageStoreV1()
	for id, data := range store.Pages {
		corrupt.Pages[id] = append([]byte(nil), data...)
	}
	corrupt.Pages[g.GenerationRef().HeaderPageID][200] = 1
	if _, err := LoadGenerationV1(corrupt, g.GenerationRef()); !errors.Is(err, ErrGenerationChecksum) {
		t.Fatalf("corruption error=%v", err)
	}
	missing := NewMemoryPageStoreV1()
	for id, data := range store.Pages {
		missing.Pages[id] = append([]byte(nil), data...)
	}
	delete(missing.Pages, g.root.pageID)
	if _, err := LoadGenerationV1(missing, g.GenerationRef()); err == nil {
		t.Fatal("missing root accepted")
	}
}

func TestFreelistGenerationV1_EveryPageKindRejectsCorruption(t *testing.T) {
	store := NewMemoryPageStoreV1()
	g := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 99, []uint64{2, 5, 8}, map[uint64]uint64{11: 7}), 2, store)
	seen := map[uint16]bool{}
	for corruptID, original := range store.Pages {
		typ := uint16(original[12])
		seen[typ] = true
		corrupt := NewMemoryPageStoreV1()
		for id, data := range store.Pages {
			corrupt.Pages[id] = append([]byte(nil), data...)
		}
		corrupt.Pages[corruptID][300] ^= 1
		if _, err := LoadGenerationV1(corrupt, g.GenerationRef()); !errors.Is(err, ErrGenerationChecksum) {
			t.Fatalf("type=%#x page=%d err=%v", typ, corruptID, err)
		}
	}
	for _, typ := range []uint16{5, 6, 7, 8} {
		if !seen[typ] {
			t.Fatalf("page type %#x not emitted", typ)
		}
	}
}

func TestFreelistGenerationV1_CanonicalAndSummaryValidationSurvivesRecomputedCRC(t *testing.T) {
	store := NewMemoryPageStoreV1()
	g := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 99, []uint64{2, 5, 8}, map[uint64]uint64{11: 7}), 2, store)
	reservedOffset := map[uint16]int{5: 29, 6: 29, 7: 28, 8: 30}
	for corruptID, original := range store.Pages {
		typ := uint16(original[12])
		offset, ok := reservedOffset[typ]
		if !ok {
			continue
		}
		corrupt := NewMemoryPageStoreV1()
		for id, data := range store.Pages {
			corrupt.Pages[id] = append([]byte(nil), data...)
		}
		corrupt.Pages[corruptID][offset] = 1
		finishPage(corrupt.Pages[corruptID])
		if _, err := LoadGenerationV1(corrupt, g.GenerationRef()); !errors.Is(err, ErrGenerationFormat) {
			t.Fatalf("type=%#x canonical err=%v", typ, err)
		}
		delete(reservedOffset, typ)
	}
	if len(reservedOffset) != 0 {
		t.Fatalf("unexercised page types: %v", reservedOffset)
	}

	for corruptID, original := range store.Pages {
		if uint16(original[12]) != 6 || original[14] == 0 {
			continue
		}
		corrupt := NewMemoryPageStoreV1()
		for id, data := range store.Pages {
			corrupt.Pages[id] = append([]byte(nil), data...)
		}
		corrupt.Pages[corruptID][80] ^= 1 // first child free-count summary
		finishPage(corrupt.Pages[corruptID])
		if _, err := LoadGenerationV1(corrupt, g.GenerationRef()); !errors.Is(err, ErrGenerationFormat) {
			t.Fatalf("forged child summary err=%v", err)
		}
		return
	}
	t.Fatal("no populated index page")
}

func TestReservationExtentsRejectCrossKindOverlap(t *testing.T) {
	_, err := normalizeExtents([]ReservationExtentV1{
		{StartPageID: 10, Count: 3, Kind: ReservationReusedData},
		{StartPageID: 12, Count: 2, Kind: ReservationTargetMetadata},
	})
	if !errors.Is(err, ErrGenerationFormat) {
		t.Fatalf("err=%v", err)
	}
}

func TestFreelistGenerationV1_ReopenReconstructsReservationOwnership(t *testing.T) {
	store := NewMemoryPageStoreV1()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 32, []uint64{4}, nil), 2, store)
	txn := NewFreelistTxn(base, NewReservationLedger())
	allocated, err := txn.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("poisoned-before-crash"), store)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadGenerationV1(store, candidate.GenerationRef())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Allocatable(allocated) {
		t.Fatalf("reopen returned reserved page %d", allocated)
	}
	var sawAllocation, sawMetadata bool
	for _, extent := range loaded.ReservationRecord().Entries() {
		if extent.Kind == ReservationReusedData && extent.StartPageID == allocated {
			sawAllocation = true
		}
		if extent.Kind == ReservationTargetMetadata {
			sawMetadata = true
		}
	}
	if !sawAllocation || !sawMetadata {
		t.Fatalf("record=%+v", loaded.ReservationRecord().Entries())
	}
}

func TestFreelistGenerationV1_OneChunkDeltaEmitsOnlyOnePath(t *testing.T) {
	store := NewMemoryPageStoreV1()
	free := make([]uint64, 0, 4096)
	for id := uint64(2); id < 1<<20; id += 256 {
		free = append(free, id)
	}
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 1<<20, free, nil), 2, store)
	txn := NewFreelistTxn(base, NewReservationLedger())
	if _, err := txn.Allocate(700_000); err != nil {
		t.Fatal(err)
	}
	candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("delta"), store)
	if err != nil {
		t.Fatal(err)
	}
	stats := txn.Stats()
	if stats.COWChunks != 1 || stats.COWPages > chunkTrieDepth+4 {
		t.Fatalf("unexpected delta stats: %+v", stats)
	}
	if stats.PageVisits > uint64((chunkTrieDepth+1)*4) {
		t.Fatalf("allocation visited %d pages", stats.PageVisits)
	}
	if len(candidate.DirtyPageIDs()) != int(stats.COWPages) {
		t.Fatalf("dirty pages=%d stats=%d", len(candidate.DirtyPageIDs()), stats.COWPages)
	}
	if stats.COWBytes != stats.COWPages*4096 {
		t.Fatalf("bytes=%d pages=%d", stats.COWBytes, stats.COWPages)
	}
}

func TestFreelistGenerationV1_ReplacedMetadataWaitsForExplicitHorizon(t *testing.T) {
	store := NewMemoryPageStoreV1()
	first := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 32, []uint64{4}, nil), 2, store)
	txn := NewFreelistTxn(first, nil)
	if _, err := txn.Allocate(0); err != nil {
		t.Fatal(err)
	}
	secondCandidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("second"), store)
	if err != nil {
		t.Fatal(err)
	}
	pending := secondCandidate.Generation().ReservationRecord().pendingMetadata()
	if len(pending) == 0 {
		t.Fatal("replaced parent metadata was not retained")
	}
	txn = NewFreelistTxn(secondCandidate.Generation(), nil)
	for _, retired := range pending {
		if txn.rootAllocatable(retired.id) {
			t.Fatalf("page %d reusable before horizon", retired.id)
		}
	}
	capability, err := NewReuseCapability(4, ^uint64(0), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	txn.PruneWithCapability(capability)
	for _, retired := range pending {
		if !txn.rootAllocatable(retired.id) {
			t.Fatalf("page %d not reusable after horizon", retired.id)
		}
	}
}

func TestBeginCandidateV1RejectsStaleParent(t *testing.T) {
	store := NewMemoryPageStoreV1()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 16, []uint64{2}, nil), 2, store)
	stale := base.GenerationRef()
	stale.Digest[0] ^= 1
	if _, err := BeginCandidateV1(base, stale, nil); !errors.Is(err, ErrGenerationParent) {
		t.Fatalf("err=%v", err)
	}
}
