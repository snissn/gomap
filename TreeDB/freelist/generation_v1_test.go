package freelist

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
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

func TestFreelistTxn_AllocateSkipsAnotherCandidateReservation(t *testing.T) {
	ledger := NewReservationLedger()
	base := MustNewFreelistGenerationV1(1, 8, []uint64{2, 3}, nil)
	first := NewFreelistTxn(base, ledger)
	if id, err := first.Allocate(0); err != nil || id != 3 {
		t.Fatalf("first allocation=(%d,%v), want page 3", id, err)
	}
	if err := first.Reserve(candidateIDFromString("first")); err != nil {
		t.Fatal(err)
	}
	second := NewFreelistTxn(base, ledger)
	if id, err := second.Allocate(0); err != nil || id != 2 {
		t.Fatalf("second allocation=(%d,%v), want unreserved page 2", id, err)
	}

	if err := second.Reserve(candidateIDFromString("second")); err != nil {
		t.Fatal(err)
	}
	third := NewFreelistTxn(base, ledger)
	if id, err := third.Allocate(0); err != nil || id != 8 {
		t.Fatalf("all-free-pages-reserved allocation=(%d,%v), want append page 8", id, err)
	}
}

func TestFreelistTxn_AppendFallbackSkipsAnotherCandidatesMetadataTail(t *testing.T) {
	ledger := NewReservationLedger()
	base := MustNewFreelistGenerationV1(1, 8, []uint64{2}, nil)
	firstID := candidateIDFromString("first-tail-owner")
	first := NewFreelistTxn(base, ledger)
	if id, err := first.Allocate(0); err != nil || id != 2 {
		t.Fatalf("first allocation=(%d,%v), want reused page 2", id, err)
	}
	if err := first.Reserve(firstID); err != nil {
		t.Fatal(err)
	}
	firstCandidate, err := first.MaterializeCandidate(2, 2, firstID, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	firstMetadata := firstCandidate.ReservationRecord().metadataPages()
	if len(firstMetadata) == 0 || firstMetadata[0] != base.HighWater() {
		t.Fatalf("first metadata=%v, want tail starting at %d", firstMetadata, base.HighWater())
	}
	if got, want := ledger.Reservations(), uint64(1+len(firstMetadata)); got != want {
		t.Fatalf("first candidate reservations=%d, want data plus metadata=%d", got, want)
	}
	for _, id := range firstMetadata {
		if !ledger.Reserved(id) {
			t.Fatalf("first candidate metadata page %d is not reserved", id)
		}
	}

	secondID := candidateIDFromString("second-tail-owner")
	second := NewFreelistTxn(base, ledger)
	allocated, err := second.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	if allocated != firstCandidate.Generation().HighWater() {
		t.Fatalf("contention append=%d, want first unreserved tail page %d", allocated, firstCandidate.Generation().HighWater())
	}
	if err := second.Reserve(secondID); err != nil {
		t.Fatal(err)
	}
	secondCandidate, err := second.MaterializeCandidate(3, 3, secondID, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range secondCandidate.DirtyPageIDs() {
		for _, firstID := range firstMetadata {
			if id == firstID {
				t.Fatalf("second candidate metadata page %d overlaps first candidate", id)
			}
		}
	}
	foundAbandoned := false
	for _, extent := range secondCandidate.ReservationRecord().Entries() {
		if extent.Kind == ReservationAbandonedAppend && extent.StartPageID == base.HighWater() && uint64(extent.Count) == uint64(len(firstMetadata)) {
			foundAbandoned = true
		}
	}
	if !foundAbandoned {
		t.Fatalf("second reservation record does not account for skipped metadata: %+v", secondCandidate.ReservationRecord().Entries())
	}
}

func TestFreelistTxn_ReservationRejectsAppendChosenBeforeCompetingMetadataTail(t *testing.T) {
	ledger := NewReservationLedger()
	base := MustNewFreelistGenerationV1(1, 8, nil, nil)
	late := NewFreelistTxn(base, ledger)
	if id, err := late.Allocate(0); err != nil || id != 8 {
		t.Fatalf("late allocation=(%d,%v), want append page 8", id, err)
	}

	earlyID := candidateIDFromString("early-metadata-owner")
	early := NewFreelistTxn(base, ledger)
	earlyCandidate, err := early.MaterializeCandidate(2, 2, earlyID, NewMemoryPageStoreV1())
	if err != nil {
		t.Fatal(err)
	}
	if metadata := earlyCandidate.ReservationRecord().metadataPages(); len(metadata) == 0 || metadata[0] != 8 {
		t.Fatalf("early metadata=%v, want tail starting at page 8", metadata)
	}

	lateID := candidateIDFromString("late-data-owner")
	if err := late.Reserve(lateID); !errors.Is(err, ErrPageReserved) {
		t.Fatalf("late reserve error=%v, want metadata-tail collision rejection", err)
	}
	if _, err := late.MaterializeCandidate(3, 3, lateID, NewMemoryPageStoreV1()); !errors.Is(err, ErrPageReserved) {
		t.Fatalf("late materialization error=%v, want metadata-tail collision rejection", err)
	}
}

type failingPageSinkV1 struct{}

func (failingPageSinkV1) WritePage(uint64, []byte) error {
	return errors.New("injected page write failure")
}

type failAfterPageSinkV1 struct {
	store     *MemoryPageStoreV1
	remaining int
}

func (s *failAfterPageSinkV1) WritePage(id uint64, data []byte) error {
	if s.remaining == 0 {
		return errors.New("injected page write failure after partial success")
	}
	s.remaining--
	return s.store.WritePage(id, data)
}

func TestFreelistTxn_SinkFailureConsumesTransaction(t *testing.T) {
	ledger := NewReservationLedger()
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, 8, []uint64{2}, nil), ledger)
	candidateID := candidateIDFromString("failed-materialization")
	if _, err := txn.MaterializeCandidate(2, 2, candidateID, failingPageSinkV1{}); err == nil {
		t.Fatal("materialization unexpectedly succeeded")
	}
	if err := ledger.Fail(candidateID); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("unsafe-retry"), NewMemoryPageStoreV1()); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatalf("retry error=%v, want consumed transaction", err)
	}
	if _, err := txn.Allocate(0); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatalf("post-failure allocation error=%v, want consumed transaction", err)
	}
}

func TestFreelistTxn_PartialSinkFailureBurnsTailUntilRetryPublishesAbandonment(t *testing.T) {
	ledger := NewReservationLedger()
	base := MustNewFreelistGenerationV1(1, 8, nil, nil)
	store := NewMemoryPageStoreV1()
	failedID := candidateIDFromString("partially-written-materialization")
	failed := NewFreelistTxn(base, ledger)
	if _, err := failed.MaterializeCandidate(2, 2, failedID, &failAfterPageSinkV1{store: store, remaining: 1}); err == nil {
		t.Fatal("materialization unexpectedly succeeded")
	}
	if _, ok := store.Pages[base.HighWater()]; !ok {
		t.Fatalf("expected first metadata page %d to be written", base.HighWater())
	}
	burnedCount := ledger.Reservations()
	if burnedCount == 0 {
		t.Fatal("partial materialization did not reserve its metadata tail")
	}
	if err := ledger.Abandon(failedID); err == nil {
		t.Fatal("ordinary abandonment released a metadata tail after a write attempt")
	}
	if err := ledger.Fail(failedID); err != nil {
		t.Fatal(err)
	}
	if !ledger.Reserved(base.HighWater()) {
		t.Fatalf("failed candidate released written metadata page %d", base.HighWater())
	}

	retryID := candidateIDFromString("retry-after-partial-write")
	retryTxn := NewFreelistTxn(base, ledger)
	if id, err := retryTxn.Allocate(0); err != nil || id != base.HighWater()+burnedCount {
		t.Fatalf("retry append=(%d,%v), want first page after burned tail %d", id, err, base.HighWater()+burnedCount)
	}
	retry, err := retryTxn.MaterializeCandidate(3, 3, retryID, store)
	if err != nil {
		t.Fatalf("retry reused a burned metadata page: %v", err)
	}
	foundAbandoned := false
	for _, extent := range retry.ReservationRecord().Entries() {
		if extent.Kind == ReservationAbandonedAppend && extent.StartPageID == base.HighWater() && uint64(extent.Count) == burnedCount {
			foundAbandoned = true
		}
	}
	if !foundAbandoned {
		t.Fatalf("retry did not persist burned tail [%d,%d): %+v", base.HighWater(), base.HighWater()+burnedCount, retry.ReservationRecord().Entries())
	}
	for _, id := range retry.DirtyPageIDs() {
		if id < base.HighWater()+burnedCount {
			t.Fatalf("retry page %d overlaps burned tail [%d,%d)", id, base.HighWater(), base.HighWater()+burnedCount)
		}
	}
	if err := ledger.Publish(retryID); err != nil {
		t.Fatal(err)
	}
	if ledger.Reserved(base.HighWater()) {
		t.Fatalf("published abandonment retained burned page %d in the process ledger", base.HighWater())
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

func TestFreelistGenerationV1_MultiPageReservationRecordRoundTripAndDigest(t *testing.T) {
	const allocations = 220
	free := make([]uint64, 0, allocations)
	for id := uint64(2); len(free) < allocations; id += 2 {
		free = append(free, id)
	}
	store := NewMemoryPageStoreV1()
	base := MustNewFreelistGenerationV1(1, 1024, free, nil)
	txn := NewFreelistTxn(base, NewReservationLedger())
	for range allocations {
		if _, err := txn.Allocate(0); err != nil {
			t.Fatal(err)
		}
	}
	candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("multi-page-record"), store)
	if err != nil {
		t.Fatal(err)
	}
	pageIDs := candidate.ReservationRecord().PageIDs()
	if len(pageIDs) < 2 || txn.Stats().ReservationRecords != uint64(len(pageIDs)) {
		t.Fatalf("reservation pages=%v stats=%+v", pageIDs, txn.Stats())
	}
	loaded, err := LoadGenerationV1(store, candidate.GenerationRef())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(loaded.ReservationRecord().Entries()), len(candidate.ReservationRecord().Entries()); got != want {
		t.Fatalf("loaded extents=%d want %d", got, want)
	}

	corrupt := NewMemoryPageStoreV1()
	for id, data := range store.Pages {
		corrupt.Pages[id] = append([]byte(nil), data...)
	}
	corrupt.Pages[pageIDs[1]][reservationHeaderSize] ^= 1
	page.UpdateChecksum(corrupt.Pages[pageIDs[1]])
	if _, err := LoadGenerationV1(corrupt, candidate.GenerationRef()); !errors.Is(err, ErrGenerationDigest) {
		t.Fatalf("second-page corruption error=%v want %v", err, ErrGenerationDigest)
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

func TestFreelistGenerationV1_ReopenAcceptsSharedAncestorPages(t *testing.T) {
	store := NewMemoryPageStoreV1()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 1024, []uint64{2, 300}, nil), 2, store)
	txn := NewFreelistTxn(base, NewReservationLedger())
	allocated, err := txn.Allocate(300)
	if err != nil {
		t.Fatal(err)
	}
	if allocated != 300 {
		t.Fatalf("allocated=%d want 300", allocated)
	}
	candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("shared-ancestor-pages"), store)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadGenerationV1(store, candidate.GenerationRef())
	if err != nil {
		t.Fatalf("LoadGenerationV1 shared ancestor pages: %v", err)
	}
	if !loaded.Allocatable(2) {
		t.Fatal("shared ancestor chunk lost its free page after reopen")
	}
	if loaded.Allocatable(300) {
		t.Fatal("changed chunk returned the allocated page after reopen")
	}

	corrupt := NewMemoryPageStoreV1()
	for id, data := range store.Pages {
		corrupt.Pages[id] = append([]byte(nil), data...)
	}
	foundSharedChunk := false
	for _, data := range corrupt.Pages {
		h := page.DecodeHeader(data)
		if page.PageType(h.Flags&0xff) != page.PageTypeFreelistChunk || binary.LittleEndian.Uint64(data[40:48]) != 0 || binary.LittleEndian.Uint64(data[32:40]) != 2 {
			continue
		}
		binary.LittleEndian.PutUint64(data[32:40], 3)
		page.UpdateChecksum(data)
		foundSharedChunk = true
		break
	}
	if !foundSharedChunk {
		t.Fatal("shared ancestor chunk page not found")
	}
	if _, err := LoadGenerationV1(corrupt, candidate.GenerationRef()); !errors.Is(err, ErrGenerationFormat) {
		t.Fatalf("newer child under ancestor page error=%v want %v", err, ErrGenerationFormat)
	}
}

func TestFreelistGenerationV1_AppendOnlyEmptySuccessorRewritesRoot(t *testing.T) {
	store := NewMemoryPageStoreV1()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 32, nil, nil), 2, store)
	oldRootID := base.root.pageID
	txn := NewFreelistTxn(base, NewReservationLedger())
	allocated, err := txn.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("append-only-empty-root"), store)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Generation().root.pageID == oldRootID {
		t.Fatalf("append-only generation reused parent root page %d", oldRootID)
	}
	loaded, err := LoadGenerationV1(store, candidate.GenerationRef())
	if err != nil {
		t.Fatalf("LoadGenerationV1 append-only empty successor: %v", err)
	}
	if loaded.Allocatable(allocated) {
		t.Fatalf("reopen returned appended page %d to the freelist", allocated)
	}
	var retiredOldRoot bool
	for _, pending := range loaded.ReservationRecord().pendingMetadata() {
		if pending.id == oldRootID {
			retiredOldRoot = true
			break
		}
	}
	if !retiredOldRoot {
		t.Fatalf("append-only successor did not retain parent root %d for deferred retirement", oldRootID)
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
