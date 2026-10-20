package freelist

import (
	"bytes"
	"errors"
	"maps"
	"math"
	"slices"
	"testing"
)

// Preserve the former construction as a semantic oracle. In particular, it
// deduplicates only IDs within the two allocation categories, not all extents.
func legacyReservationExtentsForTest(txn *FreelistTxn) ([]ReservationExtentV1, error) {
	var reused, appended, replaced []uint64
	for _, allocation := range txn.allocated {
		if allocation.kind == ReservationReusedData {
			reused = append(reused, allocation.id)
		} else {
			appended = append(appended, allocation.id)
		}
	}
	for id := range txn.replacedMetadata {
		replaced = append(replaced, id)
	}
	var extents []ReservationExtentV1
	add := func(ids []uint64, kind ReservationKindV1, seq uint64) {
		for _, id := range sortedUnique(ids) {
			extents = append(extents, ReservationExtentV1{StartPageID: id, Count: 1, Kind: kind, LastReachableCommitSeq: seq})
		}
	}
	add(reused, ReservationReusedData, 0)
	add(appended, ReservationAppendedData, 0)
	extents = append(extents, txn.abandonedAppends...)
	add(replaced, ReservationPendingMetadataRetirement, txn.base.commitSeq)
	return normalizeExtents(extents)
}

func TestFreelistTxnReservationExtentsMatchLegacyConstruction(t *testing.T) {
	tests := []struct {
		name      string
		allocated []allocatedPage
		abandoned []ReservationExtentV1
		replaced  map[uint64]struct{}
		invalid   bool
	}{
		{name: "empty"},
		{name: "same-category-duplicates", allocated: []allocatedPage{{9, ReservationReusedData}, {8, ReservationReusedData}, {9, ReservationReusedData}, {31, ReservationAppendedData}, {30, ReservationAppendedData}, {30, ReservationAppendedData}}, abandoned: []ReservationExtentV1{{StartPageID: 40, Count: 2, Kind: ReservationAbandonedAppend}}, replaced: map[uint64]struct{}{60: {}, 61: {}}},
		{name: "non-reused-category-mapping", allocated: []allocatedPage{{9, ReservationKindV1(99)}, {9, ReservationAppendedData}}},
		{name: "cross-category-duplicate", allocated: []allocatedPage{{9, ReservationReusedData}, {9, ReservationAppendedData}}, invalid: true},
		{name: "abandoned-duplicate", abandoned: []ReservationExtentV1{{StartPageID: 9, Count: 1, Kind: ReservationAbandonedAppend}, {StartPageID: 9, Count: 1, Kind: ReservationAbandonedAppend}}, invalid: true},
		{name: "abandoned-allocation-overlap", allocated: []allocatedPage{{9, ReservationAppendedData}}, abandoned: []ReservationExtentV1{{StartPageID: 8, Count: 2, Kind: ReservationAbandonedAppend}}, invalid: true},
		{name: "replaced-allocation-overlap", allocated: []allocatedPage{{9, ReservationReusedData}}, replaced: map[uint64]struct{}{9: {}}, invalid: true},
		{name: "invalid-page", allocated: []allocatedPage{{1, ReservationReusedData}}, invalid: true},
		{name: "zero-count", abandoned: []ReservationExtentV1{{StartPageID: 9, Kind: ReservationAbandonedAppend}}, invalid: true},
		{name: "extent-end-overflow", abandoned: []ReservationExtentV1{{StartPageID: math.MaxUint64, Count: 1, Kind: ReservationAbandonedAppend}}, invalid: true},
		{name: "merge-count-boundary", abandoned: []ReservationExtentV1{{StartPageID: 2, Count: math.MaxUint32, Kind: ReservationAbandonedAppend}, {StartPageID: 2 + uint64(math.MaxUint32), Count: 1, Kind: ReservationAbandonedAppend}}},
	}
	// Fragmentation crosses a physical reservation-page boundary. Compare all
	// encoded pages and their digest, not just the logical merged entries.
	fragmented := tests[0]
	fragmented.name = "multi-page"
	for i := uint64(0); i < uint64(reservationEntriesPerPage+3); i++ {
		fragmented.allocated = append(fragmented.allocated, allocatedPage{2 + 2*i, ReservationReusedData})
	}
	tests = append(tests, fragmented)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			txn := NewFreelistTxn(MustNewFreelistGenerationV1(7, 1024, nil, nil), nil)
			txn.allocated = slices.Clone(test.allocated)
			txn.abandonedAppends = slices.Clone(test.abandoned)
			txn.replacedMetadata = maps.Clone(test.replaced)
			want, wantErr := legacyReservationExtentsForTest(txn)
			got, err := txn.reservationExtents()
			if !slices.Equal(txn.allocated, test.allocated) || !slices.Equal(txn.abandonedAppends, test.abandoned) || !maps.Equal(txn.replacedMetadata, test.replaced) {
				t.Fatal("construction mutated transaction inputs")
			}
			if test.invalid {
				if !errors.Is(err, ErrGenerationFormat) || !errors.Is(wantErr, ErrGenerationFormat) {
					t.Fatalf("errors=(%v,%v), want format rejection", err, wantErr)
				}
				return
			}
			if err != nil || wantErr != nil || !slices.Equal(got, want) {
				t.Fatalf("extents=%+v err=%v; legacy=%+v err=%v", got, err, want, wantErr)
			}
			if test.name == "same-category-duplicates" {
				golden := []ReservationExtentV1{{StartPageID: 8, Count: 2, Kind: ReservationReusedData}, {StartPageID: 30, Count: 2, Kind: ReservationAppendedData}, {StartPageID: 40, Count: 2, Kind: ReservationAbandonedAppend}, {StartPageID: 60, Count: 2, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 7}}
				if !slices.Equal(got, golden) {
					t.Fatalf("canonical extents=%+v, want %+v", got, golden)
				}
			}
			record := ReservationRecordV1{CandidateID: candidateIDFromString(test.name), GenerationID: 8, BaseID: 7, Extents: got}
			gotPages, gotRecord, err := encodeNormalizedReservationPages(1000, record)
			if err != nil {
				t.Fatal(err)
			}
			record.Extents = want
			wantPages, wantRecord, err := encodeNormalizedReservationPages(1000, record)
			if err != nil {
				t.Fatal(err)
			}
			if gotRecord.Digest() != wantRecord.Digest() || len(gotPages) != len(wantPages) {
				t.Fatal("reservation chain/digest changed")
			}
			for i := range gotPages {
				if !bytes.Equal(gotPages[i], wantPages[i]) {
					t.Fatalf("reservation page %d changed", i)
				}
			}
			if len(got) != 0 {
				got[0].StartPageID = 999
				if !slices.Equal(txn.allocated, test.allocated) || !slices.Equal(txn.abandonedAppends, test.abandoned) {
					t.Fatal("construction returned borrowed transaction storage")
				}
			}
		})
	}
}

func TestNormalizeExtentsPreservesBorrowedInput(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		input := []ReservationExtentV1{{StartPageID: 11, Count: 1, Kind: ReservationAppendedData}, {StartPageID: 10, Count: 1, Kind: ReservationAppendedData}}
		if invalid {
			input[0].StartPageID = 10
		}
		before := slices.Clone(input)
		got, err := normalizeExtents(input)
		if !slices.Equal(input, before) {
			t.Fatal("normalization reordered or merged borrowed input")
		}
		if invalid {
			if !errors.Is(err, ErrGenerationFormat) {
				t.Fatalf("overlap error=%v", err)
			}
		} else {
			if err != nil || len(got) != 1 || got[0].Count != 2 {
				t.Fatalf("normalized=%+v err=%v", got, err)
			}
			got[0].StartPageID = 999
			if !slices.Equal(input, before) {
				t.Fatal("normalization result aliases borrowed input")
			}
		}
	}
}

func TestFreelistTxnReservationRecordOwnsCanonicalStorage(t *testing.T) {
	base := MustNewFreelistGenerationV1(1, 64, []uint64{3}, nil)
	txn := NewFreelistTxn(base, NewReservationLedger())
	for range 128 {
		if _, err := txn.AllocateAppend(); err != nil {
			t.Fatal(err)
		}
	}
	store := NewMemoryPageStoreV1()
	candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("owned-compact-record"), store)
	if err != nil {
		t.Fatal(err)
	}
	record := candidate.ReservationRecord()
	before := record.Entries()
	if len(before) != 2 || before[0].StartPageID != 64 || before[0].Count != 128 || before[0].Kind != ReservationAppendedData || before[1].Kind != ReservationTargetMetadata {
		t.Fatalf("compacted record=%+v", before)
	}
	// Keep the same retained-capacity bound as the former final defensive copy,
	// rather than retaining a 128-entry scratch buffer for two immutable entries.
	if cap(record.Extents) > cap(append([]ReservationExtentV1(nil), before...)) {
		t.Fatalf("record retained construction capacity %d for %d entries", cap(record.Extents), len(before))
	}
	for i := range txn.allocated {
		txn.allocated[i].id = 999
	}
	txn.abandonedAppends = append(txn.abandonedAppends, ReservationExtentV1{StartPageID: 999, Count: 1, Kind: ReservationAbandonedAppend})
	txn.replacedMetadata[999] = struct{}{}
	entries := record.Entries()
	entries[0].Count = 1
	if !slices.Equal(record.Extents, before) || !slices.Equal(base.ReservationRecord().Extents, nil) {
		t.Fatal("immutable reservation record changed through construction or accessor inputs")
	}
	loaded, err := LoadGenerationV1(store, candidate.GenerationRef())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(loaded.ReservationRecord().Extents, before) || loaded.ReservationRecord().Digest() != record.Digest() {
		t.Fatal("reopened reservation record differs from immutable candidate")
	}
}

func TestFreelistTxnReservationTailOverflowLeavesNoOwnership(t *testing.T) {
	ledger := NewReservationLedger()
	base := MustNewFreelistGenerationV1(1, math.MaxUint64-2, nil, nil)
	txn := NewFreelistTxn(base, ledger)
	store := NewMemoryPageStoreV1()
	id := candidateIDFromString("reservation-overflow")
	if _, err := txn.MaterializeCandidate(2, 2, id, store); !errors.Is(err, ErrNoAllocatablePage) {
		t.Fatalf("overflow error=%v, want no allocatable page", err)
	}
	if len(store.Pages) != 0 || ledger.Reservations() != 0 || base.root.pageID != 0 {
		t.Fatal("overflow wrote output, retained ownership, or mutated immutable base")
	}
	if _, err := txn.MaterializeCandidate(2, 2, id, store); !errors.Is(err, ErrCandidateConsumed) {
		t.Fatalf("overflow transaction retry=%v, want consumed", err)
	}
	// A fresh transaction remains usable with the same ledger after rejection.
	retry := NewFreelistTxn(MustNewFreelistGenerationV1(1, 64, nil, nil), ledger)
	candidate, err := retry.MaterializeCandidate(2, 2, id, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadGenerationV1(store, candidate.GenerationRef()); err != nil {
		t.Fatal(err)
	}
}

// Check the real tail-placement/failure paths against the former constructor,
// including persisted bytes. The target comes from actual emitted metadata IDs,
// not from the reservation extents being checked.
func assertLegacyReservationRecordForTest(t *testing.T, txn *FreelistTxn, candidate *FreelistCandidateV1) {
	t.Helper()
	extents, err := legacyReservationExtentsForTest(txn)
	if err != nil {
		t.Fatal(err)
	}
	metadata := candidate.Generation().metadataPages
	if len(metadata) == 0 {
		t.Fatal("candidate has no emitted metadata")
	}
	extents = append(extents, ReservationExtentV1{StartPageID: metadata[0], Count: uint32(len(metadata)), Kind: ReservationTargetMetadata})
	record := candidate.ReservationRecord()
	record.Extents = extents
	pages, expected, err := encodeReservationPages(record.PageID(), record)
	if err != nil {
		t.Fatal(err)
	}
	actual := candidate.ReservationRecord()
	if !slices.Equal(actual.Extents, expected.Extents) || actual.Digest() != expected.Digest() {
		t.Fatalf("materialized record=%+v differs from former construction=%+v", actual.Extents, expected.Extents)
	}
	for i, id := range actual.PageIDs() {
		found := false
		for _, emitted := range candidate.pages {
			if emitted.PageID == id {
				found = true
				if !bytes.Equal(emitted.Data, pages[i]) {
					t.Fatalf("materialized reservation page %d changed", id)
				}
			}
		}
		if !found {
			t.Fatalf("reservation page %d not emitted", id)
		}
	}
}
