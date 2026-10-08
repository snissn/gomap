package freelist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

// Public constructor/materializer counts: no oracle traversal of candidate state.
func TestCompressedAllocatorActualStatePageCounts5108(t *testing.T) {
	cases := []struct {
		name  string
		free  []uint64
		high  uint64
		state int
	}{
		{"empty", nil, 2, 1},
		{"one", []uint64{2}, 3, 1},
		{"adjacent", []uint64{2, 256}, 257, 3},
		{"high-bit", []uint64{2, (1 << 63) + 2}, (1 << 63) + 3, 3},
		{"three", []uint64{2, 256, (1 << 63) + 2}, (1 << 63) + 3, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := MustNewFreelistGenerationV1(1, tc.high, tc.free, nil)
			store := NewMemoryPageStoreV1()
			candidate, err := NewFreelistTxn(base, NewReservationLedger()).MaterializeCandidate(2, 2, candidateIDFromString("5108-"+tc.name), store)
			if err != nil {
				t.Fatal(err)
			}
			var state, branches, chunks int
			for _, image := range candidate.Pages() {
				switch page.PageType(page.DecodeHeader(image.Data).Flags & 0xff) {
				case page.PageTypeFreelistIndex:
					state++
					branches++
				case page.PageTypeFreelistChunk:
					state++
					chunks++
				}
			}
			if state != tc.state || candidate.PageCount() != tc.state+2 {
				t.Fatalf("state=%d branches=%d chunks=%d total=%d want state=%d total=%d", state, branches, chunks, candidate.PageCount(), tc.state, tc.state+2)
			}
			loaded, err := LoadGenerationV1(store, candidate.GenerationRef())
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range tc.free {
				if !loaded.Allocatable(id) {
					t.Fatalf("reopen lost free id %d", id)
				}
			}
		})
	}
}

func TestCompressedAllocatorRequiresExplicitTopologyVersion5108(t *testing.T) {
	store := NewMemoryPageStoreV1()
	candidate, err := NewFreelistTxn(MustNewFreelistGenerationV1(1, 3, []uint64{2}, nil), NewReservationLedger()).MaterializeCandidate(2, 2, candidateIDFromString("5108-marker"), store)
	if err != nil {
		t.Fatal(err)
	}
	ref := candidate.GenerationRef()
	header := store.Pages[ref.HeaderPageID]
	if !bytes.Equal(header[16:24], []byte{'F', 'L', 'G', 'E', 'N', 'V', '2', 0}) || binary.LittleEndian.Uint16(header[24:26]) != 2 {
		t.Fatal("compressed topology lacks explicit incompatible generation marker")
	}
	// Valid full CRC and matching external/header digest cannot authorize old bytes.
	copy(header[16:24], []byte{'F', 'L', 'G', 'E', 'N', 'V', '1', 0})
	binary.LittleEndian.PutUint16(header[24:26], 1)
	digest := generationDigest(header)
	copy(header[152:184], digest[:])
	page.UpdateChecksum(header)
	ref.Digest = digest
	if _, err := LoadGenerationV1(store, ref); !errors.Is(err, ErrGenerationFormat) {
		t.Fatalf("old topology accepted: %v", err)
	}
}

func TestCompressedAllocatorReservationCoalescingActualMaterializer5108(t *testing.T) {
	const entries = 162
	const minimum = 2 + 4*(entries-1) + 1
	ledger := NewReservationLedger()
	// Valid burned interval forces a one-page skip beyond the logical frontier.
	ledger.burnedTails = []reservationInterval{{start: minimum, count: 1}}
	txn := NewFreelistTxn(MustNewFreelistGenerationV1(1, minimum, nil, nil), ledger)
	for i := uint64(0); i < entries; i++ {
		txn.abandonedAppends = append(txn.abandonedAppends, ReservationExtentV1{StartPageID: 2 + 4*i, Count: 1, Kind: ReservationAbandonedAppend})
	}
	store := NewMemoryPageStoreV1()
	candidate, err := txn.MaterializeCandidate(2, 2, candidateIDFromString("5108-coalesced-tail"), store)
	if err != nil {
		t.Fatalf("conservative forecast disagreed with coalesced actual materializer: %v", err)
	}
	if candidate.PageCount() != 3 || len(candidate.ReservationRecord().PageIDs()) != 1 {
		t.Fatalf("actual pages=%d reservation pages=%d want3/1", candidate.PageCount(), len(candidate.ReservationRecord().PageIDs()))
	}
	if _, err := LoadGenerationV1(store, candidate.GenerationRef()); err != nil {
		t.Fatal(err)
	}
}
