package freelist

import (
	"errors"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestRetainedReservationSortKeepsOriginalOutputAndReleasesScratch(t *testing.T) {
	for _, count := range []int{2, 3, 4, 5, 8, 9, 33, 80, 4096} {
		original := make([]ReservationExtentV1, count)
		for i := range original {
			original[i] = ReservationExtentV1{StartPageID: 2 + 2*uint64(count-i), Count: 1, Kind: ReservationReusedData}
		}
		expected, err := normalizeExtents(original)
		if err != nil {
			t.Fatal(err)
		}
		var owner retainedalloc.Owner
		owner.Initialize(128)
		s := reservationNormalizeWorkV1{entries: original, owned: true, metadata: &owner}
		sawScratch := false
		for calls := 0; ; calls++ {
			if calls > 20000 {
				t.Fatalf("count %d stalled in phase %d", count, s.phase)
			}
			w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			ready, err := s.advance(nil, &w)
			if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
				t.Fatalf("count %d phase %d allowance=%+v: %v", count, s.phase, w, err)
			}
			if s.scratchCharge != 0 {
				sawScratch = true
				if owner.Bytes() != 128+s.scratchCharge {
					t.Fatal("live scratch lost its original producer charge")
				}
			}
			if !ready {
				continue
			}
			if !slices.Equal(s.out, expected) || &s.out[0] != &original[0] {
				t.Fatalf("count %d changed canonical values or escaped scratch output", count)
			}
			if s.scratch != nil || s.original != nil || s.scratchCharge != 0 || owner.Bytes() != 128 {
				t.Fatal("terminal sort retained finite-only backing or charge")
			}
			if count > 8 && !sawScratch {
				t.Fatal("control failed to observe retained scratch")
			}
			break
		}
	}
}

func TestRetainedReservationSortCancellationScrubsBeforeRefund(t *testing.T) {
	for _, swaps := range []bool{false, true} {
		original := make([]ReservationExtentV1, 33)
		for i := range original {
			original[i] = ReservationExtentV1{StartPageID: uint64(2 + 2*i), Count: 1, Kind: ReservationReusedData}
		}
		var owner retainedalloc.Owner
		owner.Initialize(128)
		s := reservationNormalizeWorkV1{entries: original, owned: true, metadata: &owner}
		for calls := 0; ; calls++ {
			if calls > 100 {
				t.Fatal("did not reach retained merge pass")
			}
			w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			ready, err := s.advance(nil, &w)
			if err != nil || ready {
				t.Fatal("unexpected sort completion", err)
			}
			if s.scratch != nil && (&s.entries[0] != &original[0]) == swaps {
				break
			}
		}
		before := owner.Bytes()
		var tooLarge *iterator.OrdinalUnitTooLarge
		if ready, err := s.releaseScratch(&iterator.OrdinalScanWork{ByteLimit: 1 << 20}); ready || !errors.As(err, &tooLarge) || owner.Bytes() != before || s.scratch == nil {
			t.Fatal("zero-work cancellation changed live backing or charge", err)
		}
		for calls := 0; ; calls++ {
			if calls > 100 {
				t.Fatal("scratch cancellation stalled")
			}
			w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			ready, err := s.releaseScratch(&w)
			if err != nil {
				t.Fatal(err)
			}
			if owner.Bytes() < before && (s.scratch != nil || s.original != nil || swaps && s.entries != nil) {
				t.Fatal("scratch charge refunded before all scratch aliases were removed")
			}
			if ready {
				if owner.Bytes() != 128 || s.scratchCharge != 0 {
					t.Fatal("scratch charge survived terminal cancellation")
				}
				break
			}
		}
	}
}
