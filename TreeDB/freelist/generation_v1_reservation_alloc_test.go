package freelist

import "testing"

func TestNormalizeExtentsOwnedCopyAllocations4627(t *testing.T) {
	in := []ReservationExtentV1{
		{StartPageID: 9, Count: 1, Kind: ReservationReusedData},
		{StartPageID: 3, Count: 1, Kind: ReservationReusedData},
	}
	allocs := testing.AllocsPerRun(100, func() {
		out, err := normalizeExtents(in)
		if err != nil || len(out) != 2 || out[0].StartPageID != 3 || out[1].StartPageID != 9 {
			t.Fatalf("normalize: %v, %v", out, err)
		}
		out[0].Count = 2
	})
	if in[0].StartPageID != 9 || in[1].Count != 1 {
		t.Fatal("normalization mutated input")
	}
	if allocs > 1 {
		t.Fatalf("allocations = %g, want only owned output copy", allocs)
	}
}
