package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestM0MembershipModesZeroCutElidesUsefulAndRejectsExactFiller(t *testing.T) {
	artifact := vectorpartition.Artifact{
		IDs:     make([]string, 10),
		Metrics: vectorpartition.Metrics{EdgeCut: 0},
	}
	zero := vectorpartition.OverlapResult{Memberships: []vectorpartition.Membership{{VectorOrdinal: 0, Partition: 0, Home: true}}}
	useful := zero
	exact := vectorpartition.OverlapResult{
		Memberships: append(append([]vectorpartition.Membership(nil), zero.Memberships...), vectorpartition.Membership{VectorOrdinal: 1, Partition: 1}),
		Used:        2,
		Filler:      2,
	}
	modes, err := m0MembershipModesV1(artifact, zero, useful, exact)
	if err != nil {
		t.Fatal(err)
	}
	if got := modes[1]; got.Used != 0 || got.Useful != 0 || got.Filler != 0 || got.EquivalentTo != "zero" || got.Materialize {
		t.Fatalf("useful-only zero-cut mode = %+v, want zero equivalent", got)
	}
	if got := modes[2]; got.Rejected != "exact-20 contains filler" || got.Materialize || got.Used != 2 || got.Filler != 2 {
		t.Fatalf("exact zero-cut mode = %+v, want filler rejection", got)
	}
	raw, err := json.Marshal(modes[1])
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"used":0`, `"useful":0`, `"filler":0`, `"materialize":false`} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("marshaled zero mode %s lacks %s", raw, field)
		}
	}
}
