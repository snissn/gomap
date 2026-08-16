package main

import (
	"slices"
	"testing"
)

func TestLocalHNSWM18EdgeDiagnosisContractV1(t *testing.T) {
	if !localHNSWM18EdgeDiagnosisPointsV1([]int{80, 81, 88, 96}) {
		t.Fatal("accepted grid rejected")
	}
	if localHNSWM18EdgeDiagnosisPointsV1([]int{80, 88, 96}) || localHNSWM18EdgeDiagnosisPointsV1([]int{64, 80, 96, 128}) {
		t.Fatal("non-contract grid accepted")
	}
	misses := []localHNSWM18EdgeDiagnosisHardMissV1{{QueryOrdinal: 2, QuerySHA256: "b", Rank: "a"}, {QueryOrdinal: 1, QuerySHA256: "a", Rank: "a"}}
	got := localHNSWM18EdgeDiagnosisMissesV1(misses)
	if !slices.EqualFunc(got, []localHNSWM18EdgeDiagnosisHardMissV1{{QueryOrdinal: 1, QuerySHA256: "a", Rank: "a"}, {QueryOrdinal: 2, QuerySHA256: "b", Rank: "a"}}, func(a, b localHNSWM18EdgeDiagnosisHardMissV1) bool { return a == b }) {
		t.Fatalf("non-deterministic hard-miss order: %#v", got)
	}
}
