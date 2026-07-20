package main

import "testing"

func TestOraclePartsUsesTruthMembershipNotCentroids(t *testing.T) {
	// A centroid route could prefer partition 0 for a query near its mean. The
	// oracle must instead choose the partition containing the exact truth IDs.
	assignment := []int{1, 1, 0, 0}
	got := oracleParts([]int{0, 1, 2}, assignment, 2, 1)
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("oracle=%v want truth partition 1", got)
	}
}
