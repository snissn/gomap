package main

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestM0FrontierRouterSweepCompleteV1(t *testing.T) {
	cells := make([]m0FrontierRouterSweepCellV1, 0, 12)
	for _, budget := range []int{512, 1024, 2048} {
		for _, probes := range []int{1, 2, 4} {
			cells = append(cells, m0FrontierRouterSweepCellV1{Mode: collections.VectorPartitionRouterModeApproxV1, ScoreBudget: budget, Probes: probes, Queries: 806, Candidates: 1, ElapsedNanos: 1, P50Nanos: 1, P95Nanos: 1})
		}
	}
	for _, probes := range []int{1, 2, 4} {
		cells = append(cells, m0FrontierRouterSweepCellV1{Mode: collections.VectorPartitionRouterModeExactV1, ScoreBudget: 128, Probes: probes, Queries: 806, Candidates: 1, ElapsedNanos: 1, P50Nanos: 1, P95Nanos: 1})
	}
	if !m0FrontierRouterSweepCompleteV1(cells) {
		t.Fatal("complete sweep rejected")
	}
	cells[11] = cells[0]
	if m0FrontierRouterSweepCompleteV1(cells) {
		t.Fatal("duplicate sweep accepted")
	}
}
