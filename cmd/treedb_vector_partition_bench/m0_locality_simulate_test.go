package main

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestM0ObjectiveOrdersAreDeterministicAndDistinct(t *testing.T) {
	snapshot := collections.VectorPartitionPackLayoutSnapshotV1{
		Rows: 5, EntryOrdinal: 0, RowOrdinals: []uint32{0, 1, 2, 3, 4}, VectorStride: 1, VectorOffset: 1,
		LayerOffsets: [][]uint64{{0, 2, 3, 4, 5, 6}}, LayerNeighbors: [][]uint32{{1, 2, 0, 0, 4, 3}},
		LayerOffsetsSectionOffsets: []uint64{2}, LayerNeighborOffsets: []uint64{3},
	}
	pairs := map[[2]int]uint32{{1, 3}: 9, {2, 4}: 7}
	seen := map[string][]int{}
	for _, objective := range []string{"source", "bfs", "edge_window", "gorder_like", "co_visitation", "hybrid"} {
		first, err := m0ObjectiveOrderV1(snapshot, pairs, objective)
		if err != nil {
			t.Fatalf("%s: %v", objective, err)
		}
		second, err := m0ObjectiveOrderV1(snapshot, pairs, objective)
		if err != nil || !reflect.DeepEqual(first, second) {
			t.Fatalf("%s nondeterministic: %v %v", objective, first, second)
		}
		seen[objective] = first
	}
	if reflect.DeepEqual(seen["edge_window"], seen["co_visitation"]) || reflect.DeepEqual(seen["gorder_like"], seen["hybrid"]) {
		t.Fatalf("objectives collapsed: %+v", seen)
	}
}
