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
	want := map[string][]int{"source": {0, 1, 2, 3, 4}, "bfs": {0, 1, 2, 3, 4}, "edge_window": {0, 1, 2, 3, 4}, "gorder_like": {0, 1, 2, 3, 4}, "co_visitation": {0, 1, 3, 2, 4}, "hybrid": {0, 1, 3, 2, 4}}
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
		if !reflect.DeepEqual(first, want[objective]) {
			t.Fatalf("%s got %v want %v", objective, first, want[objective])
		}
	}
	if _, err := m0ObjectiveOrderV1(snapshot, pairs, "unknown"); err == nil {
		t.Fatal("accepted unknown objective")
	}
	if reflect.DeepEqual(seen["edge_window"], seen["co_visitation"]) || reflect.DeepEqual(seen["gorder_like"], seen["hybrid"]) {
		t.Fatalf("objectives collapsed: %+v", seen)
	}
}

func TestM0EdgeWindowExpiresOldPlacement(t *testing.T) {
	snapshot := collections.VectorPartitionPackLayoutSnapshotV1{
		Rows: 5, EntryOrdinal: 0, RowOrdinals: []uint32{0, 1, 2, 3, 4}, VectorStride: 512, VectorOffset: 1,
		LayerOffsets: [][]uint64{{0, 2, 5, 8, 10, 12}}, LayerNeighbors: [][]uint32{{1, 3, 0, 2, 4, 1, 3, 4, 0, 2, 1, 2}},
		LayerOffsetsSectionOffsets: []uint64{2}, LayerNeighborOffsets: []uint64{3},
	}
	got, err := m0ObjectiveOrderV1(snapshot, nil, "edge_window")
	if err != nil || !reflect.DeepEqual(got, []int{0, 1, 2, 4, 3}) {
		t.Fatalf("got %v err=%v", got, err)
	}
}

func TestM0GorderAdmitsTwoHopFrontier(t *testing.T) {
	snapshot := collections.VectorPartitionPackLayoutSnapshotV1{
		Rows: 3, EntryOrdinal: 0, RowOrdinals: []uint32{0, 2, 1}, VectorStride: 512, VectorOffset: 1,
		LayerOffsets: [][]uint64{{0, 1, 3, 4}}, LayerNeighbors: [][]uint32{{1, 0, 2, 1}},
		LayerOffsetsSectionOffsets: []uint64{2}, LayerNeighborOffsets: []uint64{3},
	}
	got, err := m0ObjectiveOrderV1(snapshot, nil, "gorder_like")
	if err != nil || !reflect.DeepEqual(got, []int{0, 2, 1}) {
		t.Fatalf("got %v err=%v", got, err)
	}
}
