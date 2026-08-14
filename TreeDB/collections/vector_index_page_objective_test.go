package collections

import (
	"sort"
	"testing"
)

// This is the smallest M0-shaped counterexample: entry-first BFS separates
// both held-out co-visitation pairs across two-node logical blocks.  It is a
// fixture for the selected objective, not a production training-weight source.
func TestColumnVectorGraphPageObjectiveFixture4144(t *testing.T) {
	index := &VectorIndex{
		entry: 0,
		nodes: []vectorIndexNode{
			{neighbors: [][]vectorIndexNeighbor{{{nodeID: 1}, {nodeID: 2}, {nodeID: 3}, {nodeID: 4}, {nodeID: 5}}}},
			{}, {}, {}, {}, {},
		},
	}
	bfs := columnVectorGraphNativeLocalityOrder(index)
	wantBFS := []int{0, 1, 2, 3, 4, 5}
	if !samePageObjectiveOrder4144(bfs, wantBFS) {
		t.Fatalf("BFS order=%v want %v", bfs, wantBFS)
	}

	// Perturb the source-map insertion order. Canonicalization is required at
	// the training-input trust boundary so map iteration cannot choose a pack.
	weightsA := map[[2]int]uint64{{1, 4}: 10, {2, 5}: 10}
	weightsB := map[[2]int]uint64{{2, 5}: 10, {1, 4}: 10}
	if got, want := canonicalPageObjectivePairs4144(weightsA), canonicalPageObjectivePairs4144(weightsB); !samePageObjectivePairs4144(got, want) {
		t.Fatalf("perturbed co-visitation pairs=%v want %v", got, want)
	}

	// The selected co-visitation packing keeps both weighted pairs in a block.
	selected := []int{1, 4, 2, 5, 0, 3}
	if got, want := pageObjectiveScore4144(bfs, canonicalPageObjectivePairs4144(weightsA), 2), uint64(0); got != want {
		t.Fatalf("BFS co-visitation score=%d want %d", got, want)
	}
	if got, want := pageObjectiveScore4144(selected, canonicalPageObjectivePairs4144(weightsB), 2), uint64(20); got != want {
		t.Fatalf("selected co-visitation score=%d want %d", got, want)
	}
}

func canonicalPageObjectivePairs4144(weights map[[2]int]uint64) [][3]uint64 {
	pairs := make([][3]uint64, 0, len(weights))
	for pair, weight := range weights {
		pairs = append(pairs, [3]uint64{uint64(pair[0]), uint64(pair[1]), weight})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	return pairs
}

func pageObjectiveScore4144(order []int, pairs [][3]uint64, blockSize int) uint64 {
	positions := make(map[int]int, len(order))
	for position, node := range order {
		positions[node] = position
	}
	var score uint64
	for _, pair := range pairs {
		if positions[int(pair[0])]/blockSize == positions[int(pair[1])]/blockSize {
			score += pair[2]
		}
	}
	return score
}

func samePageObjectiveOrder4144(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func samePageObjectivePairs4144(left, right [][3]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
