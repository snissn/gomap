package collections

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
)

func TestVectorPartitionVamanaTwoPassBuildIsDeterministicBoundedAndReachableV1(t *testing.T) {
	makeRows := func() []columnVectorGraphAssetRow {
		rows := make([]columnVectorGraphAssetRow, 96)
		for i := range rows {
			angle := 2 * math.Pi * float64(i) / float64(len(rows))
			rows[i] = columnVectorGraphAssetRow{
				ID:     []byte(fmt.Sprintf("row-%03d", i)),
				Vector: []float32{float32(math.Cos(angle)), float32(math.Sin(angle)), float32(math.Cos(3 * angle)), 1},
			}
		}
		return rows
	}
	left, right := makeRows(), makeRows()
	var stats vectorPartitionVamanaBuildStatsV1
	if err := buildVectorPartitionVamanaWithStatsV1(context.Background(), left, 4, &stats); err != nil {
		t.Fatal(err)
	}
	if err := buildVectorPartitionVamanaV1(context.Background(), right, 4); err != nil {
		t.Fatal(err)
	}
	for row := range left {
		if !bytes.Equal(left[row].ID, right[row].ID) || !slices.Equal(left[row].Adjacency, right[row].Adjacency) {
			t.Fatalf("nondeterministic row=%d", row)
		}
		if len(left[row].Adjacency) > vectorPartitionVamanaDegreeV1 {
			t.Fatalf("row=%d degree=%d", row, len(left[row].Adjacency))
		}
		seen := make(map[uint32]struct{}, len(left[row].Adjacency))
		for _, neighbor := range left[row].Adjacency {
			if int(neighbor) == row || int(neighbor) >= len(left) {
				t.Fatalf("row=%d invalid neighbor=%d", row, neighbor)
			}
			if _, duplicate := seen[neighbor]; duplicate {
				t.Fatalf("row=%d duplicate neighbor=%d", row, neighbor)
			}
			seen[neighbor] = struct{}{}
		}
	}
	visited := make([]bool, len(left))
	queue := []int{0}
	visited[0] = true
	for head := 0; head < len(queue); head++ {
		for _, raw := range left[queue[head]].Adjacency {
			neighbor := int(raw)
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	if len(queue) != len(left) {
		t.Fatalf("entry reaches %d of %d rows", len(queue), len(left))
	}
	for pass := range stats.PassSearches {
		if stats.PassSearches[pass] != uint64(len(left)) || stats.OutgoingReplacements[pass] == 0 || stats.ReciprocalInsertions[pass] == 0 {
			t.Fatalf("pass %d did not exercise complete Vamana flow: %+v", pass+1, stats)
		}
	}
	if stats.OverflowPrunes[0]+stats.OverflowPrunes[1] == 0 {
		t.Fatalf("reciprocal overflow pruning was inactive: %+v", stats)
	}
	if stats.MaxRobustPrunePool > vectorPartitionVamanaCandidateCapV1 {
		t.Fatalf("robust-prune pool=%d exceeds cap=%d", stats.MaxRobustPrunePool, vectorPartitionVamanaCandidateCapV1)
	}
}

func TestVectorPartitionVamanaBuildObservesCancellationV1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := buildVectorPartitionVamanaV1(ctx, []columnVectorGraphAssetRow{{ID: []byte("a"), Vector: []float32{1}}}, 1)
	if err != context.Canceled {
		t.Fatalf("error=%v want context.Canceled", err)
	}
}

func TestVectorPartitionVamanaRobustPruneAlphaAndCandidateCapV1(t *testing.T) {
	var scratch vectorPartitionVamanaScratchV1
	vector := func(angle float64) []float32 {
		return []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}
	}
	vectors := append(append(vector(0), vector(.1)...), vector(.94)...)
	candidates := []vectorIndexCandidate{
		{nodeID: 1, distance: vectorPartitionVamanaDistanceV1(vectors, 2, 0, 1)},
		{nodeID: 2, distance: vectorPartitionVamanaDistanceV1(vectors, 2, 0, 2)},
	}
	if got := scratch.robustPrune(0, candidates, vectors, 2, 1, 64, nil, nil); !slices.Equal(got, []uint32{1}) {
		t.Fatalf("alpha=1 prune=%v want [1]", got)
	}
	if got := scratch.robustPrune(0, candidates, vectors, 2, vectorPartitionVamanaFinalAlphaV1, 64, nil, nil); !slices.Equal(got, []uint32{1, 2}) {
		t.Fatalf("alpha=1.2 prune=%v want [1 2]", got)
	}

	manyVectors := make([]float32, (vectorPartitionVamanaCandidateCapV1+2)*2)
	many := make([]vectorIndexCandidate, vectorPartitionVamanaCandidateCapV1+1)
	for i := range many {
		angle := math.Pi * float64(i+1) / float64(len(many)+2)
		copy(manyVectors[(i+1)*2:], vector(angle))
		many[i] = vectorIndexCandidate{nodeID: i + 1, distance: vectorPartitionVamanaDistanceV1(manyVectors, 2, 0, i+1)}
	}
	var stats vectorPartitionVamanaBuildStatsV1
	got := scratch.robustPrune(0, many, manyVectors, 2, vectorPartitionVamanaFinalAlphaV1, vectorPartitionVamanaDegreeV1, nil, &stats)
	if len(got) > vectorPartitionVamanaDegreeV1 {
		t.Fatalf("degree=%d exceeds %d", len(got), vectorPartitionVamanaDegreeV1)
	}
	if stats.MaxRobustPrunePool != vectorPartitionVamanaCandidateCapV1 {
		t.Fatalf("candidate pool=%d want cap=%d", stats.MaxRobustPrunePool, vectorPartitionVamanaCandidateCapV1)
	}

	tied := []vectorIndexCandidate{{nodeID: 2, distance: 1}, {nodeID: 1, distance: 1}}
	if got := scratch.robustPrune(0, tied, []float32{0, 0, 1, 0, 1, 0}, 2, 1, 2, nil, nil); !slices.Equal(got, []uint32{1}) {
		t.Fatalf("tie prune=%v want ordinal-first [1]", got)
	}
}

func TestVectorPartitionVamanaGreedySearchAndCurrentNeighborUnionV1(t *testing.T) {
	vectors := []float32{-1, 0, 0, 1, 0, -1, 1, 0, .7, .7}
	adjacency := [][]uint32{{2, 1}, {4}, nil, nil, {3}}
	scratch := vectorPartitionVamanaScratchV1{visited: make([]uint32, len(adjacency))}
	visited := scratch.greedySearch(0, 3, vectors, 2, adjacency, 2)
	nodes := make([]int, len(visited))
	for i := range visited {
		nodes[i] = visited[i].nodeID
	}
	slices.Sort(nodes)
	if !slices.Equal(nodes, []int{0, 1, 4}) {
		t.Fatalf("greedy visited=%v", nodes)
	}
	merged := vectorPartitionVamanaMergeCurrentNeighborsV1(nil, 3, visited[:1], []uint32{1}, vectors, 2)
	if len(merged) != 2 || merged[0].nodeID != visited[0].nodeID || merged[1].nodeID != 1 {
		t.Fatalf("current-neighbor union=%+v", merged)
	}
}

func TestVectorPartitionVamanaSingletonAndNonFiniteInputV1(t *testing.T) {
	rows := []columnVectorGraphAssetRow{{ID: []byte("only"), Vector: []float32{1}}}
	if err := buildVectorPartitionVamanaV1(context.Background(), rows, 1); err != nil {
		t.Fatal(err)
	}
	want := []uint32{columnVectorGraphLayeredAdjacencyMagic, 0, 0}
	if !slices.Equal(rows[0].Adjacency, want) {
		t.Fatalf("singleton adjacency=%v want %v", rows[0].Adjacency, want)
	}
	if err := buildVectorPartitionVamanaV1(context.Background(), []columnVectorGraphAssetRow{{ID: []byte("bad"), Vector: []float32{float32(math.NaN())}, InvNorm: 1}}, 1); err == nil {
		t.Fatal("non-finite normalized vector accepted")
	}
}
