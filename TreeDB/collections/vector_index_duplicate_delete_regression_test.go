package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
)

func TestNativePublishedDuplicateVectorsRemainSearchableAfterDeletes(t *testing.T) {
	for _, order := range []string{"ascending", "descending", "permuted"} {
		t.Run(order, func(t *testing.T) {
			index, err := newVectorIndex(nil, VectorIndexOptions{
				Name: "embedding_graph", Field: "embedding", Metric: VectorMetricCosine,
				Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8,
			})
			if err != nil {
				t.Fatal(err)
			}
			index.setNativePersistent(true)
			index.sourceDocumentRootsValid = true
			insert := func(id string, vector []float32) {
				t.Helper()
				if err := index.insertVectorLocked([]byte(id), vector); err != nil {
					t.Fatal(err)
				}
			}
			// Match the public partition delta: 32 changed replacements, restore,
			// then 32 distinct IDs with the same vector. The immutable base is separate.
			for i := 0; i < 32; i++ {
				vector := []float32{0, 1}
				if i%2 != 0 {
					vector = []float32{1, 0}
				}
				insert("base-minus-x", vector)
			}
			insert("base-minus-x", []float32{-1, 0})
			for i := 0; i < 32; i++ {
				insert(fmt.Sprintf("cost-delete-%02d", i), []float32{0, 1})
			}
			for ordinal, node := range index.nodes {
				for layer, edges := range node.neighbors {
					if len(edges) > index.maxNeighborsForLayer(layer) {
						t.Fatalf("ordinal %d layer %d degree %d exceeds bound", ordinal, layer, len(edges))
					}
					for _, edge := range edges {
						if int(edge.nodeID) >= len(index.nodes) || math.IsNaN(float64(edge.distance)) || math.IsInf(float64(edge.distance), 0) {
							t.Fatalf("ordinal %d layer %d invalid edge %+v", ordinal, layer, edge)
						}
					}
				}
			}
			for i := 0; i < 32; i++ {
				deleted := i
				if order == "descending" {
					deleted = 31 - i
				} else if order == "permuted" {
					deleted = (13*i + 7) % 32
				}
				index.tombstoneDocumentIDLocked([]byte(fmt.Sprintf("cost-delete-%02d", deleted)))
				index.acknowledgeSearchViewStateLocked()
				index.publishSearchViewLocked(false)
				view := index.acquireSearchView()
				var buffer VectorIndexSearchBuffer
				got, err := view.searchGraphOnlyWithBuffer([]float32{0, 1}, 3, 16, &buffer)
				index.releaseSearchView(view)
				if err != nil {
					t.Fatalf("delete %d (%s): %v", i, order, err)
				}
				wantCount := minInt(3, len(index.currentNode))
				if len(got) != wantCount {
					t.Fatalf("delete %d: got %d results, want %d", i, len(got), wantCount)
				}
				for _, result := range got {
					ordinal, ok := index.currentNode[string(result.ID)]
					wantScore := float32(1)
					if string(result.ID) == "base-minus-x" {
						wantScore = 0
					}
					if math.Float32bits(float32(result.Score)) != math.Float32bits(wantScore) {
						t.Fatalf("delete %d ID %q score %g want %g", i, result.ID, result.Score, wantScore)
					}
					if !ok || index.nodes[ordinal].deleted {
						t.Fatalf("delete %d returned retired ID %q", i, result.ID)
					}
				}
			}
			// The public fixture inserts and retries a fresh vector after deleting
			// the cohort, then searches with its original TopK4/Ef8 request.
			insert("fresh-y", []float32{0, 1})
			index.acknowledgeSearchViewStateLocked()
			index.publishSearchViewLocked(false)
			view := index.acquireSearchView()
			var buffer VectorIndexSearchBuffer
			buffer.nativeSearchWorkEnabled = true
			got, err := view.searchGraphOnlyWithBuffer([]float32{0, 1}, 4, 8, &buffer)
			index.releaseSearchView(view)
			if err != nil {
				t.Fatalf("fresh insert after cohort deletion (%s): %v", order, err)
			}
			if buffer.nativeSearchScratch.explorationLimit != 32 || buffer.nativeSearchScratch.explored > 32 || buffer.nativeSearchWork.scoreCalls > 32 {
				t.Fatalf("fresh insertion grew bounded work: explored=%d limit=%d scores=%d", buffer.nativeSearchScratch.explored, buffer.nativeSearchScratch.explorationLimit, buffer.nativeSearchWork.scoreCalls)
			}
			if len(got) != 2 || string(got[0].ID) != "fresh-y" || float32(got[0].Score) != 1 || string(got[1].ID) != "base-minus-x" || float32(got[1].Score) != 0 {
				t.Fatalf("fresh insert after cohort deletion: candidates=%+v want fresh-y then base-minus-x", got)
			}
		})
	}
}

func TestVectorIndexConstructionIdenticalRepresentationValidation(t *testing.T) {
	for _, metric := range []VectorMetric{VectorMetricCosine, VectorMetricL2, VectorMetricInnerProduct} {
		for _, encoding := range []VectorIndexEncoding{VectorIndexEncodingFloat32, VectorIndexEncodingInt8} {
			t.Run(fmt.Sprintf("%s/%d", metric, encoding), func(t *testing.T) {
				index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding_graph", Field: "embedding", Metric: metric, Encoding: encoding, Dimensions: 2, M: 2, EfConstruction: 8})
				if err != nil {
					t.Fatal(err)
				}
				index.nodes = []vectorIndexNode{index.newVectorIndexNode([]byte("a"), []float32{0, 1}, 0), index.newVectorIndexNode([]byte("b"), []float32{0, 1}, 0), index.newVectorIndexNode([]byte("c"), []float32{1, 0}, 0)}
				if !index.constructionNodesHaveIdenticalVectorsLocked(0, 1) || index.constructionNodesHaveIdenticalVectorsLocked(0, 2) {
					t.Fatal("identity must compare actual stored vector representation")
				}
				index.nodes[1].deleted = true
				if !index.constructionNodesHaveIdenticalVectorsLocked(0, 1) {
					t.Fatal("deleted waypoints still carry exact geometry")
				}
				index.nodes[1].vector = nil
				index.nodes[1].quantized = nil
				if index.constructionNodesHaveIdenticalVectorsLocked(0, 1) {
					t.Fatal("empty representation established identity")
				}
				index.nodes[1] = index.newVectorIndexNode([]byte("b"), []float32{0, 1}, 0)
				if encoding == VectorIndexEncodingInt8 {
					index.nodes[1].quantScale *= 2
					if index.constructionNodesHaveIdenticalVectorsLocked(0, 1) {
						t.Fatal("different quantization scale established identity")
					}
					index.nodes[1] = index.newVectorIndexNode([]byte("b"), []float32{0, 1}, 0)
				}
				index.metric = VectorMetric(255)
				if index.constructionNodesHaveIdenticalVectorsLocked(0, 1) {
					t.Fatal("unsupported metric established identity")
				}
				index.metric = metric
				index.encoding = VectorIndexEncoding(255)
				if index.constructionNodesHaveIdenticalVectorsLocked(0, 1) {
					t.Fatal("unsupported encoding established identity")
				}
				index.encoding = encoding
				if encoding == VectorIndexEncodingFloat32 {
					index.nodes[0].vector[1] = float32(math.Inf(1))
					index.nodes[1].vector[1] = float32(math.Inf(1))
				} else {
					index.nodes[0].quantScale = float32(math.Inf(1))
					index.nodes[1].quantScale = float32(math.Inf(1))
				}
				if index.constructionNodesHaveIdenticalVectorsLocked(0, 1) {
					t.Fatal("nonfinite representation established identity")
				}
			})
		}
	}
}

func TestVectorIndexRedundantConstructionLiveTieOrder(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding_graph", Field: "embedding", Metric: VectorMetricL2,
		Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Several exact-vector classes share the same source distance. Pairwise
	// identity-conditioned ties would cycle among these interleaved classes.
	for ordinal, vector := range [][]float32{{0, 1}, {1, 0}, {0, -1}, {0, 1}, {-1, 0}, {0, -1}, {0, 1}, {0, 0}} {
		index.nodes = append(index.nodes, index.newVectorIndexNode([]byte(fmt.Sprintf("geometry-%d", ordinal)), vector, 0))
	}
	const source = 7
	candidates := []vectorIndexCandidate{{nodeID: 0, distance: 1}, {nodeID: 1, distance: 1}, {nodeID: 2, distance: 1}, {nodeID: 3, distance: 1}, {nodeID: 4, distance: 1}, {nodeID: 5, distance: 1}, {nodeID: 6, distance: 1}}
	checkOrder := func(want []int) {
		t.Helper()
		if !index.constructionCandidatesHaveIdenticalVectorsLocked(candidates) {
			t.Fatal("mixed equal-distance pool must establish the whole-pool redundancy gate")
		}
		compare := func(a, b vectorIndexCandidate) int { return index.compareConstructionCandidatesLocked(a, b, source) }
		for _, a := range candidates {
			if compare(a, a) != 0 {
				t.Fatal("construction comparator is not reflexive")
			}
			for _, b := range candidates {
				if (compare(a, b) < 0) != (compare(b, a) > 0) {
					t.Fatalf("construction order is not antisymmetric: %d/%d", a.nodeID, b.nodeID)
				}
				for _, c := range candidates {
					if compare(a, b) < 0 && compare(b, c) < 0 && compare(a, c) >= 0 {
						t.Fatalf("construction comparator cycle: %d/%d/%d", a.nodeID, b.nodeID, c.nodeID)
					}
				}
			}
		}
		ordered := append([]vectorIndexCandidate(nil), candidates...)
		slices.SortFunc(ordered, compare)
		got := make([]int, len(ordered))
		for i, candidate := range ordered {
			got[i] = candidate.nodeID
		}
		if !slices.Equal(got, want) {
			t.Fatalf("construction order %v want %v", got, want)
		}
		selected, _, _, _, _ := index.selectConstructionDiverseCandidatesLocked(append([]vectorIndexCandidate(nil), candidates...), len(candidates), false, true, false, nil, nil, source)
		if len(selected) != len(want) {
			t.Fatalf("selection fast path retained %d candidates want %d", len(selected), len(want))
		}
		for i, candidate := range selected {
			if candidate.nodeID != want[i] {
				t.Fatalf("selection fast path[%d]=%d want %d", i, candidate.nodeID, want[i])
			}
		}
	}
	checkOrder([]int{6, 5, 4, 3, 2, 1, 0})
	for _, ordinal := range []int{0, 3, 6} {
		index.nodes[ordinal].deleted = true
	}
	checkOrder([]int{5, 4, 2, 1, 6, 3, 0})
	if index.compareConstructionCandidatesLocked(vectorIndexCandidate{nodeID: 0, distance: 0}, vectorIndexCandidate{nodeID: 5, distance: 1}, source) >= 0 {
		t.Fatal("live status displaced primary distance")
	}
	// The fresh live equal-geometry endpoint must survive reciprocal pruning;
	// deleted peers remain available for the remaining bounded degree slots.
	index.nodes[6].deleted = false
	neighbors := []vectorIndexNeighbor{{nodeID: 0, distance: 1}, {nodeID: 3, distance: 1}, {nodeID: 6, distance: 1}}
	pruned := index.pruneLayerNeighborsLocked(source, neighbors, 2)
	if len(pruned) != 2 || pruned[0].nodeID != 6 || pruned[1].nodeID != 3 || !index.nodes[pruned[1].nodeID].deleted || len(pruned) > index.maxNeighborsForLayer(0) {
		t.Fatalf("live shortcut/deleted backfill/degree lost: %+v", pruned)
	}
	selected, diverse, _, origins, _ := index.selectConstructionDiverseCandidatesLocked(append([]vectorIndexCandidate(nil), candidates...), 6, true, true, false, nil, nil, source)
	want := []int{6, 5, 4, 2, 1, 3}
	if len(selected) != len(want) || diverse != 4 || origins[2] || origins[3] || !index.nodes[3].deleted {
		t.Fatalf("mixed-geometry diversity/backfill lost: selected=%v diverse=%d origins=%v", selected, diverse, origins)
	}
	for i, candidate := range selected {
		if candidate.nodeID != want[i] {
			t.Fatalf("mixed-geometry prune[%d]=%d want %d", i, candidate.nodeID, want[i])
		}
	}
}

func TestVectorIndexCurrentSearchPreservesLiveAndDeletedEntryRoutes(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 2, EfSearch: 4})
	if err != nil {
		t.Fatal(err)
	}
	// The closer deleted route reaches live17, but its equal-distance chain
	// starves farther live0. Only live0's one-hop edge reaches live16 in time.
	for ordinal := 0; ordinal < 18; ordinal++ {
		vector := []float32{0, 1}
		level := 0
		if ordinal == 0 {
			vector, level = []float32{-1, 0}, 1
		} else if ordinal == 1 {
			level = 1
		} else if ordinal == 17 {
			vector = []float32{0.1, 1}
		}
		node := index.newVectorIndexNode([]byte(fmt.Sprintf("route-%02d", ordinal)), vector, level)
		node.deleted = ordinal > 0 && ordinal < 16
		index.nodes = append(index.nodes, node)
		if !node.deleted {
			index.currentNode[string(node.documentID)] = ordinal
		}
	}
	index.entry, index.maxLevel = 0, 1
	index.nodes[0].neighbors[1] = []vectorIndexNeighbor{{nodeID: 1}}
	index.nodes[0].neighbors[0] = []vectorIndexNeighbor{{nodeID: 16}, {nodeID: 0}, {nodeID: 1}, {nodeID: 16}}
	index.nodes[1].neighbors[0] = []vectorIndexNeighbor{{nodeID: 2}, {nodeID: 0}}
	index.nodes[2].neighbors[0] = []vectorIndexNeighbor{{nodeID: 17}, {nodeID: 3}}
	for ordinal := 3; ordinal < 15; ordinal++ {
		index.nodes[ordinal].neighbors[0] = []vectorIndexNeighbor{{nodeID: uint32(ordinal + 1)}}
	}
	query := []float32{0, 1}
	norm, prepared, _, err := prepareVectorIndexGraphOnlyQuery(query, index.metric, index.dimensions)
	if err != nil {
		t.Fatal(err)
	}
	// Two endpoint seeds alone still miss16, distinguishing one-hop admission
	// from merely queueing the live entry behind the tombstones.
	var control vectorIndexSearchScratch
	upper := 0
	landing := index.greedyNearestAtLayerSearchBoundedLocked(query, norm, &prepared, 0, 1, 11, &upper, &control)
	if landing != 1 || upper != 2 {
		t.Fatalf("control upper landing=%d work=%d want 1/2", landing, upper)
	}
	seeds := make([]vectorIndexCandidate, 0, 2)
	for _, ordinal := range []int{landing, 0} {
		distance, ok := index.scoreSearchNodeWithPreparedQueryLocked(query, norm, &prepared, ordinal, &control)
		if !ok {
			t.Fatal("control scoring failed")
		}
		seeds = append(seeds, vectorIndexCandidate{nodeID: ordinal, distance: distance})
	}
	got := index.searchLayerWithCandidateSeedsScratchModeObservedLocked(query, norm, &prepared, landing, seeds, 4, 14, 0, &control, true, nil)
	if len(got) != 2 || control.visitedEpochs[16] == control.visitedEpoch {
		t.Fatalf("two-endpoint control did not isolate starvation: %v", got)
	}
	var scratch vectorIndexSearchScratch
	for _, cap := range []int{0, 16, 1, 3, 5} {
		scratch.startScoreTracking(cap)
		got, err := index.searchGraphOnlyCandidatesWithLiveDocsLocked(query, 3, 4, 3, &scratch)
		if cap == 0 || cap == 16 {
			if err != nil || len(got) != 3 || got[0].nodeID != 16 || got[1].nodeID != 17 || got[2].nodeID != 0 {
				t.Fatalf("cap%d failed paired live/deleted routes: candidates=%v err=%v", cap, got, err)
			}
			if scratch.explorationLimit != 16 || scratch.explored != 16 || scratch.scoreCalls != 16 {
				t.Fatalf("duplicate/pre-scored seeds charged twice: explored=%d limit=%d scores=%d", scratch.explored, scratch.explorationLimit, scratch.scoreCalls)
			}
		} else if !errors.Is(err, ErrVectorPartitionSearchUnavailable) || scratch.scoreCalls > cap {
			t.Fatalf("cap%d did not fail closed: candidates=%v err=%v scores=%d", cap, got, err, scratch.scoreCalls)
		}
	}
	if got := index.searchCurrentCandidatesWithLiveDocsLocked(query, norm, &prepared, 4, 3, nil); len(got) != 3 {
		t.Fatalf("nil scratch lost routes: %v", got)
	}
	scratch.startScoreTracking(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scratch.setContext(ctx)
	if _, err := index.searchGraphOnlyCandidatesWithLiveDocsLocked(query, 3, 4, 3, &scratch); !errors.Is(err, context.Canceled) || scratch.scoreCalls != 0 {
		t.Fatalf("canceled search scored: err=%v scores=%d", err, scratch.scoreCalls)
	}
	scratch.clearContext()
	scratch.startScoreTracking(0)
	if _, err := index.searchGraphOnlyCandidatesWithLiveDocsLocked(query, 1, 1, 3, &scratch); !errors.Is(err, ErrVectorIndexSearchUnavailable) || scratch.explorationLimit != 4 || scratch.scoreCalls > 4 {
		t.Fatalf("tight budget path changed: err=%v limit=%d scores=%d", err, scratch.explorationLimit, scratch.scoreCalls)
	}
	index.nodes[0].deleted = true
	scratch.startScoreTracking(0)
	if _, err := index.searchGraphOnlyCandidatesWithLiveDocsLocked(query, 2, 4, 2, &scratch); !errors.Is(err, ErrVectorIndexSearchUnavailable) || scratch.visitedEpochs[16] == scratch.visitedEpoch {
		t.Fatalf("deleted original entry incorrectly admitted its neighborhood: err=%v", err)
	}
}
