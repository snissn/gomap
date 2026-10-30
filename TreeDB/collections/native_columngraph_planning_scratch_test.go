package collections

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func nativeColumnGraphScratchFixture(t *testing.T, count int, ties bool) (VectorIndexDefinition, [][]byte, [][]float32) {
	t.Helper()
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32, Dimensions: 8, M: 16, EfConstruction: 128, Strategy: VectorIndexStrategyColumnGraph}
	ids := make([][]byte, count)
	vectors := vectorIndexReciprocalParityRows4257(count, def.Dimensions, ties)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("d%06d", i))
	}
	return def, ids, vectors
}

func nativeColumnGraphScratchIndex(t *testing.T, def VectorIndexDefinition, count int) *columnGraphPartitionLiveCarrier {
	t.Helper()
	idx, err := newColumnGraphPartitionLiveDomain(nil, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	idx.parallelReciprocalLinks = true
	idx.nodes = make([]vectorIndexNode, 0, count)
	return idx
}

// The nil owner uses the original Get/Put planner. Both executions retain the
// same frozen-prefix batch cuts, reciprocal commit and observer algorithm.
func nativeColumnGraphOriginalScratchBatch(t *testing.T, idx *columnGraphPartitionLiveCarrier, ids [][]byte, vectors [][]float32) {
	t.Helper()
	ctx := context.Background()
	if err := idx.validateVectorBatchWithContext(ctx, ids, vectors); err != nil {
		t.Fatal(err)
	}
	idx.prepareSearchViewForMutationLocked()
	if len(ids) < nativeVectorFrozenPrefixBatchMinimum {
		for i := range ids {
			if err := idx.insertVectorLocked(ids[i], vectors[i]); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	if err := idx.insertVectorFrozenPrefixBatchesWithPlanningScratchLocked(ctx, ids, vectors, nil); err != nil {
		t.Fatal(err)
	}
}

func TestNativeColumnGraphPlanningScratchPreservesParallelResults(t *testing.T) {
	for _, count := range []int{1, 31, 32, 33, 258} {
		for _, ties := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows%d/ties%t", count, ties), func(t *testing.T) {
				def, ids, vectors := nativeColumnGraphScratchFixture(t, count, ties)
				bounded, original := nativeColumnGraphScratchIndex(t, def, count), nativeColumnGraphScratchIndex(t, def, count)
				bounded.decisionObserver = &vectorIndexConstructionDecisionObserverV1{}
				original.decisionObserver = &vectorIndexConstructionDecisionObserverV1{}
				nativeColumnGraphOriginalScratchBatch(t, original, ids, vectors)
				if err := bounded.insertVectorBatchWithContextLocked(context.Background(), ids, vectors); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(snapshotVectorIndexTopology4257(bounded), snapshotVectorIndexTopology4257(original)) {
					t.Fatal("bounded scratch changed adjacency or distance bits")
				}
				if !reflect.DeepEqual(snapshotVectorIndexSearches4257(t, bounded, vectors), snapshotVectorIndexSearches4257(t, original, vectors)) {
					t.Fatal("bounded scratch changed search results")
				}
				if bounded.frozenPrefixBatches != original.frozenPrefixBatches {
					t.Fatal("changed frozen-prefix batch cuts")
				}
				gotStats, wantStats := bounded.decisionObserver.snapshot(), original.decisionObserver.snapshot()
				for phase, pair := range [][2]*VectorIndexConstructionDecisionPhaseSnapshot{{&gotStats.Planning, &wantStats.Planning}, {&gotStats.Reciprocal, &wantStats.Reciprocal}} {
					// Match the maintained observer's worker-reduction contract:
					// timing and bounded pair sketches depend on scheduling.
					pair[0].ActiveWallNanos, pair[1].ActiveWallNanos = 0, 0
					pair[0].UniqueRowPairs, pair[1].UniqueRowPairs = 0, 0
					pair[0].RepeatedRowPairs, pair[1].RepeatedRowPairs = 0, 0
					pair[0].RowPairReplacements, pair[1].RowPairReplacements = 0, 0
					if !reflect.DeepEqual(*pair[0], *pair[1]) {
						t.Fatalf("observer phase%d changed", phase)
					}
				}
			})
		}
	}
}

func TestNativeColumnGraphPlanningScratchReusedAcrossBatches(t *testing.T) {
	def, ids, vectors := nativeColumnGraphScratchFixture(t, 258, false)
	idx := nativeColumnGraphScratchIndex(t, def, len(ids))
	var arena [nativeVectorFrozenPrefixBatchWidth]vectorIndexSearchScratch
	if err := idx.validateVectorBatchWithContext(context.Background(), ids, vectors); err != nil {
		t.Fatal(err)
	}
	idx.prepareSearchViewForMutationLocked()
	if err := idx.insertVectorFrozenPrefixBatchesWithPlanningScratchLocked(context.Background(), ids, vectors, &arena); err != nil {
		t.Fatal(err)
	}
	if idx.frozenPrefixBatches < 2 || idx.vectorRows == nil {
		t.Fatal("fixture did not activate ordinary parallel/matrix construction")
	}
	for worker := range arena {
		scratch := &arena[worker]
		// A worker handles at most one plan in each full batch. One plan can
		// advance at most maxLevel+1 search epochs, so this witnesses reuse
		// across batch boundaries, rather than merely across layers.
		if scratch.visitedEpoch <= uint32(idx.maxLevel+1) {
			t.Fatalf("worker%d did not reuse workspace across batches: epochs%d maxLevel%d", worker, scratch.visitedEpoch, idx.maxLevel)
		}
		if scratch.context != nil {
			t.Fatalf("worker%d retained operation context", worker)
		}
	}
	// The final one-row cut uses the independent serial fallback workspace.
	if idx.insertScratch.visitedEpoch == 0 {
		t.Fatal("fixture did not exercise serial fallback scratch")
	}
}
