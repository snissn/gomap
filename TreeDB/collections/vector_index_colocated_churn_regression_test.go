package collections

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"testing"
)

// This uses production insertion and immutable publication, rather than wiring
// a connected graph by hand. It matches the RF4 cost fixture's IDs, construction
// parameters and mutations. A pass here does not establish that its failing
// public search used this plane; that still requires the public causal capture.
func TestNativePublishedSameIDChurnRestoresAllLiveCandidatesV1(t *testing.T) {
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
		index.mu.Lock()
		defer index.mu.Unlock()
		if err := index.insertVectorLocked([]byte(id), vector); err != nil {
			t.Fatalf("insert %q: %v", id, err)
		}
		index.acknowledgeSearchViewStateLocked()
		index.publishSearchViewLocked(false)
	}
	insert("base-x", []float32{1, 0})
	insert("base-minus-x", []float32{-1, 0})
	insert("base-minus-y", []float32{0, -1})
	for i := 0; i < 32; i++ {
		vector := []float32{0, 1}
		if i%2 != 0 {
			vector = []float32{1, 0}
		}
		insert("base-minus-x", vector)
	}
	logColocatedChurnGraphV1(t, "before_restore", index, []float32{-1, 0})
	insert("base-minus-x", []float32{-1, 0})
	logColocatedChurnGraphV1(t, "after_restore", index, []float32{-1, 0})
	view := index.acquireSearchView()
	if view == nil {
		t.Fatal("restore has no published view")
	}
	defer index.releaseSearchView(view)
	index.mu.RLock()
	if len(index.nodes) != 36 || len(index.currentNode) != 3 ||
		!reflect.DeepEqual(snapshotVectorIndexTopology4257(index), snapshotVectorIndexTopology4257(&VectorIndex{nodes: view.nodes, entry: view.entry, maxLevel: view.maxLevel})) ||
		!reflect.DeepEqual(index.currentNode, view.currentNode) {
		index.mu.RUnlock()
		t.Fatal("published topology/current ordinals differ from the restored mutable graph")
	}
	index.mu.RUnlock()
	canonical, err := NewCanonicalVectorPartitionCosineScorerV1([]float32{-1, 0})
	if err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	buffer.nativeSearchWorkEnabled = true
	buffer.nativeSearchScratch.context = context.Background()
	got, err := view.searchGraphOnlyWithCanonicalScoreBudget([]float32{-1, 0}, 3, 16, 0, canonical, &buffer)
	t.Logf("published search: err=%v work=%+v scratch_explored=%d scratch_limit=%d scratch_candidates=%v", err, buffer.nativeSearchWork, buffer.nativeSearchScratch.explored, buffer.nativeSearchScratch.explorationLimit, buffer.nativeSearchScratch.out)
	if err != nil {
		t.Fatalf("first restored published TopK3/Ef16 search: %v", err)
	}
	want := []struct {
		id    string
		score float32
	}{{"base-minus-x", 1}, {"base-minus-y", 0}, {"base-x", -1}}
	if len(got) != len(want) {
		t.Fatalf("restored candidates=%v want all three live IDs", got)
	}
	for i, result := range got {
		if string(result.ID) != want[i].id || math.Float32bits(float32(result.Score)) != math.Float32bits(want[i].score) {
			t.Fatalf("restored candidate[%d]=%+v want ID=%s score=%g", i, result, want[i].id, want[i].score)
		}
	}
}

// The small-graph traversal below is diagnostic only. It never supplies search
// results or changes any adjacency, entry, construction or serving budget.
func logColocatedChurnGraphV1(t *testing.T, phase string, index *VectorIndex, query []float32) {
	t.Helper()
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.nodes) > 36 {
		t.Fatal("churn diagnostic exceeded the declared small graph")
	}
	rows := make([]map[string]any, len(index.nodes))
	for ordinal, node := range index.nodes {
		bits := make([]uint32, len(node.vector))
		for i, value := range node.vector {
			bits[i] = math.Float32bits(value)
		}
		current, known := index.currentNode[string(node.documentID)]
		neighbors := make([][]map[string]any, len(node.neighbors))
		for layer, edges := range node.neighbors {
			if len(edges) > index.maxNeighborsForLayer(layer) {
				t.Fatalf("ordinal=%d layer=%d degree=%d exceeds construction bound", ordinal, layer, len(edges))
			}
			for _, edge := range edges {
				if int(edge.nodeID) >= len(index.nodes) || math.IsNaN(float64(edge.distance)) || math.IsInf(float64(edge.distance), 0) {
					t.Fatalf("ordinal=%d layer=%d invalid edge=%+v", ordinal, layer, edge)
				}
				neighbors[layer] = append(neighbors[layer], map[string]any{"ordinal": edge.nodeID, "distance_bits": math.Float32bits(edge.distance)})
			}
		}
		rows[ordinal] = map[string]any{"ordinal": ordinal, "id": string(node.documentID), "deleted": node.deleted, "current": known && current == ordinal, "level": node.level, "vector_bits": bits, "neighbors": neighbors}
	}
	norm, prepared, _, err := prepareVectorIndexGraphOnlyQuery(query, index.metric, index.dimensions)
	if err != nil {
		t.Fatal(err)
	}
	limit := minInt(16, len(index.nodes))
	stale := len(index.nodes) - len(index.currentNode)
	explorationLimit := limit + minInt(stale, minInt(limit*(index.maxNeighborsForLayer(0)-1), len(index.nodes)-limit))
	upperLimit := maxInt(0, explorationLimit-limit-1)
	entry, upperExplored := index.entry, 0
	var upperScratch vectorIndexSearchScratch
	upperScratch.startScoreTracking(0)
	for layer := index.maxLevel; layer > 0 && upperExplored < upperLimit; layer-- {
		entry = index.greedyNearestAtLayerSearchBoundedLocked(query, norm, &prepared, entry, layer, upperLimit, &upperExplored, &upperScratch)
	}
	reachable := func(start int) ([]int, []string) {
		seen := make([]bool, len(index.nodes))
		queue := []int{start}
		ordinals := []int{}
		for len(queue) != 0 {
			ordinal := queue[0]
			queue = queue[1:]
			if ordinal < 0 || ordinal >= len(seen) || seen[ordinal] {
				continue
			}
			seen[ordinal] = true
			ordinals = append(ordinals, ordinal)
			for _, edge := range index.layerNeighborsLocked(ordinal, 0) {
				queue = append(queue, int(edge.nodeID))
			}
		}
		sort.Ints(ordinals)
		missing := []string{}
		for id, ordinal := range index.currentNode {
			if !seen[ordinal] {
				missing = append(missing, id)
			}
		}
		sort.Strings(missing)
		return ordinals, missing
	}
	fromEntry, missingEntry := reachable(index.entry)
	fromLayerZero, missingLayerZero := reachable(entry)
	var scratch vectorIndexSearchScratch
	scratch.startScoreTracking(0)
	candidates, searchErr := index.searchGraphOnlyCandidatesWithLiveDocsLocked(query, 3, 16, len(index.currentNode), &scratch)
	visited := 0
	for _, epoch := range scratch.visitedEpochs {
		if epoch == scratch.visitedEpoch {
			visited++
		}
	}
	stop := "bounded_frontier_or_distance_stop"
	if len(missingLayerZero) != 0 {
		stop = "current_live_nodes_outside_directed_reachability"
	} else if scratch.explored >= scratch.explorationLimit {
		stop = "exploration_allowance_reached"
	}
	// The production kernel clears queue/best/liveBest lengths before returning;
	// scratch.out and visited epochs retain the discovered candidate evidence.
	encodeCandidates := func(values []vectorIndexCandidate) []map[string]any {
		out := make([]map[string]any, len(values))
		for i, candidate := range values {
			out[i] = map[string]any{"ordinal": candidate.nodeID, "distance_bits": math.Float32bits(candidate.distance)}
		}
		return out
	}
	raw, err := json.Marshal(map[string]any{"phase": phase, "entry": index.entry, "max_level": index.maxLevel, "current_ordinals": index.currentNode, "physical": len(index.nodes), "live": len(index.currentNode), "nodes": rows, "layer_zero_start": entry, "upper_explored": upperExplored, "upper_scores": upperScratch.scoreCalls, "upper_limit": upperLimit, "reachable_from_entry": fromEntry, "missing_from_entry": missingEntry, "reachable_from_layer_zero": fromLayerZero, "missing_from_layer_zero": missingLayerZero, "explored": scratch.explored, "exploration_limit": scratch.explorationLimit, "visited_epoch_count": visited, "score_calls": scratch.scoreCalls, "retained_frontier_candidates": encodeCandidates(scratch.out), "returned_candidates": encodeCandidates(candidates), "search_error": errorStringColocatedChurnV1(searchErr), "diagnostic_classification": stop})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("colocated_churn_topology=%s", raw)
}

func errorStringColocatedChurnV1(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
