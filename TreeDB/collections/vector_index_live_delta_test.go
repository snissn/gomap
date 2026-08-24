package collections

import (
	"testing"
	"time"
)

func TestVectorIndexLiveDeltaStatsIncludesDeltaMaxLevel(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(true)
	index.sourceDocumentRootsValid = true
	index.mu.Lock()
	index.publishSearchViewLocked(false)
	if err := index.insertLiveVectorBatchLocked([][]byte{[]byte("a")}, [][]float32{{1, 0}}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.mu.Unlock()
	if stats := index.Stats(); stats.Nodes != 1 || stats.MaxLevel < 0 {
		t.Fatalf("live delta stats=%+v want one node with a nonnegative max level", stats)
	}
}

func TestVectorIndexLiveDeltaVisibilityShadowingAndCutover(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(true)
	index.sourceDocumentRootsValid = true
	index.mu.Lock()
	if err := index.insertVectorBatchLocked([][]byte{[]byte("a"), []byte("b")}, [][]float32{{1, 0}, {0, 1}}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.publishSearchViewLocked(false)
	index.mu.Unlock()

	var buffer VectorIndexSearchBuffer
	assertVectorIndexLiveDeltaIDs(t, index, []float32{1, 0}, 2, &buffer, "a", "b")
	if !index.liveDeltaEnabled.Load() {
		t.Fatal("first native search did not seal the base")
	}
	index.mu.Lock()
	if err := index.insertLiveVectorBatchLocked([][]byte{[]byte("a")}, [][]float32{{0, 0}}); err == nil {
		index.mu.Unlock()
		t.Fatal("invalid live update succeeded")
	}
	index.mu.Unlock()
	assertVectorIndexLiveDeltaIDs(t, index, []float32{1, 0}, 2, &buffer, "a", "b")

	index.mu.Lock()
	if err := index.insertLiveVectorBatchLocked([][]byte{[]byte("c")}, [][]float32{{0.8, 0.2}}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.publishSearchViewLocked(false)
	baseNodes := len(index.nodes)
	index.mu.Unlock()
	assertVectorIndexLiveDeltaIDs(t, index, []float32{1, 0}, 3, &buffer, "a", "c", "b")
	if stats := index.Stats(); stats.LiveDeltaDocs != 1 || stats.LiveDocs != 3 || stats.LiveDeltaCutovers != 0 {
		t.Fatalf("live delta stats=%+v", stats)
	}

	index.mu.Lock()
	if err := index.insertLiveVectorBatchLocked([][]byte{[]byte("a")}, [][]float32{{-1, 0}}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.tombstoneLiveDocumentLocked([]byte("c"))
	index.publishSearchViewLocked(false)
	if len(index.nodes) != baseNodes {
		index.mu.Unlock()
		t.Fatalf("live writes mutated base size=%d want %d", len(index.nodes), baseNodes)
	}
	retired := index.acquireSearchView()
	index.mu.Unlock()
	assertVectorIndexLiveDeltaIDs(t, index, []float32{1, 0}, 2, &buffer, "b", "a")

	cutoverDone := make(chan error, 1)
	go func() {
		index.mu.Lock()
		err := index.foldLiveDeltaLocked()
		if err == nil {
			index.publishSearchViewLocked(false)
		}
		index.mu.Unlock()
		cutoverDone <- err
	}()
	select {
	case err := <-cutoverDone:
		if err != nil {
			t.Fatalf("cutover: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cutover blocked on an active immutable reader")
	}
	index.releaseSearchView(retired)
	assertVectorIndexLiveDeltaIDs(t, index, []float32{1, 0}, 2, &buffer, "b", "a")
	if stats := index.Stats(); stats.LiveDeltaDocs != 0 || stats.LiveDocs != 2 || stats.LiveDeltaCutovers != 1 {
		t.Fatalf("cutover stats=%+v", stats)
	}
}

func TestVectorIndexLiveDeltaBoundsAllocatedNodesWithoutPartialPublication(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(true)
	index.sourceDocumentRootsValid = true
	index.mu.Lock()
	if err := index.insertVectorBatchLocked([][]byte{[]byte("a")}, [][]float32{{1, 0}}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.publishSearchViewLocked(false)
	published := index.searchView.Load()
	if err := index.insertLiveVectorBatchLocked([][]byte{[]byte("a")}, [][]float32{{0, 1}}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	delta := index.liveDelta
	delta.nodes = append(delta.nodes, make([]vectorIndexNode, defaultVectorIndexLiveDeltaRows-len(delta.nodes)-1)...)
	for nodeID := 1; nodeID < len(delta.nodes); nodeID++ {
		delta.nodes[nodeID].deleted = true
	}
	if err := index.insertLiveVectorBatchLocked([][]byte{[]byte("a")}, [][]float32{{-1, 0}}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	if index.liveDelta != nil || index.liveDeltaCutovers != 1 {
		index.mu.Unlock()
		t.Fatalf("delta=%v cutovers=%d want nil,1", index.liveDelta != nil, index.liveDeltaCutovers)
	}
	if index.searchView.Load() != published {
		index.mu.Unlock()
		t.Fatal("cutover published a partially reconciled batch")
	}
	index.publishSearchViewLocked(false)
	index.mu.Unlock()
}

func TestVectorIndexLiveDeltaFoldValidationIsAtomic(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(true)
	index.sourceDocumentRootsValid = true
	index.mu.Lock()
	defer index.mu.Unlock()
	if err := index.insertVectorBatchLocked([][]byte{[]byte("a")}, [][]float32{{1, 0}}); err != nil {
		t.Fatal(err)
	}
	index.publishSearchViewLocked(false)
	if err := index.insertLiveVectorBatchLocked(
		[][]byte{[]byte("b"), []byte("c")},
		[][]float32{{0, 1}, {-1, 0}},
	); err != nil {
		t.Fatal(err)
	}
	delta := index.liveDelta
	baseNodes := len(index.nodes)
	delta.nodes[delta.currentNode["c"]].vector = []float32{-1}
	if err := index.foldLiveDeltaLocked(); err == nil {
		t.Fatal("fold with corrupt final vector succeeded")
	}
	if len(index.nodes) != baseNodes || index.liveDelta != delta || len(delta.currentNode) != 2 {
		t.Fatalf("failed fold mutated base/delta: base=%d delta_same=%v live=%d", len(index.nodes), index.liveDelta == delta, len(delta.currentNode))
	}
}

func TestVectorIndexLiveDeltaSearchBudget(t *testing.T) {
	for _, test := range []struct {
		requested, deltaDocs, totalDocs, want int
	}{
		{100, 2_000, 12_000, 17},
		{100, 32_000, 1_000_000, 16},
		{16, 32_000, 1_000_000, 16},
		{100, 500_000, 1_000_000, 50},
	} {
		if got := vectorIndexLiveDeltaSearchBudget(test.requested, test.deltaDocs, test.totalDocs); got != test.want {
			t.Fatalf("budget(%d, %d, %d)=%d want %d", test.requested, test.deltaDocs, test.totalDocs, got, test.want)
		}
	}
}

func TestMergeVectorIndexViewResultsDeduplicatesExhaustedPlanes(t *testing.T) {
	base := []VectorIndexSearchResult{{ID: []byte("a"), Score: 0.9}, {ID: []byte("b"), Score: 0.7}}
	delta := []VectorIndexSearchResult{{ID: []byte("a"), Score: 0.8}}
	var buffer VectorIndexSearchBuffer
	got, err := mergeVectorIndexViewResults(base, delta, 4, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].ID) != "a" || string(got[1].ID) != "b" {
		t.Fatalf("merged results=%+v want unique a,b", got)
	}
}

func TestVectorIndexLiveDeltaExpandsForClusteredResults(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 16, EfConstruction: 128, EfSearch: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(true)
	index.sourceDocumentRootsValid = true
	baseIDs, baseVectors := make([][]byte, 200), make([][]float32, 200)
	deltaIDs, deltaVectors := make([][]byte, 200), make([][]float32, 200)
	for row := range baseIDs {
		baseIDs[row] = []byte("base-" + string(rune(row)))
		baseVectors[row] = []float32{0.01 + float32(row)/100_000, 1}
		deltaIDs[row] = []byte("delta-" + string(rune(row)))
		deltaVectors[row] = []float32{1, 0.01 + float32(row)/100_000}
	}
	index.mu.Lock()
	if err := index.insertVectorBatchLocked(baseIDs, baseVectors); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.publishSearchViewLocked(false)
	index.mu.Unlock()
	var buffer VectorIndexSearchBuffer
	if _, _, err := index.searchGraphOnlyWithBuffer([]float32{1, 0}, 100, 200, &buffer); err != nil {
		t.Fatal(err)
	}
	index.mu.Lock()
	if err := index.insertLiveVectorBatchLocked(deltaIDs, deltaVectors); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.publishSearchViewLocked(false)
	index.mu.Unlock()
	results, _, err := index.searchGraphOnlyWithBuffer([]float32{1, 0}, 100, 200, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 100 {
		t.Fatalf("results=%d want 100", len(results))
	}
	for _, result := range results {
		if len(result.ID) < len("delta-") || string(result.ID[:len("delta-")]) != "delta-" {
			t.Fatalf("clustered delta result=%q want delta prefix", result.ID)
		}
	}
}

func assertVectorIndexLiveDeltaIDs(t *testing.T, index *VectorIndex, query []float32, topK int, buffer *VectorIndexSearchBuffer, want ...string) {
	t.Helper()
	got, _, err := index.searchGraphOnlyWithBuffer(query, topK, 16, buffer)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("results=%+v want IDs %v", got, want)
	}
	for resultIndex := range want {
		if string(got[resultIndex].ID) != want[resultIndex] {
			t.Fatalf("results=%+v want IDs %v", got, want)
		}
	}
}
