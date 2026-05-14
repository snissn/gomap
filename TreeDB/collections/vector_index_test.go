package collections

import (
	"fmt"
	"math"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionVectorIndexSearchReranksCanonicalRows(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"tag":"keep"}`),
			[]byte(`{"embedding":[0,1],"tag":"keep"}`),
			[]byte(`{"embedding":[0.9,0.1],"tag":"keep"}`),
			[]byte(`{"embedding":[0,0.8],"tag":"keep"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      4,
	})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}

	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
	if trace.Strategy != "ann_graph" || trace.CandidatesExamined == 0 || trace.RerankCount == 0 || trace.ReturnedCount != 2 {
		t.Fatalf("unexpected trace: %+v", trace)
	}
	stats := index.Stats()
	if stats.LiveDocs != 4 || stats.DeletedDocs != 0 || stats.Dimensions != 2 || stats.AvgDegree == 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestVectorIndexPruneLayerNeighborsUsesDistanceThenDocumentID(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("from"), vector: []float32{1, 0}},
		{documentID: []byte("b"), vector: []float32{0.8, 0.2}},
		{documentID: []byte("a"), vector: []float32{0.8, 0.2}},
		{documentID: []byte("far"), vector: []float32{0, 1}},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}

	got := index.pruneLayerNeighborsLocked(0, []int{3, 1, 2}, 2)
	if len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("pruned neighbors=%v want [2 1]", got)
	}
}

func TestVectorIndexSelectLayerNeighborsReusesCandidateDistances(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("query"), vector: []float32{1, 0}, level: 0},
		{documentID: []byte("slow"), vector: []float32{1, 0}, level: 0},
		{documentID: []byte("fast"), vector: []float32{0, 1}, level: 0},
		{documentID: []byte("middle"), vector: []float32{0.5, 0.5}, level: 0},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}

	got := index.selectLayerNeighborsLocked(
		[]float32{1, 0},
		1,
		nil,
		[]vectorIndexCandidate{
			{nodeID: 1, distance: 0.9},
			{nodeID: 2, distance: 0.1},
			{nodeID: 3, distance: 0.2},
		},
		0,
		2,
		0,
	)
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("selected neighbors=%v want [2 3]", got)
	}
}

func TestVectorIndexInsertCachesStoredNorm(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if err := index.insertVectorLocked([]byte("a"), []float32{3, 4}); err != nil {
		t.Fatalf("insert vector: %v", err)
	}
	if got, want := index.nodes[0].normSquared, float64(25); got != want {
		t.Fatalf("node cached norm=%v want %v", got, want)
	}
	if got, want := index.nodes[0].cachedInvNorm, float32(0.2); got != want {
		t.Fatalf("node cached inverse norm=%v want %v", got, want)
	}
}

func TestVectorIndexInsertCachesInt8StoredNorm(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:     "embedding",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if err := index.insertVectorLocked([]byte("a"), []float32{3, 4}); err != nil {
		t.Fatalf("insert vector: %v", err)
	}
	node := index.nodes[0]
	if node.normSquared == 0 {
		t.Fatal("node cached norm is zero")
	}
	if got, want := node.normSquared, node.storedNormSquared(); got != want {
		t.Fatalf("node cached norm=%v want dequantized norm %v", got, want)
	}
}

func TestVectorIndexFloat32CosineSpecializationMatchesExactDistance(t *testing.T) {
	query := []float32{0.25, -0.5, 1.25, 2}
	node := vectorIndexNode{documentID: []byte("right"), vector: []float32{1.5, -0.25, 0.75, 3}}
	node.cacheVectorNorms()
	queryNorm := vectorNormSquared(query)
	prepared, err := prepareFloat32CosineQuery(query, queryNorm)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}

	gotQuery, err := vectorDistanceToFloat32NodeCosine(query, queryNorm, &node)
	if err != nil {
		t.Fatalf("specialized query distance: %v", err)
	}
	wantQuery, err := exactVectorDistance(query, node.vector, VectorMetricCosine)
	if err != nil {
		t.Fatalf("exact query distance: %v", err)
	}
	if math.Abs(float64(gotQuery-wantQuery)) > 1e-6 {
		t.Fatalf("specialized query distance=%v want %v", gotQuery, wantQuery)
	}
	gotPrepared, err := vectorDistanceToFloat32NodeCosinePrepared(prepared, &node)
	if err != nil {
		t.Fatalf("prepared query distance: %v", err)
	}
	if math.Abs(float64(gotPrepared-wantQuery)) > 1e-6 {
		t.Fatalf("prepared query distance=%v want %v", gotPrepared, wantQuery)
	}
	gotUnchecked := vectorDistanceToFloat32NodeCosineUnchecked(prepared, &node)
	if math.Abs(float64(gotUnchecked-wantQuery)) > 1e-6 {
		t.Fatalf("unchecked query distance=%v want %v", gotUnchecked, wantQuery)
	}

	left := vectorIndexNode{documentID: []byte("left"), vector: query}
	left.cacheVectorNorms()
	gotBetween, err := vectorDistanceBetweenFloat32NodesCosine(&left, &node)
	if err != nil {
		t.Fatalf("specialized node distance: %v", err)
	}
	wantBetween, err := exactVectorDistance(left.vector, node.vector, VectorMetricCosine)
	if err != nil {
		t.Fatalf("exact node distance: %v", err)
	}
	if math.Abs(float64(gotBetween-wantBetween)) > 1e-6 {
		t.Fatalf("specialized node distance=%v want %v", gotBetween, wantBetween)
	}
}

func TestPrepareFloat32CosineQueryCachesInverseNorm(t *testing.T) {
	query := []float32{3, 4}
	prepared, err := prepareFloat32CosineQuery(query, -1)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}
	if &prepared.vector[0] != &query[0] {
		t.Fatal("prepared query copied vector")
	}
	if got, want := prepared.invNorm, float32(0.2); got != want {
		t.Fatalf("prepared inverse norm=%v want %v", got, want)
	}
	if _, err := prepareFloat32CosineQuery([]float32{0, 0}, -1); err == nil {
		t.Fatal("prepare zero cosine query succeeded")
	}
}

func TestVectorIndexSearchLayerScratchReusesBuffers(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	index.nodes = []vectorIndexNode{
		{documentID: []byte("a"), vector: []float32{1, 0}, neighbors: [][]int{{1, 2}}},
		{documentID: []byte("b"), vector: []float32{0.9, 0.1}, neighbors: [][]int{{0, 2}}},
		{documentID: []byte("c"), vector: []float32{0, 1}, neighbors: [][]int{{0, 1}}},
	}
	for i := range index.nodes {
		index.nodes[i].cacheVectorNorms()
	}
	var scratch vectorIndexSearchScratch
	query := []float32{1, 0}
	queryNorm := vectorNormSquared(query)
	prepared, err := prepareFloat32CosineQuery(query, queryNorm)
	if err != nil {
		t.Fatalf("prepare query: %v", err)
	}

	first := index.searchLayerWithScratchLocked(query, queryNorm, &prepared, 0, 3, 0, &scratch)
	if len(first) != 3 || first[0].nodeID != 0 || first[1].nodeID != 1 || first[2].nodeID != 2 {
		t.Fatalf("first candidates=%v want node IDs [0 1 2]", first)
	}
	if len(scratch.visitedEpochs) != len(index.nodes) || cap(scratch.queue) == 0 || cap(scratch.best) == 0 || cap(scratch.out) == 0 {
		t.Fatalf("scratch was not populated: visited=%d queue_cap=%d best_cap=%d out_cap=%d", len(scratch.visitedEpochs), cap(scratch.queue), cap(scratch.best), cap(scratch.out))
	}
	visited := &scratch.visitedEpochs[0]
	queueCap := cap(scratch.queue)
	bestCap := cap(scratch.best)
	outCap := cap(scratch.out)

	second := index.searchLayerWithScratchLocked(query, queryNorm, &prepared, 0, 3, 0, &scratch)
	if len(second) != len(first) {
		t.Fatalf("second candidates=%v want len %d", second, len(first))
	}
	if &scratch.visitedEpochs[0] != visited || cap(scratch.queue) != queueCap || cap(scratch.best) != bestCap || cap(scratch.out) != outCap {
		t.Fatal("scratch buffers were not reused")
	}
}

func TestVectorIndexSearchScratchVisitedEpochsGrowGeometrically(t *testing.T) {
	var scratch vectorIndexSearchScratch

	scratch.nextVisitedEpoch(64)
	if cap(scratch.visitedEpochs) != 64 {
		t.Fatalf("initial visited cap=%d want 64", cap(scratch.visitedEpochs))
	}
	scratch.nextVisitedEpoch(65)
	if cap(scratch.visitedEpochs) != 128 {
		t.Fatalf("visited cap after one-node growth=%d want 128", cap(scratch.visitedEpochs))
	}
	visited := &scratch.visitedEpochs[0]
	for nodes := 66; nodes <= 128; nodes++ {
		scratch.nextVisitedEpoch(nodes)
		if &scratch.visitedEpochs[0] != visited {
			t.Fatalf("visited epochs reallocated at nodes=%d before capacity was exhausted", nodes)
		}
	}
}

func TestCollectionVectorIndexInt8EncodingReranksCanonicalRows(t *testing.T) {
	const (
		docs = 24
		dims = 96
		topK = 5
	)
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	ids := make([][]byte, 0, docs)
	documents := make([][]byte, 0, docs)
	for i := 0; i < docs; i++ {
		ids = append(ids, []byte(fmt.Sprintf("doc-%02d", i)))
		documents = append(documents, vectorBenchmarkDocument(i, dims))
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		t.Fatalf("insert: %v", err)
	}
	floatIndex, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding_f32",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      8,
	})
	if err != nil {
		t.Fatalf("build float32 vector index: %v", err)
	}
	int8Index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:     "embedding_i8",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		M:        8,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		t.Fatalf("build int8 vector index: %v", err)
	}
	query := vectorBenchmarkEmbedding(7, dims)
	exact, err := col.SearchVectorsExact(query, VectorSearchOptions{
		Field:  "embedding",
		Metric: VectorMetricCosine,
		TopK:   topK,
	})
	if err != nil {
		t.Fatalf("exact search: %v", err)
	}
	results, trace, err := int8Index.Search(query, VectorIndexSearchOptions{
		TopK:                 topK,
		EfSearch:             docs,
		FetchMultiplier:      docs,
		DisableExactFallback: true,
	})
	if err != nil {
		t.Fatalf("int8 search: %v", err)
	}
	if trace.RerankCount < topK {
		t.Fatalf("int8 search did not rerank canonical rows: %+v", trace)
	}
	for i := range exact {
		if string(results[i].DocumentID) != string(exact[i].DocumentID) {
			t.Fatalf("int8 result[%d]=%q want exact %q", i, results[i].DocumentID, exact[i].DocumentID)
		}
	}
	floatStats := floatIndex.Stats()
	int8Stats := int8Index.Stats()
	if int8Stats.Encoding != VectorIndexEncodingInt8 || int8Stats.Dimensions != dims {
		t.Fatalf("unexpected int8 stats: %+v", int8Stats)
	}
	if int8Stats.BytesMemory >= floatStats.BytesMemory {
		t.Fatalf("int8 memory bytes=%d want less than float32 %d", int8Stats.BytesMemory, floatStats.BytesMemory)
	}
	int8Index.mu.RLock()
	defer int8Index.mu.RUnlock()
	if len(int8Index.nodes) == 0 || len(int8Index.nodes[0].vector) != 0 || len(int8Index.nodes[0].quantized) != dims || int8Index.nodes[0].quantScale <= 0 {
		t.Fatalf("unexpected int8 node storage: %+v", int8Index.nodes[0])
	}
}

func TestCollectionVectorIndexSelectiveRangeFilterUsesExactFilteredStrategy(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollectionWithIndexes(t, d, IndexDefinition{Name: "city_idx", Field: "city", ValueType: IndexValueString})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"city":"hnl"}`),
			[]byte(`{"embedding":[1,0],"city":"sea"}`),
			[]byte(`{"embedding":[0.9,0.1],"city":"hnl"}`),
			[]byte(`{"embedding":[0,1],"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}

	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{
		TopK: 2,
		IndexRangeFilter: &VectorIndexRangeFilter{
			IndexName: "city_idx",
			Range: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
				Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("filtered search: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
	if trace.Strategy != "exact_filtered" || trace.CandidatesExamined != 3 || trace.RerankCount != 3 || trace.ReturnedCount != 2 {
		t.Fatalf("unexpected trace: %+v", trace)
	}
}

func TestCollectionVectorIndexBroadRangeFilterUsesANNPostFilter(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollectionWithIndexes(t, d, IndexDefinition{Name: "city_idx", Field: "city", ValueType: IndexValueString})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"city":"hnl"}`),
			[]byte(`{"embedding":[0.95,0.05],"city":"hnl"}`),
			[]byte(`{"embedding":[0.9,0.1],"city":"sea"}`),
			[]byte(`{"embedding":[0,1],"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}

	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{
		TopK:                 1,
		EfSearch:             8,
		FetchMultiplier:      8,
		ExactFilterMaxDocs:   1,
		DisableExactFallback: true,
		IndexRangeFilter: &VectorIndexRangeFilter{
			IndexName: "city_idx",
			Range: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
				Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("filtered search: %v", err)
	}
	requireVectorResultIDs(t, results, "a")
	if trace.Strategy != "ann_postfilter" || trace.CandidatesAfterFilter == 0 || trace.RerankCount == 0 {
		t.Fatalf("unexpected trace: %+v", trace)
	}
}

func TestCollectionVectorIndexInsertAndTombstone(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.9,0.1]}`)},
	); err != nil {
		t.Fatalf("insert c: %v", err)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after insert: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")

	if deleted, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	results, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after tombstone: %v", err)
	}
	requireVectorResultIDs(t, results, "c", "b")
	stats := index.Stats()
	if stats.LiveDocs != 2 || stats.DeletedDocs != 1 || !stats.RebuildNeeded {
		t.Fatalf("unexpected post-tombstone stats: %+v", stats)
	}
}

func TestCollectionVectorIndexTracksRegisteredMutations(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.9,0.1]}`)},
	); err != nil {
		t.Fatalf("insert tracked c: %v", err)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after tracked insert: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")

	replaced, err := col.Replace([]byte("b"), []byte(`{"embedding":[0.95,0.05]}`))
	if err != nil || !replaced {
		t.Fatalf("replace b replaced=%v err=%v", replaced, err)
	}
	results, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after tracked replace: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b", "c")

	deleted, err := col.DeleteBatch([][]byte{[]byte("a")})
	if err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	results, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after tracked delete: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "c")
	stats := index.Stats()
	if stats.LiveDocs != 2 || stats.DeletedDocs != 2 {
		t.Fatalf("tracked mutation stats=%+v want live=2 deleted=2", stats)
	}
}

func TestCollectionVectorIndexRebuildRemovesTombstones(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	index.TombstoneDocumentID([]byte("a"))
	if stats := index.Stats(); stats.DeletedDocs != 1 || !stats.RebuildNeeded {
		t.Fatalf("pre-rebuild stats=%+v want tombstone and rebuild-needed", stats)
	}
	if err := index.Rebuild(); err != nil {
		t.Fatalf("rebuild vector index: %v", err)
	}
	stats := index.Stats()
	if stats.LiveDocs != 2 || stats.DeletedDocs != 0 || stats.RebuildNeeded {
		t.Fatalf("post-rebuild stats=%+v want live=2 deleted=0 rebuild=false", stats)
	}
	if stats.LastRebuildDuration <= 0 {
		t.Fatalf("post-rebuild stats missing rebuild duration: %+v", stats)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search rebuilt index: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "c")

	if _, err := col.InsertBatch(
		[][]byte{[]byte("d")},
		[][]byte{[]byte(`{"embedding":[0.99,0.01]}`)},
	); err != nil {
		t.Fatalf("insert after rebuild: %v", err)
	}
	results, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after post-rebuild insert: %v", err)
	}
	requireVectorResultIDs(t, results, "d", "b")
}

func TestCollectionVectorIndexUpdateDocumentReplacesCurrentNode(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	replaced, err := col.Replace([]byte("b"), []byte(`{"embedding":[0.95,0.05]}`))
	if err != nil || !replaced {
		t.Fatalf("replace b replaced=%v err=%v", replaced, err)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after update: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
	stats := index.Stats()
	if stats.LiveDocs != 2 || stats.DeletedDocs != 1 {
		t.Fatalf("unexpected stats after update: %+v", stats)
	}
}

func TestCollectionVectorIndexUnderfillFallsBackToExact(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 1, EfSearch: 1})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	results, trace, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 4, EfSearch: 1, FetchMultiplier: 1})
	if err != nil {
		t.Fatalf("search with fallback: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b", "c")
	if trace.Strategy != "ann_graph_exact_fallback" || trace.ExactFallbackReason == "" {
		t.Fatalf("expected exact fallback trace, got %+v", trace)
	}
}

func TestCollectionVectorIndexCheckRecall(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	recall, err := index.CheckRecall([][]float32{{1, 0}, {0, 1}}, VectorIndexSearchOptions{TopK: 1})
	if err != nil {
		t.Fatalf("check recall: %v", err)
	}
	if recall.Queries != 2 || recall.ExactTotal != 2 || recall.Overlap != 2 || recall.Recall != 1 {
		t.Fatalf("unexpected recall: %+v", recall)
	}
}

func TestCollectionVectorIndexCheckRecallUsesIndexRangeFilter(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollectionWithIndexes(t, d, IndexDefinition{Name: "city_idx", Field: "city", ValueType: IndexValueString})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"city":"hnl"}`),
			[]byte(`{"embedding":[0.9,0.1],"city":"hnl"}`),
			[]byte(`{"embedding":[0.8,0.2],"city":"sea"}`),
			[]byte(`{"embedding":[0,1],"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	recall, err := index.CheckRecall([][]float32{{1, 0}}, VectorIndexSearchOptions{
		TopK: 2,
		IndexRangeFilter: &VectorIndexRangeFilter{
			IndexName: "city_idx",
			Range: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
				Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("check filtered recall: %v", err)
	}
	if recall.ExactTotal != 2 || recall.ANNTotal != 2 || recall.Overlap != 2 || recall.Recall != 1 {
		t.Fatalf("filtered recall used wrong baseline: %+v", recall)
	}
	if len(recall.SearchTraces) != 1 || recall.SearchTraces[0].Strategy != "exact_filtered" {
		t.Fatalf("unexpected filtered recall traces: %+v", recall.SearchTraces)
	}
}

func openVectorIndexTestCollection(tb testing.TB, d *backenddb.DB) *Collection {
	tb.Helper()
	return openVectorIndexTestCollectionWithIndexes(tb, d)
}

func openVectorIndexTestCollectionWithIndexes(tb testing.TB, d *backenddb.DB, indexes ...IndexDefinition) *Collection {
	tb.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Indexes: indexes}); err != nil {
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		tb.Fatalf("open collection: %v", err)
	}
	return col
}
