package collections

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	defaultVectorBenchmarkDocs = 10000
	defaultVectorBenchmarkDims = 64
	vectorBenchmarkTopK        = 10
)

var vectorDistanceBenchmarkSink float32

func BenchmarkVectorDistanceToFloat32NodeCosinePrepared(b *testing.B) {
	docs := minInt(vectorBenchmarkDocs(b), 4096)
	dims := vectorBenchmarkDims(b)
	query := vectorBenchmarkEmbedding(docs/3, dims)
	queryNorm := vectorNormSquared(query)
	candidates := make([]vectorIndexNode, docs)
	for i := range candidates {
		candidates[i] = vectorIndexNode{
			documentID: []byte(fmt.Sprintf("doc-%06d", i)),
			vector:     vectorBenchmarkEmbedding(i, dims),
		}
		candidates[i].cacheVectorNorms()
	}
	prepared, err := prepareFloat32CosineQuery(query, queryNorm)
	if err != nil {
		b.Fatalf("prepare query: %v", err)
	}

	b.ReportMetric(float64(docs), "candidates")
	b.ReportMetric(float64(dims), "dims")
	b.Run("wrapper", func(b *testing.B) {
		var sum float32
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range candidates {
				distance, err := vectorDistanceToFloat32NodeCosine(query, queryNorm, &candidates[j])
				if err != nil {
					b.Fatalf("distance: %v", err)
				}
				sum += distance
			}
		}
		vectorDistanceBenchmarkSink = sum
	})
	b.Run("prepared_unchecked", func(b *testing.B) {
		var sum float32
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range candidates {
				sum += vectorDistanceToFloat32NodeCosineUnchecked(prepared, &candidates[j])
			}
		}
		vectorDistanceBenchmarkSink = sum
	})
}

func BenchmarkCollectionVectorSearchExact(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)

	b.ReportMetric(float64(docs), "docs/search")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := col.SearchVectorsExact(query, VectorSearchOptions{
			Field:  "embedding",
			Metric: VectorMetricCosine,
			TopK:   vectorBenchmarkTopK,
		})
		if err != nil {
			b.Fatalf("exact vector search: %v", err)
		}
		if len(results) != vectorBenchmarkTopK {
			b.Fatalf("exact result count=%d want %d", len(results), vectorBenchmarkTopK)
		}
	}
}

func BenchmarkCollectionVectorIndexBuild(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		d, col := openVectorBenchmarkCollection(b, docs, dims)
		b.StartTimer()
		index, err := col.BuildVectorIndex(VectorIndexOptions{
			Name:   "embedding_build",
			Field:  "embedding",
			Metric: VectorMetricCosine,
			M:      16,
		})
		if err != nil {
			_ = d.Close()
			b.Fatalf("build vector index: %v", err)
		}
		stats := index.Stats()
		if stats.LiveDocs != docs {
			_ = d.Close()
			b.Fatalf("built index live docs=%d want %d", stats.LiveDocs, docs)
		}
		b.ReportMetric(float64(stats.BytesMemory), "index_bytes")
		b.StopTimer()
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
}

func BenchmarkCollectionVectorIndexBuildInt8(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		d, col := openVectorBenchmarkCollection(b, docs, dims)
		b.StartTimer()
		index, err := col.BuildVectorIndex(VectorIndexOptions{
			Name:     "embedding_build_i8",
			Field:    "embedding",
			Metric:   VectorMetricCosine,
			M:        16,
			Encoding: VectorIndexEncodingInt8,
		})
		if err != nil {
			_ = d.Close()
			b.Fatalf("build int8 vector index: %v", err)
		}
		stats := index.Stats()
		if stats.LiveDocs != docs {
			_ = d.Close()
			b.Fatalf("built int8 index live docs=%d want %d", stats.LiveDocs, docs)
		}
		b.ReportMetric(float64(stats.BytesMemory), "index_bytes")
		b.StopTimer()
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
}

func BenchmarkCollectionVectorIndexNativeRootLoad(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	dir := b.TempDir()
	def := VectorIndexDefinition{
		Name:       "embedding_native",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: dims,
		M:          16,
	}
	d, col := openVectorBenchmarkCollectionWithVectorIndex(b, dir, docs, dims, def)
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		_ = d.Close()
		b.Fatalf("build native vector index: %v", err)
	}
	status, err := index.SaveSnapshot()
	if err != nil {
		_ = d.Close()
		b.Fatalf("save native vector index: %v", err)
	}
	if !status.Loaded || status.RootID == 0 {
		_ = d.Close()
		b.Fatalf("unexpected native save status: %+v", status)
	}
	if err := d.Close(); err != nil {
		b.Fatalf("close setup db: %v", err)
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(status.BytesDisk), "native_root_bytes")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			b.Fatalf("reopen db: %v", err)
		}
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			_ = d.Close()
			b.Fatalf("open collection: %v", err)
		}
		loaded, loadStatus, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			_ = d.Close()
			b.Fatalf("load native vector index: %v", err)
		}
		if loaded == nil || !loadStatus.Loaded || loadStatus.RootID != status.RootID {
			_ = d.Close()
			b.Fatalf("unexpected native load status loaded=%v status=%+v", loaded != nil, loadStatus)
		}
		if stats := loaded.Stats(); stats.LiveDocs != docs {
			_ = d.Close()
			b.Fatalf("loaded live docs=%d want %d", stats.LiveDocs, docs)
		}
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
}

func BenchmarkCollectionVectorIndexNativeRootGraphOnlySearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, loaded, status := openLoadedNativeVectorBenchmarkIndex(b, docs, dims, "embedding_graph_only")
	defer func() { _ = d.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := loaded.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm native graph-only search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm native graph-only search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(status.BytesDisk), "native_root_bytes")
	b.ReportMetric(float64(loaded.Stats().BytesMemory), "index_bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := loaded.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			b.Fatalf("native graph-only vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("native graph-only vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexNativeRootBackedGraphOnlySearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	indexName := "embedding_graph_root_backed"
	d, col, status, setupStats := openNativeVectorBenchmarkIndexRoot(b, docs, dims, indexName)
	defer func() { _ = d.Close() }()

	reader := newVectorIndexNativeRootBackedGraphReader(b, d, col, indexName, setupStats.Nodes)
	defer func() { _ = reader.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := reader.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm native root-backed graph-only search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm native root-backed graph-only search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(status.BytesDisk), "native_root_bytes")
	b.ReportMetric(float64(setupStats.BytesMemory), "loaded_index_bytes")
	b.ReportAllocs()
	reader.resetCounters()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := reader.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			b.Fatalf("native root-backed graph-only vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("native root-backed graph-only vector search returned no results")
		}
	}
	b.StopTimer()
	if b.N > 0 {
		b.ReportMetric(float64(reader.nodeGets)/float64(b.N), "node_gets/op")
		b.ReportMetric(float64(reader.edgeGets)/float64(b.N), "edge_gets/op")
		b.ReportMetric(float64(reader.docGets)/float64(b.N), "doc_gets/op")
	}
}

func BenchmarkCollectionVectorIndexNativeRootTemplateV1GraphOnlySearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	indexName := "embedding_graph_template_v1"
	d, col, status, setupStats := openTemplateV1NativeVectorBenchmarkIndexRoot(b, docs, dims, indexName)
	defer func() { _ = d.Close() }()

	reader := newVectorIndexNativeRootTemplateV1GraphReader(b, d, col, indexName, setupStats.Nodes, status.Meta)
	defer func() { _ = reader.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := reader.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm template-v1 native root-backed graph-only search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm template-v1 native root-backed graph-only search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(status.BytesDisk), "native_root_bytes")
	b.ReportMetric(float64(setupStats.BytesMemory), "loaded_index_bytes")
	b.ReportAllocs()
	reader.resetCounters()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := reader.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			b.Fatalf("template-v1 native root-backed graph-only vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("template-v1 native root-backed graph-only vector search returned no results")
		}
	}
	b.StopTimer()
	if b.N > 0 {
		b.ReportMetric(float64(reader.nodeGets)/float64(b.N), "node_gets/op")
		b.ReportMetric(float64(reader.edgeGets)/float64(b.N), "edge_gets/op")
		b.ReportMetric(float64(reader.docGets)/float64(b.N), "doc_gets/op")
	}
}

func BenchmarkCollectionVectorIndexNativeRootTemplateV1GraphOnlySearchParallel(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	indexName := "embedding_graph_template_v1_parallel"
	d, col, status, setupStats := openTemplateV1NativeVectorBenchmarkIndexRoot(b, docs, dims, indexName)
	defer func() { _ = d.Close() }()

	query := vectorBenchmarkEmbedding(docs/3, dims)
	var readersMu sync.Mutex
	var readers []*vectorIndexNativeRootBackedGraphReader
	newReader := func() *vectorIndexNativeRootBackedGraphReader {
		reader := newVectorIndexNativeRootTemplateV1GraphReader(b, d, col, indexName, setupStats.Nodes, status.Meta)
		readersMu.Lock()
		readers = append(readers, reader)
		readersMu.Unlock()
		return reader
	}
	readerPool := sync.Pool{New: func() any { return newReader() }}
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		reader := newReader()
		warm, err := reader.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			b.Fatalf("warm template-v1 native root-backed parallel graph-only search: %v", err)
		}
		if len(warm) == 0 {
			b.Fatal("warm template-v1 native root-backed parallel graph-only search returned no results")
		}
		readerPool.Put(reader)
	}
	b.Cleanup(func() {
		readersMu.Lock()
		defer readersMu.Unlock()
		for _, reader := range readers {
			_ = reader.Close()
		}
	})

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(status.BytesDisk), "native_root_bytes")
	b.ReportMetric(float64(setupStats.BytesMemory), "loaded_index_bytes")
	b.ReportAllocs()
	for _, reader := range readers {
		reader.resetCounters()
	}
	b.ResetTimer()
	runParallelVectorSearchBenchmark(b, func() error {
		reader := readerPool.Get().(*vectorIndexNativeRootBackedGraphReader)
		results, err := reader.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			readerPool.Put(reader)
			return fmt.Errorf("template-v1 native root-backed graph-only vector search: %w", err)
		}
		if len(results) == 0 {
			readerPool.Put(reader)
			return errors.New("template-v1 native root-backed graph-only vector search returned no results")
		}
		readerPool.Put(reader)
		return nil
	})
	b.StopTimer()
	var nodeGets, edgeGets, docGets int64
	readersMu.Lock()
	for _, reader := range readers {
		nodeGets += reader.nodeGets
		edgeGets += reader.edgeGets
		docGets += reader.docGets
	}
	readersMu.Unlock()
	if b.N > 0 {
		b.ReportMetric(float64(nodeGets)/float64(b.N), "node_gets/op")
		b.ReportMetric(float64(edgeGets)/float64(b.N), "edge_gets/op")
		b.ReportMetric(float64(docGets)/float64(b.N), "doc_gets/op")
	}
}

func BenchmarkCollectionVectorIndexNativeRootGraphOnlySearchParallel(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, loaded, status := openLoadedNativeVectorBenchmarkIndex(b, docs, dims, "embedding_graph_only_parallel")
	defer func() { _ = d.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := loaded.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm native graph-only parallel search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm native graph-only parallel search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(status.BytesDisk), "native_root_bytes")
	b.ReportMetric(float64(loaded.Stats().BytesMemory), "index_bytes")
	b.ReportAllocs()
	b.ResetTimer()
	runParallelVectorSearchBenchmark(b, func() error {
		results, err := loaded.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			return fmt.Errorf("native graph-only vector search: %w", err)
		}
		if len(results) == 0 {
			return errors.New("native graph-only vector search returned no results")
		}
		return nil
	})
}

func BenchmarkCollectionVectorIndexNativeRootSearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, loaded, status := openLoadedNativeVectorBenchmarkIndex(b, docs, dims, "embedding_search")
	defer func() { _ = d.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)
	opts := VectorIndexSearchOptions{
		TopK:                 vectorBenchmarkTopK,
		EfSearch:             128,
		FetchMultiplier:      16,
		DisableExactFallback: true,
	}
	warm, trace, err := loaded.Search(query, opts)
	if err != nil {
		b.Fatalf("warm native vector search: %v", err)
	}
	if len(warm) == 0 || trace.RerankCount == 0 {
		b.Fatalf("warm native vector search results=%d rerank=%d", len(warm), trace.RerankCount)
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(status.BytesDisk), "native_root_bytes")
	b.ReportMetric(float64(loaded.Stats().BytesMemory), "index_bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, trace, err := loaded.Search(query, opts)
		if err != nil {
			b.Fatalf("native vector search: %v", err)
		}
		if len(results) == 0 || trace.RerankCount == 0 {
			b.Fatalf("native vector search results=%d rerank=%d", len(results), trace.RerankCount)
		}
	}
}

func BenchmarkCollectionVectorIndexNativeRootSearchParallel(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, loaded, status := openLoadedNativeVectorBenchmarkIndex(b, docs, dims, "embedding_search_parallel")
	defer func() { _ = d.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)
	opts := VectorIndexSearchOptions{
		TopK:                 vectorBenchmarkTopK,
		EfSearch:             128,
		FetchMultiplier:      16,
		DisableExactFallback: true,
	}
	warm, trace, err := loaded.Search(query, opts)
	if err != nil {
		b.Fatalf("warm native vector parallel search: %v", err)
	}
	if len(warm) == 0 || trace.RerankCount == 0 {
		b.Fatalf("warm native vector parallel search results=%d rerank=%d", len(warm), trace.RerankCount)
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(status.BytesDisk), "native_root_bytes")
	b.ReportMetric(float64(loaded.Stats().BytesMemory), "index_bytes")
	b.ReportAllocs()
	b.ResetTimer()
	runParallelVectorSearchBenchmark(b, func() error {
		results, trace, err := loaded.Search(query, opts)
		if err != nil {
			return fmt.Errorf("native vector search: %w", err)
		}
		if len(results) == 0 || trace.RerankCount == 0 {
			return fmt.Errorf("native vector search results=%d rerank=%d", len(results), trace.RerankCount)
		}
		return nil
	})
}

func BenchmarkCollectionVectorIndexColumnGraphMainPathLoad(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	indexName := "embedding_column_graph_load"
	fixture := setupColumnGraphVectorBenchmarkMainPath(b, docs, dims, indexName)
	d, _, _, loadStatus := openLoadedColumnGraphVectorBenchmarkMainPath(b, fixture)
	if err := d.Close(); err != nil {
		b.Fatalf("close warm load db: %v", err)
	}

	opts := vectorIndexOptionsFromDefinition(fixture.def)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := openCollectionCommandWALDB(b, fixture.dir)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			_ = d.Close()
			b.Fatalf("open collection: %v", err)
		}
		loaded, status, err := col.LoadColumnGraphVectorIndexSnapshot(opts)
		if err != nil {
			_ = d.Close()
			b.Fatalf("load column graph vector index: %v", err)
		}
		if loaded == nil || !status.ColumnGraphLoaded || !status.PhysicalColumnAssetsSupported {
			_ = d.Close()
			b.Fatalf("unexpected column_graph load status loaded=%v status=%+v", loaded != nil, status)
		}
		columnVectorGraphBenchSink += int64(loaded.Rows()) + int64(status.RootID)
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(loadStatus.RootID), "manifest_root_id")
	b.ReportMetric(float64(loadStatus.BytesDisk), "column_graph_bytes")
}

func BenchmarkCollectionVectorIndexColumnGraphMainPathSearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, _, graph, loadStatus := openColumnGraphVectorBenchmarkMainPath(b, docs, dims, "embedding_column_graph_search")
	defer func() { _ = d.Close() }()
	query, ok := graph.VectorAt(nil, docs/3)
	if !ok {
		b.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: vectorBenchmarkTopK, EfSearch: 128}
	var scratch ColumnVectorGraphSearchScratch
	warm, warmTrace, err := graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		b.Fatalf("warm column_graph main-path search: %v", err)
	}
	if len(warm) != opts.TopK {
		b.Fatalf("warm results=%d want %d", len(warm), opts.TopK)
	}

	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * graph.Dims() * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, trace, err := graph.SearchCosine(query, opts, &scratch)
		if err != nil {
			b.Fatalf("column_graph main-path search: %v", err)
		}
		if len(results) != opts.TopK {
			b.Fatalf("results=%d want %d", len(results), opts.TopK)
		}
		columnVectorGraphBenchSink += int64(len(results[0].DocumentID) + trace.CandidatesExamined + trace.EdgesVisited)
	}
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(loadStatus.RootID), "manifest_root_id")
	b.ReportMetric(float64(loadStatus.BytesDisk), "column_graph_bytes")
	b.ReportMetric(float64(graph.Edges())/float64(graph.Rows()), "edges/node")
	b.ReportMetric(float64(warmTrace.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(warmTrace.EdgesVisited), "edges/search")
}

func BenchmarkCollectionVectorIndexColumnGraphMainPathSearchParallel(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, _, graph, loadStatus := openColumnGraphVectorBenchmarkMainPath(b, docs, dims, "embedding_column_graph_search_parallel")
	defer func() { _ = d.Close() }()
	query, ok := graph.VectorAt(nil, docs/3)
	if !ok {
		b.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: vectorBenchmarkTopK, EfSearch: 128}
	b.SetParallelism(1)
	workers := runtime.GOMAXPROCS(0)
	scratches := make([]*ColumnVectorGraphSearchScratch, workers)
	var warmTrace ColumnVectorGraphSearchTrace
	for worker := 0; worker < workers; worker++ {
		scratch := new(ColumnVectorGraphSearchScratch)
		warm, trace, err := graph.SearchCosine(query, opts, scratch)
		if err != nil {
			b.Fatalf("warm column_graph main-path worker %d: %v", worker, err)
		}
		if len(warm) != opts.TopK {
			b.Fatalf("warm worker %d results=%d want %d", worker, len(warm), opts.TopK)
		}
		warmTrace = trace
		scratches[worker] = scratch
	}

	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * graph.Dims() * 4))
	b.ResetTimer()
	var nextWorker uint64
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(atomic.AddUint64(&nextWorker, 1)) - 1
		if workerID >= len(scratches) {
			b.Errorf("RunParallel spawned worker %d, but only %d scratches were prewarmed", workerID+1, len(scratches))
			return
		}
		scratch := scratches[workerID]
		var localSink int64
		for pb.Next() {
			results, trace, err := graph.SearchCosine(query, opts, scratch)
			if err != nil {
				panic(err)
			}
			if len(results) != opts.TopK {
				panic("unexpected column_graph main-path result count")
			}
			localSink += int64(len(results[0].DocumentID) + trace.CandidatesExamined + trace.EdgesVisited)
		}
		atomic.AddInt64(&columnVectorGraphBenchSink, localSink)
	})
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(loadStatus.RootID), "manifest_root_id")
	b.ReportMetric(float64(loadStatus.BytesDisk), "column_graph_bytes")
	b.ReportMetric(float64(graph.Edges())/float64(graph.Rows()), "edges/node")
	b.ReportMetric(float64(warmTrace.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(warmTrace.EdgesVisited), "edges/search")
}

func BenchmarkCollectionVectorIndexColumnGraphMainPathSearchWithDocumentFetch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col, graph, loadStatus := openColumnGraphVectorBenchmarkMainPath(b, docs, dims, "embedding_column_graph_search_docs")
	defer func() { _ = d.Close() }()
	query, ok := graph.VectorAt(nil, docs/3)
	if !ok {
		b.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: vectorBenchmarkTopK, EfSearch: 128}
	var scratch ColumnVectorGraphSearchScratch
	warm, warmTrace, err := graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		b.Fatalf("warm column_graph main-path document search: %v", err)
	}
	if len(warm) != opts.TopK {
		b.Fatalf("warm results=%d want %d", len(warm), opts.TopK)
	}
	var documentBuf []byte
	warmBytes := fetchColumnGraphBenchmarkDocuments(b, col, warm, &documentBuf)

	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * graph.Dims() * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, trace, err := graph.SearchCosine(query, opts, &scratch)
		if err != nil {
			b.Fatalf("column_graph main-path document search: %v", err)
		}
		if len(results) != opts.TopK {
			b.Fatalf("results=%d want %d", len(results), opts.TopK)
		}
		documentBytes := fetchColumnGraphBenchmarkDocuments(b, col, results, &documentBuf)
		columnVectorGraphBenchSink += int64(documentBytes + trace.CandidatesExamined + trace.EdgesVisited)
	}
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(loadStatus.RootID), "manifest_root_id")
	b.ReportMetric(float64(loadStatus.BytesDisk), "column_graph_bytes")
	b.ReportMetric(float64(graph.Edges())/float64(graph.Rows()), "edges/node")
	b.ReportMetric(float64(warmTrace.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(warmTrace.EdgesVisited), "edges/search")
	b.ReportMetric(float64(warmBytes), "document_bytes/search")
}

func BenchmarkCollectionVectorIndexColumnGraphMainPathSearchWithDocumentFetchParallel(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col, graph, loadStatus := openColumnGraphVectorBenchmarkMainPath(b, docs, dims, "embedding_column_graph_search_docs_parallel")
	defer func() { _ = d.Close() }()
	query, ok := graph.VectorAt(nil, docs/3)
	if !ok {
		b.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: vectorBenchmarkTopK, EfSearch: 128}
	b.SetParallelism(1)
	workers := runtime.GOMAXPROCS(0)
	scratches := make([]*ColumnVectorGraphSearchScratch, workers)
	documentBufs := make([][]byte, workers)
	var warmTrace ColumnVectorGraphSearchTrace
	var warmBytes int
	for worker := 0; worker < workers; worker++ {
		scratch := new(ColumnVectorGraphSearchScratch)
		warm, trace, err := graph.SearchCosine(query, opts, scratch)
		if err != nil {
			b.Fatalf("warm column_graph main-path document worker %d: %v", worker, err)
		}
		if len(warm) != opts.TopK {
			b.Fatalf("warm worker %d results=%d want %d", worker, len(warm), opts.TopK)
		}
		warmTrace = trace
		warmBytes = fetchColumnGraphBenchmarkDocuments(b, col, warm, &documentBufs[worker])
		scratches[worker] = scratch
	}

	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * graph.Dims() * 4))
	b.ResetTimer()
	var nextWorker uint64
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(atomic.AddUint64(&nextWorker, 1)) - 1
		if workerID >= len(scratches) {
			b.Errorf("RunParallel spawned worker %d, but only %d scratches were prewarmed", workerID+1, len(scratches))
			return
		}
		scratch := scratches[workerID]
		documentBuf := documentBufs[workerID]
		var localSink int64
		for pb.Next() {
			results, trace, err := graph.SearchCosine(query, opts, scratch)
			if err != nil {
				panic(err)
			}
			if len(results) != opts.TopK {
				panic("unexpected column_graph main-path document result count")
			}
			documentBytes, err := fetchColumnGraphBenchmarkDocumentsChecked(col, results, &documentBuf)
			if err != nil {
				panic(err)
			}
			localSink += int64(documentBytes + trace.CandidatesExamined + trace.EdgesVisited)
		}
		documentBufs[workerID] = documentBuf
		atomic.AddInt64(&columnVectorGraphBenchSink, localSink)
	})
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(loadStatus.RootID), "manifest_root_id")
	b.ReportMetric(float64(loadStatus.BytesDisk), "column_graph_bytes")
	b.ReportMetric(float64(graph.Edges())/float64(graph.Rows()), "edges/node")
	b.ReportMetric(float64(warmTrace.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(warmTrace.EdgesVisited), "edges/search")
	b.ReportMetric(float64(warmBytes), "document_bytes/search")
}

func BenchmarkCollectionVectorIndexColumnGraphMainPathPublicSearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	indexName := "embedding_column_graph_public_search"
	d, col, graph, loadStatus := openColumnGraphVectorBenchmarkMainPath(b, docs, dims, indexName)
	defer func() { _ = d.Close() }()
	query, ok := graph.VectorAt(nil, docs/3)
	if !ok {
		b.Fatal("missing query vector")
	}
	opts := VectorIndexSearchOptions{
		TopK:                 vectorBenchmarkTopK,
		EfSearch:             128,
		DisableExactFallback: true,
	}
	var kernelScratch ColumnVectorGraphSearchScratch
	_, kernelTrace, err := graph.SearchCosine(query, ColumnVectorGraphSearchOptions{
		TopK:     opts.TopK,
		EfSearch: opts.EfSearch,
	}, &kernelScratch)
	if err != nil {
		b.Fatalf("warm public column_graph kernel trace: %v", err)
	}
	warm, warmTrace, err := col.SearchVectorIndex(indexName, query, opts)
	if err != nil {
		b.Fatalf("warm public column_graph search: %v", err)
	}
	if len(warm) != opts.TopK || warmTrace.Strategy != columnVectorGraphStrategyCosine {
		b.Fatalf("warm public results=%d trace=%+v", len(warm), warmTrace)
	}

	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * graph.Dims() * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, trace, err := col.SearchVectorIndex(indexName, query, opts)
		if err != nil {
			b.Fatalf("public column_graph search: %v", err)
		}
		if len(results) != opts.TopK || trace.Strategy != columnVectorGraphStrategyCosine {
			b.Fatalf("public results=%d trace=%+v", len(results), trace)
		}
		columnVectorGraphBenchSink += int64(len(results[0].DocumentID) + len(results[0].Document) + trace.CandidatesExamined)
	}
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(loadStatus.RootID), "manifest_root_id")
	b.ReportMetric(float64(loadStatus.BytesDisk), "column_graph_bytes")
	b.ReportMetric(float64(graph.Edges())/float64(graph.Rows()), "edges/node")
	b.ReportMetric(float64(warmTrace.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(kernelTrace.EdgesVisited), "edges/search")
}

func BenchmarkCollectionVectorIndexColumnGraphMainPathOpenLoadSearchWithDocumentFetch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	indexName := "embedding_column_graph_open_load_search_docs"
	fixture := setupColumnGraphVectorBenchmarkMainPath(b, docs, dims, indexName)
	query, ok := fixture.graph.VectorAt(nil, docs/3)
	if !ok {
		b.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: vectorBenchmarkTopK, EfSearch: 128}
	vectorOpts := vectorIndexOptionsFromDefinition(fixture.def)

	// This deliberately measures the slower DB-facing path: open collection,
	// validate column_graph status through the real physical loader, search,
	// then fetch the selected documents. Build/index setup stays outside the
	// timed loop.
	var scratch ColumnVectorGraphSearchScratch
	var documentBuf []byte
	warmD, warmCol, warmGraph, warmLoadStatus := openLoadedColumnGraphVectorBenchmarkMainPath(b, fixture)
	warm, warmTrace, err := warmGraph.SearchCosine(query, opts, &scratch)
	if err != nil {
		_ = warmD.Close()
		b.Fatalf("warm column_graph open-load search: %v", err)
	}
	if len(warm) != opts.TopK {
		_ = warmD.Close()
		b.Fatalf("warm results=%d want %d", len(warm), opts.TopK)
	}
	warmBytes := fetchColumnGraphBenchmarkDocuments(b, warmCol, warm, &documentBuf)
	if err := warmD.Close(); err != nil {
		b.Fatalf("close warm db: %v", err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * fixture.graph.Dims() * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := backenddb.Open(backenddb.Options{Dir: fixture.dir})
		if err != nil {
			b.Fatalf("reopen db: %v", err)
		}
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			_ = d.Close()
			b.Fatalf("open collection: %v", err)
		}
		graph, status, err := col.LoadColumnGraphVectorIndexSnapshot(vectorOpts)
		if err != nil {
			_ = d.Close()
			b.Fatalf("load column graph vector index: %v", err)
		}
		if graph == nil || !status.ColumnGraphLoaded || !status.PhysicalColumnAssetsSupported {
			_ = d.Close()
			b.Fatalf("unexpected column_graph load status loaded=%v status=%+v", graph != nil, status)
		}
		results, trace, err := graph.SearchCosine(query, opts, &scratch)
		if err != nil {
			_ = d.Close()
			b.Fatalf("column_graph open-load search: %v", err)
		}
		if len(results) != opts.TopK {
			_ = d.Close()
			b.Fatalf("results=%d want %d", len(results), opts.TopK)
		}
		documentBytes := fetchColumnGraphBenchmarkDocuments(b, col, results, &documentBuf)
		columnVectorGraphBenchSink += int64(documentBytes+trace.CandidatesExamined+trace.EdgesVisited) + int64(status.RootID)
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(warmLoadStatus.RootID), "manifest_root_id")
	b.ReportMetric(float64(warmLoadStatus.BytesDisk), "column_graph_bytes")
	b.ReportMetric(float64(fixture.graph.Edges())/float64(fixture.graph.Rows()), "edges/node")
	b.ReportMetric(float64(warmTrace.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(warmTrace.EdgesVisited), "edges/search")
	b.ReportMetric(float64(warmBytes), "document_bytes/search")
}

func BenchmarkCollectionVectorIndexIncrementalWrite(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	ids, documents := vectorBenchmarkWriteBatch(docs, dims)

	b.ReportMetric(float64(docs), "docs/write")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		d, col := openEmptyVectorBenchmarkCollection(b)
		index, err := newVectorIndex(col, VectorIndexOptions{
			Name:   "embedding_write",
			Field:  "embedding",
			Metric: VectorMetricCosine,
			M:      16,
		})
		if err != nil {
			_ = d.Close()
			b.Fatalf("new vector index: %v", err)
		}
		col.RegisterVectorIndex(index)
		b.StartTimer()
		vectorBenchmarkInsertBatches(b, col, ids, documents, 512)
		b.StopTimer()
		stats := index.Stats()
		if stats.LiveDocs != docs {
			_ = d.Close()
			b.Fatalf("incremental index live docs=%d want %d", stats.LiveDocs, docs)
		}
		b.ReportMetric(float64(stats.BytesMemory), "index_bytes")
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
}

func BenchmarkCollectionVectorIndexNativeRootIncrementalWrite(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	ids, documents := vectorBenchmarkWriteBatch(docs, dims)
	def := VectorIndexDefinition{
		Name:           "embedding_write",
		Field:          "embedding",
		Metric:         VectorMetricCosine,
		Dimensions:     dims,
		M:              16,
		EfConstruction: defaultVectorIndexEfConstruction,
		EfSearch:       defaultVectorIndexEfSearch,
	}

	b.ReportMetric(float64(docs), "docs/write")
	b.ReportMetric(float64(dims), "dims")
	baseDir := b.TempDir()
	var indexBytes int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(baseDir, fmt.Sprintf("iter-%06d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatalf("create db dir: %v", err)
		}
		d, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
			_ = d.Close()
			b.Fatalf("create collection: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			_ = d.Close()
			b.Fatalf("open collection: %v", err)
		}
		index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			_ = d.Close()
			b.Fatalf("build empty native vector index: %v", err)
		}
		b.StartTimer()
		vectorBenchmarkInsertBatches(b, col, ids, documents, 512)
		if err := col.Flush(); err != nil {
			_ = d.Close()
			b.Fatalf("flush native vector index: %v", err)
		}
		b.StopTimer()
		stats := index.Stats()
		if stats.LiveDocs != docs {
			_ = d.Close()
			b.Fatalf("native incremental index live docs=%d want %d", stats.LiveDocs, docs)
		}
		if stats.SnapshotDirty {
			_ = d.Close()
			b.Fatalf("native incremental index snapshot is dirty: %+v", stats)
		}
		indexBytes = stats.BytesMemory
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		removeVectorBenchmarkDirAfterClose(b, dir)
	}
	b.ReportMetric(float64(indexBytes), "index_bytes")
}

func BenchmarkCollectionVectorIndexNativeRootRebuild(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	ids, documents := vectorBenchmarkWriteBatch(docs, dims)
	def := VectorIndexDefinition{
		Name:           "embedding_rebuild",
		Field:          "embedding",
		Metric:         VectorMetricCosine,
		Dimensions:     dims,
		M:              16,
		EfConstruction: defaultVectorIndexEfConstruction,
		EfSearch:       defaultVectorIndexEfSearch,
	}

	b.ReportMetric(float64(docs), "docs/rebuild")
	b.ReportMetric(float64(dims), "dims")
	b.ReportAllocs()
	baseDir := b.TempDir()
	b.ResetTimer()
	var lastNativeRootBytes int64
	var lastIndexBytesMemory int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(baseDir, fmt.Sprintf("rebuild-%06d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatalf("create db dir: %v", err)
		}
		d, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
			_ = d.Close()
			b.Fatalf("create collection: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			_ = d.Close()
			b.Fatalf("open collection: %v", err)
		}
		vectorBenchmarkInsertBatches(b, col, ids, documents, 512)
		b.StartTimer()
		status, err := col.RebuildVectorIndex(def.Name)
		b.StopTimer()
		if err != nil {
			_ = d.Close()
			b.Fatalf("rebuild native vector index: %v", err)
		}
		if !status.NativeRootLoaded || status.Stats.LiveDocs != docs || status.RootID == 0 {
			_ = d.Close()
			b.Fatalf("unexpected native rebuild status: %+v", status)
		}
		lastNativeRootBytes = status.NativeRootBytes
		lastIndexBytesMemory = status.Stats.BytesMemory
		if err := d.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		removeVectorBenchmarkDirAfterClose(b, dir)
	}
	b.ReportMetric(float64(lastNativeRootBytes), "native_root_bytes")
	b.ReportMetric(float64(lastIndexBytesMemory), "index_bytes_memory")
	b.ReportMetric(float64(lastNativeRootBytes)/float64(docs), "native_root_bytes/doc")
	b.ReportMetric(float64(lastIndexBytesMemory)/float64(docs), "index_bytes_memory/doc")
}

func removeVectorBenchmarkDirAfterClose(b *testing.B, dir string) {
	b.Helper()
	if err := os.RemoveAll(dir); err != nil {
		if runtime.GOOS != "windows" {
			b.Fatalf("remove db dir: %v", err)
		}
		b.Logf("best-effort remove db dir %q: %v", dir, err)
	}
}

func BenchmarkCollectionVectorIndexSearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      16,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	queries := [][]float32{
		vectorBenchmarkEmbedding(17, dims),
		vectorBenchmarkEmbedding(docs/3, dims),
		vectorBenchmarkEmbedding(docs/2, dims),
		vectorBenchmarkEmbedding(docs-11, dims),
	}
	recall, err := index.CheckRecall(queries, VectorIndexSearchOptions{
		TopK:            vectorBenchmarkTopK,
		EfSearch:        128,
		FetchMultiplier: 16,
	})
	if err != nil {
		b.Fatalf("check recall: %v", err)
	}
	query := queries[1]

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(recall.Recall*100, "recall_at_10_pct")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, _, err := index.Search(query, VectorIndexSearchOptions{
			TopK:                 vectorBenchmarkTopK,
			EfSearch:             128,
			FetchMultiplier:      16,
			DisableExactFallback: true,
		})
		if err != nil {
			b.Fatalf("indexed vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("indexed vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexGraphOnlySearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding_graph_only",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      16,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm graph-only search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm graph-only search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			b.Fatalf("graph-only vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("graph-only vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexGraphOnlySearchParallel(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding_graph_only_parallel",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      16,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm graph-only search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm graph-only search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ReportAllocs()
	b.ResetTimer()
	runParallelVectorSearchBenchmark(b, func() error {
		results, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			return fmt.Errorf("graph-only vector search: %w", err)
		}
		if len(results) == 0 {
			return errors.New("graph-only vector search returned no results")
		}
		return nil
	})
}

func BenchmarkCollectionVectorIndexSearchInt8(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:     "embedding_i8",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		M:        16,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		b.Fatalf("build int8 vector index: %v", err)
	}
	queries := [][]float32{
		vectorBenchmarkEmbedding(17, dims),
		vectorBenchmarkEmbedding(docs/3, dims),
		vectorBenchmarkEmbedding(docs/2, dims),
		vectorBenchmarkEmbedding(docs-11, dims),
	}
	recall, err := index.CheckRecall(queries, VectorIndexSearchOptions{
		TopK:            vectorBenchmarkTopK,
		EfSearch:        128,
		FetchMultiplier: 16,
	})
	if err != nil {
		b.Fatalf("check int8 recall: %v", err)
	}
	query := queries[1]

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(recall.Recall*100, "recall_at_10_pct")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, _, err := index.Search(query, VectorIndexSearchOptions{
			TopK:                 vectorBenchmarkTopK,
			EfSearch:             128,
			FetchMultiplier:      16,
			DisableExactFallback: true,
		})
		if err != nil {
			b.Fatalf("int8 indexed vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("int8 indexed vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexGraphOnlySearchInt8(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:     "embedding_graph_only_i8",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		M:        16,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		b.Fatalf("build int8 vector index: %v", err)
	}
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm int8 graph-only search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm int8 graph-only search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			b.Fatalf("int8 graph-only vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("int8 graph-only vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexFilteredSearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollectionWithIndexes(b, docs, dims, IndexDefinition{Name: "group_idx", Field: "group", ValueType: IndexValueInt64})
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      16,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	queryDoc := docs / 3
	query := vectorBenchmarkEmbedding(queryDoc, dims)
	group := int64(queryDoc % 16)
	filter := &VectorIndexRangeFilter{
		IndexName: "group_idx",
		Range: IndexRangeOptions{
			Lower: IndexRangeBound{Value: group, Inclusive: true},
			Upper: IndexRangeBound{Value: group, Inclusive: true},
		},
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(group), "filter_group")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, trace, err := index.Search(query, VectorIndexSearchOptions{
			TopK:               vectorBenchmarkTopK,
			EfSearch:           128,
			FetchMultiplier:    16,
			ExactFilterMaxDocs: 32,
			IndexRangeFilter:   filter,
		})
		if err != nil {
			b.Fatalf("filtered indexed vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("filtered indexed vector search returned no results")
		}
		if trace.CandidatesAfterFilter > 0 {
			b.ReportMetric(float64(trace.CandidatesAfterFilter), "candidates_after_filter")
		}
	}
}

func BenchmarkCollectionVectorTinyBERTFixture(b *testing.B) {
	path := os.Getenv("TREEDB_VECTOR_BENCH_JSONL")
	if path == "" {
		b.Skip("set TREEDB_VECTOR_BENCH_JSONL to a tiny_bert demo JSONL export")
	}
	fixture := loadVectorBenchmarkFixture(b, path)
	if len(fixture) == 0 {
		b.Fatal("tiny BERT fixture has no records")
	}
	d, col := openVectorFixtureCollection(b, fixture)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      8,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	topK := minInt(vectorBenchmarkTopK, len(fixture))
	query := fixture[0].Embedding
	recall, err := index.CheckRecall([][]float32{query}, VectorIndexSearchOptions{
		TopK:            topK,
		EfSearch:        64,
		FetchMultiplier: 8,
	})
	if err != nil {
		b.Fatalf("check tiny BERT recall: %v", err)
	}

	stats := index.Stats()
	b.ReportMetric(float64(len(fixture)), "docs/index")
	b.ReportMetric(float64(len(query)), "dims")
	b.ReportMetric(recall.Recall*100, "recall_pct")
	b.ReportMetric(float64(stats.BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, _, err := index.Search(query, VectorIndexSearchOptions{
			TopK:                 topK,
			EfSearch:             64,
			FetchMultiplier:      8,
			DisableExactFallback: true,
		})
		if err != nil {
			b.Fatalf("tiny BERT vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("tiny BERT vector search returned no results")
		}
	}
}

func TestLoadVectorBenchmarkFixtureJSONL(t *testing.T) {
	path := t.TempDir() + "/fixture.jsonl"
	if err := os.WriteFile(path, []byte(
		`{"id":"D01","text":"alpha","model":"demo","pooling":"mean","normalized":true,"embedding":[1,0]}`+"\n"+
			`{"id":"D02","text":"beta","model":"demo","pooling":"mean","normalized":true,"embedding":[0,1]}`+"\n",
	), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	records := loadVectorBenchmarkFixture(t, path)
	if len(records) != 2 || records[0].ID != "D01" || len(records[1].Embedding) != 2 {
		t.Fatalf("unexpected fixture records: %+v", records)
	}
}

func vectorBenchmarkDocs(tb testing.TB) int {
	return vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_DOCS", defaultVectorBenchmarkDocs)
}

func vectorBenchmarkDims(tb testing.TB) int {
	return vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_DIMS", defaultVectorBenchmarkDims)
}

func vectorBenchmarkPositiveEnvInt(tb testing.TB, name string, fallback int) int {
	tb.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		tb.Fatalf("%s=%q must be a positive integer", name, raw)
	}
	return value
}

func openVectorBenchmarkCollection(tb testing.TB, docs, dims int) (*backenddb.DB, *Collection) {
	tb.Helper()
	return openVectorBenchmarkCollectionWithIndexes(tb, docs, dims)
}

func openEmptyVectorBenchmarkCollection(tb testing.TB) (*backenddb.DB, *Collection) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		_ = d.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	return d, col
}

func openVectorBenchmarkCollectionWithIndexes(tb testing.TB, docs, dims int, indexes ...IndexDefinition) (*backenddb.DB, *Collection) {
	tb.Helper()
	var d *backenddb.DB
	var col *Collection
	if len(indexes) == 0 {
		d, col = openEmptyVectorBenchmarkCollection(tb)
	} else {
		var err error
		d, err = backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
		if err != nil {
			tb.Fatalf("open db: %v", err)
		}
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Indexes: indexes}); err != nil {
			_ = d.Close()
			tb.Fatalf("create collection: %v", err)
		}
		col, err = mgr.OpenCollection("docs")
		if err != nil {
			_ = d.Close()
			tb.Fatalf("open collection: %v", err)
		}
	}
	ids, documents := vectorBenchmarkWriteBatch(docs, dims)
	vectorBenchmarkInsertBatches(tb, col, ids, documents, 512)
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("flush benchmark collection: %v", err)
	}
	return d, col
}

func openVectorBenchmarkCollectionWithVectorIndex(tb testing.TB, dir string, docs, dims int, def VectorIndexDefinition) (*backenddb.DB, *Collection) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		_ = d.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	ids, documents := vectorBenchmarkWriteBatch(docs, dims)
	vectorBenchmarkInsertBatches(tb, col, ids, documents, 512)
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("flush benchmark collection: %v", err)
	}
	return d, col
}

func vectorBenchmarkWriteBatch(docs, dims int) ([][]byte, [][]byte) {
	ids := make([][]byte, docs)
	documents := make([][]byte, docs)
	for i := 0; i < docs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		documents[i] = vectorBenchmarkDocument(i, dims)
	}
	return ids, documents
}

func openLoadedNativeVectorBenchmarkIndex(tb testing.TB, docs, dims int, name string) (*backenddb.DB, *VectorIndex, VectorIndexLoadStatus) {
	tb.Helper()
	dir := tb.TempDir()
	def := VectorIndexDefinition{
		Name:       name,
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: dims,
		M:          16,
	}
	d, col := openVectorBenchmarkCollectionWithVectorIndex(tb, dir, docs, dims, def)
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		_ = d.Close()
		tb.Fatalf("build native vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		_ = d.Close()
		tb.Fatalf("save native vector index: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("close setup db: %v", err)
	}
	d, err = backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		tb.Fatalf("reopen db: %v", err)
	}
	col, err = NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	loaded, loadStatus, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		_ = d.Close()
		tb.Fatalf("load native vector index: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.RootID != saveStatus.RootID {
		_ = d.Close()
		tb.Fatalf("unexpected native load status loaded=%v status=%+v save=%+v", loaded != nil, loadStatus, saveStatus)
	}
	return d, loaded, loadStatus
}

type columnGraphVectorBenchmarkMainPathFixture struct {
	dir   string
	def   VectorIndexDefinition
	graph *ColumnVectorGraph
}

func setupColumnGraphVectorBenchmarkMainPath(tb testing.TB, docs, dims int, name string) columnGraphVectorBenchmarkMainPathFixture {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	def := VectorIndexDefinition{
		Name:       name,
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: dims,
		M:          16,
		EfSearch:   128,
		Strategy:   VectorIndexStrategyColumnGraph,
	}
	d := openCollectionCommandWALDB(tb, dir)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			ColumnStore: columnGraphVectorBenchmarkColumnStore(nil, dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}); err != nil {
		_ = d.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	columns := columnVectorGraphTestColumns(docs, dims, 16, false)
	ids, documents := vectorGraphBenchmarkWriteBatch(columns)
	vectorBenchmarkInsertBatches(tb, col, ids, documents, 512)
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("flush benchmark collection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("checkpoint benchmark collection: %v", err)
	}
	if err := d.Close(); err != nil {
		tb.Fatalf("close setup db: %v", err)
	}

	graph, err := NewColumnVectorGraphFromColumns(columns)
	if err != nil {
		tb.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	return columnGraphVectorBenchmarkMainPathFixture{dir: dir, def: def, graph: graph}
}

func openColumnGraphVectorBenchmarkMainPath(tb testing.TB, docs, dims int, name string) (*backenddb.DB, *Collection, *ColumnVectorGraph, VectorIndexLoadStatus) {
	tb.Helper()
	fixture := setupColumnGraphVectorBenchmarkMainPath(tb, docs, dims, name)
	return openLoadedColumnGraphVectorBenchmarkMainPath(tb, fixture)
}

func openLoadedColumnGraphVectorBenchmarkMainPath(tb testing.TB, fixture columnGraphVectorBenchmarkMainPathFixture) (*backenddb.DB, *Collection, *ColumnVectorGraph, VectorIndexLoadStatus) {
	tb.Helper()
	d := openCollectionCommandWALDB(tb, fixture.dir)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open reopened collection: %v", err)
	}
	loaded, status, err := col.LoadColumnGraphVectorIndexSnapshot(vectorIndexOptionsFromDefinition(fixture.def))
	if err != nil {
		_ = d.Close()
		tb.Fatalf("load column_graph vector index: %v", err)
	}
	if loaded == nil || !status.ColumnGraphLoaded || !status.PhysicalColumnAssetsSupported || status.RootID == 0 {
		_ = d.Close()
		tb.Fatalf("unexpected column_graph benchmark load status loaded=%v status=%+v", loaded != nil, status)
	}
	if loaded.Rows() != fixture.graph.Rows() || loaded.Dims() != fixture.graph.Dims() || loaded.Edges() != fixture.graph.Edges() {
		_ = d.Close()
		tb.Fatalf("unexpected column_graph shape rows=%d/%d dims=%d/%d edges=%d/%d", loaded.Rows(), fixture.graph.Rows(), loaded.Dims(), fixture.graph.Dims(), loaded.Edges(), fixture.graph.Edges())
	}
	return d, col, loaded, status
}

func columnGraphVectorBenchmarkColumnStore(active *ColumnManifestIdentity, dims int) *ColumnStoreConfig {
	var recovery *ColumnManifestIdentity
	if active != nil {
		copied := *active
		recovery = &copied
	}
	cfg := &ColumnStoreConfig{
		Enabled: true,
		// Keep source documents readable through the normal Collection.Get path
		// while this benchmark isolates the column_graph loader/search seam.
		RetainedPayload: ColumnRetainedPayloadFull,
		Columns: []ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: dims},
			{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
			{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList},
		},
		ActiveManifest:                active,
		RecoveryAuthoritativeManifest: recovery,
	}
	if active != nil {
		cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	}
	return cfg
}

func vectorGraphBenchmarkWriteBatch(columns ColumnVectorGraphColumns) ([][]byte, [][]byte) {
	rows := len(columns.DocumentIDs)
	ids := make([][]byte, rows)
	documents := make([][]byte, rows)
	for row := 0; row < rows; row++ {
		ids[row] = append([]byte(nil), columns.DocumentIDs[row]...)
		vector := columns.Vectors[row*columns.Dimensions : (row+1)*columns.Dimensions]
		neighbors := columns.Neighbors[columns.NeighborOffsets[row]:columns.NeighborOffsets[row+1]]
		documents[row] = vectorGraphBenchmarkDocument(row, vector, columns.InvNorms[row], neighbors)
	}
	return ids, documents
}

func vectorGraphBenchmarkDocument(row int, vector []float32, invNorm float32, neighbors []uint32) []byte {
	out := make([]byte, 0, 64+len(vector)*10+len(neighbors)*4)
	out = append(out, `{"group":`...)
	out = strconv.AppendInt(out, int64(row%16), 10)
	out = append(out, `,"embedding":[`...)
	for i, value := range vector {
		if i > 0 {
			out = append(out, ',')
		}
		out = strconv.AppendFloat(out, float64(value), 'g', 7, 32)
	}
	out = append(out, `],"embedding_inv_norm":`...)
	out = strconv.AppendFloat(out, float64(invNorm), 'g', 7, 32)
	out = append(out, `,"embedding_neighbors":[`...)
	for i, neighbor := range neighbors {
		if i > 0 {
			out = append(out, ',')
		}
		out = strconv.AppendUint(out, uint64(neighbor), 10)
	}
	out = append(out, ']', '}')
	return out
}

func fetchColumnGraphBenchmarkDocuments(tb testing.TB, col *Collection, results []VectorSearchResult, buf *[]byte) int {
	tb.Helper()
	total, err := fetchColumnGraphBenchmarkDocumentsChecked(col, results, buf)
	if err != nil {
		tb.Fatal(err)
	}
	return total
}

func fetchColumnGraphBenchmarkDocumentsChecked(col *Collection, results []VectorSearchResult, buf *[]byte) (int, error) {
	total := 0
	for _, result := range results {
		document, found, err := col.GetInto(result.DocumentID, (*buf)[:0])
		if err != nil {
			return 0, fmt.Errorf("get result document %q: %w", result.DocumentID, err)
		}
		if !found {
			return 0, fmt.Errorf("missing result document %q", result.DocumentID)
		}
		total += len(document)
		*buf = document
	}
	return total, nil
}

func runParallelVectorSearchBenchmark(b *testing.B, search func() error) {
	b.Helper()
	var failureMu sync.Mutex
	var firstFailure error
	recordFailure := func(err error) {
		failureMu.Lock()
		defer failureMu.Unlock()
		if firstFailure == nil {
			firstFailure = err
		}
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := search(); err != nil {
				recordFailure(err)
				return
			}
		}
	})
	if firstFailure != nil {
		b.Fatal(firstFailure)
	}
}

func vectorBenchmarkInsertBatches(tb testing.TB, col *Collection, ids, documents [][]byte, batchSize int) {
	tb.Helper()
	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		if _, err := col.InsertBatch(ids[start:end], documents[start:end]); err != nil {
			tb.Fatalf("insert benchmark batch %d-%d: %v", start, end, err)
		}
	}
}

type vectorBenchmarkFixtureRecord struct {
	ID         string    `json:"id"`
	Text       string    `json:"text"`
	Model      string    `json:"model"`
	Pooling    string    `json:"pooling"`
	Normalized bool      `json:"normalized"`
	Embedding  []float32 `json:"embedding"`
}

func loadVectorBenchmarkFixture(tb testing.TB, path string) []vectorBenchmarkFixtureRecord {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open vector benchmark fixture: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			tb.Fatalf("close vector benchmark fixture: %v", err)
		}
	}()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var out []vectorBenchmarkFixtureRecord
	line := 0
	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		var record vectorBenchmarkFixtureRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			tb.Fatalf("decode vector benchmark fixture line %d: %v", line, err)
		}
		if record.ID == "" {
			tb.Fatalf("vector benchmark fixture line %d missing id", line)
		}
		if len(record.Embedding) == 0 {
			tb.Fatalf("vector benchmark fixture line %d missing embedding", line)
		}
		if err := validateFloat32Vector(record.Embedding); err != nil {
			tb.Fatalf("vector benchmark fixture line %d invalid embedding: %v", line, err)
		}
		out = append(out, record)
	}
	if err := scanner.Err(); err != nil {
		tb.Fatalf("scan vector benchmark fixture: %v", err)
	}
	return out
}

func openVectorFixtureCollection(tb testing.TB, fixture []vectorBenchmarkFixtureRecord) (*backenddb.DB, *Collection) {
	tb.Helper()
	if len(fixture) == 0 {
		tb.Fatal("empty vector benchmark fixture")
	}
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		_ = d.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	ids := make([][]byte, 0, len(fixture))
	documents := make([][]byte, 0, len(fixture))
	dims := len(fixture[0].Embedding)
	for _, record := range fixture {
		if len(record.Embedding) != dims {
			_ = d.Close()
			tb.Fatalf("fixture record %q dimension=%d want %d", record.ID, len(record.Embedding), dims)
		}
		document, err := json.Marshal(map[string]any{
			"text":       record.Text,
			"model":      record.Model,
			"pooling":    record.Pooling,
			"normalized": record.Normalized,
			"embedding":  record.Embedding,
		})
		if err != nil {
			_ = d.Close()
			tb.Fatalf("encode fixture document %q: %v", record.ID, err)
		}
		ids = append(ids, []byte(record.ID))
		documents = append(documents, document)
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		_ = d.Close()
		tb.Fatalf("insert vector fixture: %v", err)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("flush vector fixture: %v", err)
	}
	return d, col
}

func vectorBenchmarkDocument(id, dims int) []byte {
	vector := vectorBenchmarkEmbedding(id, dims)
	out := make([]byte, 0, 32+dims*10)
	out = append(out, fmt.Sprintf(`{"group":%d,"embedding":[`, id%16)...)
	for i, value := range vector {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, fmt.Sprintf("%.7g", value)...)
	}
	out = append(out, ']', '}')
	return out
}

func vectorBenchmarkEmbedding(id, dims int) []float32 {
	out := make([]float32, dims)
	fillVectorBenchmarkEmbedding(out, id)
	return out
}

func fillVectorBenchmarkEmbedding(out []float32, id int) {
	var norm float64
	x := float64(id + 1)
	for i := range out {
		d := float64(i + 1)
		value := math.Sin(x*d*0.013) + math.Cos((x+17)*d*0.007) + math.Sin(float64((id%31)+1)*d*0.019)
		out[i] = float32(value)
		norm += value * value
	}
	scale := 1 / math.Sqrt(norm)
	for i := range out {
		out[i] = float32(float64(out[i]) * scale)
	}
}
