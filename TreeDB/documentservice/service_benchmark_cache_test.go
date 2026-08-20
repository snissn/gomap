package documentservice

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestBenchmarkVectorSearchResultsOwnsSource(t *testing.T) {
	source := []collections.VectorIndexSearchResult{
		{ID: []byte("first"), Ordinal: 7, Score: 0.9},
		{ID: []byte("second"), Ordinal: 11, Score: 0.8},
	}
	got := benchmarkVectorSearchResults(source)
	clear(source[0].ID)
	clear(source[1].ID)
	source[0].Ordinal, source[0].Score = 0, 0
	source[1].Ordinal, source[1].Score = 0, 0
	if len(got) != 2 || got[0].ID != "first" || got[0].Ordinal != 7 || got[0].Score != 0.9 || got[1].ID != "second" || got[1].Ordinal != 11 || got[1].Score != 0.8 {
		t.Fatalf("converted results changed after source reset: %+v", got)
	}
}

func TestServiceBenchmarkVectorSearchDoesNotMutateOrRetainQuery(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	createBenchmarkColumnGraphIndex(t, svc, "query_ownership")
	loadBenchmarkDocsDeferred(t, svc, "query_ownership", []Document{{ID: "a", Embedding: []float32{1, 0}}, {ID: "b", Embedding: []float32{0, 1}}})
	if _, err := svc.OptimizeIndex(context.Background(), "query_ownership", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex: %v", err)
	}
	query := []float32{1, 0}
	response, err := svc.SearchBenchmarkVector(context.Background(), "query_ownership", BenchmarkVectorSearchRequest{QueryEmbedding: query, TopK: 1, EfSearch: 8})
	if err != nil {
		t.Fatalf("SearchBenchmarkVector: %v", err)
	}
	if query[0] != 1 || query[1] != 0 || len(response.Results) != 1 || response.Results[0].ID != "a" {
		t.Fatalf("query=%v response=%+v", query, response)
	}
	query[0], query[1] = 0, 1
	if response.Results[0].ID != "a" {
		t.Fatalf("response retained query-backed state: %+v", response)
	}
}

func TestServiceBenchmarkVectorSearchCacheWarmOnOptimizeAndReuse(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	createBenchmarkColumnGraphIndex(t, svc, "bench_cache")
	loadBenchmarkDocsDeferred(t, svc, "bench_cache", []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}},
	})
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 0 {
		t.Fatalf("cache size before optimize=%d want 0", got)
	}
	if _, err := svc.OptimizeIndex(ctx, "bench_cache", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 1 {
		t.Fatalf("cache size after optimize=%d want 1", got)
	}

	first, err := svc.SearchBenchmarkVector(ctx, "bench_cache", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, EfSearch: 8})
	if err != nil {
		t.Fatalf("SearchBenchmarkVector first: %v", err)
	}
	assertBenchmarkCacheHit(t, first, "first search after optimize")
	if first.Diagnostics.Route != collections.VectorIndexSearchRouteExactHNSWSearchPackV1 || !first.NoDocuments || first.Stats.DocumentsFetched != 0 {
		t.Fatalf("first search left exact no-document route: response=%+v stats=%+v diagnostics=%+v", first, first.Stats, first.Diagnostics)
	}

	second, err := svc.SearchBenchmarkVector(ctx, "bench_cache", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{0, 1}, TopK: 2, EfSearch: 8})
	if err != nil {
		t.Fatalf("SearchBenchmarkVector second: %v", err)
	}
	assertBenchmarkCacheHit(t, second, "second search")
}

func TestServiceBenchmarkVectorSearchCacheConcurrentReuse(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	createBenchmarkColumnGraphIndex(t, svc, "bench_concurrent")
	loadBenchmarkDocsDeferred(t, svc, "bench_concurrent", []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}},
	})
	if _, err := svc.OptimizeIndex(ctx, "bench_concurrent", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex: %v", err)
	}

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			query := []float32{1, 0}
			if i%2 == 1 {
				query = []float32{0, 1}
			}
			res, err := svc.SearchBenchmarkVector(ctx, "bench_concurrent", BenchmarkVectorSearchRequest{QueryEmbedding: query, TopK: 2, EfSearch: 8})
			if err != nil {
				errs <- err
				return
			}
			if res.Stats.HNSWSearchPackCacheHits != 1 || res.Stats.HNSWSearchPackCacheMisses != 0 || res.Stats.HNSWSearchPackCacheBuilds != 0 || res.Stats.DocumentsFetched != 0 {
				errs <- serviceErrorf(CodeInternal, "unexpected concurrent cache stats: hits=%d misses=%d builds=%d docs=%d", res.Stats.HNSWSearchPackCacheHits, res.Stats.HNSWSearchPackCacheMisses, res.Stats.HNSWSearchPackCacheBuilds, res.Stats.DocumentsFetched)
				return
			}
			wantID := "a"
			if i%2 == 1 {
				wantID = "b"
			}
			if len(res.Results) == 0 || res.Results[0].ID != wantID {
				errs <- serviceErrorf(CodeInternal, "query %d first result=%+v want id %q", i, res.Results, wantID)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent search error: %v", err)
	}
}

func BenchmarkServiceBenchmarkVectorSearchBufferScratch(b *testing.B) {
	svc, db := newTestService(b)
	defer db.Close()
	const rows = 4096
	docs := make([]Document, rows)
	for i := range docs {
		docs[i] = Document{ID: fmt.Sprintf("doc-%04d", i), Embedding: []float32{float32(i + 1), 1}}
	}
	createBenchmarkColumnGraphIndex(b, svc, "bench_scratch")
	loadBenchmarkDocsDeferred(b, svc, "bench_scratch", docs)
	if _, err := svc.OptimizeIndex(context.Background(), "bench_scratch", OptimizeIndexRequest{}); err != nil {
		b.Fatalf("OptimizeIndex: %v", err)
	}
	req := BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 100, EfSearch: 100}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.SearchBenchmarkVector(context.Background(), "bench_scratch", req); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func TestServiceBenchmarkVectorSearchCacheInvalidatesOnLifecycleEvents(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	createBenchmarkColumnGraphIndex(t, svc, "bench_lifecycle")
	loadBenchmarkDocsDeferred(t, svc, "bench_lifecycle", []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}},
	})
	if _, err := svc.OptimizeIndex(ctx, "bench_lifecycle", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 1 {
		t.Fatalf("cache size after optimize=%d want 1", got)
	}

	if _, err := svc.UpsertDocuments(ctx, "bench_lifecycle", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "alpha updated", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("UpsertDocuments non-deferred: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 0 {
		t.Fatalf("cache size after non-deferred upsert=%d want 0", got)
	}

	col, info, err := svc.openIndex(ctx, "bench_lifecycle", 0)
	if err != nil {
		t.Fatalf("open bench_lifecycle: %v", err)
	}
	if err := svc.primeBenchmarkSearchCache("schema_create", col, info); err != nil {
		t.Fatalf("prime schema_create cache: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 1 {
		t.Fatalf("cache size after schema_create prime=%d want 1", got)
	}
	if _, err := svc.ResetIndex(ctx, "schema_create", ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: benchmarkColumnGraphOptions()}); err != nil {
		t.Fatalf("ResetIndex schema_create: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 0 {
		t.Fatalf("cache size after create/index schema boundary=%d want 0", got)
	}
}

func TestServiceBenchmarkVectorSearchCacheInvalidatesOnDeleteResetAndClose(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	createBenchmarkColumnGraphIndex(t, svc, "bench_delete")
	loadBenchmarkDocsDeferred(t, svc, "bench_delete", []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}},
	})
	if _, err := svc.OptimizeIndex(ctx, "bench_delete", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 1 {
		t.Fatalf("cache size after optimize=%d want 1", got)
	}
	if _, err := svc.DeleteDocuments(ctx, "bench_delete", DeleteDocumentsRequest{IDs: []string{"b"}}); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 0 {
		t.Fatalf("cache size after delete=%d want 0", got)
	}

	if _, err := svc.ResetIndex(ctx, "native_reset", ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}}); err != nil {
		t.Fatalf("ResetIndex native create: %v", err)
	}
	col, info, err := svc.openIndex(ctx, "native_reset", 0)
	if err != nil {
		t.Fatalf("open native_reset: %v", err)
	}
	if err := svc.primeBenchmarkSearchCache("native_reset", col, info); err != nil {
		t.Fatalf("prime native cache: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 1 {
		t.Fatalf("cache size after native prime=%d want 1", got)
	}
	if _, err := svc.ResetIndex(ctx, "native_reset", ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}}); err != nil {
		t.Fatalf("ResetIndex native existing: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 0 {
		t.Fatalf("cache size after reset=%d want 0", got)
	}

	createBenchmarkColumnGraphIndex(t, svc, "bench_close")
	loadBenchmarkDocsDeferred(t, svc, "bench_close", []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0}}})
	if _, err := svc.OptimizeIndex(ctx, "bench_close", OptimizeIndexRequest{}); err != nil {
		t.Fatalf("OptimizeIndex bench_close: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 1 {
		t.Fatalf("cache size before close=%d want 1", got)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Service.Close: %v", err)
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 0 {
		t.Fatalf("cache size after close=%d want 0", got)
	}
	if got := svc.benchmarkSearchBufferPool.Get(); got != nil {
		t.Fatalf("benchmark search buffer pool retained %T after close", got)
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "bench_close", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1}); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("search after close err=%v code=%s, want index_unavailable", err, ErrorCodeOf(err))
	}
}

func benchmarkColumnGraphOptions() *BenchmarkVectorIndexOptions {
	return &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 4, EfSearch: 8}
}

func createBenchmarkColumnGraphIndex(t testing.TB, svc *Service, index string) {
	t.Helper()
	if _, err := svc.ResetIndex(context.Background(), index, ResetIndexRequest{Dimension: 2, Metric: MetricCosine, DropOld: true, VectorIndexOptions: benchmarkColumnGraphOptions()}); err != nil {
		t.Fatalf("ResetIndex %s create: %v", index, err)
	}
}

func loadBenchmarkDocsDeferred(t testing.TB, svc *Service, index string, docs []Document) {
	t.Helper()
	if _, err := svc.UpsertDocuments(context.Background(), index, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("UpsertDocuments %s deferred: %v", index, err)
	}
}

func assertBenchmarkCacheHit(t *testing.T, response BenchmarkVectorSearchResponse, label string) {
	t.Helper()
	if response.Stats.HNSWSearchPackCacheHits != 1 || response.Stats.HNSWSearchPackCacheMisses != 0 || response.Stats.HNSWSearchPackCacheBuilds != 0 {
		t.Fatalf("%s cache stats hits=%d misses=%d builds=%d waits=%d, want one hit and no miss/build", label, response.Stats.HNSWSearchPackCacheHits, response.Stats.HNSWSearchPackCacheMisses, response.Stats.HNSWSearchPackCacheBuilds, response.Stats.HNSWSearchPackCacheWaits)
	}
}
