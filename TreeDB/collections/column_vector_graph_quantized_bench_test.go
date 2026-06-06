package collections

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const columnGraphScalarU8QuantizedBenchIndexName1926 = "embedding.scalar_u8.fast"

type columnGraphScalarU8QuantizedBenchShape1926 struct {
	rows         int
	dims         int
	m            int
	topK         int
	efSearch     int
	queryOrdinal int
}

type columnGraphScalarU8QuantizedBenchFixture1926 struct {
	close               func()
	collection          *Collection
	definition          VectorIndexDefinition
	query               []float32
	shape               columnGraphScalarU8QuantizedBenchShape1926
	quantizedAssetBytes int64
}

type columnGraphScalarU8QuantizedSearchWithBufferBenchCase2414 struct {
	name             string
	mode             VectorIndexQueryMode
	rerankCandidates int
	concurrency      int
}

func columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() []columnGraphScalarU8QuantizedSearchWithBufferBenchCase2414 {
	return []columnGraphScalarU8QuantizedSearchWithBufferBenchCase2414{
		{name: "route=quantized_only/c=1", mode: VectorIndexQueryModeQuantizedOnly, concurrency: 1},
		{name: "route=quantized_only/c=8", mode: VectorIndexQueryModeQuantizedOnly, concurrency: 8},
		{name: "route=quantized_rerank/candidates=32/c=1", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: 32, concurrency: 1},
		{name: "route=quantized_rerank/candidates=32/c=8", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: 32, concurrency: 8},
	}
}

type columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415 struct {
	name             string
	mode             VectorIndexQueryMode
	rerankCandidates int
	concurrency      int
}

func columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415() []columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415 {
	return []columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415{
		{name: "route=quantized_only/c=1", mode: VectorIndexQueryModeQuantizedOnly, concurrency: 1},
		{name: "route=quantized_only/c=8", mode: VectorIndexQueryModeQuantizedOnly, concurrency: 8},
		{name: "route=quantized_rerank/candidates=32/c=1", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: 32, concurrency: 1},
		{name: "route=quantized_rerank/candidates=32/c=8", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: 32, concurrency: 8},
	}
}

func BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 1024, dims: 128, m: 16, topK: 10, efSearch: 128, queryOrdinal: 37}
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	cases := []struct {
		name               string
		mode               VectorIndexQueryMode
		rerankCandidates   int
		quantizedIndexName string
	}{
		{name: "mode=exact", mode: VectorIndexQueryModeExact},
		{name: "mode=quantized_only", mode: VectorIndexQueryModeQuantizedOnly, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926},
		{name: "mode=quantized_rerank/candidates=10", mode: VectorIndexQueryModeQuantizedRerank, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, rerankCandidates: 10},
		{name: "mode=quantized_rerank/candidates=32", mode: VectorIndexQueryModeQuantizedRerank, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, rerankCandidates: 32},
		{name: "mode=quantized_rerank/candidates=128", mode: VectorIndexQueryModeQuantizedRerank, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, rerankCandidates: 128},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
			if err != nil {
				b.Fatalf("OpenVectorIndexSearcher: %v", err)
			}
			defer func() { _ = searcher.Close() }()
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        tc.quantizedIndexName,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
			}
			var buffer VectorIndexSearchBuffer
			warm, err := searcher.SearchWithBuffer(opts, &buffer)
			if err != nil {
				b.Fatalf("warm SearchWithBuffer: %v", err)
			}
			if len(warm.Results) == 0 {
				b.Fatalf("warm SearchWithBuffer returned no results")
			}

			var stats VectorIndexSearchStats
			var recallSum float64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response, err := searcher.SearchWithBuffer(opts, &buffer)
				if err != nil {
					b.Fatalf("SearchWithBuffer: %v", err)
				}
				if len(response.Results) == 0 {
					b.Fatalf("SearchWithBuffer returned no results")
				}
				columnPhysicalScanBenchSum += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, response.Stats)
				recallSum += columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(response.Results, exactIDs, exactCount)
			}
			b.StopTimer()
			reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
		})
	}
}

func BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 1024, dims: 128, m: 16, topK: 10, efSearch: 128, queryOrdinal: 37}
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphScalarU8QuantizedBenchIndexName1926,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphScalarU8QuantizedSearchWithBufferBench2414(b, fixture, opts, tc.concurrency, exactIDs, exactCount)
		})
	}
}

func BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 1024, dims: 128, m: 16, topK: 10, efSearch: 128, queryOrdinal: 37}
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
	defer fixture.close()
	exactIDs, exactCount := columnGraphScalarU8QuantizedBenchmarkExactIDs1926(b, fixture)

	for _, tc := range columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415() {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			opts := VectorIndexSearchOptions{
				IndexName:                 fixture.definition.Name,
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        columnGraphScalarU8QuantizedBenchIndexName1926,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				MaxDecodedBlocks:          1,
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			runColumnGraphScalarU8QuantizedCollectionWithBufferBench2415(b, fixture, opts, tc.concurrency, exactIDs, exactCount)
		})
	}
}

func runColumnGraphScalarU8QuantizedCollectionWithBufferBench2415(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearchOptions, concurrency int, exactIDs map[string]struct{}, exactCount int) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	type benchWorker struct {
		buffer VectorIndexSearchBuffer
		stats  VectorIndexSearchStats
		sink   int64
	}
	if err := fixture.collection.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
		b.Fatalf("close collection prepared cache before benchmark row: %v", err)
	}
	workers := make([]benchWorker, concurrency)
	for i := range workers {
		warmOpts := opts
		warmOpts.Query = fixture.query
		warm, err := fixture.collection.SearchVectorIndexWithBuffer(warmOpts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm SearchVectorIndexWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm SearchVectorIndexWithBuffer worker %d returned no results", i)
		}
		assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(b, warm.Stats, opts, fixture.definition.Dimensions)
	}
	statsOpts := opts
	statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
	measured, err := fixture.collection.SearchVectorIndexWithBuffer(statsOpts, &workers[0].buffer)
	if err != nil {
		b.Fatalf("measure SearchVectorIndexWithBuffer stats: %v", err)
	}
	assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(b, measured.Stats, statsOpts, fixture.definition.Dimensions)
	warmRecall := columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(measured.Results, exactIDs, exactCount)

	var next atomic.Uint64
	var sink atomic.Int64
	var failed atomic.Bool
	var firstErr atomic.Value
	recordErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	var wg sync.WaitGroup
	ready := make(chan struct{}, len(workers))
	start := make(chan struct{})
	wg.Add(len(workers))
	for i := range workers {
		worker := &workers[i]
		go func() {
			defer wg.Done()
			localOpts := opts
			localOpts.Query = fixture.query
			for warmIter := 0; warmIter < 16; warmIter++ {
				if warm, err := fixture.collection.SearchVectorIndexWithBuffer(localOpts, &worker.buffer); err != nil {
					recordErr("goroutine warm SearchVectorIndexWithBuffer: %v", err)
					break
				} else if len(warm.Results) == 0 {
					recordErr("goroutine warm SearchVectorIndexWithBuffer returned no results")
					break
				}
			}
			ready <- struct{}{}
			<-start
			var localStats VectorIndexSearchStats
			var localSink int64
			for {
				iteration := int(next.Add(1)) - 1
				if iteration >= b.N {
					break
				}
				if failed.Load() {
					continue
				}
				response, err := fixture.collection.SearchVectorIndexWithBuffer(localOpts, &worker.buffer)
				if err != nil {
					recordErr("SearchVectorIndexWithBuffer: %v", err)
					continue
				}
				if len(response.Results) == 0 {
					recordErr("SearchVectorIndexWithBuffer returned no results")
					continue
				}
				localSink += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
			}
			worker.stats = localStats
			worker.sink = localSink
		}()
	}
	for range workers {
		<-ready
	}
	if errValue := firstErr.Load(); errValue != nil {
		close(start)
		wg.Wait()
		b.Fatalf("%s", errValue.(string))
	}
	cacheBeforeTimed := fixture.collection.collectionVectorIndexPreparedSearchCacheSnapshot()
	b.ReportAllocs()
	b.ReportMetric(float64(concurrency), "concurrency")
	b.ReportMetric(1, "collection_searchvectorindex_with_buffer_seam")
	b.ReportMetric(1, "collection_buffered_quantized_row")
	b.ReportMetric(0, "open_searcher_calls/op")
	b.ReportMetric(0, "open_setup_in_timed_loop")
	b.ReportMetric(1, "reported_stats_mode_production")
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	var stats VectorIndexSearchStats
	for i := range workers {
		addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, workers[i].stats)
		sink.Add(workers[i].sink)
	}
	columnPhysicalScanBenchSum += sink.Load()
	recallSum := warmRecall * float64(b.N)
	reportCollectionVectorIndexPreparedSearchBenchMetrics2415(b, cacheBeforeTimed, fixture.collection.collectionVectorIndexPreparedSearchCacheSnapshot())
	reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
}

func reportCollectionVectorIndexPreparedSearchBenchMetrics2415(b *testing.B, before, after collectionVectorIndexPreparedSearchCacheSnapshot) {
	b.Helper()
	iterations := float64(b.N)
	if iterations <= 0 {
		iterations = 1
	}
	cacheBuilds := after.CacheBuilds - before.CacheBuilds
	cacheMisses := after.CacheMisses - before.CacheMisses
	cacheHits := after.CacheHits - before.CacheHits
	cacheWaits := after.CacheWaits - before.CacheWaits
	invalidations := after.Invalidations - before.Invalidations
	closes := after.Closes - before.Closes
	errors := after.Errors - before.Errors
	b.ReportMetric(float64(after.Entries), "collection_prepared_cache_entries")
	b.ReportMetric(float64(after.BuildingEntries), "collection_prepared_cache_building_entries")
	b.ReportMetric(float64(cacheBuilds)/iterations, "collection_prepared_cache_builds/op")
	b.ReportMetric(float64(cacheMisses)/iterations, "collection_prepared_cache_misses/op")
	b.ReportMetric(float64(cacheHits)/iterations, "collection_prepared_cache_hits/op")
	b.ReportMetric(float64(cacheWaits)/iterations, "collection_prepared_cache_waits/op")
	b.ReportMetric(float64(invalidations)/iterations, "collection_prepared_cache_invalidations/op")
	b.ReportMetric(float64(closes)/iterations, "collection_prepared_cache_closes/op")
	b.ReportMetric(float64(errors)/iterations, "collection_prepared_cache_errors/op")
	if lookups := cacheHits + cacheMisses; lookups > 0 {
		b.ReportMetric(float64(cacheHits)/float64(lookups), "collection_prepared_cache_hit_ratio")
	}
	b.ReportMetric(float64(after.ActiveHandles), "collection_prepared_active_handles")
	b.ReportMetric(float64(after.ActiveMappedBytes), "collection_prepared_mapped_B")
	b.ReportMetric(float64(after.ActiveHeapCopyBytes), "collection_prepared_heap_copy_B")
	b.ReportMetric(float64(after.ActiveDerivedMetadataBytes), "collection_prepared_derived_metadata_B")
}

func assertColumnGraphScalarU8QuantizedCollectionWithBufferGuardrails2415(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearchOptions, dims int) {
	tb.Helper()
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		tb.Fatalf("normalize query mode: %v", err)
	}
	if !vectorIndexSearchStatsAreBufferedNoDocumentQuantizedRoute(stats, queryMode, opts, dims) {
		tb.Fatalf("Collection.SearchVectorIndexWithBuffer quantized stats=%+v want no-document quantized route", stats)
	}
}

func runColumnGraphScalarU8QuantizedSearchWithBufferBench2414(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, opts VectorIndexSearcherSearchOptions, concurrency int, exactIDs map[string]struct{}, exactCount int) {
	b.Helper()
	if concurrency <= 0 {
		b.Fatalf("concurrency=%d must be positive", concurrency)
	}
	type benchWorker struct {
		searcher *VectorIndexSearcher
		buffer   VectorIndexSearchBuffer
		stats    VectorIndexSearchStats
		sink     int64
	}
	workers := make([]benchWorker, concurrency)
	for i := range workers {
		searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		defer func(searcher *VectorIndexSearcher) { _ = searcher.Close() }(searcher)
		workers[i].searcher = searcher
		warm, err := searcher.SearchWithBuffer(opts, &workers[i].buffer)
		if err != nil {
			b.Fatalf("warm SearchWithBuffer worker %d: %v", i, err)
		}
		if len(warm.Results) == 0 {
			b.Fatalf("warm SearchWithBuffer worker %d returned no results", i)
		}
		assertColumnGraphScalarU8QuantizedSearchWithBufferGuardrails2414(b, warm.Stats, opts, fixture.definition.Dimensions)
	}
	warmRecall := columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(workers[0].buffer.results, exactIDs, exactCount)

	var next atomic.Uint64
	var sink atomic.Int64
	var failed atomic.Bool
	var firstErr atomic.Value
	recordErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(len(workers))
	for i := range workers {
		worker := &workers[i]
		go func() {
			defer wg.Done()
			<-start
			var localStats VectorIndexSearchStats
			var localSink int64
			for {
				iteration := int(next.Add(1)) - 1
				if iteration >= b.N {
					break
				}
				if failed.Load() {
					continue
				}
				response, err := worker.searcher.SearchWithBuffer(opts, &worker.buffer)
				if err != nil {
					recordErr("SearchWithBuffer: %v", err)
					continue
				}
				if len(response.Results) == 0 {
					recordErr("SearchWithBuffer returned no results")
					continue
				}
				localSink += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&localStats, response.Stats)
			}
			worker.stats = localStats
			worker.sink = localSink
		}()
	}
	b.ReportAllocs()
	b.ReportMetric(float64(concurrency), "concurrency")
	b.ReportMetric(1, "searchwithbuffer_buffered_row")
	b.ReportMetric(1, "reported_stats_mode_production")
	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	var stats VectorIndexSearchStats
	for i := range workers {
		addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, workers[i].stats)
		sink.Add(workers[i].sink)
	}
	columnPhysicalScanBenchSum += sink.Load()
	recallSum := warmRecall * float64(b.N)
	reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b, fixture, stats, recallSum)
}

func assertColumnGraphScalarU8QuantizedSearchWithBufferGuardrails2414(tb testing.TB, stats VectorIndexSearchStats, opts VectorIndexSearcherSearchOptions, dims int) {
	tb.Helper()
	switch opts.QueryMode {
	case VectorIndexQueryModeQuantizedOnly:
		assertQuantizedOnlyGuardrailStats2416(tb, stats, dims)
	case VectorIndexQueryModeQuantizedRerank:
		assertQuantizedRerankNoDocumentGuardrailStats2416(tb, stats, opts.QuantizedRerankCandidates)
	default:
		tb.Fatalf("unexpected SearchWithBuffer benchmark query mode %q", opts.QueryMode)
	}
	if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("SearchWithBuffer buffered guardrails stats=%+v want no docs/materialization/fallback/scratch", stats)
	}
}

func BenchmarkColumnGraphScalarU8QuantizedTraversalCounters2271(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 1024, dims: 128, m: 16, topK: 10, efSearch: 128, queryOrdinal: 37}
	fixture := openColumnGraphScalarU8QuantizedBenchFixture1926(b, shape, true)
	defer fixture.close()

	cases := []struct {
		name               string
		mode               VectorIndexQueryMode
		rerankCandidates   int
		quantizedIndexName string
	}{
		{name: "mode=exact", mode: VectorIndexQueryModeExact},
		{name: "mode=quantized_only", mode: VectorIndexQueryModeQuantizedOnly, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926},
		{name: "mode=quantized_rerank/candidates=32", mode: VectorIndexQueryModeQuantizedRerank, quantizedIndexName: columnGraphScalarU8QuantizedBenchIndexName1926, rerankCandidates: 32},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
			if err != nil {
				b.Fatalf("OpenVectorIndexSearcher: %v", err)
			}
			defer func() { _ = searcher.Close() }()
			opts := VectorIndexSearcherSearchOptions{
				Query:                     fixture.query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        tc.quantizedIndexName,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      fixture.shape.topK,
				EfSearch:                  fixture.shape.efSearch,
				StatsMode:                 VectorIndexSearchStatsModeBenchmarkDebug,
			}
			var buffer VectorIndexSearchBuffer
			warm, err := searcher.SearchWithBuffer(opts, &buffer)
			if err != nil {
				b.Fatalf("warm SearchWithBuffer: %v", err)
			}
			if len(warm.Results) == 0 || warm.Stats.BenchmarkDebugSearches != 1 {
				b.Fatalf("warm response results=%d stats=%+v want benchmark_debug counters", len(warm.Results), warm.Stats)
			}

			var stats VectorIndexSearchStats
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response, err := searcher.SearchWithBuffer(opts, &buffer)
				if err != nil {
					b.Fatalf("SearchWithBuffer: %v", err)
				}
				if len(response.Results) == 0 {
					b.Fatalf("SearchWithBuffer returned no results")
				}
				columnPhysicalScanBenchSum += int64(response.Results[0].Ordinal)
				addColumnGraphScalarU8QuantizedBenchmarkStats1926(&stats, response.Stats)
			}
			b.StopTimer()
			reportColumnGraphScalarU8QuantizedTraversalCounterMetrics2271(b, fixture, stats)
		})
	}
}

func BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926(b *testing.B) {
	shape := columnGraphScalarU8QuantizedBenchShape1926{rows: 256, dims: 128, m: 16, topK: 10, efSearch: 128, queryOrdinal: 37}
	for _, tc := range []struct {
		name      string
		quantized bool
	}{
		{name: "mode=exact_assets", quantized: false},
		{name: "mode=scalar_u8_assets", quantized: true},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			_, d, col, def, _ := openColumnGraphScalarU8QuantizedBenchCollection1926(b, shape, tc.quantized)
			defer func() { _ = d.Close() }()
			b.ReportAllocs()
			reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b, shape)
			if tc.quantized {
				b.ReportMetric(1, "quantized_indexes")
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				status, err := col.RebuildVectorIndex(def.Name)
				if err != nil {
					b.Fatalf("RebuildVectorIndex: %v", err)
				}
				if !status.Loaded || status.RebuildNeeded {
					b.Fatalf("status=%+v, want loaded", status)
				}
				columnGraphRebuildBenchSinkV2A = status
			}
			b.StopTimer()
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
			}
			reportColumnGraphScalarU8QuantizedStorageMetrics1926(b, d, def, shape)
		})
	}
}

func openColumnGraphScalarU8QuantizedBenchFixture1926(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926, quantized bool) columnGraphScalarU8QuantizedBenchFixture1926 {
	tb.Helper()
	_, d, col, def, rows := openColumnGraphScalarU8QuantizedBenchCollection1926(tb, shape, quantized)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(tb, status, def.Name)
	if shape.queryOrdinal < 0 || shape.queryOrdinal >= len(rows) {
		_ = d.Close()
		tb.Fatalf("query ordinal=%d out of range rows=%d", shape.queryOrdinal, len(rows))
	}
	fixture := columnGraphScalarU8QuantizedBenchFixture1926{
		close:      func() { _ = d.Close() },
		collection: col,
		definition: def,
		query:      append([]float32(nil), rows[shape.queryOrdinal].vector...),
		shape:      shape,
	}
	if quantized {
		fixture.quantizedAssetBytes = columnGraphScalarU8QuantizedAssetBytes1926(tb, d, def)
	}
	return fixture
}

func openColumnGraphScalarU8QuantizedBenchCollection1926(tb testing.TB, shape columnGraphScalarU8QuantizedBenchShape1926, quantized bool) (string, *backenddb.DB, *Collection, VectorIndexDefinition, []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	if shape.rows <= 0 || shape.dims <= 0 {
		tb.Fatalf("invalid benchmark shape rows=%d dims=%d", shape.rows, shape.dims)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphRebuildVectorIndexDefinitionV2A(shape.dims, shape.m)
	if quantized {
		def.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: columnGraphScalarU8QuantizedBenchIndexName1926}}
		var err error
		def, err = normalizeVectorIndexDefinition(def)
		if err != nil {
			_ = d.Close()
			tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
		}
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(shape.dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	rows := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	insertColumnGraphRebuildRowsV2A(tb, col, rows)
	return dir, d, col, def, rows
}

func columnGraphScalarU8QuantizedBenchmarkExactIDs1926(tb testing.TB, fixture columnGraphScalarU8QuantizedBenchFixture1926) (map[string]struct{}, int) {
	tb.Helper()
	searcher, err := fixture.collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: fixture.definition.Name, MaxDecodedBlocks: 1})
	if err != nil {
		tb.Fatalf("OpenVectorIndexSearcher exact reference: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	var buffer VectorIndexSearchBuffer
	response, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: fixture.query, QueryMode: VectorIndexQueryModeExact, TopK: fixture.shape.topK, EfSearch: fixture.shape.efSearch}, &buffer)
	if err != nil {
		tb.Fatalf("exact reference SearchWithBuffer: %v", err)
	}
	if len(response.Results) == 0 {
		tb.Fatalf("exact reference returned no results")
	}
	ids := make(map[string]struct{}, len(response.Results))
	for _, result := range response.Results {
		ids[string(result.ID)] = struct{}{}
	}
	return ids, len(response.Results)
}

func columnGraphScalarU8QuantizedBenchmarkRecallAtK1926(results []VectorIndexSearchResult, exactIDs map[string]struct{}, exactCount int) float64 {
	if exactCount == 0 {
		return 0
	}
	overlap := 0
	for _, result := range results {
		if _, ok := exactIDs[string(result.ID)]; ok {
			overlap++
		}
	}
	return float64(overlap) / float64(exactCount)
}

func addColumnGraphScalarU8QuantizedBenchmarkStats1926(dst *VectorIndexSearchStats, src VectorIndexSearchStats) {
	dst.CandidateRows += src.CandidateRows
	dst.Candidates += src.Candidates
	dst.Edges += src.Edges
	dst.VisitedNodes += src.VisitedNodes
	dst.VisitedEdges += src.VisitedEdges
	dst.VectorBytesRead += src.VectorBytesRead
	dst.NormBytesRead += src.NormBytesRead
	dst.AdjacencyBytesRead += src.AdjacencyBytesRead
	dst.CandidateFetches += src.CandidateFetches
	dst.ExpansionFetches += src.ExpansionFetches
	dst.ResultFetches += src.ResultFetches
	dst.DocumentsFetched += src.DocumentsFetched
	dst.ScoreBatchCalls += src.ScoreBatchCalls
	dst.ScoreBatchCandidates += src.ScoreBatchCandidates
	if src.ScoreBatchMaxTileSize > dst.ScoreBatchMaxTileSize {
		dst.ScoreBatchMaxTileSize = src.ScoreBatchMaxTileSize
	}
	dst.ScoreBatchOptimizedCalls += src.ScoreBatchOptimizedCalls
	dst.ScoreBatchScalarFallbackCalls += src.ScoreBatchScalarFallbackCalls
	dst.PreparedScoreCalls += src.PreparedScoreCalls
	dst.QuantizedScoreCalls += src.QuantizedScoreCalls
	dst.QuantizedCodeBytesRead += src.QuantizedCodeBytesRead
	dst.QuantizedRerankCandidates += src.QuantizedRerankCandidates
	dst.QuantizedRerankExactScoreCalls += src.QuantizedRerankExactScoreCalls
	dst.QuantizedScorerActive += src.QuantizedScorerActive
	dst.QuantizedAssetMissing += src.QuantizedAssetMissing
	dst.QuantizedAssetInvalid += src.QuantizedAssetInvalid
	dst.QuantizedAssetStale += src.QuantizedAssetStale
	dst.QuantizedAssetClosed += src.QuantizedAssetClosed
	dst.QuantizedAssetUnavailable += src.QuantizedAssetUnavailable
	dst.QuantizedAssetMmapDirect += src.QuantizedAssetMmapDirect
	dst.QuantizedAssetHeapCopy += src.QuantizedAssetHeapCopy
	dst.QuantizedAssetOpenNanos += src.QuantizedAssetOpenNanos
	dst.QuantizedAssetMappedBytes += src.QuantizedAssetMappedBytes
	dst.QuantizedAssetHeapCopyBytes += src.QuantizedAssetHeapCopyBytes
	dst.QuantizedAssetActiveHandles += src.QuantizedAssetActiveHandles
	dst.ScoreFloat64Fallbacks += src.ScoreFloat64Fallbacks
	dst.PreparedGraphSearchViews += src.PreparedGraphSearchViews
	dst.GraphRowFallbacks += src.GraphRowFallbacks
	dst.SearchRouteColumnGraphPrepared += src.SearchRouteColumnGraphPrepared
	dst.SearchRouteColumnGraphFallback += src.SearchRouteColumnGraphFallback
	dst.SearchRouteHNSWSearchPack += src.SearchRouteHNSWSearchPack
	dst.SearchRouteQuantizedOnly += src.SearchRouteQuantizedOnly
	dst.SearchRouteQuantizedRerank += src.SearchRouteQuantizedRerank
	dst.HNSWSearchPackActive += src.HNSWSearchPackActive
	dst.HNSWSearchPackMissing += src.HNSWSearchPackMissing
	dst.HNSWSearchPackInvalid += src.HNSWSearchPackInvalid
	dst.HNSWSearchPackStale += src.HNSWSearchPackStale
	dst.HNSWSearchPackClosed += src.HNSWSearchPackClosed
	dst.HNSWSearchPackFallbacks += src.HNSWSearchPackFallbacks
	dst.HNSWSearchPackMmapDirect += src.HNSWSearchPackMmapDirect
	dst.HNSWSearchPackHeapCopy += src.HNSWSearchPackHeapCopy
	dst.HNSWSearchPackOpenNanos += src.HNSWSearchPackOpenNanos
	dst.HNSWSearchPackMappedBytes += src.HNSWSearchPackMappedBytes
	dst.HNSWSearchPackHeapCopyBytes += src.HNSWSearchPackHeapCopyBytes
	dst.HNSWSearchPackActiveHandles += src.HNSWSearchPackActiveHandles
	dst.TypedColumnFallbacks += src.TypedColumnFallbacks
	dst.VectorScratchDecodes += src.VectorScratchDecodes
	dst.AdjacencyLegacyFallbacks += src.AdjacencyLegacyFallbacks
	dst.AdjacencySourceFallbacks += src.AdjacencySourceFallbacks
	dst.ResultIDGraphFallbacks += src.ResultIDGraphFallbacks
	dst.ResultIDTypedBytesState += src.ResultIDTypedBytesState
	dst.RowRefVectorSourceState += src.RowRefVectorSourceState
	dst.RowRefVectorSourceLegacyGraphIDs += src.RowRefVectorSourceLegacyGraphIDs
	dst.BenchmarkDebugSearches += src.BenchmarkDebugSearches
	dst.NeighborTiles += src.NeighborTiles
	dst.NeighborTileNeighbors += src.NeighborTileNeighbors
	if src.NeighborTileMaxSize > dst.NeighborTileMaxSize {
		dst.NeighborTileMaxSize = src.NeighborTileMaxSize
	}
	dst.ScoredNeighbors += src.ScoredNeighbors
	dst.SkippedNeighbors += src.SkippedNeighbors
	dst.AlreadyVisitedSkips += src.AlreadyVisitedSkips
	dst.FilterSkips += src.FilterSkips
	dst.UpperLayerScores += src.UpperLayerScores
	dst.UpperLayerScoreTiles += src.UpperLayerScoreTiles
	dst.UpperLayerScoreTileCandidates += src.UpperLayerScoreTileCandidates
	if src.UpperLayerScoreTileMaxSize > dst.UpperLayerScoreTileMaxSize {
		dst.UpperLayerScoreTileMaxSize = src.UpperLayerScoreTileMaxSize
	}
	dst.UpperLayerAdjacencyLoads += src.UpperLayerAdjacencyLoads
	dst.UpperLayerAdjacencyNeighbors += src.UpperLayerAdjacencyNeighbors
	dst.UpperLayerEdgeVisits += src.UpperLayerEdgeVisits
	dst.Layer0Scores += src.Layer0Scores
	dst.Layer0ScoreTiles += src.Layer0ScoreTiles
	dst.Layer0ScoreTileCandidates += src.Layer0ScoreTileCandidates
	if src.Layer0ScoreTileMaxSize > dst.Layer0ScoreTileMaxSize {
		dst.Layer0ScoreTileMaxSize = src.Layer0ScoreTileMaxSize
	}
	dst.Layer0AdjacencyLoads += src.Layer0AdjacencyLoads
	dst.Layer0AdjacencyNeighbors += src.Layer0AdjacencyNeighbors
	dst.Layer0EdgeVisits += src.Layer0EdgeVisits
	dst.Layer0ScoredNeighbors += src.Layer0ScoredNeighbors
	dst.Layer0AlreadyVisitedSkips += src.Layer0AlreadyVisitedSkips
	dst.Layer0FilterSkips += src.Layer0FilterSkips
	dst.Layer0StopChecks += src.Layer0StopChecks
	dst.Layer0StopTrue += src.Layer0StopTrue
	dst.Layer0StopFalse += src.Layer0StopFalse
	dst.CandidateComparisons += src.CandidateComparisons
	dst.FrontierComparisons += src.FrontierComparisons
	dst.TopKComparisons += src.TopKComparisons
	dst.FrontierPushes += src.FrontierPushes
	dst.FrontierPops += src.FrontierPops
	dst.FrontierPopMisses += src.FrontierPopMisses
	dst.FrontierSiftUpCalls += src.FrontierSiftUpCalls
	dst.FrontierSiftDownCalls += src.FrontierSiftDownCalls
	dst.FrontierSiftUpSteps += src.FrontierSiftUpSteps
	dst.FrontierSiftDownSteps += src.FrontierSiftDownSteps
	dst.TopKInsertAttempts += src.TopKInsertAttempts
	dst.TopKInsertSuccesses += src.TopKInsertSuccesses
	dst.TopKInsertRejections += src.TopKInsertRejections
	dst.TopKShiftSteps += src.TopKShiftSteps
	dst.VisitedMarkChecks += src.VisitedMarkChecks
	dst.VisitedMarkHits += src.VisitedMarkHits
	dst.VisitedMarkMisses += src.VisitedMarkMisses
	dst.VisitedMarkInserts += src.VisitedMarkInserts
	dst.VisitedResetEpochAdvances += src.VisitedResetEpochAdvances
	dst.VisitedResetClearedRows += src.VisitedResetClearedRows
}

func reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b *testing.B, shape columnGraphScalarU8QuantizedBenchShape1926) {
	b.Helper()
	b.ReportMetric(float64(shape.rows), "rows")
	b.ReportMetric(float64(shape.dims), "dims")
	b.ReportMetric(float64(shape.m), "degree")
	b.ReportMetric(float64(shape.topK), "top_k")
	b.ReportMetric(float64(shape.efSearch), "ef_search")
}

func reportColumnGraphScalarU8QuantizedScorePlaneMetrics1926(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, stats VectorIndexSearchStats, recallSum float64) {
	b.Helper()
	reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b, fixture.shape)
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
	}
	denom := float64(b.N)
	if denom == 0 {
		denom = 1
	}
	b.ReportMetric((recallSum/denom)*100, "recall_at_k_pct")
	b.ReportMetric(float64(stats.CandidateRows)/denom, "candidate_rows/search")
	b.ReportMetric(float64(stats.Candidates)/denom, "candidates/search")
	b.ReportMetric(float64(stats.Edges)/denom, "edges/search")
	b.ReportMetric(float64(stats.VisitedNodes)/denom, "visited_nodes/search")
	b.ReportMetric(float64(stats.VisitedEdges)/denom, "visited_edges/search")
	b.ReportMetric(float64(stats.CandidateFetches)/denom, "candidate_fetches/search")
	b.ReportMetric(float64(stats.ExpansionFetches)/denom, "expansion_fetches/search")
	b.ReportMetric(float64(stats.ResultFetches)/denom, "result_fetches/search")
	b.ReportMetric(float64(stats.DocumentsFetched)/denom, "docs_fetched/search")
	b.ReportMetric(float64(stats.VectorBytesRead)/denom, "vector_B/search")
	b.ReportMetric(float64(stats.NormBytesRead)/denom, "norm_B/search")
	b.ReportMetric(float64(stats.AdjacencyBytesRead)/denom, "adjacency_B/search")
	b.ReportMetric(float64(stats.ScoreBatchCalls)/denom, "score_batch_calls/search")
	b.ReportMetric(float64(stats.ScoreBatchCandidates)/denom, "score_batch_candidates/search")
	b.ReportMetric(float64(stats.ScoreBatchMaxTileSize), "score_batch_max_tile_size")
	b.ReportMetric(float64(stats.ScoreBatchOptimizedCalls)/denom, "score_batch_optimized/search")
	b.ReportMetric(float64(stats.ScoreBatchScalarFallbackCalls)/denom, "score_batch_fallback/search")
	b.ReportMetric(float64(stats.PreparedScoreCalls)/denom, "prepared_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedScoreCalls)/denom, "quantized_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedCodeBytesRead)/denom, "quantized_code_B/search")
	b.ReportMetric(float64(stats.QuantizedRerankCandidates)/denom, "quantized_rerank_candidates/search")
	b.ReportMetric(float64(stats.QuantizedRerankExactScoreCalls)/denom, "quantized_rerank_exact_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedScorerActive)/denom, "quantized_scorer_active/search")
	b.ReportMetric(float64(stats.QuantizedAssetMissing)/denom, "quantized_asset_missing/search")
	b.ReportMetric(float64(stats.QuantizedAssetInvalid)/denom, "quantized_asset_invalid/search")
	b.ReportMetric(float64(stats.QuantizedAssetStale)/denom, "quantized_asset_stale/search")
	b.ReportMetric(float64(stats.QuantizedAssetClosed)/denom, "quantized_asset_closed/search")
	b.ReportMetric(float64(stats.QuantizedAssetUnavailable)/denom, "quantized_asset_unavailable/search")
	b.ReportMetric(float64(stats.QuantizedAssetMmapDirect)/denom, "quantized_asset_mmap_direct/search")
	b.ReportMetric(float64(stats.QuantizedAssetHeapCopy)/denom, "quantized_asset_heap_copy/search")
	b.ReportMetric(float64(stats.QuantizedAssetOpenNanos)/denom, "quantized_asset_open_ns")
	b.ReportMetric(float64(stats.QuantizedAssetMappedBytes)/denom, "quantized_asset_mapped_B")
	b.ReportMetric(float64(stats.QuantizedAssetHeapCopyBytes)/denom, "quantized_asset_heap_copy_B")
	b.ReportMetric(float64(stats.QuantizedAssetActiveHandles)/denom, "quantized_asset_active_handles")
	b.ReportMetric(float64(stats.ScoreFloat64Fallbacks)/denom, "score_float64_fallbacks/search")
	b.ReportMetric(float64(stats.PreparedGraphSearchViews)/denom, "prepared_graph_search_views/search")
	b.ReportMetric(float64(stats.GraphRowFallbacks)/denom, "graph_row_fallbacks/search")
	b.ReportMetric(float64(stats.SearchRouteColumnGraphPrepared)/denom, "search_route_column_graph_prepared/search")
	b.ReportMetric(float64(stats.SearchRouteColumnGraphFallback)/denom, "search_route_column_graph_fallback/search")
	b.ReportMetric(float64(stats.SearchRouteHNSWSearchPack)/denom, "search_route_hnsw_search_pack/search")
	b.ReportMetric(float64(stats.SearchRouteQuantizedOnly)/denom, "search_route_quantized_only/search")
	b.ReportMetric(float64(stats.SearchRouteQuantizedRerank)/denom, "search_route_quantized_rerank/search")
	b.ReportMetric(float64(stats.HNSWSearchPackActive)/denom, "hnsw_search_pack_active/search")
	b.ReportMetric(float64(stats.HNSWSearchPackMissing)/denom, "hnsw_search_pack_missing/search")
	b.ReportMetric(float64(stats.HNSWSearchPackInvalid)/denom, "hnsw_search_pack_invalid/search")
	b.ReportMetric(float64(stats.HNSWSearchPackStale)/denom, "hnsw_search_pack_stale/search")
	b.ReportMetric(float64(stats.HNSWSearchPackClosed)/denom, "hnsw_search_pack_closed/search")
	b.ReportMetric(float64(stats.HNSWSearchPackFallbacks)/denom, "hnsw_search_pack_fallbacks/search")
	b.ReportMetric(float64(stats.HNSWSearchPackMmapDirect)/denom, "hnsw_search_pack_mmap_direct/search")
	b.ReportMetric(float64(stats.HNSWSearchPackHeapCopy)/denom, "hnsw_search_pack_heap_copy/search")
	b.ReportMetric(float64(stats.HNSWSearchPackOpenNanos)/denom, "hnsw_search_pack_open_ns")
	b.ReportMetric(float64(stats.HNSWSearchPackMappedBytes)/denom, "hnsw_search_pack_mapped_B")
	b.ReportMetric(float64(stats.HNSWSearchPackHeapCopyBytes)/denom, "hnsw_search_pack_heap_copy_B")
	b.ReportMetric(float64(stats.HNSWSearchPackActiveHandles)/denom, "hnsw_search_pack_active_handles")
	b.ReportMetric(float64(stats.TypedColumnFallbacks)/denom, "typed_column_vector_fallbacks/search")
	b.ReportMetric(float64(stats.VectorScratchDecodes)/denom, "vector_scratch_decodes/search")
	b.ReportMetric(float64(stats.AdjacencyLegacyFallbacks)/denom, "adjacency_legacy_fallbacks/search")
	b.ReportMetric(float64(stats.AdjacencySourceFallbacks)/denom, "adjacency_source_fallbacks/search")
	b.ReportMetric(float64(stats.ResultIDGraphFallbacks)/denom, "result_id_graph_fallbacks/search")
	b.ReportMetric(float64(stats.ResultIDTypedBytesState)/denom, "result_id_typed_bytes_state/search")
	b.ReportMetric(float64(stats.RowRefVectorSourceState)/denom, "row_ref_vector_source_state/search")
	b.ReportMetric(float64(stats.RowRefVectorSourceLegacyGraphIDs)/denom, "row_ref_vector_source_legacy_graph_ids/search")
	b.ReportMetric(float64(fixture.shape.dims), "quantized_code_B/vector")
	b.ReportMetric(float64(fixture.shape.dims*4), "exact_vector_B/vector")
	b.ReportMetric(4, "exact_norm_B/vector")
	b.ReportMetric(float64(fixture.shape.dims*4+4), "exact_vector_norm_B/vector")
	b.ReportMetric(float64(fixture.quantizedAssetBytes), "quantized_asset_B_total")
	if fixture.shape.rows > 0 {
		b.ReportMetric(float64(fixture.quantizedAssetBytes)/float64(fixture.shape.rows), "quantized_asset_B/vector")
	}
}

func reportColumnGraphScalarU8QuantizedTraversalCounterMetrics2271(b *testing.B, fixture columnGraphScalarU8QuantizedBenchFixture1926, stats VectorIndexSearchStats) {
	b.Helper()
	reportColumnGraphScalarU8QuantizedBenchShapeMetrics1926(b, fixture.shape)
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
	}
	denom := float64(b.N)
	if denom == 0 {
		denom = 1
	}
	b.ReportMetric(2271, "traversal_counter_issue")
	b.ReportMetric(float64(stats.BenchmarkDebugSearches)/denom, "benchmark_debug_searches/search")
	b.ReportMetric(float64(stats.Candidates)/denom, "candidates/search")
	b.ReportMetric(float64(stats.VisitedEdges)/denom, "visited_edges/search")
	b.ReportMetric(float64(stats.NeighborTiles)/denom, "adjacency_layer_loads/search")
	b.ReportMetric(float64(stats.NeighborTileNeighbors)/denom, "adjacency_loaded_neighbors/search")
	b.ReportMetric(float64(stats.NeighborTileMaxSize), "adjacency_layer_max_neighbors")
	b.ReportMetric(float64(stats.UpperLayerAdjacencyLoads)/denom, "upper_layer_adjacency_loads/search")
	b.ReportMetric(float64(stats.UpperLayerAdjacencyNeighbors)/denom, "upper_layer_adjacency_neighbors/search")
	b.ReportMetric(float64(stats.UpperLayerEdgeVisits)/denom, "upper_layer_neighbors_iterated/search")
	b.ReportMetric(float64(stats.Layer0AdjacencyLoads)/denom, "layer0_adjacency_loads/search")
	b.ReportMetric(float64(stats.Layer0AdjacencyNeighbors)/denom, "layer0_adjacency_neighbors/search")
	b.ReportMetric(float64(stats.Layer0EdgeVisits)/denom, "layer0_neighbors_iterated/search")
	b.ReportMetric(float64(stats.AlreadyVisitedSkips)/denom, "already_visited_skips/search")
	b.ReportMetric(float64(stats.Layer0AlreadyVisitedSkips)/denom, "layer0_already_visited_skips/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTiles)/denom, "upper_layer_score_tiles/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTileCandidates)/denom, "upper_layer_score_tile_candidates/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTileMaxSize), "upper_layer_score_tile_max_size")
	b.ReportMetric(float64(stats.Layer0ScoreTiles)/denom, "layer0_score_tiles/search")
	b.ReportMetric(float64(stats.Layer0ScoreTileCandidates)/denom, "layer0_score_tile_candidates/search")
	b.ReportMetric(float64(stats.Layer0ScoreTileMaxSize), "layer0_score_tile_max_size")
	b.ReportMetric(float64(stats.CandidateComparisons)/denom, "candidate_comparisons/search")
	b.ReportMetric(float64(stats.FrontierComparisons)/denom, "frontier_comparisons/search")
	b.ReportMetric(float64(stats.TopKComparisons)/denom, "top_k_comparisons/search")
	b.ReportMetric(float64(stats.FrontierPushes)/denom, "frontier_pushes/search")
	b.ReportMetric(float64(stats.FrontierPops)/denom, "frontier_pops/search")
	b.ReportMetric(float64(stats.FrontierPopMisses)/denom, "frontier_pop_misses/search")
	b.ReportMetric(float64(stats.FrontierSiftUpCalls)/denom, "frontier_sift_up_calls/search")
	b.ReportMetric(float64(stats.FrontierSiftDownCalls)/denom, "frontier_sift_down_calls/search")
	b.ReportMetric(float64(stats.FrontierSiftUpSteps)/denom, "frontier_sift_up_steps/search")
	b.ReportMetric(float64(stats.FrontierSiftDownSteps)/denom, "frontier_sift_down_steps/search")
	b.ReportMetric(float64(stats.TopKInsertAttempts)/denom, "top_k_insert_attempts/search")
	b.ReportMetric(float64(stats.TopKInsertSuccesses)/denom, "top_k_insert_successes/search")
	b.ReportMetric(float64(stats.TopKInsertRejections)/denom, "top_k_insert_rejections/search")
	b.ReportMetric(float64(stats.TopKShiftSteps)/denom, "top_k_shift_steps/search")
	b.ReportMetric(float64(stats.VisitedMarkChecks)/denom, "visited_mark_checks/search")
	b.ReportMetric(float64(stats.VisitedMarkHits)/denom, "visited_mark_hits/search")
	b.ReportMetric(float64(stats.VisitedMarkMisses)/denom, "visited_mark_misses/search")
	b.ReportMetric(float64(stats.VisitedMarkInserts)/denom, "visited_mark_inserts/search")
	b.ReportMetric(float64(stats.VisitedResetEpochAdvances)/denom, "visited_reset_epoch_advances/search")
	b.ReportMetric(float64(stats.VisitedResetClearedRows)/denom, "visited_reset_cleared_rows/search")
	b.ReportMetric(float64(stats.Layer0StopChecks)/denom, "layer0_stop_checks/search")
	b.ReportMetric(float64(stats.Layer0StopTrue)/denom, "layer0_stop_true/search")
	b.ReportMetric(float64(stats.Layer0StopFalse)/denom, "layer0_stop_false/search")
	if stats.VisitedNodes > 0 {
		b.ReportMetric(float64(stats.VisitedEdges)/float64(stats.VisitedNodes), "edges_per_visited_node")
	}
}

func reportColumnGraphScalarU8QuantizedStorageMetrics1926(b *testing.B, d *backenddb.DB, def VectorIndexDefinition, shape columnGraphScalarU8QuantizedBenchShape1926) {
	b.Helper()
	graph, _ := loadAndScanColumnGraphRebuildRowsV2A(b, d, "docs", def)
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(b, d, "docs")
	state := columnVectorIndexStateFromRecords1987(b, records, def)
	b.ReportMetric(float64(graph.AssetBytes), "graph_asset_B/op")
	b.ReportMetric(float64(columnVectorIndexStateAssetsStorageBytes(state)), "state_assets_B/op")
	b.ReportMetric(float64(columnVectorGraphStorageBytesWithState(graph, state)), "graph_total_storage_B/op")
	b.ReportMetric(float64(shape.dims), "quantized_code_B/vector")
	b.ReportMetric(float64(shape.dims*4), "exact_vector_B/vector")
	b.ReportMetric(4, "exact_norm_B/vector")
	b.ReportMetric(float64(shape.dims*4+4), "exact_vector_norm_B/vector")
	var quantizedAssets int
	var quantizedBytes int64
	for _, asset := range state.Assets {
		if asset.Role == columnVectorIndexStateAssetRoleQuantizedCodes {
			quantizedAssets++
			quantizedBytes += asset.AssetBytes
		}
	}
	b.ReportMetric(float64(quantizedAssets), "quantized_assets/op")
	b.ReportMetric(float64(quantizedBytes), "quantized_asset_B/op")
	if shape.rows > 0 {
		b.ReportMetric(float64(quantizedBytes)/float64(shape.rows), "quantized_asset_B/vector")
	}
}

func columnGraphScalarU8QuantizedAssetBytes1926(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition) int64 {
	tb.Helper()
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(tb, d, "docs")
	state := columnVectorIndexStateFromRecords1987(tb, records, def)
	assets := columnVectorGraphQuantizedAssetByName(state, def)
	asset, ok := assets[columnGraphScalarU8QuantizedBenchIndexName1926]
	if !ok {
		tb.Fatalf("quantized asset %q missing from state assets: %+v", columnGraphScalarU8QuantizedBenchIndexName1926, state.Assets)
	}
	if asset.AssetBytes <= 0 {
		tb.Fatalf("quantized asset bytes=%d want positive", asset.AssetBytes)
	}
	return asset.AssetBytes
}
