//go:build usearch_bench && cgo

package collections

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	usearch "github.com/unum-cloud/usearch/golang"
)

func BenchmarkCollectionVectorUSearchBuild(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	vectors := make([][]float32, docs)
	for i := 0; i < docs; i++ {
		vectors[i] = vectorBenchmarkEmbedding(i, dims)
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := newVectorUSearchIndex(b, dims, docs, 16, 128, 128)
		for j, vector := range vectors {
			if err := index.Add(usearch.Key(j), vector); err != nil {
				_ = index.Destroy()
				b.Fatalf("add usearch vector %d: %v", j, err)
			}
		}
		memory, _ := index.MemoryUsage()
		b.ReportMetric(float64(memory), "index_bytes")
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch index: %v", err)
		}
	}
}

func BenchmarkCollectionVectorUSearchIncrementalWrite(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	vectors := make([][]float32, docs)
	for i := 0; i < docs; i++ {
		vectors[i] = vectorBenchmarkEmbedding(i, dims)
	}

	b.ReportMetric(float64(docs), "docs/write")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		index := newVectorUSearchIndex(b, dims, docs, 16, 128, 128)
		b.StartTimer()
		for j, vector := range vectors {
			if err := index.Add(usearch.Key(j), vector); err != nil {
				_ = index.Destroy()
				b.Fatalf("add usearch vector %d: %v", j, err)
			}
		}
		b.StopTimer()
		memory, _ := index.MemoryUsage()
		b.ReportMetric(float64(memory), "index_bytes")
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch index: %v", err)
		}
	}
}

func BenchmarkCollectionVectorUSearchProductionCompare(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	params := vectorUSearchProductionCompareParamsFromEnv(b, docs)
	queries, queryDocIndexes := vectorUSearchProductionQueries(docs, dims, params.queries)

	d, col, def, status := openVectorUSearchProductionTreeDBCollection(b, docs, dims, params)
	defer func() { _ = d.Close() }()
	index := openVectorUSearchBenchmarkIndex(b, docs, dims, params.m, params.efConstruction, params.efSearch)
	defer func() {
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch index: %v", err)
		}
	}()
	memory, _ := index.MemoryUsage()

	b.Run("TreeDB_SearchWithBuffer", func(b *testing.B) {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher: %v", err)
		}
		defer func() { _ = searcher.Close() }()
		timedOpts := VectorIndexSearcherSearchOptions{Query: queries[0], TopK: params.topK, EfSearch: params.efSearch, StatsMode: VectorIndexSearchStatsModeProduction}
		var buffer VectorIndexSearchBuffer
		warm, err := searcher.SearchWithBuffer(timedOpts, &buffer)
		if err != nil {
			b.Fatalf("warm SearchWithBuffer: %v", err)
		}
		if len(warm.Results) == 0 {
			b.Fatal("warm SearchWithBuffer returned no results")
		}
		top1Got, top1Want := string(warm.Results[0].ID), vectorUSearchProductionDocID(queryDocIndexes[0])
		statsOpts := timedOpts
		statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
		measured, err := searcher.SearchWithBuffer(statsOpts, &buffer)
		if err != nil {
			b.Fatalf("measure SearchWithBuffer stats: %v", err)
		}
		stats := measured.Stats
		if stats.SearchRouteHNSWSearchPack != 1 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 || stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 || stats.GraphRowFallbacks != 0 || stats.DocumentsFetched != 0 {
			b.Fatalf("TreeDB production comparison stats=%+v want active hnsw_search_pack_v1 route", stats)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			timedOpts.Query = queries[i%len(queries)]
			got, err := searcher.SearchWithBuffer(timedOpts, &buffer)
			if err != nil {
				b.Fatalf("SearchWithBuffer: %v", err)
			}
			if len(got.Results) == 0 {
				b.Fatal("SearchWithBuffer returned no results")
			}
			vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		}
		b.StopTimer()
		reportVectorUSearchProductionTop1Metric(b, top1Got, top1Want)
		reportVectorUSearchProductionCommonMetrics(b, docs, dims, params, len(queries))
		reportVectorUSearchProductionTreeDBFootprintMetrics(b, status)
		reportVectorIndexSearchStatsModeBenchMetric2126(b, params.statsMode())
		b.ReportMetric(1, "reported_stats_mode_full_diagnostics")
		reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
	})

	b.Run("TreeDB_SearchWithBufferParallel", func(b *testing.B) {
		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		type preparedWorker struct {
			searcher *VectorIndexSearcher
			buffer   VectorIndexSearchBuffer
		}
		benchWorkers := make([]preparedWorker, workers)
		opts := VectorIndexSearcherSearchOptions{TopK: params.topK, EfSearch: params.efSearch, StatsMode: VectorIndexSearchStatsModeProduction}
		for i := range benchWorkers {
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
			}
			defer func(searcher *VectorIndexSearcher) { _ = searcher.Close() }(searcher)
			benchWorkers[i].searcher = searcher
			opts.Query = queries[i%len(queries)]
			warm, err := searcher.SearchWithBuffer(opts, &benchWorkers[i].buffer)
			if err != nil {
				b.Fatalf("warm SearchWithBuffer worker %d: %v", i, err)
			}
			if len(warm.Results) == 0 {
				b.Fatalf("warm SearchWithBuffer worker %d returned no results", i)
			}
		}
		opts.Query = queries[0]
		statsOpts := opts
		statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
		measured, err := benchWorkers[0].searcher.SearchWithBuffer(statsOpts, &benchWorkers[0].buffer)
		if err != nil {
			b.Fatalf("measure SearchWithBuffer stats: %v", err)
		}
		stats := measured.Stats
		if stats.SearchRouteHNSWSearchPack != 1 || stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackFallbacks != 0 || stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 || stats.GraphRowFallbacks != 0 || stats.DocumentsFetched != 0 {
			b.Fatalf("TreeDB production parallel comparison stats=%+v want active hnsw_search_pack_v1 route", stats)
		}
		top1Got, top1Want := string(measured.Results[0].ID), vectorUSearchProductionDocID(queryDocIndexes[0])
		var nextWorker atomic.Uint64
		var sink atomic.Int64
		var firstErr atomic.Value
		var failed atomic.Bool
		recordParallelErr := func(format string, args ...any) {
			if failed.CompareAndSwap(false, true) {
				firstErr.Store(fmt.Sprintf(format, args...))
			}
		}
		b.SetParallelism(1)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			workerIndex := int(nextWorker.Add(1)) - 1
			if workerIndex < 0 || workerIndex >= len(benchWorkers) {
				recordParallelErr("parallel worker requested more than %d prepared searchers", workers)
				for pb.Next() {
				}
				return
			}
			worker := &benchWorkers[workerIndex]
			localOpts := opts
			localSink := int64(0)
			queryIndex := workerIndex
			for pb.Next() {
				if failed.Load() {
					continue
				}
				localOpts.Query = queries[queryIndex%len(queries)]
				queryIndex++
				got, err := worker.searcher.SearchWithBuffer(localOpts, &worker.buffer)
				if err != nil {
					recordParallelErr("SearchWithBuffer: %v", err)
					continue
				}
				if len(got.Results) == 0 {
					recordParallelErr("SearchWithBuffer returned no results")
					continue
				}
				localSink += int64(got.Results[0].Ordinal)
			}
			sink.Add(localSink)
		})
		b.StopTimer()
		reportVectorUSearchProductionTop1Metric(b, top1Got, top1Want)
		reportVectorUSearchProductionCommonMetrics(b, docs, dims, params, len(queries))
		reportVectorUSearchProductionTreeDBFootprintMetrics(b, status)
		reportVectorIndexSearchStatsModeBenchMetric2126(b, params.statsMode())
		b.ReportMetric(1, "reported_stats_mode_full_diagnostics")
		b.ReportMetric(float64(workers), "parallel_workers")
		reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b, col.columnVectorGraphSharedPreparedSearchCacheSnapshot(), workers)
		if errValue := firstErr.Load(); errValue != nil {
			b.Fatalf("%s", errValue.(string))
		}
		vectorSearchBenchSinkOrdinalV4 += int(sink.Load())
		reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
	})

	b.Run("TreeDB_CollectionSearchVectorIndexWithBuffer", func(b *testing.B) {
		timedOpts := VectorIndexSearchOptions{
			IndexName:        def.Name,
			Query:            queries[0],
			QueryMode:        VectorIndexQueryModeExact,
			TopK:             params.topK,
			EfSearch:         params.efSearch,
			MaxDecodedBlocks: 1,
			StatsMode:        VectorIndexSearchStatsModeProduction,
		}
		var buffer VectorIndexSearchBuffer
		warm, err := col.SearchVectorIndexWithBuffer(timedOpts, &buffer)
		if err != nil {
			b.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
		}
		if len(warm.Results) == 0 {
			b.Fatal("warm SearchVectorIndexWithBuffer returned no results")
		}
		top1Got, top1Want := string(warm.Results[0].ID), vectorUSearchProductionDocID(queryDocIndexes[0])
		statsOpts := timedOpts
		statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
		measured, err := col.SearchVectorIndexWithBuffer(statsOpts, &buffer)
		if err != nil {
			b.Fatalf("measure SearchVectorIndexWithBuffer stats: %v", err)
		}
		stats := measured.Stats
		if stats.SearchRouteHNSWSearchPack != 1 ||
			stats.HNSWSearchPackActive != 1 ||
			stats.HNSWSearchPackFallbacks != 0 ||
			stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 0 ||
			stats.TypedColumnFallbacks != 0 ||
			stats.VectorScratchDecodes != 0 ||
			stats.GraphRowFallbacks != 0 ||
			stats.DocumentsFetched != 0 {
			b.Fatalf("TreeDB collection buffered stats=%+v want exact no-document hnsw_search_pack_v1 route", stats)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			timedOpts.Query = queries[i%len(queries)]
			got, err := col.SearchVectorIndexWithBuffer(timedOpts, &buffer)
			if err != nil {
				b.Fatalf("SearchVectorIndexWithBuffer: %v", err)
			}
			if len(got.Results) == 0 {
				b.Fatal("SearchVectorIndexWithBuffer returned no results")
			}
			vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		}
		b.StopTimer()
		reportVectorUSearchProductionTop1Metric(b, top1Got, top1Want)
		reportVectorUSearchProductionCommonMetrics(b, docs, dims, params, len(queries))
		reportVectorUSearchProductionTreeDBFootprintMetrics(b, status)
		reportVectorIndexSearchStatsModeBenchMetric2126(b, params.statsMode())
		b.ReportMetric(1, "reported_stats_mode_full_diagnostics")
		b.ReportMetric(1, "collection_searchvectorindex_with_buffer_seam")
		b.ReportMetric(0, "open_searcher_calls/op")
		b.ReportMetric(0, "open_setup_in_timed_loop")
		b.ReportMetric(0, "response_owned_result_alloc/op")
		reportCollectionVectorIndexPreparedSearchBenchMetrics2363(b, col.collectionVectorIndexPreparedSearchCacheSnapshot())
		reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
	})

	b.Run("TreeDB_CollectionSearchVectorIndexNoDocsOneShot", func(b *testing.B) {
		timedOpts := VectorIndexSearchOptions{
			IndexName:        def.Name,
			Query:            queries[0],
			QueryMode:        VectorIndexQueryModeExact,
			TopK:             params.topK,
			EfSearch:         params.efSearch,
			MaxDecodedBlocks: 1,
			StatsMode:        VectorIndexSearchStatsModeProduction,
		}
		warm, err := col.SearchVectorIndex(timedOpts)
		if err != nil {
			b.Fatalf("warm SearchVectorIndex: %v", err)
		}
		if len(warm.Results) == 0 {
			b.Fatal("warm SearchVectorIndex returned no results")
		}
		top1Got, top1Want := string(warm.Results[0].ID), vectorUSearchProductionDocID(queryDocIndexes[0])
		statsOpts := timedOpts
		statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
		measured, err := col.SearchVectorIndex(statsOpts)
		if err != nil {
			b.Fatalf("measure SearchVectorIndex stats: %v", err)
		}
		stats := measured.Stats
		if !vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(stats) ||
			stats.TypedColumnFallbacks != 0 ||
			stats.VectorScratchDecodes != 0 ||
			stats.GraphRowFallbacks != 0 ||
			stats.DocumentsFetched != 0 {
			b.Fatalf("TreeDB collection no-doc stats=%+v want cached hnsw_search_pack_v1 SearchVectorIndex route with docs/fallback/decode counters clear", stats)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			timedOpts.Query = queries[i%len(queries)]
			got, err := col.SearchVectorIndex(timedOpts)
			if err != nil {
				b.Fatalf("SearchVectorIndex: %v", err)
			}
			if len(got.Results) == 0 {
				b.Fatal("SearchVectorIndex returned no results")
			}
			vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		}
		b.StopTimer()
		reportVectorUSearchProductionTop1Metric(b, top1Got, top1Want)
		reportVectorUSearchProductionCommonMetrics(b, docs, dims, params, len(queries))
		reportVectorUSearchProductionTreeDBFootprintMetrics(b, status)
		reportVectorIndexSearchStatsModeBenchMetric2126(b, params.statsMode())
		b.ReportMetric(1, "reported_stats_mode_full_diagnostics")
		b.ReportMetric(1, "collection_searchvectorindex_one_shot")
		b.ReportMetric(0, "open_searcher_calls/op")
		b.ReportMetric(0, "open_setup_in_timed_loop")
		b.ReportMetric(1, "response_owned_result_alloc/op")
		reportCollectionVectorIndexPreparedSearchBenchMetrics2363(b, col.collectionVectorIndexPreparedSearchCacheSnapshot())
		reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
	})

	b.Run("TreeDB_CollectionSearchVectorIndexWithDocumentsOneShot", func(b *testing.B) {
		timedOpts := VectorIndexSearchOptions{
			IndexName:        def.Name,
			Query:            queries[0],
			QueryMode:        VectorIndexQueryModeExact,
			TopK:             params.topK,
			EfSearch:         params.efSearch,
			IncludeDocuments: true,
			MaxDecodedBlocks: 1,
			StatsMode:        VectorIndexSearchStatsModeProduction,
		}
		warm, err := col.SearchVectorIndex(timedOpts)
		if err != nil {
			b.Fatalf("warm SearchVectorIndex IncludeDocuments: %v", err)
		}
		if len(warm.Results) == 0 || len(warm.Results[0].Document) == 0 {
			b.Fatal("warm SearchVectorIndex IncludeDocuments returned no materialized documents")
		}
		top1Got, top1Want := string(warm.Results[0].ID), vectorUSearchProductionDocID(queryDocIndexes[0])
		statsOpts := timedOpts
		statsOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
		measured, err := col.SearchVectorIndex(statsOpts)
		if err != nil {
			b.Fatalf("measure SearchVectorIndex IncludeDocuments stats: %v", err)
		}
		stats := measured.Stats
		if stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 1 ||
			stats.SearchRouteHNSWSearchPack != 0 ||
			stats.HNSWSearchPackActive != 1 ||
			stats.TypedColumnFallbacks != 0 ||
			stats.VectorScratchDecodes != 0 ||
			stats.GraphRowFallbacks != 0 ||
			stats.DocumentsFetched != uint64(len(measured.Results)) ||
			stats.DocumentsFetched == 0 ||
			stats.DocumentBytes == 0 ||
			stats.DocumentOutputBytes == 0 {
			b.Fatalf("TreeDB collection with-documents stats=%+v want explicit post-top-k document materialization row", stats)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			timedOpts.Query = queries[i%len(queries)]
			got, err := col.SearchVectorIndex(timedOpts)
			if err != nil {
				b.Fatalf("SearchVectorIndex IncludeDocuments: %v", err)
			}
			if len(got.Results) == 0 || len(got.Results[0].Document) == 0 {
				b.Fatal("SearchVectorIndex IncludeDocuments returned no materialized documents")
			}
			vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		}
		b.StopTimer()
		reportVectorUSearchProductionTop1Metric(b, top1Got, top1Want)
		reportVectorUSearchProductionCommonMetrics(b, docs, dims, params, len(queries))
		reportVectorUSearchProductionTreeDBFootprintMetrics(b, status)
		reportVectorIndexSearchStatsModeBenchMetric2126(b, params.statsMode())
		b.ReportMetric(1, "reported_stats_mode_full_diagnostics")
		b.ReportMetric(1, "collection_searchvectorindex_with_documents_one_shot")
		b.ReportMetric(1, "document_fetch_in_timed_loop")
		b.ReportMetric(1, "open_searcher_calls/op")
		b.ReportMetric(1, "open_setup_in_timed_loop")
		b.ReportMetric(1, "response_owned_result_alloc/op")
		reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
	})

	b.Run("USearch_Search", func(b *testing.B) {
		keys, _, err := index.Search(queries[0], uint(params.topK))
		if err != nil {
			b.Fatalf("warm usearch search: %v", err)
		}
		if len(keys) == 0 {
			b.Fatal("warm usearch search returned no results")
		}
		top1Got, top1Want := fmt.Sprintf("%d", keys[0]), fmt.Sprintf("%d", queryDocIndexes[0])
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			keys, _, err := index.Search(queries[i%len(queries)], uint(params.topK))
			if err != nil {
				b.Fatalf("usearch search: %v", err)
			}
			if len(keys) == 0 {
				b.Fatal("usearch search returned no results")
			}
			vectorSearchBenchSinkOrdinalV4 += int(keys[0])
		}
		b.StopTimer()
		reportVectorUSearchProductionTop1Metric(b, top1Got, top1Want)
		reportVectorUSearchProductionCommonMetrics(b, docs, dims, params, len(queries))
		b.ReportMetric(float64(memory), "index_bytes")
		reportVectorUSearchOpsMetric(b, b.N)
	})

	b.Run("USearch_SearchParallel", func(b *testing.B) {
		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		keys, _, err := index.Search(queries[0], uint(params.topK))
		if err != nil {
			b.Fatalf("warm parallel usearch search: %v", err)
		}
		if len(keys) == 0 {
			b.Fatal("warm parallel usearch search returned no results")
		}
		top1Got, top1Want := fmt.Sprintf("%d", keys[0]), fmt.Sprintf("%d", queryDocIndexes[0])
		var nextWorker atomic.Uint64
		var sink atomic.Int64
		var firstErr atomic.Value
		var failed atomic.Bool
		recordParallelErr := func(format string, args ...any) {
			if failed.CompareAndSwap(false, true) {
				firstErr.Store(fmt.Sprintf(format, args...))
			}
		}
		b.SetParallelism(1)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			workerIndex := int(nextWorker.Add(1)) - 1
			queryIndex := workerIndex
			localSink := int64(0)
			for pb.Next() {
				if failed.Load() {
					continue
				}
				keys, _, err := index.Search(queries[queryIndex%len(queries)], uint(params.topK))
				queryIndex++
				if err != nil {
					recordParallelErr("usearch search: %v", err)
					continue
				}
				if len(keys) == 0 {
					recordParallelErr("usearch search returned no results")
					continue
				}
				localSink += int64(keys[0])
			}
			sink.Add(localSink)
		})
		b.StopTimer()
		reportVectorUSearchProductionTop1Metric(b, top1Got, top1Want)
		reportVectorUSearchProductionCommonMetrics(b, docs, dims, params, len(queries))
		b.ReportMetric(float64(memory), "index_bytes")
		b.ReportMetric(float64(workers), "parallel_workers")
		if errValue := firstErr.Load(); errValue != nil {
			b.Fatalf("%s", errValue.(string))
		}
		vectorSearchBenchSinkOrdinalV4 += int(sink.Load())
		reportVectorUSearchOpsMetric(b, b.N)
	})
}

func BenchmarkCollectionVectorUSearchBaseline(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	index := openVectorUSearchBenchmarkIndex(b, docs, dims, 16, 128, 128)
	defer func() {
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch index: %v", err)
		}
	}()
	query := vectorBenchmarkEmbedding(docs/3, dims)

	keys, _, err := index.Search(query, uint(vectorBenchmarkTopK))
	if err != nil {
		b.Fatalf("warm usearch search: %v", err)
	}
	if len(keys) == 0 {
		b.Fatal("warm usearch search returned no results")
	}
	memory, _ := index.MemoryUsage()
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(memory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys, _, err := index.Search(query, uint(vectorBenchmarkTopK))
		if err != nil {
			b.Fatalf("usearch search: %v", err)
		}
		if len(keys) == 0 {
			b.Fatal("usearch search returned no results")
		}
	}
}

func BenchmarkCollectionVectorUSearchFilteredBaseline(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	index := openVectorUSearchBenchmarkIndex(b, docs, dims, 16, 128, 128)
	defer func() {
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch index: %v", err)
		}
	}()
	queryDoc := docs / 3
	query := vectorBenchmarkEmbedding(queryDoc, dims)
	group := queryDoc % 16
	handler := &usearch.FilteredSearchHandler{
		Callback: func(key usearch.Key, _ *usearch.FilteredSearchHandler) int {
			if int(key)%16 == group {
				return 1
			}
			return 0
		},
	}

	keys, _, err := index.FilteredSearch(query, uint(vectorBenchmarkTopK), handler)
	if err != nil {
		b.Fatalf("warm filtered usearch search: %v", err)
	}
	if len(keys) == 0 {
		b.Fatal("warm filtered usearch search returned no results")
	}
	memory, _ := index.MemoryUsage()
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(group), "filter_group")
	b.ReportMetric(float64(memory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys, _, err := index.FilteredSearch(query, uint(vectorBenchmarkTopK), handler)
		if err != nil {
			b.Fatalf("filtered usearch search: %v", err)
		}
		if len(keys) == 0 {
			b.Fatal("filtered usearch search returned no results")
		}
	}
}

func BenchmarkCollectionVectorTinyBERTUSearchBaseline(b *testing.B) {
	path := os.Getenv("TREEDB_VECTOR_BENCH_JSONL")
	if path == "" {
		b.Skip("set TREEDB_VECTOR_BENCH_JSONL to a tiny_bert demo JSONL export")
	}
	fixture := loadVectorBenchmarkFixture(b, path)
	if len(fixture) == 0 {
		b.Fatal("tiny BERT fixture has no records")
	}
	index := openVectorUSearchFixtureIndex(b, fixture, 8, 128, 64)
	defer func() {
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch fixture index: %v", err)
		}
	}()
	query := fixture[0].Embedding
	topK := minInt(vectorBenchmarkTopK, len(fixture))

	keys, _, err := index.Search(query, uint(topK))
	if err != nil {
		b.Fatalf("warm tiny BERT usearch search: %v", err)
	}
	if len(keys) == 0 {
		b.Fatal("warm tiny BERT usearch search returned no results")
	}
	memory, _ := index.MemoryUsage()
	b.ReportMetric(float64(len(fixture)), "docs/index")
	b.ReportMetric(float64(len(query)), "dims")
	b.ReportMetric(float64(memory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys, _, err := index.Search(query, uint(topK))
		if err != nil {
			b.Fatalf("tiny BERT usearch search: %v", err)
		}
		if len(keys) == 0 {
			b.Fatal("tiny BERT usearch search returned no results")
		}
	}
}

type vectorUSearchProductionCompareParams struct {
	m              int
	efConstruction int
	efSearch       int
	topK           int
	queries        int
}

func (p vectorUSearchProductionCompareParams) statsMode() VectorIndexSearchStatsMode {
	return VectorIndexSearchStatsModeProduction
}

func vectorUSearchProductionCompareParamsFromEnv(tb testing.TB, docs int) vectorUSearchProductionCompareParams {
	tb.Helper()
	params := vectorUSearchProductionCompareParams{
		m:              vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_M", 16),
		efConstruction: vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_EF_CONSTRUCTION", 128),
		efSearch:       vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_EF_SEARCH", 128),
		topK:           vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_TOPK", vectorBenchmarkTopK),
		queries:        vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_QUERIES", minInt(16, docs)),
	}
	if params.queries > docs {
		params.queries = docs
	}
	if params.topK > docs {
		params.topK = docs
	}
	if params.efConstruction < params.m {
		tb.Fatalf("TREEDB_VECTOR_BENCH_EF_CONSTRUCTION=%d must be >= TREEDB_VECTOR_BENCH_M=%d", params.efConstruction, params.m)
	}
	return params
}

func vectorUSearchProductionQueries(docs, dims, queryCount int) ([][]float32, []int) {
	queries := make([][]float32, queryCount)
	docIndexes := make([]int, queryCount)
	for i := 0; i < queryCount; i++ {
		docIndex := 0
		if docs > 1 {
			docIndex = (docs/3 + i*7919) % docs
		}
		docIndexes[i] = docIndex
		queries[i] = vectorBenchmarkEmbedding(docIndex, dims)
	}
	return queries, docIndexes
}

func vectorUSearchProductionDocID(doc int) string {
	return fmt.Sprintf("doc-%06d", doc)
}

func openVectorUSearchProductionTreeDBCollection(tb testing.TB, docs, dims int, params vectorUSearchProductionCompareParams) (*backenddb.DB, *Collection, VectorIndexDefinition, VectorIndexStatus) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:           "embedding_graph_production",
		Field:          "embedding",
		Metric:         VectorMetricCosine,
		Dimensions:     dims,
		M:              params.m,
		EfConstruction: params.efConstruction,
		EfSearch:       params.efSearch,
		Strategy:       VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalize vector index definition: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	insertVectorUSearchProductionTreeDBRows(tb, col, docs, dims, 512)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	if status.Name != def.Name || status.Strategy != VectorIndexStrategyColumnGraph || status.State != VectorIndexStateColumnGraphLoaded || !status.Loaded || status.RebuildNeeded {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex status=%+v want loaded column_graph index", status)
	}
	return d, col, def, status
}

func insertVectorUSearchProductionTreeDBRows(tb testing.TB, col *Collection, docs, dims, batchSize int) {
	tb.Helper()
	ids := make([][]byte, 0, batchSize)
	documents := make([][]byte, 0, batchSize)
	flush := func() {
		if len(ids) == 0 {
			return
		}
		if _, err := col.InsertBatch(ids, documents); err != nil {
			tb.Fatalf("InsertBatch: %v", err)
		}
		ids = ids[:0]
		documents = documents[:0]
	}
	for i := 0; i < docs; i++ {
		docID := vectorUSearchProductionDocID(i)
		raw, err := json.Marshal(map[string]any{
			"time_us":   int64(i + 1),
			"kind":      "vector",
			"did":       docID,
			"embedding": vectorBenchmarkEmbedding(i, dims),
		})
		if err != nil {
			tb.Fatalf("json.Marshal row %q: %v", docID, err)
		}
		ids = append(ids, []byte(docID))
		documents = append(documents, raw)
		if len(ids) == batchSize {
			flush()
		}
	}
	flush()
	if err := col.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
}

func reportVectorUSearchProductionCommonMetrics(b *testing.B, docs, dims int, params vectorUSearchProductionCompareParams, queries int) {
	b.Helper()
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(params.m), "hnsw_m")
	b.ReportMetric(float64(params.efConstruction), "ef_construction")
	b.ReportMetric(float64(params.efSearch), "ef_search")
	b.ReportMetric(float64(params.topK), "top_k")
	b.ReportMetric(float64(queries), "queries")
}

func reportVectorUSearchProductionTreeDBFootprintMetrics(b *testing.B, status VectorIndexStatus) {
	b.Helper()
	b.ReportMetric(float64(status.Stats.BytesDisk), "index_bytes_disk")
	if status.NativeRootBytes > 0 {
		b.ReportMetric(float64(status.NativeRootBytes), "native_root_bytes")
	}
	if status.Stats.BytesMemory > 0 {
		b.ReportMetric(float64(status.Stats.BytesMemory), "index_bytes_memory")
	}
	if status.Duration > 0 {
		b.ReportMetric(float64(status.Duration.Nanoseconds()), "rebuild_ns")
	}
}

func reportVectorUSearchProductionTop1Metric(b *testing.B, got, want string) {
	b.Helper()
	if got == want {
		b.ReportMetric(1, "top1_hit")
		return
	}
	b.ReportMetric(0, "top1_hit")
}

func reportVectorUSearchOpsMetric(b *testing.B, n int) {
	b.Helper()
	if n <= 0 {
		return
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(n)/elapsed.Seconds(), "ops/sec")
	}
}

func openVectorUSearchBenchmarkIndex(tb testing.TB, docs, dims, m, efConstruction, efSearch int) *usearch.Index {
	tb.Helper()
	index := newVectorUSearchIndex(tb, dims, docs, m, efConstruction, efSearch)
	for i := 0; i < docs; i++ {
		if err := index.Add(usearch.Key(i), vectorBenchmarkEmbedding(i, dims)); err != nil {
			tb.Fatalf("add usearch vector %d: %v", i, err)
		}
	}
	return index
}

func reportCollectionVectorIndexPreparedSearchBenchMetrics2363(b *testing.B, snap collectionVectorIndexPreparedSearchCacheSnapshot) {
	b.Helper()
	iterations := float64(b.N)
	if iterations <= 0 {
		iterations = 1
	}
	b.ReportMetric(float64(snap.Entries), "collection_prepared_cache_entries")
	b.ReportMetric(float64(snap.BuildingEntries), "collection_prepared_cache_building_entries")
	b.ReportMetric(float64(snap.CacheBuilds)/iterations, "collection_prepared_cache_builds/op")
	b.ReportMetric(float64(snap.CacheMisses)/iterations, "collection_prepared_cache_misses/op")
	b.ReportMetric(float64(snap.CacheHits)/iterations, "collection_prepared_cache_hits/op")
	b.ReportMetric(float64(snap.CacheWaits)/iterations, "collection_prepared_cache_waits/op")
	b.ReportMetric(float64(snap.Invalidations)/iterations, "collection_prepared_cache_invalidations/op")
	b.ReportMetric(float64(snap.Closes)/iterations, "collection_prepared_cache_closes/op")
	b.ReportMetric(float64(snap.Errors)/iterations, "collection_prepared_cache_errors/op")
	if lookups := snap.CacheHits + snap.CacheMisses; lookups > 0 {
		b.ReportMetric(float64(snap.CacheHits)/float64(lookups), "collection_prepared_cache_hit_ratio")
	}
	b.ReportMetric(float64(snap.ActiveHandles), "collection_prepared_active_handles")
	b.ReportMetric(float64(snap.ActiveMappedBytes), "collection_prepared_mapped_B")
	b.ReportMetric(float64(snap.ActiveHeapCopyBytes), "collection_prepared_heap_copy_B")
	b.ReportMetric(float64(snap.ActiveDerivedMetadataBytes), "collection_prepared_derived_metadata_B")
}

func openVectorUSearchFixtureIndex(tb testing.TB, fixture []vectorBenchmarkFixtureRecord, m, efConstruction, efSearch int) *usearch.Index {
	tb.Helper()
	dims := len(fixture[0].Embedding)
	index := newVectorUSearchIndex(tb, dims, len(fixture), m, efConstruction, efSearch)
	for i, record := range fixture {
		if len(record.Embedding) != dims {
			tb.Fatalf("fixture record %q dimension=%d want %d", record.ID, len(record.Embedding), dims)
		}
		if err := index.Add(usearch.Key(i), record.Embedding); err != nil {
			tb.Fatalf("add usearch fixture vector %q: %v", record.ID, err)
		}
	}
	return index
}

func newVectorUSearchIndex(tb testing.TB, dims, docs, m, efConstruction, efSearch int) *usearch.Index {
	tb.Helper()
	conf := usearch.DefaultConfig(uint(dims))
	conf.Quantization = usearch.F32
	conf.Metric = usearch.Cosine
	conf.Connectivity = uint(m)
	conf.ExpansionAdd = uint(efConstruction)
	conf.ExpansionSearch = uint(efSearch)
	index, err := usearch.NewIndex(conf)
	if err != nil {
		tb.Fatalf("new usearch index: %v", err)
	}
	if err := index.Reserve(uint(docs)); err != nil {
		_ = index.Destroy()
		tb.Fatalf("reserve usearch index: %v", err)
	}
	_ = index.ChangeThreadsAdd(uint(runtime.NumCPU()))
	_ = index.ChangeThreadsSearch(1)
	return index
}
