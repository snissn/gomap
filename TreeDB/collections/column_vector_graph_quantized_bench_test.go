package collections

import (
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
	dst.ScoreFloat64Fallbacks += src.ScoreFloat64Fallbacks
	dst.PreparedGraphSearchViews += src.PreparedGraphSearchViews
	dst.GraphRowFallbacks += src.GraphRowFallbacks
	dst.TypedColumnFallbacks += src.TypedColumnFallbacks
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
	b.ReportMetric(float64(stats.ScoreFloat64Fallbacks)/denom, "score_float64_fallbacks/search")
	b.ReportMetric(float64(stats.PreparedGraphSearchViews)/denom, "prepared_graph_search_views/search")
	b.ReportMetric(float64(stats.GraphRowFallbacks)/denom, "graph_row_fallbacks/search")
	b.ReportMetric(float64(stats.TypedColumnFallbacks)/denom, "typed_column_vector_fallbacks/search")
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
