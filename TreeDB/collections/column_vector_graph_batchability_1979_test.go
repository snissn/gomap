package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestColumnVectorGraphNativeSearchBenchmarkDebugCounters1979(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{1, 0}, InvNorm: 1, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 3, 1, 2, 3, 1, 1}},
		{ID: []byte("doc-cycle"), Vector: []float32{0.9, 0.1}, InvNorm: 1, Adjacency: []uint32{0, 2, 4}},
		{ID: []byte("doc-filtered-a"), Vector: []float32{0.1, 0.9}, InvNorm: 1},
		{ID: []byte("doc-neighbor"), Vector: []float32{0.8, 0.2}, InvNorm: 1, Adjacency: []uint32{0, 1, 4}},
		{ID: []byte("doc-tail"), Vector: []float32{0.7, 0.3}, InvNorm: 1, Adjacency: []uint32{1, 5}},
		{ID: []byte("doc-filtered-b"), Vector: []float32{0.2, 0.8}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(t, 2, 3, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	candidateRows, err := typedcolumn.NewSparseRowSelection(len(rows), []int{0, 1, 3, 4})
	if err != nil {
		t.Fatalf("NewSparseRowSelection: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine([]float32{1, 0}, columnVectorGraphNativeSearchOptions{
		TopK:             2,
		EfSearch:         len(rows),
		StatsMode:        columnVectorGraphNativeSearchStatsModeBenchmarkDebug,
		CandidateRows:    candidateRows,
		HasCandidateRows: true,
	}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine benchmark_debug: %v", err)
	}
	if len(got) != 2 || got[0].Ordinal != 0 {
		t.Fatalf("results=%+v want doc-entry first", got)
	}
	assertColumnVectorGraphBenchmarkDebugStats1979(t, stats, true)
	if stats.Layer0AlreadyVisitedSkips == 0 || stats.Layer0FilterSkips == 0 {
		t.Fatalf("stats=%+v want benchmark_debug already-visited and filter skip buckets", stats)
	}
	if stats.UpperLayerScores == 0 || stats.Layer0Scores == 0 {
		t.Fatalf("stats=%+v want upper-layer and layer-0 score buckets", stats)
	}
	if stats.ScoreBatchSingletons != stats.ScoreBatchCalls {
		t.Fatalf("stats=%+v want scalar debug search to report singleton score batches", stats)
	}
}

func BenchmarkColumnVectorGraphSearchBatchability1979(b *testing.B) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		b.Skip("batchability benchmark requires mmap_direct prepared typed-column views")
	}
	baseShape := columnVectorGraphSearchTopologyParityProductionShape2091()
	exactShape := baseShape
	exactShape.efSearch = exactShape.rows
	cases := []struct {
		name           string
		shape          columnVectorGraphSearchTopologyParityShape2091
		scoreBatchMode columnVectorGraphScoreBatchMode
		exactMode      bool
	}{
		{name: "ef_search_128/score=scalar", shape: baseShape, scoreBatchMode: columnVectorGraphScoreBatchModeScalar},
		{name: "ef_search_128/score=indexed", shape: baseShape, scoreBatchMode: columnVectorGraphScoreBatchModeIndexed},
		{name: "exact/score=scalar", shape: exactShape, scoreBatchMode: columnVectorGraphScoreBatchModeScalar, exactMode: true},
		{name: "exact/score=indexed", shape: exactShape, scoreBatchMode: columnVectorGraphScoreBatchModeIndexed, exactMode: true},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkColumnVectorGraphSearchBatchability1979(b, tc.shape, tc.scoreBatchMode, tc.exactMode)
		})
	}
}

func benchmarkColumnVectorGraphSearchBatchability1979(b *testing.B, shape columnVectorGraphSearchTopologyParityShape2091, scoreBatchMode columnVectorGraphScoreBatchMode, exactMode bool) {
	b.Helper()
	rows := columnVectorGraphSearchTopologyParityRows2091(b, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(b, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	var scratch columnVectorGraphNativeSearchScratch
	opts := columnVectorGraphSearchTopologyParityOptions2091(shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091)
	opts.StatsMode = columnVectorGraphNativeSearchStatsModeBenchmarkDebug
	opts.ScoreBatchMode = scoreBatchMode
	warm, warmStats, err := reader.SearchCosine(query, opts, &scratch)
	if err != nil {
		b.Fatalf("warm SearchCosine: %v", err)
	}
	if len(warm) == 0 {
		b.Fatalf("warm SearchCosine returned no results")
	}
	assertColumnVectorGraphSearchTopologyParityCurrentStats2091(b, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, warmStats)
	assertColumnVectorGraphBenchmarkDebugStats1979(b, warmStats, exactMode)
	baseReaderStats := reader.Stats()
	var totals columnVectorGraphNativeSearchStats
	var checksum int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, stats, err := reader.SearchCosine(query, opts, &scratch)
		if err != nil {
			b.Fatalf("SearchCosine: %v", err)
		}
		if len(got) == 0 {
			b.Fatalf("SearchCosine returned no results")
		}
		checksum += int64(got[0].Ordinal)
		addColumnVectorGraphSearchTopologyParityStats2091(&totals, stats)
	}
	b.StopTimer()
	columnPhysicalScanBenchSum += checksum
	b.ReportMetric(1979, "batchability_issue")
	b.ReportMetric(1, "batchability_score_mode_"+scoreBatchMode.String())
	if exactMode {
		b.ReportMetric(1, "batchability_exact_mode")
	}
	reportColumnVectorGraphSearchTopologyParityMetrics2091(b, shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, b.N, baseReaderStats, reader.Stats(), totals)
}

func assertColumnVectorGraphBenchmarkDebugStats1979(tb testing.TB, stats columnVectorGraphNativeSearchStats, exactMode bool) {
	tb.Helper()
	if stats.BenchmarkDebugSearches != 1 {
		tb.Fatalf("stats=%+v want one benchmark_debug search", stats)
	}
	if stats.VisitedEdges != stats.Edges || stats.UpperLayerEdgeVisits+stats.Layer0EdgeVisits != stats.VisitedEdges {
		tb.Fatalf("stats=%+v want upper/layer-0 edge buckets to reconcile with visited_edges", stats)
	}
	if stats.UpperLayerScores+stats.Layer0Scores != stats.CandidateFetches || stats.ScoreBatchCandidates != stats.CandidateFetches {
		tb.Fatalf("stats=%+v want score buckets to reconcile with candidate fetches", stats)
	}
	if stats.Layer0Scores != stats.Candidates {
		tb.Fatalf("stats=%+v want layer-0 scores to equal candidate applications", stats)
	}
	if stats.Layer0ScoredNeighbors+stats.Layer0AlreadyVisitedSkips+stats.Layer0FilterSkips != stats.Layer0EdgeVisits {
		tb.Fatalf("stats=%+v want layer-0 scored/skip buckets to reconcile with layer-0 edges", stats)
	}
	if stats.UpperLayerScoredNeighbors+stats.UpperLayerFilterSkips != stats.UpperLayerEdgeVisits {
		tb.Fatalf("stats=%+v want upper-layer scored/filter buckets to reconcile with upper-layer edges", stats)
	}
	if stats.ScoredNeighbors+stats.SkippedNeighbors != stats.VisitedEdges {
		tb.Fatalf("stats=%+v want scored+skipped neighbor buckets to reconcile with visited_edges", stats)
	}
	if stats.VisitedMarkHits+stats.VisitedMarkMisses != stats.VisitedMarkChecks || stats.VisitedMarkHits != stats.Layer0AlreadyVisitedSkips || stats.VisitedMarkInserts < stats.VisitedMarkMisses || stats.VisitedResetEpochAdvances != 1 {
		tb.Fatalf("stats=%+v want visited-mark hits/misses/inserts/reset work to reconcile", stats)
	}
	if stats.FrontierPushes != stats.TopKInsertSuccesses || stats.TopKInsertAttempts != stats.Candidates || stats.TopKInsertSuccesses+stats.TopKInsertRejections != stats.TopKInsertAttempts {
		tb.Fatalf("stats=%+v want frontier/top-k operation counts to reconcile with candidates", stats)
	}
	if stats.FrontierSiftUpCalls != stats.FrontierPushes || stats.FrontierSiftDownCalls > stats.FrontierPops || stats.CandidateComparisons != stats.FrontierComparisons+stats.TopKComparisons {
		tb.Fatalf("stats=%+v want frontier/top-k comparison and sift counters to reconcile", stats)
	}
	if stats.Layer0StopTrue+stats.Layer0StopFalse != stats.Layer0StopChecks || (stats.Layer0StopChecks == 0 && stats.WavefrontSearches == 0) {
		tb.Fatalf("stats=%+v want layer0 stop checks to reconcile", stats)
	}
	if stats.NeighborTileSize0+stats.NeighborTileSize1+stats.NeighborTileSize2To4+stats.NeighborTileSize5To8+stats.NeighborTileSize9To16+stats.NeighborTileSize17Plus != stats.NeighborTiles {
		tb.Fatalf("stats=%+v want neighbor tile histogram to sum to neighbor_tiles", stats)
	}
	if stats.UpperLayerAdjacencyLoads+stats.Layer0AdjacencyLoads != stats.NeighborTiles || stats.UpperLayerAdjacencyNeighbors+stats.Layer0AdjacencyNeighbors != stats.NeighborTileNeighbors {
		tb.Fatalf("stats=%+v want phase adjacency load counters to reconcile", stats)
	}
	if stats.ScoreBatchSingletons+stats.ScoreBatchSize2To4+stats.ScoreBatchSize5To8+stats.ScoreBatchSize9To16+stats.ScoreBatchSize17Plus != stats.ScoreBatchCalls {
		tb.Fatalf("stats=%+v want score batch histogram to sum to score_batch_calls", stats)
	}
	if stats.UpperLayerScoreTileCandidates != stats.UpperLayerScores || stats.Layer0ScoreTileCandidates != stats.Layer0Scores || stats.UpperLayerScoreTiles+stats.Layer0ScoreTiles == 0 {
		tb.Fatalf("stats=%+v want phase score tile counters to reconcile", stats)
	}
	if exactMode {
		if stats.ExactModeSearches != 1 || stats.ExactCandidateOrderObservations != stats.Candidates {
			tb.Fatalf("stats=%+v want exact-mode candidate order observations for each candidate", stats)
		}
		if stats.ExactCandidateOrderObservations > 0 && stats.ExactCandidateOrderTransitions+1 != stats.ExactCandidateOrderObservations {
			tb.Fatalf("stats=%+v want exact candidate order transitions to reconcile", stats)
		}
		return
	}
	if stats.ExactModeSearches != 0 || stats.ExactCandidateOrderObservations != 0 {
		tb.Fatalf("stats=%+v want exact-mode counters disabled for bounded ef_search", stats)
	}
}

func addColumnVectorGraphNativeSearchDebugStats1979(dst *columnVectorGraphNativeSearchStats, src columnVectorGraphNativeSearchStats) {
	if dst == nil {
		return
	}
	dst.BenchmarkDebugSearches += src.BenchmarkDebugSearches
	dst.NeighborTiles += src.NeighborTiles
	dst.NeighborTileNeighbors += src.NeighborTileNeighbors
	if src.NeighborTileMaxSize > dst.NeighborTileMaxSize {
		dst.NeighborTileMaxSize = src.NeighborTileMaxSize
	}
	dst.NeighborTileSize0 += src.NeighborTileSize0
	dst.NeighborTileSize1 += src.NeighborTileSize1
	dst.NeighborTileSize2To4 += src.NeighborTileSize2To4
	dst.NeighborTileSize5To8 += src.NeighborTileSize5To8
	dst.NeighborTileSize9To16 += src.NeighborTileSize9To16
	dst.NeighborTileSize17Plus += src.NeighborTileSize17Plus
	dst.ScoreBatchSingletons += src.ScoreBatchSingletons
	dst.ScoreBatchSize2To4 += src.ScoreBatchSize2To4
	dst.ScoreBatchSize5To8 += src.ScoreBatchSize5To8
	dst.ScoreBatchSize9To16 += src.ScoreBatchSize9To16
	dst.ScoreBatchSize17Plus += src.ScoreBatchSize17Plus
	dst.ScoredNeighbors += src.ScoredNeighbors
	dst.SkippedNeighbors += src.SkippedNeighbors
	dst.AlreadyVisitedSkips += src.AlreadyVisitedSkips
	dst.FilterSkips += src.FilterSkips
	dst.UpperLayerScores += src.UpperLayerScores
	dst.UpperLayerEntryScores += src.UpperLayerEntryScores
	dst.UpperLayerNeighborScores += src.UpperLayerNeighborScores
	dst.UpperLayerScoreTiles += src.UpperLayerScoreTiles
	dst.UpperLayerScoreTileCandidates += src.UpperLayerScoreTileCandidates
	if src.UpperLayerScoreTileMaxSize > dst.UpperLayerScoreTileMaxSize {
		dst.UpperLayerScoreTileMaxSize = src.UpperLayerScoreTileMaxSize
	}
	dst.UpperLayerAdjacencyLoads += src.UpperLayerAdjacencyLoads
	dst.UpperLayerAdjacencyNeighbors += src.UpperLayerAdjacencyNeighbors
	dst.UpperLayerEdgeVisits += src.UpperLayerEdgeVisits
	dst.UpperLayerScoredNeighbors += src.UpperLayerScoredNeighbors
	dst.UpperLayerFilterSkips += src.UpperLayerFilterSkips
	dst.Layer0Scores += src.Layer0Scores
	dst.Layer0SeedScores += src.Layer0SeedScores
	dst.Layer0NeighborScores += src.Layer0NeighborScores
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
	dst.TopKHeapSiftSteps += src.TopKHeapSiftSteps
	dst.VisitedMarkChecks += src.VisitedMarkChecks
	dst.VisitedMarkHits += src.VisitedMarkHits
	dst.VisitedMarkMisses += src.VisitedMarkMisses
	dst.VisitedMarkInserts += src.VisitedMarkInserts
	dst.VisitedResetEpochAdvances += src.VisitedResetEpochAdvances
	dst.VisitedResetClearedRows += src.VisitedResetClearedRows
	dst.ExactModeSearches += src.ExactModeSearches
	dst.ExactCandidateOrderObservations += src.ExactCandidateOrderObservations
	dst.ExactCandidateOrderTransitions += src.ExactCandidateOrderTransitions
	dst.ExactCandidateOrderAdjacentForward += src.ExactCandidateOrderAdjacentForward
	dst.ExactCandidateOrderNonAdjacentForward += src.ExactCandidateOrderNonAdjacentForward
	dst.ExactCandidateOrderBackwardJumps += src.ExactCandidateOrderBackwardJumps
	if src.ExactCandidateOrderMaxForwardRun > dst.ExactCandidateOrderMaxForwardRun {
		dst.ExactCandidateOrderMaxForwardRun = src.ExactCandidateOrderMaxForwardRun
	}
}

func reportColumnVectorGraphNativeSearchDebugMetrics1979(b *testing.B, n int, stats columnVectorGraphNativeSearchStats) {
	b.Helper()
	if n <= 0 {
		return
	}
	denom := float64(n)
	b.ReportMetric(float64(stats.BenchmarkDebugSearches)/denom, "benchmark_debug_searches/search")
	b.ReportMetric(float64(stats.NeighborTiles)/denom, "neighbor_tiles/search")
	b.ReportMetric(float64(stats.NeighborTileNeighbors)/denom, "neighbor_tile_neighbors/search")
	b.ReportMetric(float64(stats.NeighborTileMaxSize), "neighbor_tile_max_size")
	if stats.NeighborTiles > 0 {
		b.ReportMetric(float64(stats.NeighborTileNeighbors)/float64(stats.NeighborTiles), "neighbor_tile_avg_size")
	}
	b.ReportMetric(float64(stats.NeighborTileSize0)/denom, "neighbor_tile_size_0/search")
	b.ReportMetric(float64(stats.NeighborTileSize1)/denom, "neighbor_tile_size_1/search")
	b.ReportMetric(float64(stats.NeighborTileSize2To4)/denom, "neighbor_tile_size_2_4/search")
	b.ReportMetric(float64(stats.NeighborTileSize5To8)/denom, "neighbor_tile_size_5_8/search")
	b.ReportMetric(float64(stats.NeighborTileSize9To16)/denom, "neighbor_tile_size_9_16/search")
	b.ReportMetric(float64(stats.NeighborTileSize17Plus)/denom, "neighbor_tile_size_17_plus/search")
	b.ReportMetric(float64(stats.ScoreBatchSingletons)/denom, "score_batch_singletons/search")
	b.ReportMetric(float64(stats.ScoreBatchSize2To4)/denom, "score_batch_size_2_4/search")
	b.ReportMetric(float64(stats.ScoreBatchSize5To8)/denom, "score_batch_size_5_8/search")
	b.ReportMetric(float64(stats.ScoreBatchSize9To16)/denom, "score_batch_size_9_16/search")
	b.ReportMetric(float64(stats.ScoreBatchSize17Plus)/denom, "score_batch_size_17_plus/search")
	b.ReportMetric(float64(stats.ScoredNeighbors)/denom, "scored_neighbors/search")
	b.ReportMetric(float64(stats.SkippedNeighbors)/denom, "skipped_neighbors/search")
	b.ReportMetric(float64(stats.AlreadyVisitedSkips)/denom, "already_visited_skips/search")
	b.ReportMetric(float64(stats.FilterSkips)/denom, "filter_skips/search")
	b.ReportMetric(float64(stats.UpperLayerScores)/denom, "upper_layer_scores/search")
	b.ReportMetric(float64(stats.UpperLayerEntryScores)/denom, "upper_layer_entry_scores/search")
	b.ReportMetric(float64(stats.UpperLayerNeighborScores)/denom, "upper_layer_neighbor_scores/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTiles)/denom, "upper_layer_score_tiles/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTileCandidates)/denom, "upper_layer_score_tile_candidates/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTileMaxSize), "upper_layer_score_tile_max_size")
	b.ReportMetric(float64(stats.UpperLayerAdjacencyLoads)/denom, "upper_layer_adjacency_loads/search")
	b.ReportMetric(float64(stats.UpperLayerAdjacencyNeighbors)/denom, "upper_layer_adjacency_neighbors/search")
	b.ReportMetric(float64(stats.UpperLayerEdgeVisits)/denom, "upper_layer_edge_visits/search")
	b.ReportMetric(float64(stats.UpperLayerScoredNeighbors)/denom, "upper_layer_scored_neighbors/search")
	b.ReportMetric(float64(stats.UpperLayerFilterSkips)/denom, "upper_layer_filter_skips/search")
	b.ReportMetric(float64(stats.Layer0Scores)/denom, "layer0_scores/search")
	b.ReportMetric(float64(stats.Layer0SeedScores)/denom, "layer0_seed_scores/search")
	b.ReportMetric(float64(stats.Layer0NeighborScores)/denom, "layer0_neighbor_scores/search")
	b.ReportMetric(float64(stats.Layer0ScoreTiles)/denom, "layer0_score_tiles/search")
	b.ReportMetric(float64(stats.Layer0ScoreTileCandidates)/denom, "layer0_score_tile_candidates/search")
	b.ReportMetric(float64(stats.Layer0ScoreTileMaxSize), "layer0_score_tile_max_size")
	b.ReportMetric(float64(stats.Layer0AdjacencyLoads)/denom, "layer0_adjacency_loads/search")
	b.ReportMetric(float64(stats.Layer0AdjacencyNeighbors)/denom, "layer0_adjacency_neighbors/search")
	b.ReportMetric(float64(stats.Layer0EdgeVisits)/denom, "layer0_edge_visits/search")
	b.ReportMetric(float64(stats.Layer0ScoredNeighbors)/denom, "layer0_scored_neighbors/search")
	b.ReportMetric(float64(stats.Layer0AlreadyVisitedSkips)/denom, "layer0_already_visited_skips/search")
	b.ReportMetric(float64(stats.Layer0FilterSkips)/denom, "layer0_filter_skips/search")
	b.ReportMetric(float64(stats.Layer0StopChecks)/denom, "layer0_stop_checks/search")
	b.ReportMetric(float64(stats.Layer0StopTrue)/denom, "layer0_stop_true/search")
	b.ReportMetric(float64(stats.Layer0StopFalse)/denom, "layer0_stop_false/search")
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
	b.ReportMetric(float64(stats.TopKHeapSiftSteps)/denom, "top_k_heap_sift_steps/search")
	b.ReportMetric(float64(stats.VisitedMarkChecks)/denom, "visited_mark_checks/search")
	b.ReportMetric(float64(stats.VisitedMarkHits)/denom, "visited_mark_hits/search")
	b.ReportMetric(float64(stats.VisitedMarkMisses)/denom, "visited_mark_misses/search")
	b.ReportMetric(float64(stats.VisitedMarkInserts)/denom, "visited_mark_inserts/search")
	b.ReportMetric(float64(stats.VisitedResetEpochAdvances)/denom, "visited_reset_epoch_advances/search")
	b.ReportMetric(float64(stats.VisitedResetClearedRows)/denom, "visited_reset_cleared_rows/search")
	b.ReportMetric(float64(stats.ExactModeSearches)/denom, "exact_mode_searches/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderObservations)/denom, "exact_candidate_order_observations/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderTransitions)/denom, "exact_candidate_order_transitions/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderAdjacentForward)/denom, "exact_candidate_order_adjacent_forward/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderNonAdjacentForward)/denom, "exact_candidate_order_non_adjacent_forward/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderBackwardJumps)/denom, "exact_candidate_order_backward_jumps/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderMaxForwardRun), "exact_candidate_order_max_forward_run")
	if stats.VisitedNodes > 0 {
		b.ReportMetric(float64(stats.VisitedEdges)/float64(stats.VisitedNodes), "edges_per_visited_node")
	}
}
