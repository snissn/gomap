package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type vectorIndexSearchPromotionBoundary2105 string

const (
	vectorIndexSearchPromotionBoundaryResultID2105                vectorIndexSearchPromotionBoundary2105 = "result_id"
	vectorIndexSearchPromotionBoundaryDocumentMaterialization2105 vectorIndexSearchPromotionBoundary2105 = "document_materialization"
)

type vectorIndexSearchPromotionRow2105 struct {
	mode      columnVectorGraphSearchTopologyParityMode2091
	scoreName string
	scoreMode columnVectorGraphScoreBatchMode
}

func TestColumnVectorGraphPublicVectorIndexSearcherSearchPromotion2105(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("public promotion matrix requires mmap_direct prepared views")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	shape.efSearch = shape.rows
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)

	t.Run("prepared_default_scalar_indexed_equivalent", func(t *testing.T) {
		closeFn, searcher, query := openVectorIndexSearcherPromotion2105(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
		defer closeFn()
		if searcher.reader.preparedSearch == nil || !searcher.reader.preparedSearch.indexedScoringDefaultEligible() {
			t.Fatalf("preparedSearch=%v eligible=%v want default-indexed eligible prepared public searcher", searcher.reader.preparedSearch != nil, searcher.reader.preparedSearch != nil && searcher.reader.preparedSearch.indexedScoringDefaultEligible())
		}
		for _, boundary := range []vectorIndexSearchPromotionBoundary2105{
			vectorIndexSearchPromotionBoundaryResultID2105,
			vectorIndexSearchPromotionBoundaryDocumentMaterialization2105,
		} {
			boundary := boundary
			t.Run(string(boundary), func(t *testing.T) {
				scalarResults, scalarStats := searchVectorIndexSearcherPromotion2105(t, searcher, query, shape, boundary, columnVectorGraphScoreBatchModeScalar)
				defaultResults, defaultStats := searchVectorIndexSearcherPromotion2105(t, searcher, query, shape, boundary, columnVectorGraphScoreBatchModeDefault)
				indexedResults, indexedStats := searchVectorIndexSearcherPromotion2105(t, searcher, query, shape, boundary, columnVectorGraphScoreBatchModeIndexed)

				if mismatch := vectorIndexSearchResultsMismatch1969(defaultResults, scalarResults); mismatch != "" {
					t.Fatalf("default vs scalar mismatch: %s", mismatch)
				}
				if mismatch := vectorIndexSearchResultsMismatch1969(defaultResults, indexedResults); mismatch != "" {
					t.Fatalf("default vs indexed mismatch: %s", mismatch)
				}
				if boundary == vectorIndexSearchPromotionBoundaryDocumentMaterialization2105 {
					assertVectorIndexSearchPromotionDocumentsEqual2105(t, defaultResults, scalarResults)
					assertVectorIndexSearchPromotionDocumentsEqual2105(t, defaultResults, indexedResults)
				}
				assertVectorIndexSearchPromotionCurrentStats2105(t, boundary, defaultStats)
				assertVectorIndexSearchPromotionCurrentStats2105(t, boundary, scalarStats)
				assertVectorIndexSearchPromotionCurrentStats2105(t, boundary, indexedStats)
				assertVectorIndexSearchDefaultIndexedScoreBatches2105(t, defaultStats, indexedStats, scalarStats, shape.dims)
			})
		}
	})

	t.Run("legacy_default_remains_scalar", func(t *testing.T) {
		closeFn, searcher, query := openVectorIndexSearcherPromotion2105(t, shape, rows, columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091)
		defer closeFn()
		if searcher.reader.preparedSearch != nil {
			t.Fatalf("legacy public searcher preparedSearch=%v want nil compatibility graph-row path", searcher.reader.preparedSearch)
		}
		scalarResults, scalarStats := searchVectorIndexSearcherPromotion2105(t, searcher, query, shape, vectorIndexSearchPromotionBoundaryResultID2105, columnVectorGraphScoreBatchModeScalar)
		defaultResults, defaultStats := searchVectorIndexSearcherPromotion2105(t, searcher, query, shape, vectorIndexSearchPromotionBoundaryResultID2105, columnVectorGraphScoreBatchModeDefault)
		if mismatch := vectorIndexSearchResultsMismatch1969(defaultResults, scalarResults); mismatch != "" {
			t.Fatalf("legacy default vs scalar mismatch: %s", mismatch)
		}
		assertVectorIndexSearchPromotionLegacyStats2105(t, defaultStats)
		if defaultStats.ScoreBatchCalls != scalarStats.ScoreBatchCalls || defaultStats.ScoreBatchCandidates != scalarStats.ScoreBatchCandidates {
			t.Fatalf("legacy default stats=%+v scalar stats=%+v want scalar-equivalent score batches", defaultStats, scalarStats)
		}
		assertVectorIndexSearchScalarScoreBatches2105(t, defaultStats)
	})
}

func BenchmarkVectorIndexSearcherSearchPromotion2105(b *testing.B) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		b.Skip("public promotion matrix requires mmap_direct prepared views")
	}
	boundedShape := columnVectorGraphSearchTopologyParityProductionShape2091()
	exactShape := boundedShape
	exactShape.efSearch = exactShape.rows
	resultIDRows := []vectorIndexSearchPromotionRow2105{
		{mode: columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091, scoreName: "default", scoreMode: columnVectorGraphScoreBatchModeDefault},
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "default", scoreMode: columnVectorGraphScoreBatchModeDefault},
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "scalar", scoreMode: columnVectorGraphScoreBatchModeScalar},
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "indexed", scoreMode: columnVectorGraphScoreBatchModeIndexed},
	}
	documentRows := []vectorIndexSearchPromotionRow2105{
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "default", scoreMode: columnVectorGraphScoreBatchModeDefault},
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "scalar", scoreMode: columnVectorGraphScoreBatchModeScalar},
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "indexed", scoreMode: columnVectorGraphScoreBatchModeIndexed},
	}
	cases := []struct {
		searchName string
		shape      columnVectorGraphSearchTopologyParityShape2091
		boundary   vectorIndexSearchPromotionBoundary2105
		rows       []vectorIndexSearchPromotionRow2105
	}{
		{searchName: "ef_search_128", shape: boundedShape, boundary: vectorIndexSearchPromotionBoundaryResultID2105, rows: resultIDRows},
		{searchName: "ef_search_128", shape: boundedShape, boundary: vectorIndexSearchPromotionBoundaryDocumentMaterialization2105, rows: documentRows},
		{searchName: "exact", shape: exactShape, boundary: vectorIndexSearchPromotionBoundaryResultID2105, rows: resultIDRows},
	}
	for _, tc := range cases {
		tc := tc
		fixtureRows := columnVectorGraphSearchTopologyParityRows2091(b, tc.shape)
		b.Run("search="+tc.searchName+"/boundary="+string(tc.boundary), func(b *testing.B) {
			for _, row := range tc.rows {
				row := row
				b.Run("mode="+string(row.mode)+"/score="+row.scoreName, func(b *testing.B) {
					benchmarkVectorIndexSearcherSearchPromotion2105(b, tc.searchName, tc.shape, fixtureRows, tc.boundary, row)
				})
			}
		})
	}
}

func benchmarkVectorIndexSearcherSearchPromotion2105(b *testing.B, searchName string, shape columnVectorGraphSearchTopologyParityShape2091, rows []columnVectorGraphAssetRow, boundary vectorIndexSearchPromotionBoundary2105, row vectorIndexSearchPromotionRow2105) {
	b.Helper()
	closeFn, searcher, query := openVectorIndexSearcherPromotion2105(b, shape, rows, row.mode)
	defer closeFn()
	opts := vectorIndexSearchPromotionOptions2105(shape, boundary, row.scoreMode)
	opts.Query = query
	warm, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	if len(warm.Results) == 0 {
		b.Fatalf("warm Search returned no results")
	}
	measured, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search: %v", err)
	}
	if len(measured.Results) == 0 {
		b.Fatalf("measure Search returned no results")
	}
	assertVectorIndexSearchPromotionModeStats2105(b, boundary, row.mode, measured.Stats)
	assertVectorIndexSearchBenchmarkDebugStats2105(b, measured.Stats, shape.efSearch >= shape.rows)
	assertVectorIndexSearchPromotionScoreStats2105(b, row.mode, row.scoreName, measured.Stats, shape.dims)
	var checksum int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		if len(got.Results) == 0 {
			b.Fatalf("Search returned no results")
		}
		if boundary == vectorIndexSearchPromotionBoundaryDocumentMaterialization2105 {
			checksum += int64(len(got.Results[0].Document))
		} else {
			checksum += int64(got.Results[0].Ordinal)
		}
	}
	b.StopTimer()
	vectorSearchBenchSinkOrdinalV4 += int(checksum)
	reportVectorIndexSearchBenchMetricsV4(b, b.N, measured.Stats, false)
	reportVectorIndexSearchPromotionLabels2105(b, searchName, shape, boundary, row)
}

func openVectorIndexSearcherPromotion2105(tb testing.TB, shape columnVectorGraphSearchTopologyParityShape2091, rows []columnVectorGraphAssetRow, mode columnVectorGraphSearchTopologyParityMode2091) (func(), *VectorIndexSearcher, []float32) {
	tb.Helper()
	var d *backenddb.DB
	var col *Collection
	var def VectorIndexDefinition
	switch mode {
	case columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091:
		d, col, def = publishColumnVectorGraphPhysicalReaderTestAssetWithShapeAndAdjacencyState1989(tb, shape.dims, shape.degree, cloneColumnVectorGraphTopologyParityRows2091(rows))
	case columnVectorGraphSearchTopologyParityModeCurrentPrepared2091:
		d, col, def = publishColumnVectorGraphCurrentPreparedTopologyParityCollection2091(tb, shape.dims, shape.degree, cloneColumnVectorGraphTopologyParityRows2091(rows))
	default:
		tb.Fatalf("unsupported promotion mode %q", mode)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenVectorIndexSearcher mode=%s: %v", mode, err)
	}
	if mode == columnVectorGraphSearchTopologyParityModeCurrentPrepared2091 {
		if searcher.reader == nil || searcher.reader.preparedSearch == nil || !searcher.reader.preparedSearch.ready() {
			_ = searcher.Close()
			_ = d.Close()
			tb.Fatalf("current prepared public searcher preparedSearch=%v ready=%v", searcher.reader != nil && searcher.reader.preparedSearch != nil, searcher.reader != nil && searcher.reader.preparedSearch != nil && searcher.reader.preparedSearch.ready())
		}
		if stats := searcher.reader.Stats(); stats.Rows != 0 {
			_ = searcher.Close()
			_ = d.Close()
			tb.Fatalf("current prepared public searcher graph rows=%d want 0", stats.Rows)
		}
	}
	query := append([]float32(nil), rows[shape.queryOrdinal].Vector...)
	return func() { _ = searcher.Close(); _ = d.Close() }, searcher, query
}

func vectorIndexSearchPromotionOptions2105(shape columnVectorGraphSearchTopologyParityShape2091, boundary vectorIndexSearchPromotionBoundary2105, scoreMode columnVectorGraphScoreBatchMode) VectorIndexSearcherSearchOptions {
	opts := VectorIndexSearcherSearchOptions{
		Query:          nil,
		TopK:           shape.topK,
		EfSearch:       shape.efSearch,
		StatsMode:      VectorIndexSearchStatsModeBenchmarkDebug,
		scoreBatchMode: scoreMode,
	}
	if boundary == vectorIndexSearchPromotionBoundaryDocumentMaterialization2105 {
		opts.IncludeDocuments = true
	}
	return opts
}

func searchVectorIndexSearcherPromotion2105(tb testing.TB, searcher *VectorIndexSearcher, query []float32, shape columnVectorGraphSearchTopologyParityShape2091, boundary vectorIndexSearchPromotionBoundary2105, mode columnVectorGraphScoreBatchMode) ([]VectorIndexSearchResult, VectorIndexSearchStats) {
	tb.Helper()
	opts := vectorIndexSearchPromotionOptions2105(shape, boundary, mode)
	opts.Query = query
	response, err := searcher.Search(opts)
	if err != nil {
		tb.Fatalf("Search boundary=%s score mode=%s: %v", boundary, mode.String(), err)
	}
	if len(response.Results) == 0 {
		tb.Fatalf("Search boundary=%s score mode=%s returned no results", boundary, mode.String())
	}
	assertVectorIndexSearchBenchmarkDebugStats2105(tb, response.Stats, shape.efSearch >= shape.rows)
	return cloneVectorIndexSearchResults1969(response.Results), response.Stats
}

func assertVectorIndexSearchPromotionDocumentsEqual2105(tb testing.TB, got, want []VectorIndexSearchResult) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("document results len=%d want %d", len(got), len(want))
	}
	for i := range want {
		if len(got[i].Document) == 0 || len(want[i].Document) == 0 {
			tb.Fatalf("document result[%d] empty got=%d want=%d", i, len(got[i].Document), len(want[i].Document))
		}
		if !bytes.Equal(got[i].Document, want[i].Document) {
			tb.Fatalf("document result[%d] mismatch got=%q want=%q", i, string(got[i].Document), string(want[i].Document))
		}
	}
}

func reportVectorIndexSearchPromotionLabels2105(b *testing.B, searchName string, shape columnVectorGraphSearchTopologyParityShape2091, boundary vectorIndexSearchPromotionBoundary2105, row vectorIndexSearchPromotionRow2105) {
	b.Helper()
	b.ReportMetric(2105, "promotion_public_issue")
	b.ReportMetric(1, "promotion_public_search_"+searchName)
	b.ReportMetric(1, "promotion_public_mode_"+string(row.mode))
	b.ReportMetric(1, "promotion_public_boundary_"+string(boundary))
	b.ReportMetric(1, "promotion_public_score_mode_"+row.scoreName)
	b.ReportMetric(float64(shape.rows), "rows")
	b.ReportMetric(float64(shape.dims), "dims")
	b.ReportMetric(float64(shape.degree), "degree")
	b.ReportMetric(float64(shape.topK), "top_k")
	b.ReportMetric(float64(shape.efSearch), "ef_search")
	b.ReportMetric(float64(shape.queryOrdinal), "query_ordinal")
}

func assertVectorIndexSearchPromotionModeStats2105(tb testing.TB, boundary vectorIndexSearchPromotionBoundary2105, mode columnVectorGraphSearchTopologyParityMode2091, stats VectorIndexSearchStats) {
	tb.Helper()
	switch mode {
	case columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091:
		assertVectorIndexSearchPromotionLegacyStats2105(tb, stats)
	case columnVectorGraphSearchTopologyParityModeCurrentPrepared2091:
		assertVectorIndexSearchPromotionCurrentStats2105(tb, boundary, stats)
	default:
		tb.Fatalf("unsupported promotion mode %q", mode)
	}
}

func assertVectorIndexSearchPromotionCurrentStats2105(tb testing.TB, boundary vectorIndexSearchPromotionBoundary2105, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.GraphRows != 0 || stats.PreparedGraphSearchViews != 1 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 || stats.NormScratchDecodes != 0 || stats.ResultIDGraphFallbacks != 0 {
		tb.Fatalf("current public %s stats=%+v want prepared current-format path without graph rows/fallback", boundary, stats)
	}
	if stats.PreparedScoreCalls != stats.CandidateFetches || stats.VectorPreparedDirectViews != stats.CandidateFetches || stats.NormPreparedDirectViews != stats.CandidateFetches {
		tb.Fatalf("current public %s stats=%+v want prepared scoring to cover all candidates", boundary, stats)
	}
	if stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencyPreparedCSRMmapDirectViews == 0 || stats.AdjacencySourceFallbacks != 0 || stats.RowRefVectorSourceLegacyGraphIDs != 0 || stats.RowRefStateSourceFallbacks != 0 {
		tb.Fatalf("current public %s stats=%+v want prepared adjacency/result sources with no fallback", boundary, stats)
	}
	if stats.ResultFetches == 0 || stats.ResultIDPreparedBytesViews != 1 || stats.ResultIDTypedBytesState != stats.ResultFetches || stats.RowRefStateResultRefs != stats.ResultFetches {
		tb.Fatalf("current public %s stats=%+v want public result-ID and row-ref materialization", boundary, stats)
	}
	if boundary == vectorIndexSearchPromotionBoundaryDocumentMaterialization2105 {
		if stats.DocumentsFetched != stats.ResultFetches || stats.DocumentRowRefStateFetches != stats.ResultFetches || stats.DocumentRowRefLookupFallbacks != 0 {
			tb.Fatalf("current public document stats=%+v want post-top-k row-ref document materialization", stats)
		}
	} else if stats.DocumentsFetched != 0 || stats.DocumentBytes != 0 {
		tb.Fatalf("current public result-ID stats=%+v want no document materialization", stats)
	}
}

func assertVectorIndexSearchPromotionLegacyStats2105(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.GraphRows == 0 || stats.PreparedGraphSearchViews != 0 || stats.GraphRowFallbacks == 0 || stats.TypedColumnFallbacks == 0 || stats.VectorScratchDecodes == 0 {
		tb.Fatalf("legacy public stats=%+v want graph-row/direct compatibility path", stats)
	}
	if stats.ResultFetches == 0 || stats.ResultIDGraphFallbacks != stats.ResultFetches {
		tb.Fatalf("legacy public stats=%+v want graph-row result-ID fallback for each result", stats)
	}
	if stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencyPreparedCSRMmapDirectViews == 0 || stats.AdjacencySourceFallbacks != 0 {
		tb.Fatalf("legacy public stats=%+v want shared prepared-CSR adjacency without source fallback", stats)
	}
}

func assertVectorIndexSearchDefaultIndexedScoreBatches2105(tb testing.TB, defaultStats, indexedStats, scalarStats VectorIndexSearchStats, dims int) {
	tb.Helper()
	if defaultStats.ScoreBatchCalls != indexedStats.ScoreBatchCalls || defaultStats.ScoreBatchCandidates != indexedStats.ScoreBatchCandidates || defaultStats.ScoreBatchMaxTileSize != indexedStats.ScoreBatchMaxTileSize || defaultStats.ScoreBatchOptimizedCalls != indexedStats.ScoreBatchOptimizedCalls || defaultStats.ScoreBatchScalarFallbackCalls != indexedStats.ScoreBatchScalarFallbackCalls {
		tb.Fatalf("default stats=%+v indexed stats=%+v want default to select identical indexed prepared score batching", defaultStats, indexedStats)
	}
	if defaultStats.ScoreBatchCalls != scalarStats.ScoreBatchCalls || defaultStats.ScoreBatchCandidates != scalarStats.ScoreBatchCandidates || defaultStats.ScoreBatchMaxTileSize != scalarStats.ScoreBatchMaxTileSize || defaultStats.ScoreBatchMaxTileSize < 2 {
		tb.Fatalf("default stats=%+v scalar stats=%+v want prepared scalar and indexed modes to share neighbor-tile grouping", defaultStats, scalarStats)
	}
	assertVectorIndexSearchPreparedScalarScoreBatches2105(tb, scalarStats)
	if defaultStats.ScoreBatchSingletons+defaultStats.ScoreBatchSize2To4+defaultStats.ScoreBatchSize5To8+defaultStats.ScoreBatchSize9To16+defaultStats.ScoreBatchSize17Plus != defaultStats.ScoreBatchCalls {
		tb.Fatalf("default stats=%+v want score-batch histogram to reconcile", defaultStats)
	}
	if defaultStats.GraphRowFallbacks != 0 || defaultStats.TypedColumnFallbacks != 0 || defaultStats.VectorScratchDecodes != 0 || defaultStats.NormScratchDecodes != 0 || defaultStats.AdjacencySourceFallbacks != 0 || defaultStats.ResultIDGraphFallbacks != 0 {
		tb.Fatalf("default stats=%+v want healthy prepared path without graph-row/source fallback", defaultStats)
	}
	assertColumnVectorGraphPreparedIndexedBackendCounters2125(tb, defaultStats.ScoreBatchOptimizedCalls, defaultStats.ScoreBatchScalarFallbackCalls, int(defaultStats.ScoreBatchMaxTileSize), dims)
}

func assertVectorIndexSearchScalarScoreBatches2105(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.ScoreBatchCalls == 0 || stats.ScoreBatchCalls != stats.ScoreBatchCandidates || stats.ScoreBatchMaxTileSize != 1 || stats.ScoreBatchSingletons != stats.ScoreBatchCalls {
		tb.Fatalf("stats=%+v want scalar singleton score batches", stats)
	}
}

func assertVectorIndexSearchPreparedScalarScoreBatches2105(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.ScoreBatchCalls == 0 || stats.ScoreBatchCalls >= stats.ScoreBatchCandidates || stats.ScoreBatchMaxTileSize < 2 || stats.ScoreBatchOptimizedCalls != 0 || stats.ScoreBatchScalarFallbackCalls != stats.ScoreBatchCalls {
		tb.Fatalf("stats=%+v want prepared scalar neighbor-tile batches with scalar fallback backend", stats)
	}
}

func assertVectorIndexSearchPromotionScoreStats2105(tb testing.TB, mode columnVectorGraphSearchTopologyParityMode2091, scoreName string, stats VectorIndexSearchStats, dims int) {
	tb.Helper()
	if mode == columnVectorGraphSearchTopologyParityModeCurrentPrepared2091 {
		if scoreName == "scalar" {
			assertVectorIndexSearchPreparedScalarScoreBatches2105(tb, stats)
			return
		}
		if scoreName == "default" || scoreName == "indexed" {
			if stats.ScoreBatchCalls >= stats.ScoreBatchCandidates || stats.ScoreBatchMaxTileSize < 2 {
				tb.Fatalf("mode=%s score=%s stats=%+v want grouped prepared indexed score batches", mode, scoreName, stats)
			}
			assertColumnVectorGraphPreparedIndexedBackendCounters2125(tb, stats.ScoreBatchOptimizedCalls, stats.ScoreBatchScalarFallbackCalls, int(stats.ScoreBatchMaxTileSize), dims)
			return
		}
	}
	assertVectorIndexSearchScalarScoreBatches2105(tb, stats)
}

func assertVectorIndexSearchBenchmarkDebugStats2105(tb testing.TB, stats VectorIndexSearchStats, exactMode bool) {
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
		tb.Fatalf("stats=%+v want layer-0 scored/skip buckets to reconcile", stats)
	}
	if stats.UpperLayerScoredNeighbors+stats.UpperLayerFilterSkips != stats.UpperLayerEdgeVisits {
		tb.Fatalf("stats=%+v want upper-layer scored/filter buckets to reconcile", stats)
	}
	if stats.ScoredNeighbors+stats.SkippedNeighbors != stats.VisitedEdges {
		tb.Fatalf("stats=%+v want scored+skipped neighbor buckets to reconcile", stats)
	}
	if stats.VisitedMarkHits+stats.VisitedMarkMisses != stats.VisitedMarkChecks || stats.VisitedMarkHits != stats.Layer0AlreadyVisitedSkips || stats.VisitedMarkInserts < stats.VisitedMarkMisses || stats.VisitedResetEpochAdvances != 1 {
		tb.Fatalf("stats=%+v want visited-mark hits/misses/inserts/reset work to reconcile", stats)
	}
	if stats.FrontierPushes != stats.Candidates || stats.TopKInsertAttempts != stats.Candidates || stats.TopKInsertSuccesses+stats.TopKInsertRejections != stats.TopKInsertAttempts {
		tb.Fatalf("stats=%+v want frontier/top-k operation counts to reconcile with candidates", stats)
	}
	if stats.FrontierSiftUpCalls != stats.FrontierPushes || stats.FrontierSiftDownCalls > stats.FrontierPops || stats.CandidateComparisons != stats.FrontierComparisons+stats.TopKComparisons {
		tb.Fatalf("stats=%+v want frontier/top-k comparison and sift counters to reconcile", stats)
	}
	if stats.Layer0StopTrue+stats.Layer0StopFalse != stats.Layer0StopChecks || stats.Layer0StopChecks == 0 {
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
		return
	}
	if stats.ExactModeSearches != 0 || stats.ExactCandidateOrderObservations != 0 {
		tb.Fatalf("stats=%+v want exact-mode counters disabled for bounded ef_search", stats)
	}
}

func reportVectorIndexSearchBenchmarkDebugMetrics2105(b *testing.B, stats VectorIndexSearchStats) {
	b.Helper()
	b.ReportMetric(float64(stats.BenchmarkDebugSearches), "benchmark_debug_searches/search")
	b.ReportMetric(float64(stats.NeighborTiles), "neighbor_tiles/search")
	b.ReportMetric(float64(stats.NeighborTileNeighbors), "neighbor_tile_neighbors/search")
	b.ReportMetric(float64(stats.NeighborTileMaxSize), "neighbor_tile_max_size")
	neighborTileAvg := 0.0
	if stats.NeighborTiles > 0 {
		neighborTileAvg = float64(stats.NeighborTileNeighbors) / float64(stats.NeighborTiles)
	}
	b.ReportMetric(neighborTileAvg, "neighbor_tile_avg_size")
	b.ReportMetric(float64(stats.NeighborTileSize0), "neighbor_tile_size_0/search")
	b.ReportMetric(float64(stats.NeighborTileSize1), "neighbor_tile_size_1/search")
	b.ReportMetric(float64(stats.NeighborTileSize2To4), "neighbor_tile_size_2_4/search")
	b.ReportMetric(float64(stats.NeighborTileSize5To8), "neighbor_tile_size_5_8/search")
	b.ReportMetric(float64(stats.NeighborTileSize9To16), "neighbor_tile_size_9_16/search")
	b.ReportMetric(float64(stats.NeighborTileSize17Plus), "neighbor_tile_size_17_plus/search")
	b.ReportMetric(float64(stats.ScoreBatchSingletons), "score_batch_singletons/search")
	b.ReportMetric(float64(stats.ScoreBatchSize2To4), "score_batch_size_2_4/search")
	b.ReportMetric(float64(stats.ScoreBatchSize5To8), "score_batch_size_5_8/search")
	b.ReportMetric(float64(stats.ScoreBatchSize9To16), "score_batch_size_9_16/search")
	b.ReportMetric(float64(stats.ScoreBatchSize17Plus), "score_batch_size_17_plus/search")
	b.ReportMetric(float64(stats.ScoredNeighbors), "scored_neighbors/search")
	b.ReportMetric(float64(stats.SkippedNeighbors), "skipped_neighbors/search")
	b.ReportMetric(float64(stats.AlreadyVisitedSkips), "already_visited_skips/search")
	b.ReportMetric(float64(stats.FilterSkips), "filter_skips/search")
	b.ReportMetric(float64(stats.UpperLayerScores), "upper_layer_scores/search")
	b.ReportMetric(float64(stats.UpperLayerEntryScores), "upper_layer_entry_scores/search")
	b.ReportMetric(float64(stats.UpperLayerNeighborScores), "upper_layer_neighbor_scores/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTiles), "upper_layer_score_tiles/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTileCandidates), "upper_layer_score_tile_candidates/search")
	b.ReportMetric(float64(stats.UpperLayerScoreTileMaxSize), "upper_layer_score_tile_max_size")
	b.ReportMetric(float64(stats.UpperLayerAdjacencyLoads), "upper_layer_adjacency_loads/search")
	b.ReportMetric(float64(stats.UpperLayerAdjacencyNeighbors), "upper_layer_adjacency_neighbors/search")
	b.ReportMetric(float64(stats.UpperLayerEdgeVisits), "upper_layer_edge_visits/search")
	b.ReportMetric(float64(stats.UpperLayerScoredNeighbors), "upper_layer_scored_neighbors/search")
	b.ReportMetric(float64(stats.UpperLayerFilterSkips), "upper_layer_filter_skips/search")
	b.ReportMetric(float64(stats.Layer0Scores), "layer0_scores/search")
	b.ReportMetric(float64(stats.Layer0SeedScores), "layer0_seed_scores/search")
	b.ReportMetric(float64(stats.Layer0NeighborScores), "layer0_neighbor_scores/search")
	b.ReportMetric(float64(stats.Layer0ScoreTiles), "layer0_score_tiles/search")
	b.ReportMetric(float64(stats.Layer0ScoreTileCandidates), "layer0_score_tile_candidates/search")
	b.ReportMetric(float64(stats.Layer0ScoreTileMaxSize), "layer0_score_tile_max_size")
	b.ReportMetric(float64(stats.Layer0AdjacencyLoads), "layer0_adjacency_loads/search")
	b.ReportMetric(float64(stats.Layer0AdjacencyNeighbors), "layer0_adjacency_neighbors/search")
	b.ReportMetric(float64(stats.Layer0EdgeVisits), "layer0_edge_visits/search")
	b.ReportMetric(float64(stats.Layer0ScoredNeighbors), "layer0_scored_neighbors/search")
	b.ReportMetric(float64(stats.Layer0AlreadyVisitedSkips), "layer0_already_visited_skips/search")
	b.ReportMetric(float64(stats.Layer0FilterSkips), "layer0_filter_skips/search")
	b.ReportMetric(float64(stats.Layer0StopChecks), "layer0_stop_checks/search")
	b.ReportMetric(float64(stats.Layer0StopTrue), "layer0_stop_true/search")
	b.ReportMetric(float64(stats.Layer0StopFalse), "layer0_stop_false/search")
	b.ReportMetric(float64(stats.CandidateComparisons), "candidate_comparisons/search")
	b.ReportMetric(float64(stats.FrontierComparisons), "frontier_comparisons/search")
	b.ReportMetric(float64(stats.TopKComparisons), "top_k_comparisons/search")
	b.ReportMetric(float64(stats.FrontierPushes), "frontier_pushes/search")
	b.ReportMetric(float64(stats.FrontierPops), "frontier_pops/search")
	b.ReportMetric(float64(stats.FrontierPopMisses), "frontier_pop_misses/search")
	b.ReportMetric(float64(stats.FrontierSiftUpCalls), "frontier_sift_up_calls/search")
	b.ReportMetric(float64(stats.FrontierSiftDownCalls), "frontier_sift_down_calls/search")
	b.ReportMetric(float64(stats.FrontierSiftUpSteps), "frontier_sift_up_steps/search")
	b.ReportMetric(float64(stats.FrontierSiftDownSteps), "frontier_sift_down_steps/search")
	b.ReportMetric(float64(stats.TopKInsertAttempts), "top_k_insert_attempts/search")
	b.ReportMetric(float64(stats.TopKInsertSuccesses), "top_k_insert_successes/search")
	b.ReportMetric(float64(stats.TopKInsertRejections), "top_k_insert_rejections/search")
	b.ReportMetric(float64(stats.TopKHeapSiftSteps), "top_k_heap_sift_steps/search")
	b.ReportMetric(float64(stats.VisitedMarkChecks), "visited_mark_checks/search")
	b.ReportMetric(float64(stats.VisitedMarkHits), "visited_mark_hits/search")
	b.ReportMetric(float64(stats.VisitedMarkMisses), "visited_mark_misses/search")
	b.ReportMetric(float64(stats.VisitedMarkInserts), "visited_mark_inserts/search")
	b.ReportMetric(float64(stats.VisitedResetEpochAdvances), "visited_reset_epoch_advances/search")
	b.ReportMetric(float64(stats.VisitedResetClearedRows), "visited_reset_cleared_rows/search")
	b.ReportMetric(float64(stats.ExactModeSearches), "exact_mode_searches/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderObservations), "exact_candidate_order_observations/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderTransitions), "exact_candidate_order_transitions/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderAdjacentForward), "exact_candidate_order_adjacent_forward/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderNonAdjacentForward), "exact_candidate_order_non_adjacent_forward/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderBackwardJumps), "exact_candidate_order_backward_jumps/search")
	b.ReportMetric(float64(stats.ExactCandidateOrderMaxForwardRun), "exact_candidate_order_max_forward_run")
	if stats.VisitedNodes > 0 {
		b.ReportMetric(float64(stats.VisitedEdges)/float64(stats.VisitedNodes), "edges_per_visited_node")
	}
}

func (b vectorIndexSearchPromotionBoundary2105) String() string { return string(b) }
