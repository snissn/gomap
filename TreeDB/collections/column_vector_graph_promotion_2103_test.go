package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

func TestColumnVectorGraphPreparedDefaultIndexedScoring2103(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("prepared default indexed scoring requires mmap_direct prepared views")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	shape.efSearch = shape.rows
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	if reader.preparedSearch == nil || !reader.preparedSearch.indexedScoringDefaultEligible() {
		t.Fatalf("preparedSearch=%v eligible=%v want default-indexed eligible prepared search", reader.preparedSearch != nil, reader.preparedSearch != nil && reader.preparedSearch.indexedScoringDefaultEligible())
	}

	for _, boundary := range []columnVectorGraphSearchTopologyParityBoundary2091{
		columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091,
		columnVectorGraphSearchTopologyParityBoundaryResultID2091,
	} {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			scalarResults, scalarStats := searchColumnVectorGraphScoreMode2103(t, reader, query, shape, boundary, columnVectorGraphScoreBatchModeScalar)
			defaultResults, defaultStats := searchColumnVectorGraphScoreMode2103(t, reader, query, shape, boundary, columnVectorGraphScoreBatchModeDefault)
			indexedResults, indexedStats := searchColumnVectorGraphScoreMode2103(t, reader, query, shape, boundary, columnVectorGraphScoreBatchModeIndexed)

			if mismatch := columnVectorGraphIndexedScoringResultsMismatch1969(defaultResults, scalarResults); mismatch != "" {
				t.Fatalf("default vs scalar mismatch: %s", mismatch)
			}
			if mismatch := columnVectorGraphIndexedScoringResultsMismatch1969(defaultResults, indexedResults); mismatch != "" {
				t.Fatalf("default vs indexed mismatch: %s", mismatch)
			}
			assertColumnVectorGraphPreparedExactIndexedWorkStatsEqual2098(t, scalarStats, defaultStats)
			assertColumnVectorGraphPreparedExactIndexedWorkStatsEqual2098(t, scalarStats, indexedStats)
			assertColumnVectorGraphDefaultIndexedScoreBatches2103(t, defaultStats, indexedStats, scalarStats, shape.dims)
			assertColumnVectorGraphSearchTopologyParityCurrentStats2091(t, boundary, defaultStats)
		})
	}
}

func TestColumnVectorGraphNonPreparedDefaultScoringRemainsScalar2103(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("non-prepared default scalar scoring comparison requires mmap_direct state")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	shape.efSearch = shape.rows
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	reader.preparedSearch = nil

	for _, boundary := range []columnVectorGraphSearchTopologyParityBoundary2091{
		columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091,
		columnVectorGraphSearchTopologyParityBoundaryResultID2091,
	} {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			scalarResults, scalarStats := searchColumnVectorGraphScoreMode2103(t, reader, query, shape, boundary, columnVectorGraphScoreBatchModeScalar)
			defaultResults, defaultStats := searchColumnVectorGraphScoreMode2103(t, reader, query, shape, boundary, columnVectorGraphScoreBatchModeDefault)
			if mismatch := columnVectorGraphIndexedScoringResultsMismatch1969(defaultResults, scalarResults); mismatch != "" {
				t.Fatalf("non-prepared default vs scalar mismatch: %s", mismatch)
			}
			if defaultStats.PreparedGraphSearchViews != 0 || defaultStats.GraphRowFallbacks != 0 || defaultStats.TypedColumnFallbacks != 0 || defaultStats.AdjacencySourceFallbacks != 0 || defaultStats.ResultIDGraphFallbacks != 0 {
				t.Fatalf("non-prepared default stats=%+v want typed-column source route without graph-row/source fallback", defaultStats)
			}
			if defaultStats.ScoreBatchCalls != scalarStats.ScoreBatchCalls || defaultStats.ScoreBatchCandidates != scalarStats.ScoreBatchCandidates {
				t.Fatalf("non-prepared default stats=%+v scalar stats=%+v want scalar-equivalent score batches", defaultStats, scalarStats)
			}
			assertColumnVectorGraphScalarScoreBatches2103(t, defaultStats)
		})
	}
}

func TestColumnVectorGraphLegacyDefaultScoringRemainsScalar2103(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("legacy default scalar scoring comparison requires mmap_direct adjacency state")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	shape.efSearch = shape.rows
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091)
	defer closeFn()
	if reader.preparedSearch != nil {
		t.Fatalf("legacy reader preparedSearch=%v want nil compatibility graph-row path", reader.preparedSearch)
	}

	for _, boundary := range []columnVectorGraphSearchTopologyParityBoundary2091{
		columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091,
		columnVectorGraphSearchTopologyParityBoundaryResultID2091,
	} {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			scalarResults, scalarStats := searchColumnVectorGraphScoreMode2103(t, reader, query, shape, boundary, columnVectorGraphScoreBatchModeScalar)
			defaultResults, defaultStats := searchColumnVectorGraphScoreMode2103(t, reader, query, shape, boundary, columnVectorGraphScoreBatchModeDefault)
			if mismatch := columnVectorGraphIndexedScoringResultsMismatch1969(defaultResults, scalarResults); mismatch != "" {
				t.Fatalf("legacy default vs scalar mismatch: %s", mismatch)
			}
			assertColumnVectorGraphSearchTopologyParityLegacyStats2091(t, boundary, defaultStats)
			if defaultStats.ScoreBatchCalls != scalarStats.ScoreBatchCalls || defaultStats.ScoreBatchCandidates != scalarStats.ScoreBatchCandidates {
				t.Fatalf("legacy default stats=%+v scalar stats=%+v want scalar-equivalent score batches", defaultStats, scalarStats)
			}
			assertColumnVectorGraphScalarScoreBatches2103(t, defaultStats)
		})
	}
}

func BenchmarkColumnVectorGraphSearchPromotion2103(b *testing.B) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		b.Skip("promotion matrix requires mmap_direct prepared views")
	}
	shape := columnVectorGraphSearchTopologyParityProductionShape2091()
	type promotionRow struct {
		mode      columnVectorGraphSearchTopologyParityMode2091
		scoreName string
		scoreMode columnVectorGraphScoreBatchMode
	}
	rows := []promotionRow{
		{mode: columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091, scoreName: "default", scoreMode: columnVectorGraphScoreBatchModeDefault},
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "default", scoreMode: columnVectorGraphScoreBatchModeDefault},
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "scalar", scoreMode: columnVectorGraphScoreBatchModeScalar},
		{mode: columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, scoreName: "indexed", scoreMode: columnVectorGraphScoreBatchModeIndexed},
	}
	for _, boundary := range []columnVectorGraphSearchTopologyParityBoundary2091{
		columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091,
		columnVectorGraphSearchTopologyParityBoundaryResultID2091,
	} {
		boundary := boundary
		b.Run("boundary="+string(boundary), func(b *testing.B) {
			for _, row := range rows {
				row := row
				b.Run("mode="+string(row.mode)+"/score="+row.scoreName, func(b *testing.B) {
					benchmarkColumnVectorGraphSearchPromotion2103(b, shape, boundary, row.mode, row.scoreName, row.scoreMode)
				})
			}
		})
	}
}

func benchmarkColumnVectorGraphSearchPromotion2103(b *testing.B, shape columnVectorGraphSearchTopologyParityShape2091, boundary columnVectorGraphSearchTopologyParityBoundary2091, mode columnVectorGraphSearchTopologyParityMode2091, scoreName string, scoreMode columnVectorGraphScoreBatchMode) {
	b.Helper()
	rows := columnVectorGraphSearchTopologyParityRows2091(b, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(b, shape, rows, mode)
	defer closeFn()
	var scratch columnVectorGraphNativeSearchScratch
	opts := columnVectorGraphSearchPromotionOptions2103(shape, boundary, scoreMode)
	warm, warmStats, err := reader.SearchCosine(query, opts, &scratch)
	if err != nil {
		b.Fatalf("warm SearchCosine: %v", err)
	}
	if len(warm) == 0 {
		b.Fatalf("warm SearchCosine returned no results")
	}
	assertColumnVectorGraphSearchTopologyParityModeStats2091(b, boundary, mode, warmStats)
	assertColumnVectorGraphBenchmarkDebugStats1979(b, warmStats, false)
	assertColumnVectorGraphSearchPromotionScoreStats2103(b, mode, scoreName, warmStats, shape.dims)
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
	reportColumnVectorGraphSearchTopologyParityMetrics2091(b, shape, boundary, mode, b.N, baseReaderStats, reader.Stats(), totals)
	b.ReportMetric(2103, "promotion_issue")
	b.ReportMetric(1, "promotion_score_mode_"+scoreName)
}

func columnVectorGraphSearchPromotionOptions2103(shape columnVectorGraphSearchTopologyParityShape2091, boundary columnVectorGraphSearchTopologyParityBoundary2091, scoreMode columnVectorGraphScoreBatchMode) columnVectorGraphNativeSearchOptions {
	opts := columnVectorGraphNativeSearchOptions{
		TopK:           shape.topK,
		EfSearch:       shape.efSearch,
		ScoreBatchMode: scoreMode,
		StatsMode:      columnVectorGraphNativeSearchStatsModeBenchmarkDebug,
	}
	if boundary == columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091 {
		opts.OmitResultMaterialization = true
	}
	return opts
}

func searchColumnVectorGraphScoreMode2103(tb testing.TB, reader *columnVectorGraphPhysicalRowReader, query []float32, shape columnVectorGraphSearchTopologyParityShape2091, boundary columnVectorGraphSearchTopologyParityBoundary2091, mode columnVectorGraphScoreBatchMode) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats) {
	tb.Helper()
	var scratch columnVectorGraphNativeSearchScratch
	opts := columnVectorGraphSearchPromotionOptions2103(shape, boundary, mode)
	results, stats, err := reader.SearchCosine(query, opts, &scratch)
	if err != nil {
		tb.Fatalf("SearchCosine boundary=%s score mode=%s: %v", boundary, mode.String(), err)
	}
	if len(results) == 0 {
		tb.Fatalf("SearchCosine boundary=%s score mode=%s returned no results", boundary, mode.String())
	}
	assertColumnVectorGraphBenchmarkDebugStats1979(tb, stats, shape.efSearch >= shape.rows)
	return cloneColumnVectorGraphPreparedResults2045(results), stats
}

func assertColumnVectorGraphDefaultIndexedScoreBatches2103(tb testing.TB, defaultStats, indexedStats, scalarStats columnVectorGraphNativeSearchStats, dims int) {
	tb.Helper()
	if defaultStats.ScoreBatchCalls != indexedStats.ScoreBatchCalls || defaultStats.ScoreBatchCandidates != indexedStats.ScoreBatchCandidates || defaultStats.ScoreBatchMaxTileSize != indexedStats.ScoreBatchMaxTileSize || defaultStats.ScoreBatchOptimizedCalls != indexedStats.ScoreBatchOptimizedCalls || defaultStats.ScoreBatchScalarFallbackCalls != indexedStats.ScoreBatchScalarFallbackCalls {
		tb.Fatalf("default stats=%+v indexed stats=%+v want default to select identical indexed prepared score batching", defaultStats, indexedStats)
	}
	if defaultStats.ScoreBatchCalls != scalarStats.ScoreBatchCalls || defaultStats.ScoreBatchCandidates != scalarStats.ScoreBatchCandidates || defaultStats.ScoreBatchMaxTileSize != scalarStats.ScoreBatchMaxTileSize || defaultStats.ScoreBatchMaxTileSize < 2 {
		tb.Fatalf("default stats=%+v scalar stats=%+v want prepared scalar and indexed modes to share neighbor-tile grouping", defaultStats, scalarStats)
	}
	assertColumnVectorGraphPreparedScalarScoreBatches2103(tb, scalarStats)
	if defaultStats.ScoreBatchSingletons+defaultStats.ScoreBatchSize2To4+defaultStats.ScoreBatchSize5To8+defaultStats.ScoreBatchSize9To16+defaultStats.ScoreBatchSize17Plus != defaultStats.ScoreBatchCalls {
		tb.Fatalf("default stats=%+v want score-batch histogram to reconcile", defaultStats)
	}
	if defaultStats.GraphRowFallbacks != 0 || defaultStats.TypedColumnFallbacks != 0 || defaultStats.VectorScratchDecodes != 0 || defaultStats.NormScratchDecodes != 0 || defaultStats.AdjacencySourceFallbacks != 0 || defaultStats.ResultIDGraphFallbacks != 0 {
		tb.Fatalf("default stats=%+v want healthy prepared path without graph-row/source fallback", defaultStats)
	}
	assertColumnVectorGraphPreparedIndexedBackendCounters2125(tb, defaultStats.ScoreBatchOptimizedCalls, defaultStats.ScoreBatchScalarFallbackCalls, int(defaultStats.ScoreBatchMaxTileSize), dims)
}

func assertColumnVectorGraphPreparedIndexedBackendCounters2125(tb testing.TB, optimizedCalls, fallbackCalls uint64, maxTileSize int, dims int) {
	tb.Helper()
	if vectorops.DotFloat32IndexedOptimizedEligible(maxTileSize, dims) {
		if optimizedCalls == 0 {
			tb.Fatalf("indexed backend eligible for max_tile=%d dims=%d but optimized score batches=0 fallback=%d", maxTileSize, dims, fallbackCalls)
		}
		return
	}
	if optimizedCalls != 0 || fallbackCalls == 0 {
		tb.Fatalf("indexed backend ineligible for max_tile=%d dims=%d: optimized=%d fallback=%d want fallback-only counters", maxTileSize, dims, optimizedCalls, fallbackCalls)
	}
}

func assertColumnVectorGraphScalarScoreBatches2103(tb testing.TB, stats columnVectorGraphNativeSearchStats) {
	tb.Helper()
	if stats.ScoreBatchCalls == 0 || stats.ScoreBatchCalls != stats.ScoreBatchCandidates || stats.ScoreBatchMaxTileSize != 1 || stats.ScoreBatchSingletons != stats.ScoreBatchCalls {
		tb.Fatalf("stats=%+v want scalar singleton score batches", stats)
	}
}

func assertColumnVectorGraphPreparedScalarScoreBatches2103(tb testing.TB, stats columnVectorGraphNativeSearchStats) {
	tb.Helper()
	if stats.ScoreBatchCalls == 0 || stats.ScoreBatchCalls >= stats.ScoreBatchCandidates || stats.ScoreBatchMaxTileSize < 2 || stats.ScoreBatchOptimizedCalls != 0 || stats.ScoreBatchScalarFallbackCalls != stats.ScoreBatchCalls {
		tb.Fatalf("stats=%+v want prepared scalar neighbor-tile batches with scalar fallback backend", stats)
	}
}

func assertColumnVectorGraphSearchPromotionScoreStats2103(tb testing.TB, mode columnVectorGraphSearchTopologyParityMode2091, scoreName string, stats columnVectorGraphNativeSearchStats, dims int) {
	tb.Helper()
	if mode == columnVectorGraphSearchTopologyParityModeCurrentPrepared2091 {
		if scoreName == "scalar" {
			assertColumnVectorGraphPreparedScalarScoreBatches2103(tb, stats)
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
	assertColumnVectorGraphScalarScoreBatches2103(tb, stats)
}
