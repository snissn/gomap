package collections

import (
	"errors"
	"math"
	"testing"
)

type columnVectorGraphResultDelta1981 struct {
	Queries                int
	TopK                   int
	TopKOverlap            int
	OrderingMismatches     int
	MaxScoreAbsDelta       float64
	ExactOnly              int
	CandidateOnly          int
	CandidateResultShorter int
}

func (d columnVectorGraphResultDelta1981) recallPct() float64 {
	denom := d.Queries * d.TopK
	if denom <= 0 {
		return 0
	}
	return float64(d.TopKOverlap) * 100 / float64(denom)
}

func (d columnVectorGraphResultDelta1981) orderingMismatchesPerQuery() float64 {
	if d.Queries <= 0 {
		return 0
	}
	return float64(d.OrderingMismatches) / float64(d.Queries)
}

func addColumnVectorGraphResultDelta1981(dst *columnVectorGraphResultDelta1981, exact, candidate []columnVectorGraphNativeSearchResult, topK int) {
	if dst == nil {
		return
	}
	dst.Queries++
	if topK > dst.TopK {
		dst.TopK = topK
	}
	limit := topK
	if limit > len(exact) {
		limit = len(exact)
	}
	if len(candidate) < limit {
		dst.CandidateResultShorter += limit - len(candidate)
		limit = len(candidate)
	}
	exactScores := make(map[int]float64, len(exact))
	candidateScores := make(map[int]float64, len(candidate))
	for _, result := range exact {
		exactScores[result.Ordinal] = result.Score
	}
	for _, result := range candidate {
		candidateScores[result.Ordinal] = result.Score
	}
	for i := 0; i < limit; i++ {
		exactOrdinal := exact[i].Ordinal
		candidateOrdinal := candidate[i].Ordinal
		if exactOrdinal != candidateOrdinal {
			dst.OrderingMismatches++
		}
		if candidateScore, ok := candidateScores[exactOrdinal]; ok {
			dst.TopKOverlap++
			if delta := math.Abs(candidateScore - exact[i].Score); delta > dst.MaxScoreAbsDelta {
				dst.MaxScoreAbsDelta = delta
			}
		}
	}
	for ordinal := range exactScores {
		if _, ok := candidateScores[ordinal]; !ok {
			dst.ExactOnly++
		}
	}
	for ordinal := range candidateScores {
		if _, ok := exactScores[ordinal]; !ok {
			dst.CandidateOnly++
		}
	}
}

func reportColumnVectorGraphResultDeltaMetrics1981(b *testing.B, delta columnVectorGraphResultDelta1981) {
	b.Helper()
	b.ReportMetric(1981, "wavefront_issue")
	b.ReportMetric(float64(delta.Queries), "result_delta_queries")
	b.ReportMetric(float64(delta.TopK), "result_delta_top_k")
	b.ReportMetric(delta.recallPct(), "result_delta_recall_pct")
	b.ReportMetric(float64(delta.TopKOverlap), "result_delta_top_k_overlap")
	b.ReportMetric(delta.orderingMismatchesPerQuery(), "result_delta_ordering_mismatches/query")
	b.ReportMetric(delta.MaxScoreAbsDelta, "result_delta_max_score_abs_delta")
	b.ReportMetric(float64(delta.ExactOnly), "result_delta_exact_only")
	b.ReportMetric(float64(delta.CandidateOnly), "result_delta_candidate_only")
	b.ReportMetric(float64(delta.CandidateResultShorter), "result_delta_candidate_shortfall")
}

func TestColumnVectorGraphResultDeltaHarness1981(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("result-delta harness requires mmap_direct prepared typed-column views")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)
	closeFn, reader, _ := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	opts := columnVectorGraphSearchTopologyParityOptions2091(shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091)
	opts.ScoreBatchMode = columnVectorGraphScoreBatchModeIndexed
	queryOrdinals := []int{0, shape.queryOrdinal, shape.rows / 2, shape.rows - 1}
	var delta columnVectorGraphResultDelta1981
	for _, ordinal := range queryOrdinals {
		query := rows[ordinal].Vector
		var exactScratch columnVectorGraphNativeSearchScratch
		exact, exactStats, err := reader.SearchCosine(query, opts, &exactScratch)
		if err != nil {
			t.Fatalf("exact SearchCosine query ordinal=%d: %v", ordinal, err)
		}
		assertColumnVectorGraphSearchTopologyParityCurrentStats2091(t, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, exactStats)
		addColumnVectorGraphResultDelta1981(&delta, exact, exact, shape.topK)
	}
	if delta.Queries != len(queryOrdinals) || delta.TopKOverlap != len(queryOrdinals)*shape.topK || delta.recallPct() != 100 || delta.OrderingMismatches != 0 || delta.MaxScoreAbsDelta != 0 || delta.ExactOnly != 0 || delta.CandidateOnly != 0 || delta.CandidateResultShorter != 0 {
		t.Fatalf("exact self-delta=%+v want perfect fixed-query oracle", delta)
	}
}

func TestColumnVectorGraphWavefrontOptions1981(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("wavefront options require mmap_direct prepared typed-column views")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	exactOpts := columnVectorGraphSearchTopologyParityOptions2091(shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091)
	exactOpts.ScoreBatchMode = columnVectorGraphScoreBatchModeIndexed
	exactOpts.StatsMode = columnVectorGraphNativeSearchStatsModeBenchmarkDebug

	var defaultScratch columnVectorGraphNativeSearchScratch
	defaultResults, defaultStats, err := reader.SearchCosine(query, exactOpts, &defaultScratch)
	if err != nil {
		t.Fatalf("default SearchCosine: %v", err)
	}
	if defaultStats.WavefrontSearches != 0 || defaultStats.WavefrontWidth != 0 {
		t.Fatalf("default stats=%+v unexpectedly enabled wavefront", defaultStats)
	}

	explicitExactOpts := exactOpts
	explicitExactOpts.TraversalMode = columnVectorGraphNativeSearchTraversalModeExact
	var explicitScratch columnVectorGraphNativeSearchScratch
	explicitResults, explicitStats, err := reader.SearchCosine(query, explicitExactOpts, &explicitScratch)
	if err != nil {
		t.Fatalf("explicit exact SearchCosine: %v", err)
	}
	if mismatch := columnGraphNativeSearchResultsMismatchV3(explicitResults, defaultResults); mismatch != "" {
		t.Fatalf("explicit exact mismatch: %s", mismatch)
	}
	if explicitStats.WavefrontSearches != 0 || explicitStats.WavefrontWidth != 0 {
		t.Fatalf("explicit exact stats=%+v unexpectedly enabled wavefront", explicitStats)
	}

	var invalidScratch columnVectorGraphNativeSearchScratch
	_, _, err = reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch, TraversalMode: columnVectorGraphNativeSearchTraversalModeWavefront}, &invalidScratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchWavefrontWidthInvalid) {
		t.Fatalf("wavefront missing width err=%v want width invalid", err)
	}
	_, _, err = reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch, WavefrontWidth: 4}, &invalidScratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchWavefrontWidthWithoutMode) {
		t.Fatalf("wavefront width without mode err=%v want fail-closed", err)
	}
	_, _, err = reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch, TraversalMode: columnVectorGraphNativeSearchTraversalMode(99), WavefrontWidth: 4}, &invalidScratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchTraversalModeInvalid) {
		t.Fatalf("invalid traversal mode err=%v want mode invalid", err)
	}
	_, _, err = reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch, TraversalMode: columnVectorGraphNativeSearchTraversalModeWavefront, WavefrontWidth: shape.efSearch + 1}, &invalidScratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchWavefrontWidthInvalid) {
		t.Fatalf("wavefront width above ef_search err=%v want width invalid", err)
	}
	_, _, err = reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 0, EfSearch: shape.efSearch, TraversalMode: columnVectorGraphNativeSearchTraversalModeWavefront}, nil)
	if !errors.Is(err, errColumnVectorGraphNativeSearchWavefrontWidthInvalid) {
		t.Fatalf("zero top_k invalid wavefront err=%v want fail-closed width invalid", err)
	}

	wavefrontOpts := exactOpts
	wavefrontOpts.TraversalMode = columnVectorGraphNativeSearchTraversalModeWavefront
	wavefrontOpts.WavefrontWidth = 4
	var wavefrontScratch columnVectorGraphNativeSearchScratch
	wavefrontResults, wavefrontStats, err := reader.SearchCosine(query, wavefrontOpts, &wavefrontScratch)
	if err != nil {
		t.Fatalf("wavefront SearchCosine: %v", err)
	}
	if len(wavefrontResults) == 0 {
		t.Fatal("wavefront SearchCosine returned no results")
	}
	assertColumnVectorGraphSearchTopologyParityCurrentStats2091(t, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, wavefrontStats)
	assertColumnVectorGraphBenchmarkDebugStats1979(t, wavefrontStats, false)
	if wavefrontStats.WavefrontSearches != 1 || wavefrontStats.WavefrontWidth != 4 || wavefrontStats.WavefrontRounds == 0 || wavefrontStats.WavefrontCandidatePops == 0 || wavefrontStats.WavefrontStagedNeighbors == 0 || wavefrontStats.WavefrontMaxTileSize == 0 {
		t.Fatalf("wavefront stats=%+v want explicit wavefront accounting", wavefrontStats)
	}
}

func TestColumnVectorGraphWavefrontRetainsEfFrontierBreadth1981(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("wavefront ef-frontier regression requires mmap_direct prepared typed-column views")
	}
	shape := columnVectorGraphSearchTopologyParityShape2091{rows: 6, dims: 2, degree: 3, topK: 1, efSearch: 5, queryOrdinal: 5}
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{0.8, 0.6}, Adjacency: []uint32{1, 2, 3}},
		{ID: []byte("doc-bridge-outside-topk"), Vector: []float32{0.5, 0.8660254}, Adjacency: []uint32{5}},
		{ID: []byte("doc-decoy"), Vector: []float32{0.7, 0.71414286}},
		{ID: []byte("doc-filler"), Vector: []float32{0.4, 0.9165151}},
		{ID: []byte("doc-seed-filler"), Vector: []float32{0.3, 0.9539392}},
		{ID: []byte("doc-best"), Vector: []float32{1, 0}},
	}
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()

	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{
		TopK:           shape.topK,
		EfSearch:       shape.efSearch,
		ScoreBatchMode: columnVectorGraphScoreBatchModeIndexed,
		TraversalMode:  columnVectorGraphNativeSearchTraversalModeWavefront,
		WavefrontWidth: 3,
	}, &scratch)
	if err != nil {
		t.Fatalf("wavefront SearchCosine: %v", err)
	}
	if len(got) != 1 || got[0].Ordinal != 5 {
		t.Fatalf("wavefront results=%+v want bridge outside topK retained under efSearch to reach best ordinal 5", got)
	}
	if stats.Candidates != uint64(shape.efSearch) || stats.WavefrontCandidatePops < 2 {
		t.Fatalf("wavefront stats=%+v want efSearch-scored candidates and multiple frontier pops", stats)
	}
}

func TestColumnVectorGraphWavefrontProcessesPartialWaveBeforeSeeding1981(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("partial-wave regression requires mmap_direct prepared typed-column views")
	}
	shape := columnVectorGraphSearchTopologyParityShape2091{rows: 4, dims: 2, degree: 1, topK: 1, efSearch: 2, queryOrdinal: 0}
	rows := columnVectorGraphNativeSearchBenchAssetRowsV3(t, shape.rows, shape.dims, shape.degree)
	for i := range rows {
		rows[i].Adjacency = nil
	}
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()

	var scratch columnVectorGraphNativeSearchScratch
	_, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{
		TopK:           shape.topK,
		EfSearch:       shape.efSearch,
		ScoreBatchMode: columnVectorGraphScoreBatchModeIndexed,
		TraversalMode:  columnVectorGraphNativeSearchTraversalModeWavefront,
		WavefrontWidth: shape.efSearch,
		StatsMode:      columnVectorGraphNativeSearchStatsModeBenchmarkDebug,
	}, &scratch)
	if err != nil {
		t.Fatalf("wavefront partial wave SearchCosine: %v", err)
	}
	if stats.WavefrontRounds != 1 || stats.WavefrontCandidatePops != 1 {
		t.Fatalf("partial-wave stats=%+v want popped candidate expanded before fallback seeding", stats)
	}
}

func TestColumnVectorGraphWavefrontRequiresPrepared1981(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("wavefront prepared guard requires mmap_direct adjacency state")
	}
	shape := columnVectorGraphSearchTopologyParityTestShape2091()
	rows := columnVectorGraphSearchTopologyParityRows2091(t, shape)
	closeFn, reader, query := openColumnVectorGraphSearchTopologyParityReader2091(t, shape, rows, columnVectorGraphSearchTopologyParityModeLegacyGraphRowDirect2091)
	defer closeFn()
	var scratch columnVectorGraphNativeSearchScratch
	_, _, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{
		TopK:           shape.topK,
		EfSearch:       shape.efSearch,
		ScoreBatchMode: columnVectorGraphScoreBatchModeIndexed,
		TraversalMode:  columnVectorGraphNativeSearchTraversalModeWavefront,
		WavefrontWidth: 4,
	}, &scratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchWavefrontRequiresPrepared) {
		t.Fatalf("legacy wavefront err=%v want prepared guard", err)
	}
}

func BenchmarkColumnVectorGraphResultDeltaHarness1981(b *testing.B) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		b.Skip("result-delta harness requires mmap_direct prepared typed-column views")
	}
	shape := columnVectorGraphSearchTopologyParityProductionShape2091()
	rows := columnVectorGraphSearchTopologyParityRows2091(b, shape)
	closeFn, reader, _ := openColumnVectorGraphSearchTopologyParityReader2091(b, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	opts := columnVectorGraphSearchTopologyParityOptions2091(shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091)
	opts.ScoreBatchMode = columnVectorGraphScoreBatchModeIndexed
	opts.StatsMode = columnVectorGraphNativeSearchStatsModeBenchmarkDebug
	queryOrdinals := []int{0, shape.rows / 16, shape.rows / 4, shape.queryOrdinal, shape.rows - shape.rows/8, shape.rows - 1}
	queries := make([][]float32, 0, len(queryOrdinals))
	var delta columnVectorGraphResultDelta1981
	for _, ordinal := range queryOrdinals {
		queries = append(queries, rows[ordinal].Vector)
		var scratch columnVectorGraphNativeSearchScratch
		exact, stats, err := reader.SearchCosine(rows[ordinal].Vector, opts, &scratch)
		if err != nil {
			b.Fatalf("exact SearchCosine query ordinal=%d: %v", ordinal, err)
		}
		if len(exact) == 0 {
			b.Fatalf("exact SearchCosine query ordinal=%d returned no results", ordinal)
		}
		assertColumnVectorGraphSearchTopologyParityCurrentStats2091(b, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, stats)
		assertColumnVectorGraphBenchmarkDebugStats1979(b, stats, false)
		addColumnVectorGraphResultDelta1981(&delta, exact, exact, shape.topK)
	}
	baseReaderStats := reader.Stats()
	var scratch columnVectorGraphNativeSearchScratch
	var totals columnVectorGraphNativeSearchStats
	var checksum int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
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
	reportColumnVectorGraphSearchTopologyParityMetrics2091(b, shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, b.N, baseReaderStats, reader.Stats(), totals)
	reportColumnVectorGraphResultDeltaMetrics1981(b, delta)
	b.ReportMetric(float64(len(queries)), "query_set_size")
	b.ReportMetric(1, "wavefront_mode_exact")
}

func BenchmarkColumnVectorGraphWavefrontSearch1981(b *testing.B) {
	benchmarkColumnVectorGraphWavefrontSearchStatsMode1981(b, columnVectorGraphNativeSearchStatsModeBenchmarkDebug)
}

func BenchmarkColumnVectorGraphWavefrontSearchMinimal1981(b *testing.B) {
	benchmarkColumnVectorGraphWavefrontSearchStatsMode1981(b, columnVectorGraphNativeSearchStatsModeMinimal)
}

func benchmarkColumnVectorGraphWavefrontSearchStatsMode1981(b *testing.B, statsMode columnVectorGraphNativeSearchStatsMode) {
	b.Helper()
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		b.Skip("wavefront benchmark requires mmap_direct prepared typed-column views")
	}
	shape := columnVectorGraphSearchTopologyParityProductionShape2091()
	rows := columnVectorGraphSearchTopologyParityRows2091(b, shape)
	queryOrdinals := []int{0, shape.rows / 16, shape.rows / 4, shape.queryOrdinal, shape.rows - shape.rows/8, shape.rows - 1}
	queries := make([][]float32, 0, len(queryOrdinals))
	for _, ordinal := range queryOrdinals {
		queries = append(queries, rows[ordinal].Vector)
	}
	type wavefrontCase struct {
		name  string
		mode  columnVectorGraphNativeSearchTraversalMode
		width int
	}
	cases := []wavefrontCase{
		{name: "mode=exact", mode: columnVectorGraphNativeSearchTraversalModeExact},
		{name: "mode=wavefront/width=2", mode: columnVectorGraphNativeSearchTraversalModeWavefront, width: 2},
		{name: "mode=wavefront/width=4", mode: columnVectorGraphNativeSearchTraversalModeWavefront, width: 4},
		{name: "mode=wavefront/width=8", mode: columnVectorGraphNativeSearchTraversalModeWavefront, width: 8},
		{name: "mode=wavefront/width=16", mode: columnVectorGraphNativeSearchTraversalModeWavefront, width: 16},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkColumnVectorGraphWavefrontSearch1981(b, shape, rows, queries, tc.mode, tc.width, statsMode)
		})
	}
}

func benchmarkColumnVectorGraphWavefrontSearch1981(b *testing.B, shape columnVectorGraphSearchTopologyParityShape2091, rows []columnVectorGraphAssetRow, queries [][]float32, mode columnVectorGraphNativeSearchTraversalMode, width int, statsMode columnVectorGraphNativeSearchStatsMode) {
	b.Helper()
	closeFn, reader, _ := openColumnVectorGraphSearchTopologyParityReader2091(b, shape, rows, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091)
	defer closeFn()
	exactOpts := columnVectorGraphSearchTopologyParityOptions2091(shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091)
	exactOpts.ScoreBatchMode = columnVectorGraphScoreBatchModeIndexed
	exactOpts.StatsMode = statsMode
	candidateOpts := exactOpts
	candidateOpts.TraversalMode = mode
	candidateOpts.WavefrontWidth = width
	var delta columnVectorGraphResultDelta1981
	for i, query := range queries {
		var exactScratch columnVectorGraphNativeSearchScratch
		exact, exactStats, err := reader.SearchCosine(query, exactOpts, &exactScratch)
		if err != nil {
			b.Fatalf("exact SearchCosine query=%d: %v", i, err)
		}
		if len(exact) == 0 {
			b.Fatalf("exact SearchCosine query=%d returned no results", i)
		}
		assertColumnVectorGraphSearchTopologyParityCurrentStats2091(b, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, exactStats)
		if statsMode == columnVectorGraphNativeSearchStatsModeBenchmarkDebug {
			assertColumnVectorGraphBenchmarkDebugStats1979(b, exactStats, false)
		}
		var candidateScratch columnVectorGraphNativeSearchScratch
		candidate, candidateStats, err := reader.SearchCosine(query, candidateOpts, &candidateScratch)
		if err != nil {
			b.Fatalf("candidate SearchCosine query=%d mode=%s width=%d: %v", i, mode.String(), width, err)
		}
		if len(candidate) == 0 {
			b.Fatalf("candidate SearchCosine query=%d returned no results", i)
		}
		assertColumnVectorGraphSearchTopologyParityCurrentStats2091(b, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, candidateStats)
		if statsMode == columnVectorGraphNativeSearchStatsModeBenchmarkDebug {
			assertColumnVectorGraphBenchmarkDebugStats1979(b, candidateStats, false)
		}
		addColumnVectorGraphResultDelta1981(&delta, exact, candidate, shape.topK)
	}
	baseReaderStats := reader.Stats()
	var scratch columnVectorGraphNativeSearchScratch
	var totals columnVectorGraphNativeSearchStats
	var checksum int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		got, stats, err := reader.SearchCosine(query, candidateOpts, &scratch)
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
	reportColumnVectorGraphSearchTopologyParityMetrics2091(b, shape, columnVectorGraphSearchTopologyParityBoundaryGraphOnly2091, columnVectorGraphSearchTopologyParityModeCurrentPrepared2091, b.N, baseReaderStats, reader.Stats(), totals)
	reportColumnVectorGraphResultDeltaMetrics1981(b, delta)
	b.ReportMetric(float64(len(queries)), "query_set_size")
	if statsMode == columnVectorGraphNativeSearchStatsModeMinimal {
		b.ReportMetric(1, "wavefront_stats_mode_minimal")
	} else {
		b.ReportMetric(1, "wavefront_stats_mode_benchmark_debug")
	}
	if mode == columnVectorGraphNativeSearchTraversalModeWavefront {
		b.ReportMetric(1, "wavefront_mode_relaxed")
		b.ReportMetric(float64(width), "wavefront_config_width")
	} else {
		b.ReportMetric(1, "wavefront_mode_exact")
	}
}
