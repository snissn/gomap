package collections

import (
	"math"
	"testing"
)

type columnVectorGraphResultDelta1981 struct {
	Queries                int
	TopK                   int
	TopKOverlap            int
	OrderingMismatches     int
	ScoreOverlapCount      int
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
			dst.ScoreOverlapCount++
			if delta := math.Abs(candidateScore - exact[i].Score); delta > dst.MaxScoreAbsDelta {
				dst.MaxScoreAbsDelta = delta
			}
		}
		_ = candidateOrdinal
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
