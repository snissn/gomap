package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestSearchHybridVectorCandidatesNoDocumentsStableIDs2503(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0.3, 0.7, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	query := []float32{1, 0, 0}
	opts := HybridVectorQuery{IndexName: def.Name, Query: query, CandidateLimit: 3, EfSearch: len(rows), QueryMode: VectorIndexQueryModeExact}
	got, err := col.SearchHybridVectorCandidates(opts)
	if err != nil {
		t.Fatalf("SearchHybridVectorCandidates: %v", err)
	}

	want := exactColumnGraphTopKForTest(t, rows, query, opts.CandidateLimit)
	assertHybridVectorCandidates2503(t, got.Candidates, want, def.Name)
	if got.Stats.VectorCandidatesRequested != uint64(opts.CandidateLimit) || got.Stats.VectorCandidatesReturned != uint64(len(want)) {
		t.Fatalf("stats=%+v want requested=%d returned=%d", got.Stats, opts.CandidateLimit, len(want))
	}
	if got.Stats.VectorCandidatesExamined == 0 || got.Stats.VectorEdgesVisited == 0 {
		t.Fatalf("stats=%+v want non-zero vector graph candidate/edge counters", got.Stats)
	}
	if got.Stats.DocumentsFetched != 0 || got.Stats.DocumentsMissing != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want no document fetch/fallback/fail-closed counters", got.Stats)
	}
	if got.Stats.Truncated == 0 {
		t.Fatalf("stats=%+v want truncation because candidate limit is below row count", got.Stats)
	}
	firstID := append([]byte(nil), got.Candidates[0].ID...)

	again, err := col.SearchHybridVectorCandidates(HybridVectorQuery{IndexName: def.Name, Query: []float32{0, 1, 0}, CandidateLimit: 2, EfSearch: len(rows)})
	if err != nil {
		t.Fatalf("second SearchHybridVectorCandidates: %v", err)
	}
	if !bytes.Equal(got.Candidates[0].ID, firstID) {
		t.Fatalf("first response ID changed after later search: got %q want stable %q", got.Candidates[0].ID, firstID)
	}
	if len(again.Candidates) != 2 || again.Candidates[0].SourceRank != 1 || again.Candidates[1].SourceRank != 2 {
		t.Fatalf("second candidates=%+v want bounded one-based ranks", again.Candidates)
	}
}

func TestHybridVectorAllowSetExactScanBudget2729(t *testing.T) {
	allowSet := hybridScalarAllowSet{"doc-a": {}}
	query := HybridVectorQuery{CandidateLimit: 1, Query: []float32{1, 0, 0}}
	if !hybridVectorCandidateUseExactAllowSet(query, allowSet) {
		t.Fatalf("expected small allow-set to be eligible before row-count budget")
	}
	if !hybridVectorAllowSetExactScanEligible(hybridVectorScalarPrefilterExactMaxScanRows) {
		t.Fatalf("row count at scan budget should be eligible")
	}
	if hybridVectorAllowSetExactScanEligible(hybridVectorScalarPrefilterExactMaxScanRows + 1) {
		t.Fatalf("row count above scan budget should not use exact allow-set scan")
	}
}

func TestSearchHybridVectorCandidatesUnsupportedShapesFailClosed2503(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	base := HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 2, EfSearch: 2}
	tests := []struct {
		name   string
		mutate func(*HybridVectorQuery)
	}{
		{name: "invalid_query_mode", mutate: func(q *HybridVectorQuery) { q.QueryMode = VectorIndexQueryMode("future_mode") }},
		{name: "zero_exact_with_quantized_index", mutate: func(q *HybridVectorQuery) { q.QuantizedIndexName = "embedding.scalar_u8.fast" }},
		{name: "explicit_exact_with_quantized_index", mutate: func(q *HybridVectorQuery) {
			q.QueryMode = VectorIndexQueryModeExact
			q.QuantizedIndexName = "embedding.scalar_u8.fast"
		}},
		{name: "exact_with_quantized_rerank_candidates", mutate: func(q *HybridVectorQuery) {
			q.QueryMode = VectorIndexQueryModeExact
			q.QuantizedRerankCandidates = 8
		}},
		{name: "quantized_only_missing_index_name", mutate: func(q *HybridVectorQuery) { q.QueryMode = VectorIndexQueryModeQuantizedOnly }},
		{name: "valid_looking_quantized_only", mutate: func(q *HybridVectorQuery) {
			q.QueryMode = VectorIndexQueryModeQuantizedOnly
			q.QuantizedIndexName = "embedding.scalar_u8.fast"
		}},
		{name: "quantized_only_with_rerank_candidates", mutate: func(q *HybridVectorQuery) {
			q.QueryMode = VectorIndexQueryModeQuantizedOnly
			q.QuantizedIndexName = "embedding.scalar_u8.fast"
			q.QuantizedRerankCandidates = 8
		}},
		{name: "quantized_rerank_missing_index_name", mutate: func(q *HybridVectorQuery) { q.QueryMode = VectorIndexQueryModeQuantizedRerank }},
		{name: "quantized_rerank_negative_candidates", mutate: func(q *HybridVectorQuery) {
			q.QueryMode = VectorIndexQueryModeQuantizedRerank
			q.QuantizedIndexName = "embedding.scalar_u8.fast"
			q.QuantizedRerankCandidates = -1
		}},
		{name: "quantized_rerank_candidates_below_limit", mutate: func(q *HybridVectorQuery) {
			q.QueryMode = VectorIndexQueryModeQuantizedRerank
			q.QuantizedIndexName = "embedding.scalar_u8.fast"
			q.QuantizedRerankCandidates = 1
		}},
		{name: "valid_looking_quantized_rerank", mutate: func(q *HybridVectorQuery) {
			q.QueryMode = VectorIndexQueryModeQuantizedRerank
			q.QuantizedIndexName = "embedding.scalar_u8.fast"
			q.QuantizedRerankCandidates = q.CandidateLimit
		}},
		{name: "negative_ef_search", mutate: func(q *HybridVectorQuery) { q.EfSearch = -1 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := base
			tc.mutate(&opts)
			got, err := col.SearchHybridVectorCandidates(opts)
			if !errors.Is(err, ErrHybridSearchUnsupported) {
				t.Fatalf("SearchHybridVectorCandidates err=%v want ErrHybridSearchUnsupported", err)
			}
			if errors.Is(err, ErrHybridSearchIndexUnavailable) {
				t.Fatalf("SearchHybridVectorCandidates err=%v must not wrap ErrHybridSearchIndexUnavailable for unsupported shape", err)
			}
			if len(got.Candidates) != 0 || got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != HybridFailClosedReasonUnsupported {
				t.Fatalf("response=%+v want unsupported fail-closed stats and no candidates", got)
			}
			if got.Stats.DocumentsFetched != 0 || got.Stats.DocumentsMissing != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("stats=%+v want no document fetch or fallback on unsupported vector candidate shape", got.Stats)
			}
		})
	}
}

func TestNativeScalarHybridVectorValidationPreservesFailureStats(t *testing.T) {
	query := HybridVectorQuery{
		IndexName:      "embedding_native",
		Query:          []float32{1, 0},
		CandidateLimit: 3,
		EfSearch:       -1,
	}
	got, err := (&Collection{}).searchHybridVectorCandidatesNativeScalar(query, nil)
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("searchHybridVectorCandidatesNativeScalar err=%v want ErrHybridSearchUnsupported", err)
	}
	if len(got.Candidates) != 0 ||
		got.Stats.VectorCandidatesRequested != uint64(query.CandidateLimit) ||
		got.Stats.VectorCandidateBudgetEffective != uint64(query.CandidateLimit) ||
		got.Stats.FailClosed != 1 ||
		got.Stats.FailClosedReason != HybridFailClosedReasonUnsupported {
		t.Fatalf("response=%+v want requested budget and unsupported fail-closed diagnostics", got)
	}
}

func TestSearchHybridVectorCandidatesMissingIndexFailsClosed2503(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	got, err := col.SearchHybridVectorCandidates(HybridVectorQuery{IndexName: "missing_embedding_graph", Query: []float32{1, 0, 0}, CandidateLimit: 1, EfSearch: 1})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) {
		t.Fatalf("SearchHybridVectorCandidates err=%v want ErrHybridSearchIndexUnavailable", err)
	}
	if len(got.Candidates) != 0 || got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != HybridFailClosedReasonVectorIndexUnavailable {
		t.Fatalf("response=%+v want fail-closed vector-index-unavailable stats and no candidates", got)
	}
	if got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("stats=%+v want no document fetch or full scan fallback on unavailable vector index", got.Stats)
	}
}

func TestHybridVectorCandidatesRejectDocumentMaterialization2503(t *testing.T) {
	got, err := hybridVectorCandidatesFromSearchResponse(1, "embedding_graph", VectorIndexSearchResponse{
		IndexName: "embedding_graph",
		Stats: VectorIndexSearchStats{
			CandidateRows:      1,
			DocumentsFetched:   1,
			DocumentBytes:      14,
			DocumentFetchNanos: 7,
		},
		Results: []VectorIndexSearchResult{{ID: []byte("doc-a"), Score: 1, Document: []byte(`{"did":"doc-a"}`)}},
	})
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("hybridVectorCandidatesFromSearchResponse err=%v want ErrHybridSearchUnsupported", err)
	}
	if len(got.Candidates) != 0 || got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != HybridFailClosedReasonFullDocumentScanForbidden {
		t.Fatalf("response=%+v want fail-closed document materialization guard", got)
	}
	if got.Stats.DocumentsFetched != 1 || got.Stats.VectorCandidatesReturned != 0 {
		t.Fatalf("stats=%+v want document-fetch counter preserved and no returned candidates", got.Stats)
	}
}

func assertHybridVectorCandidates2503(tb testing.TB, got []HybridSearchCandidate, want []columnVectorGraphNativeSearchResult, indexName string) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("candidates=%d want %d", len(got), len(want))
	}
	for i := range want {
		candidate := got[i]
		if !bytes.Equal(candidate.ID, want[i].ID) || math.Abs(candidate.Score-want[i].Score) > 1e-6 {
			tb.Fatalf("candidate[%d]=%+v want id=%q score=%v", i, candidate, want[i].ID, want[i].Score)
		}
		if candidate.Source != HybridCandidateSourceVector || candidate.IndexName != indexName || candidate.SourceRank != i+1 || candidate.ScoreKind != HybridScoreKindVectorSimilarity {
			tb.Fatalf("candidate[%d]=%+v want vector source/index/rank/score kind", i, candidate)
		}
		if len(candidate.TextMatches) != 0 {
			tb.Fatalf("candidate[%d] text matches=%+v want none for vector source", i, candidate.TextMatches)
		}
		if len(candidate.ID) == 0 || cap(candidate.ID) != len(candidate.ID) {
			tb.Fatalf("candidate[%d] id len/cap=%d/%d want response-owned cap-isolated stable ID", i, len(candidate.ID), cap(candidate.ID))
		}
	}
}

var hybridVectorCandidateBenchmarkSink2503 HybridCandidateResponse

func BenchmarkSearchHybridVectorCandidates2503(b *testing.B) {
	rows := make([]columnGraphRebuildInputRowV2A, 128)
	for i := range rows {
		rows[i] = columnGraphRebuildInputRowV2A{
			id: fmt.Sprintf("doc-%03d", i),
			vector: []float32{
				1 + float32(i%11)*0.01,
				float32((i*7)%17) * 0.01,
				float32((i*13)%19) * 0.01,
			},
		}
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, 3, 8, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0.1, 0.05}, CandidateLimit: 10, EfSearch: 64, QueryMode: VectorIndexQueryModeExact}
	warm, err := col.SearchHybridVectorCandidates(opts)
	if err != nil {
		b.Fatalf("warm SearchHybridVectorCandidates: %v", err)
	}
	if len(warm.Candidates) != opts.CandidateLimit || warm.Stats.DocumentsFetched != 0 {
		b.Fatalf("warm response=%+v want %d no-document candidates", warm, opts.CandidateLimit)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var sink HybridCandidateResponse
	for i := 0; i < b.N; i++ {
		got, err := col.SearchHybridVectorCandidates(opts)
		if err != nil {
			b.Fatalf("SearchHybridVectorCandidates: %v", err)
		}
		if len(got.Candidates) != opts.CandidateLimit {
			b.Fatalf("candidates=%d want %d", len(got.Candidates), opts.CandidateLimit)
		}
		sink = got
	}
	b.StopTimer()
	hybridVectorCandidateBenchmarkSink2503 = sink
	b.ReportMetric(float64(sink.Stats.VectorCandidatesReturned), "candidates/search")
	b.ReportMetric(float64(sink.Stats.VectorCandidatesExamined), "examined/search")
	b.ReportMetric(float64(sink.Stats.VectorEdgesVisited), "edges/search")
	b.ReportMetric(float64(sink.Stats.DocumentsFetched), "docs_fetched/search")
}
