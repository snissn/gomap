package collections

import (
	"math"
	"testing"
)

func TestHybridAdaptiveCandidateBudgetParity2875(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-a", title: "refund", body: "refund policy", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-b", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0.99, 0.01, 0}},
		{id: "doc-c", title: "refund", body: "shipping", city: "sfo", score: 30, vector: []float32{0.2, 0.8, 0}},
		{id: "doc-d", title: "shipping", body: "refund", city: "sfo", score: 40, vector: []float32{0.1, 0.9, 0}},
		{id: "doc-e", title: "shipping", body: "shipping", city: "sea", score: 50, vector: []float32{0, 0, 1}},
	})
	defer func() { _ = d.Close() }()

	baseText := &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4}
	baseVector := &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact}
	tests := []struct {
		name             string
		opts             HybridSearchOptions
		wantTextBudget   uint64
		wantVectorBudget uint64
		wantStop         HybridCandidateBudgetStopReason
	}{
		{
			name:           "text_only",
			opts:           HybridSearchOptions{TopK: 2, Text: baseText},
			wantTextBudget: 2,
			wantStop:       HybridCandidateBudgetStopReasonSingleSourceTopK,
		},
		{
			name:             "vector_only",
			opts:             HybridSearchOptions{TopK: 2, Vector: baseVector},
			wantVectorBudget: 2,
			wantStop:         HybridCandidateBudgetStopReasonSingleSourceTopK,
		},
		{
			name:             "hybrid",
			opts:             HybridSearchOptions{TopK: 2, Text: baseText, Vector: baseVector},
			wantTextBudget:   2,
			wantVectorBudget: 2,
			wantStop:         HybridCandidateBudgetStopReasonExactRRFBound,
		},
		{
			name:             "hybrid_scalar",
			opts:             HybridSearchOptions{TopK: 2, Text: baseText, Vector: baseVector, ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"}},
			wantTextBudget:   2,
			wantVectorBudget: 2,
			wantStop:         HybridCandidateBudgetStopReasonExactRRFBound,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixed, err := col.searchHybridWithCandidateBudgetPolicy(tc.opts, hybridCandidateBudgetPolicyFixed)
			if err != nil {
				t.Fatalf("fixed SearchHybrid: %v", err)
			}
			adaptive, err := col.searchHybridWithCandidateBudgetPolicy(tc.opts, hybridCandidateBudgetPolicyAdaptive)
			if err != nil {
				t.Fatalf("adaptive SearchHybrid: %v", err)
			}
			assertHybridResponsesEqual2875(t, adaptive, fixed)
			assertHybridNoDocumentGuardrails2875(t, adaptive.Stats)
			if adaptive.Stats.CandidateBudgetPolicy != HybridCandidateBudgetPolicyAdaptiveRRF || adaptive.Stats.CandidateBudgetStopReason != tc.wantStop || adaptive.Stats.CandidateBudgetFallbacks != 0 {
				t.Fatalf("adaptive stats=%+v want adaptive stop=%s without fallback", adaptive.Stats, tc.wantStop)
			}
			if tc.opts.Text != nil {
				if adaptive.Stats.TextCandidatesRequested != uint64(tc.opts.Text.CandidateLimit) || adaptive.Stats.TextCandidateBudgetEffective != tc.wantTextBudget {
					t.Fatalf("text budgets stats=%+v want requested=%d effective=%d", adaptive.Stats, tc.opts.Text.CandidateLimit, tc.wantTextBudget)
				}
			}
			if tc.opts.Vector != nil {
				if adaptive.Stats.VectorCandidatesRequested != uint64(tc.opts.Vector.CandidateLimit) || adaptive.Stats.VectorCandidateBudgetEffective != tc.wantVectorBudget {
					t.Fatalf("vector budgets stats=%+v want requested=%d effective=%d", adaptive.Stats, tc.opts.Vector.CandidateLimit, tc.wantVectorBudget)
				}
			}
		})
	}
}

func TestHybridDefaultCandidateBudgetKeepsFixedHybridWhenBoundsNeedProbing2875(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-a", title: "refund", body: "refund policy", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-b", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0.99, 0.01, 0}},
		{id: "doc-c", title: "refund", body: "shipping", city: "sfo", score: 30, vector: []float32{0.2, 0.8, 0}},
		{id: "doc-d", title: "shipping", body: "refund", city: "sfo", score: 40, vector: []float32{0.1, 0.9, 0}},
	})
	defer func() { _ = d.Close() }()

	opts := HybridSearchOptions{
		TopK:   2,
		Text:   &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector: &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact},
	}
	fixed, err := col.searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyFixed)
	if err != nil {
		t.Fatalf("fixed SearchHybrid: %v", err)
	}
	got, err := col.SearchHybrid(opts)
	if err != nil {
		t.Fatalf("default SearchHybrid: %v", err)
	}
	assertHybridResponsesEqual2875(t, got, fixed)
	if got.Stats.CandidateBudgetPolicy != HybridCandidateBudgetPolicyFixed || got.Stats.CandidateBudgetFallbackReason != HybridCandidateBudgetStopReasonExactBoundInsufficient || got.Stats.CandidateBudgetIterations != 1 || got.Stats.TextCandidateBudgetEffective != 4 || got.Stats.VectorCandidateBudgetEffective != 4 {
		t.Fatalf("default stats=%+v want fixed fallback without probing overhead", got.Stats)
	}
}

func TestHybridAdaptiveVectorBudgetPreservesScalarAllowSetRankSemantics2875(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-global", title: "refund", body: "refund", city: "sfo", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-allowed-a", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0.99, 0.01, 0}},
		{id: "doc-allowed-b", title: "refund", body: "refund", city: "sea", score: 30, vector: []float32{0.98, 0.02, 0}},
	})
	defer func() { _ = d.Close() }()

	opts := HybridSearchOptions{
		TopK:         1,
		Vector:       &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 2, EfSearch: 8, QueryMode: VectorIndexQueryModeExact},
		ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"},
	}
	fixed, err := col.searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyFixed)
	if err != nil {
		t.Fatalf("fixed SearchHybrid: %v", err)
	}
	adaptive, err := col.searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyAdaptive)
	if err != nil {
		t.Fatalf("adaptive SearchHybrid: %v", err)
	}
	assertHybridResponsesEqual2875(t, adaptive, fixed)
	assertHybridNoDocumentGuardrails2875(t, adaptive.Stats)
	if got := hybridResultIDs2505(adaptive.Results); len(got) != 1 || got[0] != "doc-allowed-a" {
		t.Fatalf("adaptive ids=%v want top allowed vector hit", got)
	}
	if adaptive.Results[0].Sources[0].SourceRank != 1 {
		t.Fatalf("adaptive result=%+v want scalar allow-set vector rank semantics", adaptive.Results[0])
	}
	if adaptive.Stats.CandidateBudgetPolicy != HybridCandidateBudgetPolicyAdaptiveRRF || adaptive.Stats.CandidateBudgetStopReason != HybridCandidateBudgetStopReasonExactRRFBound || adaptive.Stats.CandidateBudgetFallbacks != 0 || adaptive.Stats.CandidateBudgetIterations != 1 {
		t.Fatalf("adaptive stats=%+v want exact scalar allow-set proof without fallback", adaptive.Stats)
	}
	if adaptive.Stats.VectorCandidatesRequested != 2 || adaptive.Stats.VectorCandidateBudgetEffective != 1 || adaptive.Stats.VectorCandidatesReturned != 1 {
		t.Fatalf("adaptive stats=%+v want requested/effective/returned vector budgets 2/1/1", adaptive.Stats)
	}
}

func TestHybridAdaptiveCandidateBudgetFallbacks2875(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-a", title: "refund", body: "refund policy", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-b", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0.99, 0.01, 0}},
		{id: "doc-c", title: "refund", body: "shipping", city: "sfo", score: 30, vector: []float32{0.2, 0.8, 0}},
		{id: "doc-d", title: "shipping", body: "refund", city: "sfo", score: 40, vector: []float32{0.1, 0.9, 0}},
	})
	defer func() { _ = d.Close() }()

	normalized := HybridSearchOptions{
		TopK:   2,
		Text:   &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector: &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact},
		Fusion: HybridFusionOptions{Method: HybridFusionMethodNormalizedScore},
	}
	fixed, err := col.searchHybridWithCandidateBudgetPolicy(normalized, hybridCandidateBudgetPolicyFixed)
	if err != nil {
		t.Fatalf("fixed normalized SearchHybrid: %v", err)
	}
	adaptive, err := col.SearchHybrid(normalized)
	if err != nil {
		t.Fatalf("adaptive normalized SearchHybrid: %v", err)
	}
	assertHybridResponsesEqual2875(t, adaptive, fixed)
	if adaptive.Stats.CandidateBudgetPolicy != HybridCandidateBudgetPolicyFixed || adaptive.Stats.CandidateBudgetFallbackReason != HybridCandidateBudgetStopReasonUnsupportedFusion || adaptive.Stats.TextCandidateBudgetEffective != 4 || adaptive.Stats.VectorCandidateBudgetEffective != 4 {
		t.Fatalf("normalized stats=%+v want fixed unsupported-fusion fallback at requested budgets", adaptive.Stats)
	}
}

func TestHybridAdaptiveCandidateBudgetTieFallbackDeterminism2875(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-text", title: "refund", body: "refund", city: "sea", score: 10, vector: []float32{0, 1, 0}},
		{id: "doc-vector-1", title: "shipping", body: "shipping", city: "sea", score: 20, vector: []float32{1, 0, 0}},
		{id: "doc-vector-2", title: "shipping", body: "shipping", city: "sea", score: 30, vector: []float32{0.99, 0.01, 0}},
		{id: "doc-vector-3", title: "shipping", body: "shipping", city: "sea", score: 40, vector: []float32{0.98, 0.02, 0}},
		{id: "doc-vector-4", title: "shipping", body: "shipping", city: "sea", score: 50, vector: []float32{0.97, 0.03, 0}},
	})
	defer func() { _ = d.Close() }()

	opts := HybridSearchOptions{
		TopK:   1,
		Text:   &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector: &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 8, QueryMode: VectorIndexQueryModeExact},
	}
	fixed, err := col.searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyFixed)
	if err != nil {
		t.Fatalf("fixed SearchHybrid: %v", err)
	}
	if got := hybridResultIDs2505(fixed.Results); len(got) != 1 || got[0] != "doc-text" {
		t.Fatalf("fixed tie ids=%v want text source tie winner", got)
	}
	for i := 0; i < 5; i++ {
		adaptive, err := col.searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyAdaptive)
		if err != nil {
			t.Fatalf("adaptive SearchHybrid run %d: %v", i, err)
		}
		assertHybridResponsesEqual2875(t, adaptive, fixed)
		assertHybridNoDocumentGuardrails2875(t, adaptive.Stats)
		if adaptive.Stats.CandidateBudgetPolicy != HybridCandidateBudgetPolicyFixed || adaptive.Stats.CandidateBudgetFallbackReason != HybridCandidateBudgetStopReasonExactBoundInsufficient || adaptive.Stats.CandidateBudgetIterations <= 1 {
			t.Fatalf("adaptive stats=%+v want deterministic fixed fallback after insufficient exact bound", adaptive.Stats)
		}
	}
}

func TestHybridAdaptiveTextBudgetPreservesRequestedScanGuardrail2875(t *testing.T) {
	fixture := openIndexInsertSearchInsertedTextV2Fixture2564(t, 20000, 16, 8)
	defer func() { _ = fixture.db.Close() }()

	opts := HybridSearchOptions{
		TopK:       10,
		Text:       &HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 200},
		ResultMode: HybridResultModeScoreOnly,
	}
	fixed, err := fixture.col.searchHybridWithCandidateBudgetPolicy(opts, hybridCandidateBudgetPolicyFixed)
	if err != nil {
		t.Fatalf("fixed SearchHybrid: %v", err)
	}
	adaptive, err := fixture.col.SearchHybrid(opts)
	if err != nil {
		t.Fatalf("adaptive SearchHybrid: %v", err)
	}
	assertHybridResponsesEqual2875(t, adaptive, fixed)
	assertHybridNoDocumentGuardrails2875(t, adaptive.Stats)
	if adaptive.Stats.CandidateBudgetPolicy != HybridCandidateBudgetPolicyAdaptiveRRF || adaptive.Stats.CandidateBudgetStopReason != HybridCandidateBudgetStopReasonSingleSourceTopK || adaptive.Stats.CandidateBudgetFallbacks != 0 {
		t.Fatalf("adaptive stats=%+v want exact single-source adaptive without fallback", adaptive.Stats)
	}
	if adaptive.Stats.TextCandidatesRequested != 200 || adaptive.Stats.TextCandidateBudgetEffective != 10 || adaptive.Stats.TextCandidatesReturned != 10 {
		t.Fatalf("adaptive stats=%+v want requested/effective/returned text budgets 200/10/10", adaptive.Stats)
	}
	if adaptive.Stats.TextCandidatesScored <= uint64(hybridTextCandidateDefaultScanCandidateLimit) {
		t.Fatalf("adaptive stats=%+v want requested text scan guardrail to allow scoring past the default minimum", adaptive.Stats)
	}
}

func assertHybridResponsesEqual2875(tb testing.TB, got, want HybridSearchResponse) {
	tb.Helper()
	if len(got.Results) != len(want.Results) {
		tb.Fatalf("results len got=%d want=%d got=%+v want=%+v", len(got.Results), len(want.Results), got.Results, want.Results)
	}
	for i := range want.Results {
		g, w := got.Results[i], want.Results[i]
		if string(g.ID) != string(w.ID) || g.Rank != w.Rank || math.Abs(g.FusedScore-w.FusedScore) > 1e-12 || len(g.Sources) != len(w.Sources) {
			tb.Fatalf("result[%d] got=%+v want=%+v", i, g, w)
		}
		for j := range w.Sources {
			gs, ws := g.Sources[j], w.Sources[j]
			if gs.Source != ws.Source || gs.IndexName != ws.IndexName || gs.SourceRank != ws.SourceRank || gs.ScoreKind != ws.ScoreKind || math.Abs(gs.Score-ws.Score) > 1e-12 || math.Abs(gs.FusionScore-ws.FusionScore) > 1e-12 {
				tb.Fatalf("result[%d] source[%d] got=%+v want=%+v", i, j, gs, ws)
			}
		}
	}
}

func assertHybridNoDocumentGuardrails2875(tb testing.TB, stats HybridSearchStats) {
	tb.Helper()
	if stats.DocumentsFetched != 0 || stats.DocumentsMissing != 0 || stats.FullDocumentScanFallbacks != 0 || stats.FailClosed != 0 {
		tb.Fatalf("stats=%+v want no document fetch/fallback/fail-closed", stats)
	}
}
