package collections

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestHybridParentCollapsePreservesFusionOrderAndContributions4291(t *testing.T) {
	fused, stats, err := FuseHybridSearchCandidates(hybridParentCollapseCandidates4291(), HybridFusionOptions{}, 7)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates: %v", err)
	}
	wantScore := fused[0].FusedScore
	wantSources := append([]HybridSourceContribution(nil), fused[0].Sources...)

	got := hybridCollapseResultsByParent(fused, 6, 1, &stats)
	if ids := hybridResultIDs2505(got); !reflect.DeepEqual(ids, []string{"parent-a#0", "parent-b#0", "parent-a#bad", "parent-a#1#0", "parent-a#01", "plain"}) {
		t.Fatalf("cap=1 ids=%v", ids)
	}
	if got[0].FusedScore != wantScore || !reflect.DeepEqual(got[0].Sources, wantSources) {
		t.Fatalf("top result changed score/sources: got=%+v want_score=%v want_sources=%+v", got[0], wantScore, wantSources)
	}
	if stats.CollapseRejections != 1 || stats.CollapseExhaustions != 0 || stats.Truncated != 1 {
		t.Fatalf("cap=1 stats=%+v", stats)
	}
	for i := range got {
		if got[i].Rank != i+1 {
			t.Fatalf("result %d rank=%d", i, got[i].Rank)
		}
	}

	fused, stats, err = FuseHybridSearchCandidates(hybridParentCollapseCandidates4291(), HybridFusionOptions{}, 7)
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates cap=2: %v", err)
	}
	got = hybridCollapseResultsByParent(fused, 4, 2, &stats)
	if ids := hybridResultIDs2505(got); !reflect.DeepEqual(ids, []string{"parent-a#0", "parent-a#1", "parent-b#0", "parent-a#bad"}) {
		t.Fatalf("cap=2 ids=%v", ids)
	}
	if stats.CollapseRejections != 0 || stats.CollapseExhaustions != 0 || stats.Truncated != 3 {
		t.Fatalf("cap=2 stats=%+v", stats)
	}
}

func TestHybridParentCollapseReportsBoundedExhaustion4291(t *testing.T) {
	candidates := []HybridSearchCandidate{
		{ID: []byte("parent#0"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 1, Score: 3, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("parent#1"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 2, Score: 2, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("parent#2"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 3, Score: 1, ScoreKind: HybridScoreKindBM25},
	}
	fused, stats, err := FuseHybridSearchCandidates(candidates, HybridFusionOptions{}, len(candidates))
	if err != nil {
		t.Fatalf("FuseHybridSearchCandidates: %v", err)
	}
	got := hybridCollapseResultsByParent(fused, 3, 1, &stats)
	if ids := hybridResultIDs2505(got); !reflect.DeepEqual(ids, []string{"parent#0"}) {
		t.Fatalf("ids=%v", ids)
	}
	if stats.CollapseRejections != 2 || stats.CollapseExhaustions != 1 || stats.Truncated != 2 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestSearchHybridParentCollapseFilterFetchAndDisabledParity4291(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "parent-a#0", title: "refund refund", body: "refund refund", city: "sea", vector: []float32{1, 0, 0}},
		{id: "parent-a#1", title: "refund", body: "refund", city: "sea", vector: []float32{0.99, 0.01, 0}},
		{id: "parent-b#0", title: "refund", body: "refund", city: "sea", vector: []float32{0.8, 0.2, 0}},
		{id: "plain", title: "refund", body: "refund", city: "sea", vector: []float32{0.7, 0.3, 0}},
		{id: "excluded#0", title: "refund refund refund", body: "refund", city: "sfo", vector: []float32{1, 0, 0}},
	})
	defer func() { _ = d.Close() }()

	base := HybridSearchOptions{
		TopK:             3,
		Text:             &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 5},
		Vector:           &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 5, EfSearch: 5, QueryMode: VectorIndexQueryModeExact},
		ScalarFilter:     &HybridScalarFilter{IndexName: "city", Value: "sea"},
		IncludeDocuments: true,
	}
	disabled, err := col.SearchHybrid(base)
	if err != nil {
		t.Fatalf("SearchHybrid disabled: %v", err)
	}
	zero := base
	zero.MaxChunksPerParent = 0
	zeroResponse, err := col.SearchHybrid(zero)
	if err != nil {
		t.Fatalf("SearchHybrid explicit zero: %v", err)
	}
	if !reflect.DeepEqual(disabled.Results, zeroResponse.Results) || !reflect.DeepEqual(disabled.Stats, zeroResponse.Stats) {
		t.Fatalf("disabled parity changed: omitted=%+v explicit_zero=%+v", disabled, zeroResponse)
	}
	if disabled.Stats.CollapseRejections != 0 || disabled.Stats.CollapseExhaustions != 0 {
		t.Fatalf("disabled collapse stats=%+v", disabled.Stats)
	}

	enabled := base
	enabled.MaxChunksPerParent = 1
	got, err := col.SearchHybrid(enabled)
	if err != nil {
		t.Fatalf("SearchHybrid cap=1: %v", err)
	}
	if len(got.Results) != 3 || got.Plan.MaxChunksPerParent != 1 {
		t.Fatalf("results=%+v plan=%+v", got.Results, got.Plan)
	}
	if !reflect.DeepEqual(got.Snapshot, disabled.Snapshot) {
		t.Fatalf("collapse changed snapshot identity: enabled=%+v disabled=%+v", got.Snapshot, disabled.Snapshot)
	}
	parentA := 0
	for _, result := range got.Results {
		if bytes.Equal(result.ID, []byte("excluded#0")) || !bytes.Contains(result.Document, []byte(`"city":"sea"`)) {
			t.Fatalf("filtered result escaped: %+v", result)
		}
		if bytes.HasPrefix(result.ID, []byte("parent-a#")) {
			parentA++
		}
		if !hybridResultHasSource2505(result, HybridCandidateSourceText) || !hybridResultHasSource2505(result, HybridCandidateSourceVector) {
			t.Fatalf("mixed source contribution lost: %+v", result)
		}
	}
	if parentA > 1 || got.Stats.CollapseRejections == 0 || got.Stats.CollapseExhaustions != 0 {
		t.Fatalf("cap/stats mismatch parent_a=%d stats=%+v", parentA, got.Stats)
	}
	if got.Stats.DocumentsFetched != uint64(len(got.Results)) || got.Stats.DocumentsFetched > uint64(enabled.TopK) || got.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("final fetch was not bounded: %+v", got.Stats)
	}

	failed, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, MaxChunksPerParent: -1, Text: base.Text})
	if !errors.Is(err, ErrHybridSearchUnsupported) || failed.Stats.FailClosed != 1 {
		t.Fatalf("negative cap response=%+v err=%v", failed, err)
	}
}

func TestSearchHybridParentCollapseFetchesOnlyExhaustedResult4291(t *testing.T) {
	_, d, col, _ := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "parent#0", title: "refund refund refund", body: "refund", city: "sea", vector: []float32{1, 0, 0}},
		{id: "parent#1", title: "refund refund", body: "refund", city: "sea", vector: []float32{0.9, 0.1, 0}},
		{id: "parent#2", title: "refund", body: "refund", city: "sea", vector: []float32{0.8, 0.2, 0}},
	})
	defer func() { _ = d.Close() }()

	got, err := col.SearchHybrid(HybridSearchOptions{
		TopK:               3,
		MaxChunksPerParent: 1,
		Text:               &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 3},
		IncludeDocuments:   true,
	})
	if err != nil {
		t.Fatalf("SearchHybrid: %v", err)
	}
	if len(got.Results) != 1 || got.Stats.CollapseRejections != 2 || got.Stats.CollapseExhaustions != 1 {
		t.Fatalf("response=%+v", got)
	}
	if got.Stats.DocumentsFetched != 1 || got.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("fetch stats=%+v", got.Stats)
	}
}

func hybridParentCollapseCandidates4291() []HybridSearchCandidate {
	return []HybridSearchCandidate{
		{ID: []byte("parent-a#0"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 1, Score: 6, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("parent-a#1"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 2, Score: 5, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("parent-b#0"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 3, Score: 4, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("parent-a#bad"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 4, Score: 3, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("parent-a#1#0"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 5, Score: 2, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("parent-a#01"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 6, Score: 1.5, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("plain"), Source: HybridCandidateSourceText, IndexName: "lexical", SourceRank: 7, Score: 1, ScoreKind: HybridScoreKindBM25},
		{ID: []byte("parent-a#0"), Source: HybridCandidateSourceVector, IndexName: "embedding", SourceRank: 1, Score: 1, ScoreKind: HybridScoreKindVectorSimilarity},
		{ID: []byte("parent-a#1"), Source: HybridCandidateSourceVector, IndexName: "embedding", SourceRank: 2, Score: 0.9, ScoreKind: HybridScoreKindVectorSimilarity},
		{ID: []byte("parent-b#0"), Source: HybridCandidateSourceVector, IndexName: "embedding", SourceRank: 3, Score: 0.8, ScoreKind: HybridScoreKindVectorSimilarity},
	}
}
