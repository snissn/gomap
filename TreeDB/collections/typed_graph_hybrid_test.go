package collections

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestTypedGraphHybridScalarStrategiesAndBudgets(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{columns[0].Float32Vectors[0], columns[0].Float32Vectors[0]}}, {Name: "content", Strings: []string{"selected", "selected"}}, {Name: "user", Strings: []string{"selected", "selected"}}, {Name: "path", Strings: []string{"selected", "selected"}}}
	if _, err := col.ReplaceTypedBatch(ids[:2], retained[:2], changed); err != nil {
		t.Fatal(err)
	}
	filters := []*HybridScalarFilter{{IndexName: "path", Value: "selected"}, {And: []HybridScalarFilter{{IndexName: "path", Value: "selected"}, {IndexName: "user", Value: "selected"}}}}
	for _, filter := range filters {
		for _, strategy := range []HybridScalarFilterStrategy{HybridScalarFilterStrategyPrefilter, HybridScalarFilterStrategyPostfilter, HybridScalarFilterStrategyTextFirst, HybridScalarFilterStrategyVectorFirst, HybridScalarFilterStrategyUnionFusion} {
			query := HybridSearchOptions{TopK: 2, Vector: &HybridVectorQuery{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], CandidateLimit: 8, EfSearch: 16}, ScalarFilter: filter, ScalarFilterStrategy: strategy}
			for _, combined := range []bool{false, true} {
				if combined {
					query.Text = &HybridTextQuery{IndexName: "content", Query: "selected", CandidateLimit: 8}
				}
				out, err := col.SearchHybrid(query)
				if err != nil || len(out.Results) != 2 || string(out.Results[0].ID) != string(ids[0]) || string(out.Results[1].ID) != string(ids[1]) || out.Stats.DocumentsFetched != 0 || out.Stats.FullDocumentScanFallbacks != 0 {
					t.Fatalf("strategy=%s combined=%t out=%+v err=%v", strategy, combined, out, err)
				}
			}
		}
		// Source top-k is smaller than the admitted allowed set. Prefilter must
		// score both allowed rows; postfilter must retain its unfiltered top-k.
		query := HybridSearchOptions{TopK: 1, Vector: &HybridVectorQuery{IndexName: base.indexName, Query: columns[0].Float32Vectors[7], CandidateLimit: 1, EfSearch: 16}, ScalarFilter: filter}
		out, err := col.SearchHybrid(query)
		if err != nil || len(out.Results) != 1 || string(out.Results[0].ID) != string(ids[0]) || out.Plan.VectorCandidateLimit != 1 {
			t.Fatalf("small source prefilter=%+v err=%v", out, err)
		}
		query.ScalarFilterStrategy = HybridScalarFilterStrategyPostfilter
		out, err = col.SearchHybrid(query)
		if err != nil || len(out.Results) != 0 {
			t.Fatalf("postfilter was pushed into source: %+v err=%v", out, err)
		}
	}
	empty, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, Vector: &HybridVectorQuery{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], CandidateLimit: 1}, ScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "missing"}})
	if err != nil || len(empty.Results) != 0 || empty.Stats.VectorCandidatesReturned != 0 {
		t.Fatalf("empty=%+v err=%v", empty, err)
	}
	// The current-only prepared openers still fail closed after mutations,
	// preserving the declared name when their graph snapshot view fails.
	search := VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1}
	for _, quantized := range []bool{false, true} {
		var prepared *collectionVectorIndexPreparedSearch
		if quantized {
			prepared, _, err = col.openCollectionVectorIndexPreparedQuantizedSearch(search, columnVectorGraphNativeSearchQueryModeQuantizedOnly)
		} else {
			prepared, _, err = col.openCollectionVectorIndexPreparedExactSearch(search)
		}
		if prepared != nil {
			_ = prepared.Close()
		}
		if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), base.indexName) || errors.Is(err, ErrIndexNotFound) {
			t.Fatalf("quantized=%t graph error lost declaration: %v", quantized, err)
		}
	}
}

func TestTypedGraphHybridKeepsIndependentFilterAdmission(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, _, _, columns, _ := openTypedGraphQualityFixture(t, 8)
	opts := typedGraphPublicTestOptions()
	opts.Filter.SourceIDs = 1
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	// Hybrid can admit eight scalar IDs, but selected serving's tighter
	// producer budget must still reject rather than return partial candidates.
	out, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, Vector: &HybridVectorQuery{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], CandidateLimit: 1, EfSearch: 16}, ScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "source"}})
	if !errors.Is(err, ErrColumnGraphSearchBudget) || len(out.Results) != 0 || out.Stats.FailClosed != 1 || out.Stats.ScalarFilterInputIDs != 8 || out.Stats.DocumentsFetched != 0 {
		t.Fatalf("independent admission=%+v err=%v", out, err)
	}
}
