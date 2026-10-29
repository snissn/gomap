package collections

import (
	"errors"
	"strings"
	"testing"
)

func TestSearchVectorIndexColumnGraphRejectsUnsupportedFiltersBeforeSearch(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, db, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = db.Close() }()
	if col.nativeJSON != nil || col.typedGraphServingPolicy() != nil {
		t.Fatal("fixture must exercise ordinary persisted ColumnGraph serving")
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	for _, includeDocuments := range []bool{false, true} {
		name := "no_documents"
		if includeDocuments {
			name = "documents"
		}
		t.Run(name, func(t *testing.T) {
			base := VectorIndexSearchOptions{
				IndexName:        def.Name,
				Query:            []float32{1, 0, 0},
				TopK:             1,
				EfSearch:         len(rows),
				IncludeDocuments: includeDocuments,
			}
			// Prove this source-bound index can return the row that a false
			// callback (or missing scalar range) must never expose.
			control, err := col.SearchVectorIndex(base)
			if err != nil {
				t.Fatalf("unfiltered SearchVectorIndex: %v", err)
			}
			if len(control.Results) != 1 || string(control.Results[0].ID) != "doc-a" {
				t.Fatalf("unfiltered results=%+v; want doc-a", control.Results)
			}
			if includeDocuments && len(control.Results[0].Document) == 0 {
				t.Fatal("document control did not materialize its result")
			}
			if !includeDocuments && len(control.Results[0].Document) != 0 {
				t.Fatal("no-document control materialized its result")
			}

			callbackCalls := 0
			for _, tc := range []struct {
				name   string
				option string
				mutate func(*VectorIndexSearchOptions)
			}{
				{name: "callback", option: "Filter", mutate: func(opts *VectorIndexSearchOptions) {
					opts.Filter = func(DocumentRecord) (bool, error) {
						callbackCalls++
						return false, nil
					}
				}},
				{name: "missing_scalar_range", option: "IndexRangeFilter", mutate: func(opts *VectorIndexSearchOptions) {
					opts.IndexRangeFilter = &VectorIndexRangeFilter{IndexName: "missing_scalar_index"}
				}},
				{name: "declared_scalar", option: "DeclaredScalarFilter", mutate: func(opts *VectorIndexSearchOptions) {
					opts.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "missing_scalar_index", Value: "absent"}
				}},
				{name: "fetch_multiplier", option: "FetchMultiplier", mutate: func(opts *VectorIndexSearchOptions) { opts.FetchMultiplier = 2 }},
				{name: "negative_fetch_multiplier", option: "FetchMultiplier", mutate: func(opts *VectorIndexSearchOptions) { opts.FetchMultiplier = -1 }},
				{name: "exact_filter_max_docs", option: "ExactFilterMaxDocs", mutate: func(opts *VectorIndexSearchOptions) { opts.ExactFilterMaxDocs = 2 }},
				{name: "negative_exact_filter_max_docs", option: "ExactFilterMaxDocs", mutate: func(opts *VectorIndexSearchOptions) { opts.ExactFilterMaxDocs = -1 }},
				{name: "disable_exact_fallback", option: "DisableExactFallback", mutate: func(opts *VectorIndexSearchOptions) { opts.DisableExactFallback = true }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					opts := base
					tc.mutate(&opts)
					got, err := col.SearchVectorIndex(opts)
					if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "does not support "+tc.option+";") {
						t.Fatalf("SearchVectorIndex error=%v; want unavailable refusal for %s", err, tc.option)
					}
					if len(got.Results) != 0 || got.Path != "" || got.Stats.OpenSearcherCalls != 0 || got.Stats.DocumentsFetched != 0 {
						t.Fatalf("refusal performed search or materialization: %+v", got)
					}
					if callbackCalls != 0 {
						t.Fatalf("refusal invoked callback %d times", callbackCalls)
					}
				})
			}
		})
	}
}
