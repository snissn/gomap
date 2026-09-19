package collections

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTypedGraphHybridSelectedOwnerSurvivesConcurrentPublication4767(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	ids := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	oldRetained := [][]byte{[]byte(`{"id":"a","version":"old-a"}`), []byte(`{"id":"b","version":"old-b"}`), []byte(`{"id":"c","version":"old-c"}`)}
	oldColumns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}, {0, 0, 1, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"refund", "shipping", "other"}},
		{Name: "user", Strings: []string{"u", "u", "u"}},
		{Name: "path", Strings: []string{"old", "old", "old"}},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, oldRetained, oldColumns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}

	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hybridSearchAfterReadOwnerAcquiredHookForTest.Lock()
	hybridSearchAfterReadOwnerAcquiredHookForTest.fn = func(got *Collection, view *CollectionReadView) {
		if got == col && view != nil {
			once.Do(func() { close(entered) })
			<-release
		}
	}
	hybridSearchAfterReadOwnerAcquiredHookForTest.Unlock()
	defer func() {
		hybridSearchAfterReadOwnerAcquiredHookForTest.Lock()
		hybridSearchAfterReadOwnerAcquiredHookForTest.fn = nil
		hybridSearchAfterReadOwnerAcquiredHookForTest.Unlock()
	}()
	request := HybridSearchOptions{
		TopK: 1,
		Text: &HybridTextQuery{IndexName: "content", Query: "refund", CandidateLimit: 3},
		Vector: &HybridVectorQuery{
			IndexName: "embedding_graph", Query: oldColumns[0].Float32Vectors[0], CandidateLimit: 3, EfSearch: 3,
			QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.legacy", QuantizedRerankCandidates: 3,
		},
		IncludeDocuments: true, DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
	}
	type searchResult struct {
		response HybridSearchResponse
		err      error
	}
	searchDone := make(chan searchResult, 1)
	go func() {
		response, err := col.SearchHybrid(request)
		searchDone <- searchResult{response, err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("selected hybrid did not capture an owner")
	}

	newRetained := [][]byte{[]byte(`{"id":"a","version":"new-a"}`), []byte(`{"id":"b","version":"new-b"}`), []byte(`{"id":"c","version":"new-c"}`)}
	newColumns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{0, 1, 0, 0, 0, 0, 0, 0}, {1, 0, 0, 0, 0, 0, 0, 0}, {0, 0, 1, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"shipping", "refund", "other"}},
		{Name: "user", Strings: []string{"u", "u", "u"}},
		{Name: "path", Strings: []string{"new", "new", "new"}},
	}
	mutationDone := make(chan error, 1)
	go func() {
		_, err := col.ReplaceTypedBatch(ids, newRetained, newColumns)
		mutationDone <- err
	}()
	select {
	case err := <-mutationDone:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("concurrent typed publication did not complete while old owner was retained")
	}
	close(release)
	var old searchResult
	select {
	case old = <-searchDone:
	case <-time.After(10 * time.Second):
		t.Fatal("selected hybrid did not finish after release")
	}
	if old.err != nil || len(old.response.Results) != 1 || string(old.response.Results[0].ID) != "a" || !bytes.Contains(old.response.Results[0].Document, []byte(`"version":"old-a"`)) || !bytes.Contains(old.response.Results[0].Document, []byte(`"content":"refund"`)) || bytes.Contains(old.response.Results[0].Document, []byte(`"embedding"`)) || old.response.Stats.VectorRoute == nil || old.response.Stats.VectorRoute.Route != "typed_hnsw" {
		t.Fatalf("captured-owner response=%+v err=%v", old.response, old.err)
	}
	hybridSearchAfterReadOwnerAcquiredHookForTest.Lock()
	hybridSearchAfterReadOwnerAcquiredHookForTest.fn = nil
	hybridSearchAfterReadOwnerAcquiredHookForTest.Unlock()
	fresh, err := col.SearchHybrid(request)
	if err != nil || len(fresh.Results) != 1 || string(fresh.Results[0].ID) != "b" || !bytes.Contains(fresh.Results[0].Document, []byte(`"version":"new-b"`)) || !bytes.Contains(fresh.Results[0].Document, []byte(`"content":"refund"`)) {
		t.Fatalf("fresh-owner response=%+v err=%v", fresh, err)
	}
}

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
