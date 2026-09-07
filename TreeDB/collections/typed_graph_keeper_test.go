package collections

import (
	"bytes"
	"context"
	"testing"
)

func TestTypedGraphPublicEnsureKeepsNewHandleBase(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer col.db.Close()
	defer base.Close()
	ctx := context.Background()
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(ctx, base.indexName, opts); err != nil {
		t.Fatal(err)
	}
	// Real suffix work must remain charged across repeated explicit Ensure.
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]}, {Name: "content", Strings: []string{"keeper-latest"}}, {Name: "user", Strings: []string{"keeper-user"}}, {Name: "path", Strings: []string{"keeper-path"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	other, err := NewCollectionManager(col.db).OpenCollection(col.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer other.CloseVectorIndexPreparedSearchCache()
	coord := col.collectionSchemaCoordinator()
	beforeState := coord.typedPublication.Load()
	coord.typedPublicationDebtMu.Lock()
	beforeDebt, beforeEpoch := coord.typedPublicationDebt, coord.typedGraphWorkEpoch
	coord.typedPublicationDebtMu.Unlock()
	for range 3 {
		if err := other.EnsureColumnGraphServing(ctx, base.indexName, opts); err != nil {
			t.Fatal(err)
		}
	}
	prepared := other.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if prepared.Entries != 1 || prepared.CacheBuilds != 1 || prepared.Refs != 1 || prepared.Opens == 0 {
		t.Fatalf("new handle Ensure did not retain one prepared base: %+v", prepared)
	}
	query := VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal, DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "keeper-path"}}
	for range 3 {
		var buffer VectorIndexSearchBuffer
		out, view, err := other.SearchVectorIndexWithBufferReadView(query, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		docs, err := view.FetchDocumentsForVectorIndexSearchResults(out.Results, DocumentFetchOptions{})
		closeErr := view.Close()
		if err != nil || closeErr != nil || len(docs.Results) != 1 || !bytes.Equal(docs.Results[0].ID, ids[0]) || !bytes.Contains(docs.Results[0].Document, []byte("keeper-latest")) {
			t.Fatalf("current payload=%+v err=%v close=%v", docs, err, closeErr)
		}
	}
	after := other.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if after.CacheBuilds != prepared.CacheBuilds || after.Opens != prepared.Opens || after.Refs != 1 || after.CacheHits <= prepared.CacheHits {
		t.Fatalf("queries rebuilt/opened base: before=%+v after=%+v", prepared, after)
	}
	coord.typedPublicationDebtMu.Lock()
	afterDebt, afterEpoch := coord.typedPublicationDebt, coord.typedGraphWorkEpoch
	coord.typedPublicationDebtMu.Unlock()
	if coord.typedPublication.Load() != beforeState || afterDebt != beforeDebt || afterEpoch != beforeEpoch {
		t.Fatal("repeated Ensure changed publication authority or reset work debt")
	}
}
