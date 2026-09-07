package documentservice

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestServiceTypedPreparedHandleLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(collections.NewCollectionManager(db))
	defer func() { _ = svc.Close(); _ = db.Close() }()
	create := CreateIndexRequest{Name: "keeper", Dimension: 8, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}}}
	info, err := svc.CreateIndex(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	docs := []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "u", "residual": "keep"}}, {ID: "b", Content: "beta", Embedding: []float32{0, 1, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "v"}}}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	options := typedServiceTestOptions()
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	cached := func() *collections.Collection {
		t.Helper()
		svc.benchmarkSearchCacheMu.RLock()
		entry := svc.benchmarkSearchCache[create.Name]
		svc.benchmarkSearchCacheMu.RUnlock()
		if entry == nil || entry.collection == nil {
			t.Fatal("successful explicit setup did not retain its service collection")
		}
		return entry.collection
	}
	handle := cached()
	query := DenseVectorSearchRequest{QueryEmbedding: docs[0].Embedding, TopK: 1, EfSearch: 8, ReturnEmbedding: true, ExpectedGeneration: info.Generation}
	check := func(content string) {
		t.Helper()
		for range 2 {
			var httpOut DenseVectorSearchResponse
			postJSON(t, NewHandler(svc), "/v1/indexes/keeper/search/vector", query, http.StatusOK, &httpOut)
			if httpOut.Route != RouteAnn || len(httpOut.Documents) != 1 {
				t.Fatalf("HTTP=%+v", httpOut)
			}
			raw, err := svc.SearchDenseVectorNativeRaw(ctx, create.Name, query)
			if err != nil || !raw.TypedColumnGraph || raw.Route != RouteAnn || len(raw.Results) != 1 {
				t.Fatalf("native=%+v err=%v", raw, err)
			}
			var nativeDoc Document
			if err := json.Unmarshal(raw.Results[0].Document, &nativeDoc); err != nil {
				t.Fatal(err)
			}
			for _, doc := range []Document{httpOut.Documents[0], nativeDoc} {
				if doc.ID != "a" || doc.Content != content || doc.Meta["residual"] != "keep" || !reflect.DeepEqual(doc.Embedding, docs[0].Embedding) {
					t.Fatalf("full payload=%+v", doc)
				}
			}
			if cached() != handle {
				t.Fatal("ordinary query replaced the explicit setup handle")
			}
		}
	}
	check("alpha")
	docs[0].Content = "updated"
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs[:1]}); err != nil {
		t.Fatal(err)
	}
	check("updated")
	if _, err := svc.DeleteDocuments(ctx, create.Name, DeleteDocumentsRequest{IDs: []string{"b"}}); err != nil {
		t.Fatal(err)
	}
	check("updated")
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "fold"}); err != nil {
		t.Fatal(err)
	}
	check("updated")
	if err := svc.invalidateBenchmarkSearchCache(create.Name); err != nil {
		t.Fatal(err)
	}
	if svc.benchmarkSearchCacheSizeForTest() != 0 {
		t.Fatal("explicit invalidation retained service handle")
	}
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "ensure", ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	next := cached()
	if next == handle {
		t.Fatal("invalidation did not replace old handle")
	}
	handle = next
	check("updated")
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	if svc.benchmarkSearchCacheSizeForTest() != 0 {
		t.Fatal("Close retained service handle")
	}
	svc = New(collections.NewCollectionManager(db))
	create.ColumnGraphServing = &options
	if _, err := svc.CreateIndex(ctx, create); err != nil {
		t.Fatal(err)
	}
	handle = cached()
	check("updated")
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	svc = New(collections.NewCollectionManager(db))
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "ensure", ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	handle = cached()
	check("updated")
}
