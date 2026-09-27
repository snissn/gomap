package documentservice

import (
	"context"
	"net/http"
	"reflect"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Parent #4765 acceptance composes the four production seams in one small
// fixture. Child tests retain ownership of crash injection and performance.
func TestProductionRetrievalSourceAndACLFilterLifecycle4765(t *testing.T) {
	requireTypedServiceServingTest(t)
	svc, db := newTestService(t)
	defer func() { _ = svc.Close(); _ = db.Close() }()
	dir, ctx := db.Dir(), context.Background()
	const name, quantized = "retrieval-lifecycle", "embedding.scalar_u8.public"
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name: name, Dimension: 2, TypedInput: true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy:         collections.VectorIndexStrategyColumnGraph,
			Representation:   collections.VectorIndexRepresentationCosineNormalizedF32V1,
			QuantizedIndexes: []QuantizedIndexInfo{{Name: quantized, Codec: collections.QuantizedVectorCodecScalarU8}},
		},
		ScalarFields: []ScalarFieldDeclaration{{Field: "meta.acl", ValueType: ScalarFieldString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	docs := []Document{
		{ID: "source#0", Content: "alpha and beta obsolete", Embedding: []float32{1, 0}, Meta: map[string]any{"acl": "allowed"}},
		{ID: "source#1", Content: "alpha and beta obsolete", Embedding: []float32{0.9, 0.1}, Meta: map[string]any{"acl": "allowed"}},
		{ID: "source#2", Content: "alpha and beta obsolete", Embedding: []float32{0, 1}, Meta: map[string]any{"acl": "allowed"}},
		{ID: "other#0", Content: "alpha and beta denied", Embedding: []float32{1, 0}, Meta: map[string]any{"acl": "denied"}},
	}
	if _, err := svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{ExpectedGeneration: info.Generation, Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	serving := typedServiceTestOptions()
	if _, err := svc.OptimizeIndex(ctx, name, OptimizeIndexRequest{ColumnGraphServing: &serving}); err != nil {
		t.Fatal(err)
	}
	// The application supplies this eligibility filter; this fixture does not
	// exercise a separate server-side authentication/authorization policy.
	filter := &Filter{Field: "meta.acl", Operator: "==", Value: "allowed"}
	hybrid := HybridSearchRequest{ExpectedGeneration: info.Generation, Query: `alpha AND (beta)`, TextQueryMode: collections.TextSearchQueryModeLiteral, QueryEmbedding: []float32{1, 0}, TopK: 8, TextCandidateLimit: 8, VectorCandidateLimit: 8, MaxPostingsScanned: 64, EfSearch: 8, VectorQueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: quantized, QuantizedRerankCandidates: 8}
	// Without a small allow-set the selected public path really uses SQ8 and
	// packed canonical reranking; selective filters may truthfully use typed_exact.
	initial, err := svc.SearchHybrid(ctx, name, hybrid)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Stats.VectorRoute == nil || initial.Stats.VectorRoute.QuantizedIndexName != quantized || initial.Stats.VectorQuantizedScoreCalls == 0 || initial.Stats.VectorPackedExactScoreCalls == 0 || initial.Stats.EmbeddingOutputBytes != 0 || initial.Stats.DocumentsFetched > uint64(len(initial.Documents)) {
		t.Fatalf("selected public route=%+v", initial.Stats)
	}
	check := func(stage string, want []string) {
		t.Helper()
		keyword, err := svc.SearchKeyword(ctx, name, KeywordSearchRequest{ExpectedGeneration: info.Generation, Query: hybrid.Query, TextQueryMode: collections.TextSearchQueryModeLiteral, Operator: collections.TextSearchOperatorAND, TopK: 8, CandidateLimit: 8, MaxPostingsScanned: 64, Filter: filter})
		if err != nil {
			t.Fatalf("%s keyword: %v", stage, err)
		}
		dense, err := svc.SearchDenseVector(ctx, name, DenseVectorSearchRequest{ExpectedGeneration: info.Generation, QueryEmbedding: hybrid.QueryEmbedding, TopK: 8, Route: RouteAnn, EfSearch: 8, Filter: filter, QueryMode: hybrid.VectorQueryMode, QuantizedIndexName: quantized, QuantizedRerankCandidates: 8})
		if err != nil {
			t.Fatalf("%s dense: %v", stage, err)
		}
		h := hybrid
		h.Filter = filter
		mixed, err := svc.SearchHybrid(ctx, name, h)
		if err != nil {
			t.Fatalf("%s hybrid: %v", stage, err)
		}
		for mode, found := range map[string][]Document{"bm25": keyword.Documents, "dense": dense.Documents, "hybrid": mixed.Documents} {
			got := make([]string, 0, len(found))
			for _, doc := range found {
				got = append(got, doc.ID)
				if doc.Meta["acl"] != "allowed" || len(doc.Embedding) != 0 {
					t.Fatalf("%s/%s unauthorized or vector-bearing result=%+v", stage, mode, doc)
				}
			}
			slices.Sort(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s/%s ids=%v want=%v", stage, mode, got, want)
			}
		}
		if mixed.Stats.DocumentsFetched > uint64(len(mixed.Documents)) || mixed.Stats.EmbeddingOutputBytes != 0 || mixed.Stats.FullDocumentScanFallbacks != 0 {
			t.Fatalf("%s hybrid fetch work=%+v", stage, mixed.Stats)
		}
	}
	check("initial", []string{"source#0", "source#1", "source#2"})
	live := docs[:1]
	live[0].Content = "alpha and beta fresh"
	var replaced ReplaceSourceByIDResponse
	postJSON(t, NewHandler(svc), "/v1/indexes/"+name+"/documents/replace_source_by_id", ReplaceSourceByIDRequest{ExpectedGeneration: info.Generation, DeleteIDs: []string{"source#0", "source#1", "source#2"}, Documents: live}, http.StatusOK, &replaced)
	if replaced.DeletedCount != 3 || replaced.InsertedCount != 1 {
		t.Fatalf("source replacement=%+v", replaced)
	}
	check("shrunk", []string{"source#0"})
	if old, err := svc.SearchKeyword(ctx, name, KeywordSearchRequest{Query: "obsolete", TopK: 8}); err != nil || len(old.Documents) != 0 {
		t.Fatalf("obsolete chunks=%+v err=%v", old.Documents, err)
	}
	for _, acl := range []string{"revoked", "allowed"} {
		var changed UpdateMetadataByIDResponse
		postJSON(t, NewHandler(svc), "/v1/indexes/"+name+"/documents/update_metadata_by_id", UpdateMetadataByIDRequest{ExpectedGeneration: info.Generation, IDs: []string{"source#0"}, Set: map[string]any{"meta.acl": acl}, Unset: []string{}}, http.StatusOK, &changed)
		if changed.MatchedCount != 1 || changed.ModifiedCount != 1 {
			t.Fatalf("permission change=%+v", changed)
		}
		want := []string{}
		if acl == "allowed" {
			want = []string{"source#0"}
		}
		check(acl, want)
	}
	if err := svc.Close(); err != nil {
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
	if _, err := svc.OptimizeIndex(ctx, name, OptimizeIndexRequest{ColumnGraphAction: "ensure", ColumnGraphServing: &serving}); err != nil {
		t.Fatal(err)
	}
	check("reopened", []string{"source#0"})
	all, err := svc.SearchHybrid(ctx, name, hybrid)
	if err != nil || len(all.Documents) != 2 {
		t.Fatalf("reopened live set=%+v err=%v", all.Documents, err)
	}
	for _, doc := range all.Documents {
		if doc.ID != "source#0" && doc.ID != "other#0" {
			t.Fatalf("retired chunk resurrected=%+v", doc)
		}
		if doc.ID == "source#0" && doc.Content != "alpha and beta fresh" {
			t.Fatalf("stale content=%+v", doc)
		}
	}
}
