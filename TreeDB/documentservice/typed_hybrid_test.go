package documentservice

import (
	"context"
	"errors"
	"math"
	"net/http"
	"reflect"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestServiceTypedHybridQuantizedRerankPublicRoute4767(t *testing.T) {
	requireTypedServiceServingTest(t)
	svc, db := newTestService(t)
	defer func() { _ = svc.Close(); _ = db.Close() }()
	ctx := context.Background()
	create := CreateIndexRequest{
		Name:       "hybrid-quantized-public",
		Dimension:  2,
		TypedInput: true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
			QuantizedIndexes: []QuantizedIndexInfo{{
				Name: "embedding.scalar_u8.public", Codec: collections.QuantizedVectorCodecScalarU8,
			}},
		},
		ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}},
	}
	if _, err := svc.CreateIndex(ctx, create); err != nil {
		t.Fatal(err)
	}
	docs := []Document{
		{ID: "a", Content: "refund", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "u"}},
		{ID: "b", Content: "policy", Embedding: []float32{0.9, 0.1}, Meta: map[string]any{"user_id": "u"}},
		{ID: "c", Content: "shipping", Embedding: []float32{0, 1}, Meta: map[string]any{"user_id": "v"}},
	}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	serving := typedServiceTestOptions()
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphServing: &serving}); err != nil {
		t.Fatal(err)
	}
	request := HybridSearchRequest{
		Query: "refund", QueryEmbedding: []float32{1, 0}, TopK: 2,
		TextCandidateLimit: 3, VectorCandidateLimit: 3, EfSearch: 3,
		VectorQueryMode:    collections.VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName: "embedding.scalar_u8.public", QuantizedRerankCandidates: 3,
	}
	out, err := svc.SearchHybrid(ctx, create.Name, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Documents) != 2 || out.Plan.VectorQueryMode != collections.VectorIndexQueryModeQuantizedRerank || out.Plan.QuantizedIndexName != request.QuantizedIndexName || out.Plan.QuantizedRerankCandidates != 3 {
		t.Fatalf("selected hybrid response=%+v", out)
	}
	for _, doc := range out.Documents {
		if len(doc.Embedding) != 0 {
			t.Fatalf("default hybrid response returned embedding: %+v", doc)
		}
	}
	if out.Stats.VectorRoute == nil || out.Stats.VectorRoute.Route != "typed_hnsw" || out.Stats.VectorRoute.QueryMode != collections.VectorIndexQueryModeQuantizedRerank || out.Stats.VectorRoute.QuantizedIndexName != request.QuantizedIndexName || out.Stats.VectorQuantizedScoreCalls == 0 || out.Stats.VectorQuantizedRerankExactScoreCalls == 0 || out.Stats.DocumentsFetched != uint64(len(out.Documents)) || out.Stats.EmbeddingOutputBytes != 0 {
		t.Fatalf("selected hybrid stats=%+v", out.Stats)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := svc.SearchHybrid(canceled, create.Name, request); !errors.Is(err, context.Canceled) || ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("selected hybrid cancellation=%v", err)
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		mapped := mapHybridSearchError(errors.Join(collections.ErrHybridSearchUnsupported, cause))
		if !errors.Is(mapped, cause) || ErrorCodeOf(mapped) != CodeIndexUnavailable {
			t.Fatalf("hybrid mapped cancellation cause=%v mapped=%v code=%s", cause, mapped, ErrorCodeOf(mapped))
		}
	}
	var httpOut HybridSearchResponse
	postJSON(t, NewHandler(svc), "/v1/indexes/"+create.Name+"/search/hybrid", request, http.StatusOK, &httpOut)
	if httpOut.Stats.VectorRoute == nil || httpOut.Stats.VectorRoute.QueryMode != collections.VectorIndexQueryModeQuantizedRerank || len(httpOut.Documents) != len(out.Documents) {
		t.Fatalf("selected hybrid HTTP response=%+v", httpOut)
	}

	filteredRequest := request
	filteredRequest.Filter = &Filter{Field: "meta.user_id", Operator: "==", Value: "u"}
	filtered, err := svc.SearchHybrid(ctx, create.Name, filteredRequest)
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Stats.VectorRoute == nil || filtered.Stats.VectorRoute.Route != "typed_exact" || filtered.Stats.VectorQuantizedScoreCalls != 0 || filtered.Stats.VectorCandidatesReturned != 2 {
		t.Fatalf("selected small-filter response=%+v", filtered)
	}
	emptyRequest := request
	emptyRequest.Filter = &Filter{Field: "meta.user_id", Operator: "==", Value: "missing"}
	empty, err := svc.SearchHybrid(ctx, create.Name, emptyRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Documents) != 0 || empty.Stats.VectorRoute != nil || empty.Stats.VectorCandidatesReturned != 0 || empty.Plan.VectorQueryMode != collections.VectorIndexQueryModeQuantizedRerank || empty.Stats.CandidateBudgetPolicy != collections.HybridCandidateBudgetPolicyFixed || empty.Stats.CandidateBudgetStopReason != collections.HybridCandidateBudgetStopReasonFixedPolicy {
		t.Fatalf("selected empty-filter response=%+v", empty)
	}

	for name, malformed := range map[string]HybridSearchRequest{
		"fields without embedding":          {Query: "refund", TopK: 1, VectorQueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: request.QuantizedIndexName},
		"exact with quantized name":         {QueryEmbedding: []float32{1, 0}, TopK: 1, VectorQueryMode: collections.VectorIndexQueryModeExact, QuantizedIndexName: request.QuantizedIndexName},
		"quantized only":                    {QueryEmbedding: []float32{1, 0}, TopK: 1, VectorQueryMode: collections.VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: request.QuantizedIndexName},
		"rerank below effective candidates": {QueryEmbedding: []float32{1, 0}, TopK: 1, VectorCandidateLimit: 3, VectorQueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: request.QuantizedIndexName, QuantizedRerankCandidates: 2},
		"unknown quantized index":           {QueryEmbedding: []float32{1, 0}, TopK: 1, VectorQueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "missing.scalar_u8"},
		"vector budget without embedding":   {Query: "refund", TopK: 1, VectorCandidateLimit: 3},
		"vector ef without embedding":       {Query: "refund", TopK: 1, EfSearch: 3},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.SearchHybrid(ctx, create.Name, malformed); err == nil {
				t.Fatal("malformed hybrid quantized request was accepted")
			}
		})
	}
}

func TestServiceTypedHybridCurrentMutationLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("selected serving fixture requires Linux namespace authority and mmap")
	}
	ctx := context.Background()
	dir := t.TempDir()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(collections.NewCollectionManager(db))
	defer func() { _ = svc.Close(); _ = db.Close() }()
	before := workstats.Read()
	defer func() {
		after := workstats.Read()
		if after.IndexedJSON != before.IndexedJSON || after.Runtime != before.Runtime || after.Scans != before.Scans {
			t.Errorf("selected hybrid used JSON/runtime/scan work: before=%+v after=%+v", before, after)
		}
		if after.Typed.TextRows <= before.Typed.TextRows || after.Typed.TextOldRows <= before.Typed.TextOldRows {
			t.Error("typed text mutation producers did not run")
		}
	}()
	create := CreateIndexRequest{Name: "hybridtyped", Dimension: 8, TypedInput: true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2},
		ScalarFields:       []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}, {Field: "meta.fpath", ValueType: ScalarFieldString}}}
	if _, err := svc.CreateIndex(ctx, create); err != nil {
		t.Fatal(err)
	}
	expected := map[string]Document{}
	row := func(id, content string, x, y float32, owner string) Document {
		d := Document{ID: id, Content: content, Embedding: []float32{x, y, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": owner, "fpath": "/" + id, "residual": "kept"}}
		expected[id] = d
		return d
	}
	docs := []Document{row("shared", "refund refund", 1, 0, "owner"), row("text", "refund policy", 0, 1, "owner"), row("vector", "shipping update", .99, .01, "owner"), row("background", "other", 0, 1, "owner"), row("distractor", "refund refund refund", 1, 0, "other")}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	selection := &Filter{Field: "meta.user_id", Operator: "==", Value: "owner"}
	checkDocs := func(t *testing.T, docs []Document, owner bool) {
		t.Helper()
		for _, d := range docs {
			e, ok := expected[d.ID]
			if !ok || d.Content != e.Content || !reflect.DeepEqual(d.Embedding, e.Embedding) || d.Meta["residual"] != "kept" || d.Meta["fpath"] != e.Meta["fpath"] || d.Meta["user_id"] != e.Meta["user_id"] || owner && d.Meta["user_id"] != "owner" {
				t.Fatalf("current full document=%+v expected=%+v", d, e)
			}
		}
	}
	keyword := func(query string, want []string) {
		t.Helper()
		out, err := svc.SearchKeyword(ctx, create.Name, KeywordSearchRequest{Query: query, TopK: 8, CandidateLimit: 32, Filter: selection, ReturnEmbedding: true})
		if err != nil || !reflect.DeepEqual(documentIDs(out.Documents), want) {
			t.Fatalf("keyword %s IDs=%v want=%v err=%v", query, documentIDs(out.Documents), want, err)
		}
		checkDocs(t, out.Documents, true)
	}
	keyword("refund", []string{"shared", "text"})
	if _, err := svc.SearchHybrid(ctx, create.Name, HybridSearchRequest{Query: "refund", QueryEmbedding: docs[0].Embedding, TopK: 1, Filter: selection}); err == nil {
		t.Fatal("hybrid vector source succeeded before graph admission")
	}
	options := typedServiceTestOptions()
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	check := func(stage, winner string, vector []float32, score float64) {
		t.Helper()
		t.Run(stage+"/filtered", func(t *testing.T) {
			out, err := svc.SearchHybrid(ctx, create.Name, HybridSearchRequest{Query: "refund", QueryEmbedding: vector, TopK: 1, TextCandidateLimit: 8, VectorCandidateLimit: 8, EfSearch: 32, Filter: selection, ReturnEmbedding: true})
			if err != nil {
				t.Fatal(err)
			}
			checkDocs(t, out.Documents, true)
			if len(out.Documents) != 1 || out.Documents[0].ID != winner || out.Documents[0].Score == nil || math.Abs(*out.Documents[0].Score-score) > 1e-12 || !searchMetaHasSources(searchMeta(t, out.Documents[0]), "text", "vector") || out.Stats.FailClosed != 0 || out.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("hybrid current winner/sources/score=%+v stats=%+v", out.Documents, out.Stats)
			}
		})
		t.Run(stage+"/unfiltered", func(t *testing.T) {
			out, err := svc.SearchHybrid(ctx, create.Name, HybridSearchRequest{Query: "refund", QueryEmbedding: vector, TopK: 8, TextCandidateLimit: 8, VectorCandidateLimit: 8, EfSearch: 32, ReturnEmbedding: true})
			if err != nil {
				t.Fatal(err)
			}
			checkDocs(t, out.Documents, false)
			if len(out.Documents) != len(expected) {
				t.Fatalf("hybrid returned %d current rows want %d", len(out.Documents), len(expected))
			}
		})
		t.Run(stage+"/direct", func(t *testing.T) {
			col, _, err := svc.openIndex(ctx, create.Name, 0)
			if err != nil {
				t.Fatal(err)
			}
			out, err := col.SearchHybridVectorCandidates(collections.HybridVectorQuery{IndexName: defaultVectorIndexName, Query: vector, CandidateLimit: 8, EfSearch: 32})
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Candidates) != len(expected) || out.Stats.DocumentsFetched != 0 || out.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("direct candidates=%+v", out)
			}
			for i, candidate := range out.Candidates {
				if _, ok := expected[string(candidate.ID)]; !ok || candidate.SourceRank != i+1 {
					t.Fatalf("candidate=%+v", candidate)
				}
			}
		})
	}
	check("initial", "shared", docs[0].Embedding, 2.0/61)
	changed := row("shared", "shipping revised", 1, 0, "owner")
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: []Document{changed}}); err != nil {
		t.Fatal(err)
	}
	keyword("refund", []string{"text"})
	if _, err := svc.DeleteDocuments(ctx, create.Name, DeleteDocumentsRequest{IDs: []string{"text"}}); err != nil {
		t.Fatal(err)
	}
	delete(expected, "text")
	keyword("refund", []string{})
	fresh := row("fresh", "refund refund", 0, 1, "owner")
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: []Document{fresh}}); err != nil {
		t.Fatal(err)
	}
	keyword("refund", []string{"fresh"})
	check("mutations_before_fold", "fresh", fresh.Embedding, 1.0/61+1.0/62)
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "fold"}); err != nil {
		t.Fatal(err)
	}
	check("folded", "fresh", fresh.Embedding, 1.0/61+1.0/62)
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
	keyword("refund", []string{"fresh"})
	check("reopened", "fresh", fresh.Embedding, 1.0/61+1.0/62)
}
