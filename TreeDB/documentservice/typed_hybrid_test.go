package documentservice

import (
	"context"
	"math"
	"reflect"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

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
