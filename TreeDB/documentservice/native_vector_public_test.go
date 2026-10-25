package documentservice

import (
	"context"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// The service keeps its ordinary HNSW declaration and borrows one native cut
// for filtering, ANN candidates and projected result documents.
func TestNativeVectorDocumentServicePublic(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(profile, dir)
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			var owner *treedb.DB
			var manager *collections.CollectionManager
			var svc *Service
			closeAll := func() {
				if svc != nil {
					if err := svc.Close(); err != nil {
						t.Error(err)
					}
					svc = nil
				}
				if manager != nil {
					if err := manager.Close(); err != nil {
						t.Error(err)
					}
					manager = nil
				}
				if owner != nil {
					if err := owner.Close(); err != nil {
						t.Error(err)
					}
					owner = nil
				}
			}
			t.Cleanup(closeAll)
			open := func() {
				var err error
				owner, err = treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				backend, err := owner.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager = collections.NewCollectionManager(backend)
				svc = New(manager)
			}
			open()
			ctx := context.Background()
			req := CreateIndexRequest{Name: "docs", Dimension: 2, Metric: MetricCosine, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}}
			info, err := svc.CreateIndex(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if !info.Capabilities.HybridSearch {
				t.Fatalf("hybrid capability: %+v", info)
			}
			_, err = svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{
				{ID: "a", Content: "alpha beta", Embedding: []float32{1, 0}, Meta: map[string]any{"tenant": "one"}},
				{ID: "b", Content: "alpha gamma", Embedding: []float32{0, 1}, Meta: map[string]any{"tenant": "two"}},
			}})
			if err != nil {
				t.Fatal(err)
			}
			filter := &Filter{Field: "meta.tenant", Operator: "==", Value: "one"}
			phase := 0
			check := func() {
				phase++
				t.Logf("lifecycle phase %d", phase)
				dense, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, Filter: filter})
				if err != nil || len(dense.Documents) != 1 || dense.Documents[0].ID != "a" || len(dense.Documents[0].Embedding) != 0 {
					t.Fatalf("dense: %+v err=%v", dense, err)
				}
				for _, mode := range []string{"text", "vector", "combined"} {
					query := HybridSearchRequest{TopK: 2, Filter: filter}
					if mode != "vector" {
						query.Query = "alpha"
						query.TextCandidateLimit = 2
					}
					if mode != "text" {
						query.QueryEmbedding = []float32{1, 0}
						query.VectorCandidateLimit = 2
					}
					result, err := svc.SearchHybrid(ctx, "docs", query)
					if err != nil || len(result.Documents) != 1 || result.Documents[0].ID != "a" || len(result.Documents[0].Embedding) != 0 {
						t.Fatalf("%s hybrid: %+v err=%v", mode, result, err)
					}
					if !result.Snapshot.Native || result.Snapshot.CollectionIncarnation == 0 || result.Stats.FullDocumentScanFallbacks != 0 || result.Stats.FailClosed != 0 {
						t.Fatalf("capture/work: %+v", result)
					}
					if mode != "text" && result.Stats.VectorCandidatesReturned != 1 {
						t.Fatalf("vector candidates: %+v", result.Stats)
					}
				}
			}
			check()
			if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "alpha changed", Embedding: []float32{0.9, 0.1}, Meta: map[string]any{"tenant": "one"}}}}); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{IDs: []string{"b"}}); err != nil {
				t.Fatal(err)
			}
			check()
			if _, err := svc.OptimizeIndex(ctx, "docs", OptimizeIndexRequest{}); err != nil {
				t.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			closeAll()
			open()
			check()
		})
	}
}
