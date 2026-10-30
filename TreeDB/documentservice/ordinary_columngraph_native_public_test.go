package documentservice

import (
	"context"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// This is a source-only test-first discriminator, not a recorded passing test.
// The strategy is deliberately omitted: ordinary cosine must remain ColumnGraph.
// Its seed matches the command corpus, including rank and declared scalar fields.
func TestOrdinaryColumnGraphNativeService4096(t *testing.T) {
	for _, native := range []bool{false, true} {
		role := "legacy"
		if native {
			role = "native"
		}
		t.Run(role, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			ctx := context.Background()
			var manager *collections.CollectionManager
			var svc *Service
			var closeOwner func() error
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
				if closeOwner != nil {
					if err := closeOwner(); err != nil {
						t.Error(err)
					}
					closeOwner = nil
				}
			}
			t.Cleanup(closeAll)
			open := func() {
				var maintenance *treedb.DeferredVectorBuildMaintenance
				if native {
					owner, err := treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					closeOwner = owner.Close
					backend, err := owner.NativeSemanticBackend()
					if err != nil {
						t.Fatal(err)
					}
					manager = collections.NewCollectionManager(backend)
					maintenance = owner.DeferredVectorBuildMaintenance()
				} else {
					backend, cleanup, _, m, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
					if err != nil {
						t.Fatal(err)
					}
					closeOwner, maintenance = cleanup, m
					manager = collections.NewCollectionManager(backend)
				}
				svc = NewWithDeferredVectorBuildMaintenance(manager, maintenance)
			}
			open()
			info, err := svc.CreateIndex(ctx, CreateIndexRequest{
				Name: "docs", Dimension: 8,
				ScalarFields: []ScalarFieldDeclaration{
					{Field: "meta.tenant", ValueType: ScalarFieldString},
					{Field: "meta.rank", ValueType: ScalarFieldInt64},
				},
			})
			if err != nil {
				t.Fatalf("ordinary Create: %v", err)
			}
			if info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
				t.Fatalf("strategy=%s", info.VectorStrategy)
			}
			document := func(i int) Document {
				vector := make([]float32, 8)
				vector[0] = 1
				for d := 1; d < 8; d++ {
					vector[d] = float32((i+d)%17) / 17
				}
				return Document{ID: fmt.Sprintf("d%06d", i), Content: "refund policy " + strings.Repeat("payload ", 32), Embedding: vector, Meta: map[string]any{"tenant": fmt.Sprintf("t%02d", i%16), "rank": i}}
			}
			for first := 0; first < 4096; first += 128 {
				docs := make([]Document, 128)
				for j := range docs {
					docs[j] = document(first + j)
				}
				out, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true})
				if err != nil {
					t.Fatalf("seed[%d:%d): %v", first, first+128, err)
				}
				if out.Upserted != 128 {
					t.Fatalf("seed upserted=%d", out.Upserted)
				}
			}
			if _, err := svc.OptimizeIndex(ctx, "docs", OptimizeIndexRequest{}); err != nil {
				t.Fatalf("Optimize: %v", err)
			}
			check := func(wantCount int, deleted bool) {
				t.Helper()
				count, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{})
				if err != nil || count.Count != wantCount {
					t.Fatalf("count=%d want=%d err=%v", count.Count, wantCount, err)
				}
				filter := &Filter{Field: "meta.tenant", Operator: "==", Value: "t00"}
				verify := func(docs []Document, filtered bool) {
					t.Helper()
					if len(docs) != 10 {
						t.Fatalf("documents=%d", len(docs))
					}
					seen := make(map[string]bool)
					for _, doc := range docs {
						if seen[doc.ID] || (filtered && doc.Meta["tenant"] != "t00") || len(doc.Embedding) != 0 || (deleted && doc.ID == "d000000") {
							t.Fatalf("wrong projected result: %+v", doc)
						}
						seen[doc.ID] = true
					}
				}
				keyword, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "policy", TopK: 10, Filter: filter})
				if err != nil {
					t.Fatalf("Keyword: %v", err)
				}
				verify(keyword.Documents, true)
				if keyword.Stats.FullDocumentScanFallbacks != 0 {
					t.Fatal("keyword scan fallback")
				}
				// ColumnGraph's maintained public ANN route is unfiltered.
				// Filtered ANN must refuse; exact document scan is explicit.
				dense, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: document(0).Embedding, TopK: 10, Route: RouteAnn, EfSearch: 64})
				if err != nil {
					t.Fatalf("Dense: %v", err)
				}
				verify(dense.Documents, false)
				if dense.Exact || dense.Route != RouteAnn {
					t.Fatalf("dense route=%s exact=%v", dense.Route, dense.Exact)
				}
				for _, route := range []Route{"", RouteAnn} {
					refused, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: document(0).Embedding, TopK: 10, Filter: filter, Route: route})
					if ErrorCodeOf(err) != CodeUnsupported || len(refused.Documents) != 0 {
						t.Fatalf("filtered ANN route=%q: documents=%d err=%v", route, len(refused.Documents), err)
					}
				}
				filteredDense, err := svc.SearchDenseVector(ctx, "docs", DenseVectorSearchRequest{QueryEmbedding: document(0).Embedding, TopK: 10, Filter: filter, Route: RouteExact})
				if err != nil {
					t.Fatalf("filtered Dense: %v", err)
				}
				verify(filteredDense.Documents, true)
				if !filteredDense.Exact || filteredDense.Route != RouteExact {
					t.Fatalf("filtered dense route=%s exact=%v", filteredDense.Route, filteredDense.Exact)
				}
				hybridRequest := HybridSearchRequest{Query: "policy", QueryEmbedding: document(0).Embedding, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 64, Filter: filter}
				if refused, err := svc.SearchHybrid(ctx, "docs", hybridRequest); ErrorCodeOf(err) != CodeUnsupported || len(refused.Documents) != 0 {
					t.Fatalf("filtered Hybrid: documents=%d err=%v", len(refused.Documents), err)
				}
				hybridRequest.Filter = nil
				hybrid, err := svc.SearchHybrid(ctx, "docs", hybridRequest)
				if err != nil {
					t.Fatalf("Hybrid: %v", err)
				}
				verify(hybrid.Documents, false)
			}
			check(4096, false)
			closeAll()
			if t.Failed() {
				return
			}
			open()
			info, err = svc.OpenIndex(ctx, "docs")
			if err != nil || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
				t.Fatalf("reopen strategy=%s err=%v", info.VectorStrategy, err)
			}
			check(4096, false)
			changed := document(16)
			changed.Content = "updated refund policy " + strings.Repeat("payload ", 32)
			if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{changed}, DeferVectorIndexRebuild: true}); err != nil {
				t.Fatalf("update: %v", err)
			}
			if _, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{IDs: []string{"d000000"}}); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if _, err := svc.OptimizeIndex(ctx, "docs", OptimizeIndexRequest{}); err != nil {
				t.Fatalf("mutation Optimize: %v", err)
			}
			check(4095, true)
			closeAll()
			if t.Failed() {
				return
			}
			open()
			check(4095, true)
			closeAll()
		})
	}
}
