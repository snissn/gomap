package documentservice

import (
	"context"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// Reopening deliberately starts without the owner installed by Optimize. Each
// first query must install the same service-owned collection used by the other
// query, so an immutable prepared pack survives across Dense and Hybrid calls.
func TestOrdinaryColumnGraphSearchOwnerReopenDenseHybrid(t *testing.T) {
	ctx := context.Background()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	var owner *treedb.DB
	var manager *collections.CollectionManager
	var svc *Service
	closeOwner := func() {
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
	t.Cleanup(closeOwner)
	openOwner := func() {
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
		svc = NewWithDeferredVectorBuildMaintenance(manager, owner.DeferredVectorBuildMaintenance())
	}
	openOwner()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name: "search_owner", Dimension: 8, Metric: MetricCosine,
		ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatalf("strategy=%s", info.VectorStrategy)
	}
	docs := make([]Document, 64)
	for i := range docs {
		docs[i] = Document{ID: fmt.Sprintf("d%03d", i), Content: "refund policy", Embedding: []float32{1, float32(i+1) / 64, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"tenant": "all"}}
	}
	if _, err := svc.UpsertDocuments(ctx, "search_owner", UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.OptimizeIndex(ctx, "search_owner", OptimizeIndexRequest{}); err != nil {
		t.Fatal(err)
	}
	closeOwner()

	for _, first := range []string{"dense", "hybrid", "concurrent"} {
		t.Run(first+"_first", func(t *testing.T) {
			openOwner()
			defer closeOwner()
			if got := svc.benchmarkSearchCacheSizeForTest(); got != 0 {
				t.Fatalf("cold cache size=%d", got)
			}
			denseReq := DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1, 0, 0, 0, 0, 0, 0}, TopK: 4, EfSearch: 64, Route: RouteAnn, ExpectedGeneration: info.Generation}
			hybridReq := HybridSearchRequest{Query: "policy", QueryEmbedding: denseReq.QueryEmbedding, TopK: 4, EfSearch: 64, TextCandidateLimit: 16, VectorCandidateLimit: 16, ExpectedGeneration: info.Generation}
			var retained []Document
			checkDocuments := func(got []Document) {
				t.Helper()
				if len(got) != 4 {
					t.Fatalf("documents=%d want 4", len(got))
				}
				seen := make(map[string]bool)
				for _, doc := range got {
					var ordinal int
					if _, err := fmt.Sscanf(doc.ID, "d%03d", &ordinal); err != nil || ordinal < 0 || ordinal >= 64 || doc.ID != fmt.Sprintf("d%03d", ordinal) || seen[doc.ID] {
						t.Fatalf("unexpected ID %q", doc.ID)
					}
					seen[doc.ID] = true
					if doc.Content != "refund policy" || doc.Meta["tenant"] != "all" || len(doc.Embedding) != 0 {
						t.Fatalf("projection=%+v", doc)
					}
				}
				retained = got
			}
			dense := func() {
				out, err := svc.SearchDenseVector(ctx, "search_owner", denseReq)
				if err != nil {
					t.Fatal(err)
				}
				if out.Route != RouteAnn {
					t.Fatalf("route=%s", out.Route)
				}
				checkDocuments(out.Documents)
			}
			hybrid := func() {
				out, err := svc.SearchHybrid(ctx, "search_owner", hybridReq)
				if err != nil {
					t.Fatal(err)
				}
				if out.TextIndex != defaultTextIndexName || out.VectorIndex != defaultVectorIndexName || out.Stats.TextCandidatesReturned == 0 || out.Stats.VectorCandidatesReturned == 0 || out.Stats.FullDocumentScanFallbacks != 0 {
					t.Fatalf("hybrid sources/plan: %+v", out)
				}
				checkDocuments(out.Documents)
			}
			assertPreparedHit := func() *collections.Collection {
				t.Helper()
				svc.benchmarkSearchCacheMu.RLock()
				defer svc.benchmarkSearchCacheMu.RUnlock()
				entry := svc.benchmarkSearchCache["search_owner"]
				if entry == nil || entry.collection == nil {
					t.Fatal("query did not install its service-owned collection")
				}
				response, err := entry.collection.WarmVectorIndexPreparedSearch(collections.VectorIndexSearchOptions{IndexName: defaultVectorIndexName, QueryMode: collections.VectorIndexQueryModeExact, TopK: 1, EfSearch: info.VectorEfSearch, StatsMode: collections.VectorIndexSearchStatsModeProduction})
				if err != nil {
					t.Fatal(err)
				}
				if response.Stats.HNSWSearchPackCacheHits != 1 || response.Stats.HNSWSearchPackCacheMisses != 0 || response.Stats.HNSWSearchPackCacheBuilds != 0 {
					t.Fatalf("prepared pack was not reused: %+v", response.Stats)
				}
				return entry.collection
			}
			if first == "concurrent" {
				// Both first requests start from an empty service cache. Join
				// all workers before assertions, output access, or Close.
				type queryResult struct {
					dense  *DenseVectorSearchResponse
					hybrid *HybridSearchResponse
					err    error
				}
				start := make(chan struct{})
				done := make(chan queryResult, 2)
				go func() {
					<-start
					out, err := svc.SearchDenseVector(ctx, "search_owner", denseReq)
					done <- queryResult{dense: &out, err: err}
				}()
				go func() {
					<-start
					out, err := svc.SearchHybrid(ctx, "search_owner", hybridReq)
					done <- queryResult{hybrid: &out, err: err}
				}()
				close(start)
				results := [2]queryResult{<-done, <-done}
				for _, result := range results {
					if result.err != nil {
						t.Fatal(result.err)
					}
					if result.dense != nil {
						if result.dense.Route != RouteAnn {
							t.Fatalf("route=%s", result.dense.Route)
						}
						checkDocuments(result.dense.Documents)
					} else {
						out := result.hybrid
						if out.TextIndex != defaultTextIndexName || out.VectorIndex != defaultVectorIndexName || out.Stats.TextCandidatesReturned == 0 || out.Stats.VectorCandidatesReturned == 0 || out.Stats.FullDocumentScanFallbacks != 0 {
							t.Fatalf("hybrid sources/plan: %+v", out)
						}
						checkDocuments(out.Documents)
					}
				}
				firstOwner := assertPreparedHit()
				dense()
				hybrid()
				if got := assertPreparedHit(); got != firstOwner {
					t.Fatal("concurrent first queries changed collection owner")
				}
			} else {
				if first == "dense" {
					dense()
				} else {
					hybrid()
				}
				firstOwner := assertPreparedHit()
				if first == "dense" {
					hybrid()
				} else {
					dense()
				}
				if got := assertPreparedHit(); got != firstOwner {
					t.Fatal("Dense and Hybrid changed collection owner")
				}
			}
			staleDense := denseReq
			staleDense.ExpectedGeneration = info.Generation + 1
			if _, err := svc.SearchDenseVector(ctx, "search_owner", staleDense); ErrorCodeOf(err) != CodeIndexStale {
				t.Fatalf("stale Dense: %v", err)
			}
			staleHybrid := hybridReq
			staleHybrid.ExpectedGeneration = info.Generation + 1
			if _, err := svc.SearchHybrid(ctx, "search_owner", staleHybrid); ErrorCodeOf(err) != CodeIndexStale {
				t.Fatalf("stale Hybrid: %v", err)
			}
			closedService := svc
			closeOwner()
			checkDocuments(retained)
			if _, err := closedService.SearchDenseVector(ctx, "search_owner", denseReq); ErrorCodeOf(err) != CodeIndexUnavailable {
				t.Fatalf("closed Dense: %v", err)
			}
			if _, err := closedService.SearchHybrid(ctx, "search_owner", hybridReq); ErrorCodeOf(err) != CodeIndexUnavailable {
				t.Fatalf("closed Hybrid: %v", err)
			}
		})
	}
}
