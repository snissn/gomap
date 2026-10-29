package documentservice

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// Candidate-only correctness coverage for #5238. Do not copy this file to the
// ea129 baseline: ordinary filtered vector requests were supported there.
// The measured benchmark remains unchanged. This test uses its actual native
// durable owner, default limits, 4096-row fixture and 32 deferred batch requests.
func TestServiceSharedCOWWriteLoadedFilteredVectorRefusal(t *testing.T) {
	for _, writers := range []int{1, 4} {
		t.Run(fmt.Sprintf("writers%d", writers), func(t *testing.T) {
			ctx := context.Background()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			owner, err := openSharedCOWWriteOwner(opts, "native")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.close(); err != nil {
					t.Errorf("close current owner: %v", err)
				}
			})
			info, err := owner.service.CreateIndex(ctx, CreateIndexRequest{Name: sharedCOWWriteIndex, Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
			if err != nil {
				t.Fatal(err)
			}
			checkSharedCOWWriteRefusalIndex(t, info)
			docs := make([]Document, sharedCOWWriteRows)
			for i := range docs {
				docs[i] = Document{ID: fmt.Sprintf("doc-%04d", i), Content: fmt.Sprintf("content-%04d", i), Embedding: []float32{float32(i + 1), 1}, Meta: map[string]any{"tenant": fmt.Sprintf("tenant-%02d", i%16)}}
			}
			type result struct {
				response  UpsertDocumentsResponse
				err       error
				attempted bool
			}
			var results [sharedCOWWriteBatches]result
			start := make(chan struct{})
			var ready, done sync.WaitGroup
			ready.Add(writers)
			done.Add(writers)
			for worker := 0; worker < writers; worker++ {
				go func(worker int) {
					defer done.Done()
					ready.Done()
					<-start
					for batch := worker; batch < sharedCOWWriteBatches; batch += writers {
						first := batch * sharedCOWWriteBatchSize
						results[batch].attempted = true
						results[batch].response, results[batch].err = owner.service.UpsertDocuments(ctx, sharedCOWWriteIndex, UpsertDocumentsRequest{Documents: docs[first : first+sharedCOWWriteBatchSize], DeferVectorIndexRebuild: true})
					}
				}(worker)
			}
			ready.Wait()
			close(start)
			done.Wait()
			for batch, got := range results {
				if !got.attempted || got.err != nil {
					t.Fatalf("batch%d attempted=%t error=%v; no caller retry", batch, got.attempted, got.err)
				}
				checkSharedCOWWriteRefusalIndex(t, got.response.Index)
				if got.response.Upserted != sharedCOWWriteBatchSize || got.response.Inserted != sharedCOWWriteBatchSize || got.response.Updated != 0 || len(got.response.IDs) != sharedCOWWriteBatchSize {
					t.Fatalf("batch%d incorrect ACK: %+v", batch, got.response)
				}
				for i, id := range got.response.IDs {
					if want := docs[batch*sharedCOWWriteBatchSize+i].ID; id != want {
						t.Fatalf("batch%d ACK ID%d=%q want%q", batch, i, id, want)
					}
				}
			}
			checkSharedCOWWriteRefusalDocuments(t, owner.service, docs)
			t.Run("post-load", func(t *testing.T) {
				checkSharedCOWWriteRefusals(t, owner.service)
			})
			if _, err := owner.service.OptimizeIndex(ctx, sharedCOWWriteIndex, OptimizeIndexRequest{}); err != nil {
				t.Fatalf("Optimize: %v", err)
			}
			t.Run("post-optimize", func(t *testing.T) {
				checkSharedCOWWriteRefusalSupported(t, owner.service)
				checkSharedCOWWriteRefusals(t, owner.service)
			})
			if err := owner.close(); err != nil {
				t.Fatalf("close before reopen: %v", err)
			}
			owner, err = openSharedCOWWriteOwner(opts, "native")
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			info, err = owner.service.OpenIndex(ctx, sharedCOWWriteIndex)
			if err != nil {
				t.Fatal(err)
			}
			checkSharedCOWWriteRefusalIndex(t, info)
			checkSharedCOWWriteRefusalDocuments(t, owner.service, docs)
			t.Run("post-reopen", func(t *testing.T) {
				checkSharedCOWWriteRefusalSupported(t, owner.service)
				checkSharedCOWWriteRefusals(t, owner.service)
			})
		})
	}
}

func checkSharedCOWWriteRefusalIndex(t *testing.T, info IndexInfo) {
	t.Helper()
	if info.Name != sharedCOWWriteIndex || info.Dimension != 2 || info.Metric != MetricCosine || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph || info.TypedInput || info.TextIndexName != defaultTextIndexName || len(info.ScalarFields) != 1 || info.ScalarFields[0].Field != "meta.tenant" || info.ScalarFields[0].ValueType != ScalarFieldString {
		t.Fatalf("incorrect ordinary omitted-strategy declaration: %+v", info)
	}
}

func checkSharedCOWWriteRefusalDocuments(t *testing.T, svc *Service, want []Document) {
	t.Helper()
	ctx := context.Background()
	count, err := svc.CountDocuments(ctx, sharedCOWWriteIndex, CountDocumentsRequest{})
	if err != nil || count.Count != sharedCOWWriteRows {
		t.Fatalf("count=%d want%d error=%v", count.Count, sharedCOWWriteRows, err)
	}
	all, err := svc.FilterDocuments(ctx, sharedCOWWriteIndex, FilterDocumentsRequest{ReturnEmbedding: true})
	if err != nil || all.Truncated || all.MatchedCount != len(want) || len(all.Documents) != len(want) {
		t.Fatalf("document verification: got%d matched%d truncated=%t error=%v", len(all.Documents), all.MatchedCount, all.Truncated, err)
	}
	for i, got := range all.Documents {
		if !reflect.DeepEqual(got, want[i]) {
			t.Fatalf("document%d mismatch: got%+v want%+v", i, got, want[i])
		}
	}
	for tenant := 0; tenant < 16; tenant++ {
		filter := &Filter{Field: "meta.tenant", Operator: "==", Value: fmt.Sprintf("tenant-%02d", tenant)}
		count, err := svc.CountDocuments(ctx, sharedCOWWriteIndex, CountDocumentsRequest{Filter: filter})
		if err != nil || count.Count != sharedCOWWriteRows/16 {
			t.Fatalf("tenant%d count=%d error=%v", tenant, count.Count, err)
		}
	}
}

func checkSharedCOWWriteRefusals(t *testing.T, svc *Service) {
	t.Helper()
	ctx := context.Background()
	for _, value := range []string{"tenant-00", "missing"} {
		t.Run(value, func(t *testing.T) {
			filter := &Filter{Field: "meta.tenant", Operator: "==", Value: value}
			for _, route := range []Route{"", RouteAnn} {
				label := string(route)
				if label == "" {
					label = "omitted"
				}
				t.Run("dense-"+label, func(t *testing.T) {
					before := workstats.Read()
					out, err := svc.SearchDenseVector(ctx, sharedCOWWriteIndex, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Filter: filter, Route: route})
					after := workstats.Read()
					if ErrorCodeOf(err) != CodeUnsupported || !reflect.DeepEqual(out, DenseVectorSearchResponse{}) {
						t.Errorf("filtered Dense must refuse before route/scan/fetch work: response=%+v error=%v", out, err)
					}
					checkSharedCOWWriteRefusalWork(t, before, after)
				})
			}
			for _, query := range []string{"content", ""} {
				label := "mixed"
				if query == "" {
					label = "vector-only"
				}
				t.Run("hybrid-"+label, func(t *testing.T) {
					before := workstats.Read()
					out, err := svc.SearchHybrid(ctx, sharedCOWWriteIndex, HybridSearchRequest{Query: query, QueryEmbedding: []float32{1, 1}, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 100, Filter: filter})
					after := workstats.Read()
					// Comparing all stats includes text postings/candidates,
					// vector traversal/scoring, scalar work, fetches and fallbacks.
					if ErrorCodeOf(err) != CodeUnsupported || len(out.Documents) != 0 || !reflect.DeepEqual(out.Stats, collections.HybridSearchStats{}) {
						t.Errorf("filtered Hybrid must refuse before text/vector/scan/fetch work: response=%+v error=%v", out, err)
					}
					checkSharedCOWWriteRefusalWork(t, before, after)
				})
			}
		})
	}
}

func checkSharedCOWWriteRefusalWork(t *testing.T, before, after workstats.Snapshot) {
	t.Helper()
	// No parallel tests or in-flight load requests overlap these windows.
	// Graph observes selected typed routes only; ordinary text/vector coverage
	// comes from the public refusal response above, not this process counter.
	if before.Scans != after.Scans || before.Output != after.Output || before.Graph != after.Graph || before.IndexedJSON.MaterializationRows != after.IndexedJSON.MaterializationRows {
		t.Errorf("refusal performed observed scan/output/graph/materialization work: scans=%+v -> %+v output=%+v -> %+v graph=%+v -> %+v materialization=%d -> %d", before.Scans, after.Scans, before.Output, after.Output, before.Graph, after.Graph, before.IndexedJSON.MaterializationRows, after.IndexedJSON.MaterializationRows)
	}
}

func checkSharedCOWWriteRefusalSupported(t *testing.T, svc *Service) {
	t.Helper()
	ctx := context.Background()
	filter := &Filter{Field: "meta.tenant", Operator: "==", Value: "tenant-00"}
	keyword, err := svc.SearchKeyword(ctx, sharedCOWWriteIndex, KeywordSearchRequest{Query: "content", TopK: 10, Filter: filter})
	if err != nil || len(keyword.Documents) != 10 || keyword.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("supported filtered Keyword: count=%d stats=%+v error=%v", len(keyword.Documents), keyword.Stats, err)
	}
	exact, err := svc.SearchDenseVector(ctx, sharedCOWWriteIndex, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Filter: filter, Route: RouteExact})
	if err != nil || len(exact.Documents) != 10 || exact.Route != RouteExact || !exact.Exact {
		t.Fatalf("supported filtered exact: response=%+v error=%v", exact, err)
	}
	for _, docs := range [][]Document{keyword.Documents, exact.Documents} {
		for _, doc := range docs {
			if doc.Meta["tenant"] != "tenant-00" || len(doc.Embedding) != 0 {
				t.Fatalf("supported filtered projection: %+v", doc)
			}
		}
	}
	hybrid, err := svc.SearchHybrid(ctx, sharedCOWWriteIndex, HybridSearchRequest{Query: "content", QueryEmbedding: []float32{1, 1}, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 100})
	if err != nil || len(hybrid.Documents) != 10 || hybrid.Stats.VectorCandidatesReturned == 0 || hybrid.Stats.FullDocumentScanFallbacks != 0 || hybrid.Stats.FailClosed != 0 || hybrid.Stats.DocumentsFetched > 10 {
		t.Fatalf("supported unfiltered Hybrid: count=%d stats=%+v error=%v", len(hybrid.Documents), hybrid.Stats, err)
	}
}
