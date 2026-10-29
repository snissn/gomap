package documentservice

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// TestSharedCOWFrozenFixturePreflight is a correctness-only fresh-process
// control for the fixed comparative benchmark; it does not collect timings.
func TestSharedCOWFrozenFixturePreflight(t *testing.T) {
	ctx := context.Background()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	svc := New(manager)
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Error(err)
		}
	})
	const name = "shared_cow_consumers"
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
	if err != nil || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph || info.Metric != MetricCosine || info.TypedInput {
		t.Fatalf("index=%+v err=%v", info, err)
	}
	for start := 0; start < 4096; start += 128 {
		docs := make([]Document, 128)
		for j := range docs {
			i := start + j
			docs[j] = Document{ID: fmt.Sprintf("doc-%04d", i), Content: fmt.Sprintf("content-%04d", i), Embedding: []float32{float32(i + 1), 1}, Meta: map[string]any{"tenant": fmt.Sprintf("tenant-%02d", i%16)}}
		}
		if _, err := svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
			t.Fatalf("batch %d: %v", start, err)
		}
	}
	if _, err := svc.OptimizeIndex(ctx, name, OptimizeIndexRequest{}); err != nil {
		t.Fatal(err)
	}
	ann, err := svc.SearchDenseVector(ctx, name, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Route: RouteAnn})
	if err != nil {
		t.Fatal(err)
	}
	checkSharedCOWFrozenResult(t, ann.Documents, 1, false, 1e-6)
	if ann.Route != RouteAnn || ann.Exact || ann.ExactFallbacks != 0 || ann.FullDocumentScanFallbacks != 0 || ann.DocumentsScanned != 0 || ann.PrimaryDocumentScans != 0 {
		t.Fatalf("ANN route/work: %+v", ann)
	}
	filter := &Filter{Field: "meta.tenant", Operator: "==", Value: "tenant-00"}
	exact, err := svc.SearchDenseVector(ctx, name, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Filter: filter, Route: RouteExact})
	if err != nil {
		t.Fatal(err)
	}
	checkSharedCOWFrozenResult(t, exact.Documents, 16, false, 1e-12)
	if exact.Route != RouteExact || !exact.Exact {
		t.Fatalf("exact route: %+v", exact)
	}
	hybrid, err := svc.SearchHybrid(ctx, name, HybridSearchRequest{Query: "content", QueryEmbedding: []float32{1, 1}, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 100})
	if err != nil {
		t.Fatal(err)
	}
	checkSharedCOWFrozenResult(t, hybrid.Documents, 1, true, 1e-12)
	if hybrid.Plan.TextCandidateLimit != 64 || hybrid.Plan.VectorCandidateLimit != 64 || hybrid.Stats.FailClosed != 0 || hybrid.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("hybrid plan/work: %+v %+v", hybrid.Plan, hybrid.Stats)
	}
	for label, value := range map[string]any{"ann": ann, "exact": exact, "hybrid": hybrid} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("FROZEN_FIXTURE_RESULT %s %s", label, raw)
	}
}
