package documentservice

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// Optimize's durable publication and same-owner maintenance acknowledgment must
// survive process exit, without relying on a later Checkpoint, Flush, or Close.
// This is process-crash recovery coverage, not a power-loss simulation or a
// construction-capacity test (the separate original4096 lifecycle covers fit).
func TestOrdinaryColumnGraphNativeOptimizeCrashReopen(t *testing.T) {
	const helper = "GOMAP_NATIVE_ORDINARY_COLUMNGRAPH_OPTIMIZE_CRASH_DIR"
	const marker = "NATIVE_ORDINARY_COLUMNGRAPH_OPTIMIZE_ACCEPTED "
	type acceptedManifest struct {
		Directory         string
		Active            collections.ColumnManifestIdentity
		Recovery          collections.ColumnManifestIdentity
		AppliedCommandLSN uint64
		SchemaHash        uint64
	}
	document := func(i int) Document {
		return Document{ID: fmt.Sprintf("d%03d", i), Content: fmt.Sprintf("refund policy item%d", i), Embedding: []float32{1, float32(i) / 8, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"tenant": fmt.Sprintf("t%d", i%2)}}
	}
	open := func(dir string) (*treedb.DB, *collections.CollectionManager, *Service) {
		t.Helper()
		opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
		opts.MemtableMode = "cow_btree"
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		owner, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			if closeErr := owner.Close(); closeErr != nil {
				t.Error(closeErr)
			}
			t.Fatal(err)
		}
		manager := collections.NewCollectionManager(backend)
		return owner, manager, NewWithDeferredVectorBuildMaintenance(manager, owner.DeferredVectorBuildMaintenance())
	}
	manifest := func(svc *Service, dir string) acceptedManifest {
		t.Helper()
		col, info, err := svc.openIndex(context.Background(), "crash_docs", 0)
		if err != nil {
			t.Fatal(err)
		}
		if info.TypedInput || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
			t.Fatalf("ordinary strategy=%+v", info)
		}
		status, err := col.VectorIndexStatus(defaultVectorIndexName)
		if err != nil || !status.Loaded || status.RebuildNeeded || status.Stats.RebuildNeeded || status.Stats.SnapshotDirty || status.Stats.LiveDocs != 8 {
			t.Fatalf("persisted graph status=%+v err=%v", status, err)
		}
		cfg := col.Meta().Options.ColumnStore
		if cfg == nil || cfg.RetainedPayload != collections.ColumnRetainedPayloadFull || cfg.RetainedPayloadEncoding != collections.ColumnRetainedPayloadEncodingJSON || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
			t.Fatalf("ordinary FullJSON manifest missing: %+v", cfg)
		}
		if *cfg.ActiveManifest != *cfg.RecoveryAuthoritativeManifest || cfg.ActiveManifest.Generation == 0 || cfg.ActiveManifest.Checksum == 0 || cfg.RecoveryAuthoritativeAppliedCommandLSN == 0 {
			t.Fatalf("active/recovery identity=%+v", cfg)
		}
		return acceptedManifest{Directory: dir, Active: *cfg.ActiveManifest, Recovery: *cfg.RecoveryAuthoritativeManifest, AppliedCommandLSN: cfg.RecoveryAuthoritativeAppliedCommandLSN, SchemaHash: cfg.SchemaHash}
	}
	ctx := context.Background()
	if dir := os.Getenv(helper); dir != "" {
		// Intentionally no cleanup registration or deferred owner close here.
		_, _, svc := open(dir)
		info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "crash_docs", Dimension: 8, Metric: MetricCosine, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
		if err != nil {
			t.Fatal(err)
		}
		if info.TypedInput || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
			t.Fatalf("ordinary strategy=%+v", info)
		}
		docs := make([]Document, 8)
		for i := range docs {
			docs[i] = document(i)
		}
		inserted, err := svc.UpsertDocuments(ctx, "crash_docs", UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true})
		if err != nil || inserted.Upserted != 8 {
			t.Fatalf("upsert=%+v err=%v", inserted, err)
		}
		optimized, err := svc.OptimizeIndex(ctx, "crash_docs", OptimizeIndexRequest{})
		if err != nil || !optimized.Status.Loaded || optimized.Status.RebuildNeeded {
			t.Fatalf("Optimize=%+v err=%v", optimized, err)
		}
		accepted := manifest(svc, dir)
		encoded, err := json.Marshal(accepted)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Printf("%s%s\n", marker, encoded); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // No trailing maintenance, checkpoint, flush, or Close.
	}

	dir := t.TempDir()
	childCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestOrdinaryColumnGraphNativeOptimizeCrashReopen$")
	cmd.Env = append(os.Environ(), helper+"="+dir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Optimize process exit: %v\n%s", err, output)
	}
	var accepted acceptedManifest
	markers := 0
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, marker) {
			markers++
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, marker)), &accepted); err != nil {
				t.Fatalf("invalid acceptance marker: %v\n%s", err, output)
			}
		}
	}
	if markers != 1 || accepted.Directory != dir || accepted.Active.Generation == 0 || accepted.Active != accepted.Recovery || accepted.AppliedCommandLSN == 0 {
		t.Fatalf("missing intended Optimize acknowledgment before exit: markers=%d accepted=%+v\n%s", markers, accepted, output)
	}
	owner, manager, svc := open(dir)
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Error(err)
		}
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	// This inspection precedes all serving. Neither inspection nor search may
	// rebuild/Optimize the graph or change the accepted manifest identity.
	if got := manifest(svc, dir); got != accepted {
		t.Fatalf("recovered manifest=%+v want accepted=%+v", got, accepted)
	}
	full, err := svc.FilterDocuments(ctx, "crash_docs", FilterDocumentsRequest{ReturnEmbedding: true})
	if err != nil || len(full.Documents) != 8 || full.MatchedCount != 8 {
		t.Fatalf("FullJSON fetch=%+v err=%v", full, err)
	}
	for i, got := range full.Documents {
		want := document(i)
		if got.ID != want.ID || got.Content != want.Content || got.Meta["tenant"] != want.Meta["tenant"] || len(got.Embedding) != 8 {
			t.Fatalf("FullJSON document=%+v want=%+v", got, want)
		}
		for d := range want.Embedding {
			if got.Embedding[d] != want.Embedding[d] {
				t.Fatalf("embedding[%d] document=%+v want=%+v", d, got, want)
			}
		}
	}
	dense, err := svc.SearchDenseVector(ctx, "crash_docs", DenseVectorSearchRequest{QueryEmbedding: document(0).Embedding, TopK: 1, EfSearch: 64, Route: RouteAnn})
	if err != nil || dense.Route != RouteAnn || dense.Exact || len(dense.Documents) != 1 || dense.Documents[0].ID != "d000" || dense.Documents[0].Content != document(0).Content || len(dense.Documents[0].Embedding) != 0 {
		t.Fatalf("recovered ANN Dense=%+v err=%v", dense, err)
	}
	checkFiltered := func(docs []Document) {
		t.Helper()
		if len(docs) != 4 {
			t.Fatalf("filtered count=%d", len(docs))
		}
		seen := make(map[string]bool)
		for _, doc := range docs {
			if seen[doc.ID] || doc.Meta["tenant"] != "t0" || len(doc.Embedding) != 0 {
				t.Fatalf("filtered projection=%+v", doc)
			}
			seen[doc.ID] = true
		}
		for i := 0; i < 8; i += 2 {
			if !seen[document(i).ID] {
				t.Fatalf("missing filtered ID=%s", document(i).ID)
			}
		}
	}
	filter := &Filter{Field: "meta.tenant", Operator: "==", Value: "t0"}
	keyword, err := svc.SearchKeyword(ctx, "crash_docs", KeywordSearchRequest{Query: "policy", TopK: 4, Filter: filter})
	if err != nil || keyword.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("recovered Keyword=%+v err=%v", keyword, err)
	}
	checkFiltered(keyword.Documents)
	exact, err := svc.SearchDenseVector(ctx, "crash_docs", DenseVectorSearchRequest{QueryEmbedding: document(0).Embedding, TopK: 4, Filter: filter, Route: RouteExact})
	if err != nil || !exact.Exact || exact.Route != RouteExact {
		t.Fatalf("recovered filtered exact Dense=%+v err=%v", exact, err)
	}
	checkFiltered(exact.Documents)
	hybridRequest := HybridSearchRequest{Query: "policy", QueryEmbedding: document(0).Embedding, TopK: 4, TextCandidateLimit: 8, VectorCandidateLimit: 8, EfSearch: 64, Filter: filter}
	if refused, err := svc.SearchHybrid(ctx, "crash_docs", hybridRequest); ErrorCodeOf(err) != CodeUnsupported || len(refused.Documents) != 0 {
		t.Fatalf("recovered filtered Hybrid: documents=%d err=%v", len(refused.Documents), err)
	}
	hybridRequest.Filter = nil
	hybrid, err := svc.SearchHybrid(ctx, "crash_docs", hybridRequest)
	if err != nil || hybrid.TextIndex != defaultTextIndexName || hybrid.VectorIndex != defaultVectorIndexName || hybrid.Stats.TextCandidatesReturned == 0 || hybrid.Stats.VectorCandidatesReturned == 0 || hybrid.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("recovered mixed Hybrid=%+v err=%v", hybrid, err)
	}
	if len(hybrid.Documents) != 4 {
		t.Fatalf("recovered mixed Hybrid count=%d", len(hybrid.Documents))
	}
	seen := make(map[string]bool)
	for _, doc := range hybrid.Documents {
		var ordinal int
		if _, err := fmt.Sscanf(doc.ID, "d%03d", &ordinal); err != nil || ordinal < 0 || ordinal >= 8 || doc.ID != document(ordinal).ID || seen[doc.ID] || doc.Content != document(ordinal).Content || doc.Meta["tenant"] != document(ordinal).Meta["tenant"] || len(doc.Embedding) != 0 {
			t.Fatalf("recovered mixed Hybrid projection=%+v", doc)
		}
		seen[doc.ID] = true
	}
	if got := manifest(svc, dir); got != accepted {
		t.Fatalf("serving changed accepted manifest: %+v want=%+v", got, accepted)
	}
}
