package documentservice

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// The maintained write-load geometry must also serve its first mixed request
// after reopening. Explicit exact Dense and an ANN warm-up must not hide cold
// pack validation's temporary admission overlapping text candidate admission.
func TestOrdinaryColumnGraphNativeColdHybrid4096(t *testing.T) {
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
		svc = New(manager)
	}
	openOwner()
	const name = "shared_cow_consumers"
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
	if err != nil {
		t.Fatal(err)
	}
	if info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatalf("strategy=%s", info.VectorStrategy)
	}
	expected := make([]Document, 0, 4096)
	for start := 0; start < 4096; start += 128 {
		docs := make([]Document, 128)
		for j := range docs {
			i := start + j
			docs[j] = Document{ID: fmt.Sprintf("doc-%04d", i), Content: fmt.Sprintf("content-%04d", i), Embedding: []float32{float32(i + 1), 1}, Meta: map[string]any{"tenant": fmt.Sprintf("tenant-%02d", i%16)}}
		}
		out, err := svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true})
		if err != nil || out.Upserted != 128 {
			t.Fatalf("seed[%d:%d) upserted=%d err=%v cause=%v", start, start+128, out.Upserted, err, errors.Unwrap(err))
		}
		expected = append(expected, docs...)
	}
	if _, err := svc.OptimizeIndex(ctx, name, OptimizeIndexRequest{}); err != nil {
		t.Fatalf("Optimize: %v cause=%v", err, errors.Unwrap(err))
	}
	request := HybridSearchRequest{Query: "content", QueryEmbedding: []float32{1, 1}, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 100}
	check := func(out HybridSearchResponse, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("Hybrid: %v cause=%v stats=%+v snapshot=%+v", err, errors.Unwrap(err), out.Stats, out.Snapshot)
		}
		if out.TextIndex != defaultTextIndexName || out.VectorIndex != defaultVectorIndexName || out.Index.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
			t.Fatalf("source identity: %+v", out)
		}
		if !out.Snapshot.Native || out.Snapshot.CollectionIncarnation == 0 {
			t.Fatalf("native snapshot: %+v", out.Snapshot)
		}
		if len(out.Documents) != 10 {
			t.Fatalf("documents=%d", len(out.Documents))
		}
		seen := make(map[string]bool)
		for _, doc := range out.Documents {
			var i int
			if _, err := fmt.Sscanf(doc.ID, "doc-%04d", &i); err != nil || i < 0 || i >= 4096 || doc.ID != fmt.Sprintf("doc-%04d", i) || seen[doc.ID] {
				t.Fatalf("invalid or duplicate ID %q", doc.ID)
			}
			seen[doc.ID] = true
			if doc.Content != fmt.Sprintf("content-%04d", i) || doc.Meta["tenant"] != fmt.Sprintf("tenant-%02d", i%16) || len(doc.Embedding) != 0 {
				t.Fatalf("projection=%+v", doc)
			}
		}
		stats := out.Stats
		if stats.TextCandidatesRequested != 64 || stats.VectorCandidatesRequested != 64 || stats.TextCandidatesReturned == 0 || stats.VectorCandidatesReturned == 0 || stats.TextPostingsScanned == 0 || stats.VectorCandidatesExamined == 0 || stats.ScalarFilterLookups != 0 || stats.DocumentsFetched != 10 || stats.EmbeddingOutputBytes != 0 || stats.FullDocumentScanFallbacks != 0 || stats.FailClosed != 0 {
			t.Fatalf("work=%+v", stats)
		}
	}
	check(svc.SearchHybrid(ctx, name, request))
	// Keep the original seed-to-Optimize path intact. Full content checks start
	// only after its first successful mixed request, and compare against every
	// acknowledged input rather than only the returned top-K projections.
	checkContents := func(stage string) {
		t.Helper()
		full, err := svc.FilterDocuments(ctx, name, FilterDocumentsRequest{ReturnEmbedding: true})
		if err != nil || full.Truncated || full.MatchedCount != len(expected) || len(full.Documents) != len(expected) {
			t.Fatalf("%s full contents: returned=%d matched=%d truncated=%t err=%v", stage, len(full.Documents), full.MatchedCount, full.Truncated, err)
		}
		for i, want := range expected {
			if got := full.Documents[i]; !reflect.DeepEqual(got, want) {
				t.Fatalf("%s acknowledged document[%d]=%+v want=%+v", stage, i, got, want)
			}
		}
	}
	checkContents("optimized")
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("explicit checkpoint: %v", err)
	}
	checkContents("checkpointed")
	closeOwner()
	if t.Failed() {
		return
	}
	openOwner()
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 0 {
		t.Fatalf("new service already has %d query owners", got)
	}
	// No OpenIndex/Keyword/Dense/ANN/warm call intervenes before the cold request.
	cold, err := svc.SearchHybrid(ctx, name, request)
	check(cold, err)
	// The first Hybrid must have installed its prepared pack. Inspect through the
	// same service-owned collection only AFTER that first successful request.
	svc.benchmarkSearchCacheMu.RLock()
	entry := svc.benchmarkSearchCache[name]
	if entry == nil || entry.collection == nil {
		svc.benchmarkSearchCacheMu.RUnlock()
		t.Fatal("cold Hybrid did not install its service owner")
	}
	warm, err := entry.collection.WarmVectorIndexPreparedSearch(collections.VectorIndexSearchOptions{IndexName: defaultVectorIndexName, QueryMode: collections.VectorIndexQueryModeExact, TopK: 1, EfSearch: 100, StatsMode: collections.VectorIndexSearchStatsModeProduction})
	svc.benchmarkSearchCacheMu.RUnlock()
	if err != nil || warm.Stats.HNSWSearchPackCacheHits != 1 || warm.Stats.HNSWSearchPackCacheBuilds != 0 || warm.Stats.HNSWSearchPackCacheMisses != 0 {
		t.Fatalf("first Hybrid did not retain its prepared pack: err=%v stats=%+v", err, warm.Stats)
	}
	again, err := svc.SearchHybrid(ctx, name, request)
	check(again, err)
	if again.Snapshot != cold.Snapshot {
		t.Fatalf("unmutated capture changed: cold=%+v warm=%+v", cold.Snapshot, again.Snapshot)
	}
	for i := range cold.Documents {
		if cold.Documents[i].ID != again.Documents[i].ID {
			t.Fatalf("cold/warm rank[%d]=%q/%q", i, cold.Documents[i].ID, again.Documents[i].ID)
		}
	}
	filtered := request
	filtered.Filter = &Filter{Field: "meta.tenant", Operator: "==", Value: "tenant-00"}
	if out, err := svc.SearchHybrid(ctx, name, filtered); ErrorCodeOf(err) != CodeUnsupported || len(out.Documents) != 0 {
		t.Fatalf("filtered Hybrid must refuse: documents=%d err=%v", len(out.Documents), err)
	}
	checkContents("reopened")
	closeOwner()
}
