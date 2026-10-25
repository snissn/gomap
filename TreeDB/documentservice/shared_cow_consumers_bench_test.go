package documentservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// BenchmarkServiceSharedCOWConsumers uses the existing dense visibility fixture
// geometry at the ordinary service boundary. The strategy is deliberately
// omitted: comparisons must report the resolved strategy as well as storage.
func BenchmarkServiceSharedCOWConsumers(b *testing.B) {
	backend := os.Getenv("GOMAP_SERVICE_BENCH_BACKEND")
	if backend == "" {
		backend = "legacy"
	}
	profile := treedb.Profile(os.Getenv("GOMAP_SERVICE_BENCH_PROFILE"))
	if profile == "" {
		profile = treedb.ProfileCommandWALDurable
	}
	switch profile {
	case treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast:
	default:
		b.Fatalf("unsupported service benchmark profile %q", profile)
	}
	opts := treedb.OptionsFor(profile, b.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	var manager *collections.CollectionManager
	var closeOwner func() error
	switch backend {
	case "legacy":
		db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
		if err != nil {
			b.Fatal(err)
		}
		closeOwner = cleanup
		manager = collections.NewCollectionManager(db)
	case "native":
		db, err := treedb.Open(opts)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := db.Close(); err != nil {
				b.Error(err)
			}
		})
		handle, err := db.NativeSemanticBackend()
		if err != nil {
			b.Fatal(err)
		}
		manager = collections.NewCollectionManager(handle)
	default:
		b.Fatalf("unsupported service benchmark backend %q", backend)
	}
	if closeOwner != nil {
		b.Cleanup(func() {
			if err := closeOwner(); err != nil {
				b.Error(err)
			}
		})
	}
	b.Cleanup(func() {
		if err := manager.Close(); err != nil {
			b.Error(err)
		}
	})
	svc := New(manager)
	b.Cleanup(func() {
		if err := svc.Close(); err != nil {
			b.Error(err)
		}
	})
	ctx := context.Background()
	const rows = 4096
	const loadBatchSize = 128
	const name = "shared_cow_consumers"
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("backend=%s profile=%s requested_memtable=cow_btree resolved_strategy=%s rows=%d load_batch=%d", backend, profile, info.VectorStrategy, rows, loadBatchSize)
	docs := make([]Document, rows)
	for i := range docs {
		docs[i] = Document{ID: fmt.Sprintf("doc-%04d", i), Content: fmt.Sprintf("content-%04d", i), Embedding: []float32{float32(i + 1), 1}, Meta: map[string]any{"tenant": fmt.Sprintf("tenant-%02d", i%16)}}
	}
	// Fixed public request boundaries are identical across every arm. This
	// constructs a read corpus; it does not qualify one atomic4096-row write.
	for start := 0; start < len(docs); start += loadBatchSize {
		end := min(start+loadBatchSize, len(docs))
		if _, err := svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{Documents: docs[start:end], DeferVectorIndexRebuild: true}); err != nil {
			b.Fatalf("UpsertDocuments batch [%d:%d): %v; cause: %v", start, end, err, errors.Unwrap(err))
		}
	}
	if _, err := svc.OptimizeIndex(ctx, name, OptimizeIndexRequest{}); err != nil {
		b.Fatal(err)
	}
	col, err := manager.OpenCollection(name)
	if err != nil {
		b.Fatal(err)
	}
	filter := &Filter{Field: "meta.tenant", Operator: "==", Value: "tenant-00"}
	check := func(tb testing.TB, docs []Document) {
		tb.Helper()
		if len(docs) != 10 {
			tb.Fatalf("returned %d documents, want10", len(docs))
		}
		for _, doc := range docs {
			if doc.Meta["tenant"] != "tenant-00" || len(doc.Embedding) != 0 {
				tb.Fatalf("incorrect filtered/projected document %+v", doc)
			}
		}
	}
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			raw, err := col.Get([]byte("doc-0000"))
			if err != nil || len(raw) == 0 {
				b.Fatalf("Get: bytes=%d err=%v", len(raw), err)
			}
		}
	})
	b.Run("keyword", func(b *testing.B) {
		req := KeywordSearchRequest{Query: "content", TopK: 10, Filter: filter}
		out, err := svc.SearchKeyword(ctx, name, req)
		if err != nil {
			b.Fatal(err)
		}
		check(b, out.Documents)
		b.ReportAllocs()
		for b.Loop() {
			out, err = svc.SearchKeyword(ctx, name, req)
			if err != nil {
				b.Fatal(err)
			}
		}
		check(b, out.Documents)
		if out.Stats.FullDocumentScanFallbacks != 0 {
			b.Fatal("keyword document scan fallback")
		}
		b.ReportMetric(float64(out.Stats.PostingsScanned), "postings/op")
	})
	b.Run("dense", func(b *testing.B) {
		req := DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Filter: filter}
		out, err := svc.SearchDenseVector(ctx, name, req)
		if err != nil {
			b.Fatal(err)
		}
		check(b, out.Documents)
		b.ReportAllocs()
		for b.Loop() {
			out, err = svc.SearchDenseVector(ctx, name, req)
			if err != nil {
				b.Fatal(err)
			}
		}
		check(b, out.Documents)
		// Older defaults may select exact. Report the actual route rather than
		// claiming algorithm-identical ANN across different public defaults.
		b.Logf("route=%s exact=%t", out.Route, out.Exact)
	})
	b.Run("hybrid", func(b *testing.B) {
		req := HybridSearchRequest{Query: "content", QueryEmbedding: []float32{1, 1}, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 100, Filter: filter}
		out, err := svc.SearchHybrid(ctx, name, req)
		if err != nil {
			b.Fatal(err)
		}
		check(b, out.Documents)
		b.ReportAllocs()
		for b.Loop() {
			out, err = svc.SearchHybrid(ctx, name, req)
			if err != nil {
				b.Fatal(err)
			}
		}
		check(b, out.Documents)
		// Inspect the public representation outside timing so this identical
		// harness also compiles against the original pre-native snapshot type.
		raw, err := json.Marshal(out.Snapshot)
		if err != nil {
			b.Fatal(err)
		}
		var snapshot struct {
			Native bool `json:"native"`
		}
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			b.Fatal(err)
		}
		if snapshot.Native != (backend == "native") || out.Stats.FullDocumentScanFallbacks != 0 || out.Stats.FailClosed != 0 || out.Stats.DocumentsFetched > 10 {
			b.Fatalf("incorrect capture/work: snapshot=%+v stats=%+v", out.Snapshot, out.Stats)
		}
		b.ReportMetric(float64(out.Stats.DocumentsFetched), "docs_fetched/op")
		b.ReportMetric(float64(out.Stats.TextPostingsScanned), "postings/op")
	})
}
