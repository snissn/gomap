package documentservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	if opts.COWMemtableLimits.MaxGenerationBytes != 64<<20 || opts.COWMemtableLimits.MaxInFlightBytes != 64<<20 {
		b.Fatalf("unexpected COW limits: %+v", opts.COWMemtableLimits)
	}
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
	if info.VectorStrategy != collections.VectorIndexStrategyColumnGraph || info.Metric != MetricCosine || info.Dimension != 2 || info.TypedInput {
		b.Fatalf("unexpected public index declaration: %+v", info)
	}
	b.Logf("backend=%s profile=%s requested_memtable=cow_btree resolved_strategy=%s rows=%d load_batch=%d", backend, profile, info.VectorStrategy, rows, loadBatchSize)
	b.Logf("cow_limits=%+v", opts.COWMemtableLimits)
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
	b.Run("dense_exact_filtered", func(b *testing.B) {
		req := DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Filter: filter, Route: RouteExact}
		out, err := svc.SearchDenseVector(ctx, name, req)
		if err != nil {
			b.Fatal(err)
		}
		check(b, out.Documents)
		checkSharedCOWFrozenResult(b, out.Documents, 16, false, 1e-12)
		b.ReportAllocs()
		for b.Loop() {
			out, err = svc.SearchDenseVector(ctx, name, req)
			if err != nil {
				b.Fatal(err)
			}
		}
		check(b, out.Documents)
		checkSharedCOWFrozenResult(b, out.Documents, 16, false, 1e-12)
		if out.Route != RouteExact || !out.Exact {
			b.Fatalf("unexpected dense route=%s exact=%t", out.Route, out.Exact)
		}
	})
	b.Run("dense_ann_unfiltered", func(b *testing.B) {
		req := DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Route: RouteAnn}
		out, err := svc.SearchDenseVector(ctx, name, req)
		if err != nil {
			b.Fatal(err)
		}
		checkSharedCOWFrozenResult(b, out.Documents, 1, false, 1e-6)
		b.ReportAllocs()
		for b.Loop() {
			out, err = svc.SearchDenseVector(ctx, name, req)
			if err != nil {
				b.Fatal(err)
			}
		}
		checkSharedCOWFrozenResult(b, out.Documents, 1, false, 1e-6)
		if out.Route != RouteAnn || out.Exact || out.FullDocumentScanFallbacks != 0 || out.ExactFallbacks != 0 || out.DocumentsScanned != 0 || out.PrimaryDocumentScans != 0 {
			b.Fatalf("unexpected dense ANN work: %+v", out)
		}
		b.Logf("route=%s prepared=%d search_pack=%d delta_scored=%d candidates=%d materialized=%d", out.Route, out.ColumnGraphPreparedSearch, out.SearchPackSearch, out.ColumnGraphDeltaScored, out.Candidates, out.DocumentMaterializationRows)
	})
	b.Run("hybrid_unfiltered", func(b *testing.B) {
		req := HybridSearchRequest{Query: "content", QueryEmbedding: []float32{1, 1}, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 100}
		out, err := svc.SearchHybrid(ctx, name, req)
		if err != nil {
			b.Fatal(err)
		}
		checkSharedCOWFrozenResult(b, out.Documents, 1, true, 1e-12)
		b.ReportAllocs()
		for b.Loop() {
			out, err = svc.SearchHybrid(ctx, name, req)
			if err != nil {
				b.Fatal(err)
			}
		}
		checkSharedCOWFrozenResult(b, out.Documents, 1, true, 1e-12)
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
		if out.Plan.TextCandidateLimit != 64 || out.Plan.VectorCandidateLimit != 64 {
			b.Fatalf("unexpected candidate plan: %+v", out.Plan)
		}
		b.Logf("hybrid_plan=%+v hybrid_stats=%+v", out.Plan, out.Stats)
		b.ReportMetric(float64(out.Stats.DocumentsFetched), "docs_fetched/op")
		b.ReportMetric(float64(out.Stats.TextPostingsScanned), "postings/op")
	})
}

// This source-derived oracle is fixed before performance collection. The
// corpus vector for ID i is [i+1,1]; cosine against [1,1] decreases with i.
// Text ranks tie by ID and the default RRF constant is 60. A preflight must
// establish these source-rank assumptions on both baseline and candidate.
func checkSharedCOWFrozenResult(tb testing.TB, docs []Document, step int, hybrid bool, tolerance float64) {
	tb.Helper()
	if len(docs) != 10 {
		tb.Fatalf("returned %d documents, want 10", len(docs))
	}
	for rank, doc := range docs {
		i := rank * step
		if doc.ID != fmt.Sprintf("doc-%04d", i) || doc.Content != fmt.Sprintf("content-%04d", i) || len(doc.Embedding) != 0 || doc.Meta["tenant"] != fmt.Sprintf("tenant-%02d", i%16) || doc.Score == nil {
			tb.Fatalf("rank %d violates frozen ID/content/projection: %+v", rank+1, doc)
		}
		x := float64(i + 1)
		want := (x + 1) / (math.Sqrt(2) * math.Sqrt(x*x+1))
		if hybrid {
			want = 2 / float64(60+rank+1)
		}
		if math.IsNaN(*doc.Score) || math.IsInf(*doc.Score, 0) || math.Abs(*doc.Score-want) > tolerance {
			tb.Fatalf("rank %d score=%g want=%g tolerance=%g", rank+1, *doc.Score, want, tolerance)
		}
		if !hybrid {
			if len(doc.Meta) != 1 {
				tb.Fatalf("unexpected dense projection: %+v", doc.Meta)
			}
			continue
		}
		meta, ok := doc.Meta["_treedb_search"].(map[string]any)
		if !ok || len(doc.Meta) != 2 || meta["type"] != "hybrid" || meta["rank"] != rank+1 || meta["fusion_method"] != "rrf" || meta["fused_score"] != *doc.Score {
			tb.Fatalf("unexpected hybrid attribution at rank %d: %+v", rank+1, doc.Meta)
		}
		sources, ok := meta["sources"].([]map[string]any)
		if !ok || len(sources) != 2 {
			tb.Fatalf("unexpected hybrid sources: %+v", meta)
		}
		seen := map[string]bool{}
		for _, source := range sources {
			name, _ := source["source"].(string)
			contribution, ok := source["fusion_score"].(float64)
			if (name != "text" && name != "vector") || seen[name] || source["source_rank"] != rank+1 || !ok || math.Abs(contribution-1/float64(60+rank+1)) > tolerance {
				tb.Fatalf("unexpected hybrid source rank %d: %+v", rank+1, source)
			}
			seen[name] = true
		}
	}
}
