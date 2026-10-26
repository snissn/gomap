package documentservice

import (
	"context"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

// The reviewed benchmark's unchanged4096/128 corpus is also an explicit native
// runtime persistence lifecycle: Optimize, filtered reads, Close, and cold reopen.
func TestNativeHNSWServicePersistence4096(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	ctx := context.Background()
	const name = "shared_cow_consumers"
	var svc *Service
	var closeOwners func()
	open := func() {
		db, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		handle, err := db.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager := collections.NewCollectionManager(handle)
		svc = New(manager)
		closed := false
		closeOwners = func() {
			if closed {
				return
			}
			closed = true
			if err := svc.Close(); err != nil {
				t.Error(err)
			}
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
		t.Cleanup(closeOwners)
	}
	open()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
	if err != nil {
		t.Fatal(err)
	}
	if info.VectorStrategy != collections.VectorIndexStrategyNativeRuntime {
		t.Fatalf("strategy=%s", info.VectorStrategy)
	}
	docs := make([]Document, 4096)
	for i := range docs {
		docs[i] = Document{ID: fmt.Sprintf("doc-%04d", i), Content: fmt.Sprintf("content-%04d", i), Embedding: []float32{float32(i + 1), 1}, Meta: map[string]any{"tenant": fmt.Sprintf("tenant-%02d", i%16)}}
	}
	for start := 0; start < len(docs); start += 128 {
		if _, err := svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{Documents: docs[start : start+128], DeferVectorIndexRebuild: true}); err != nil {
			t.Fatalf("batch[%d:%d): %v", start, start+128, err)
		}
	}
	if _, err := svc.OptimizeIndex(ctx, name, OptimizeIndexRequest{}); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		filter := &Filter{Field: "meta.tenant", Operator: "==", Value: "tenant-00"}
		verify := func(docs []Document) {
			t.Helper()
			if len(docs) != 10 {
				t.Fatalf("documents=%d", len(docs))
			}
			for _, doc := range docs {
				if doc.Meta["tenant"] != "tenant-00" || len(doc.Embedding) != 0 {
					t.Fatalf("filtered/projected document=%+v", doc)
				}
			}
		}
		keyword, err := svc.SearchKeyword(ctx, name, KeywordSearchRequest{Query: "content", TopK: 10, Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		verify(keyword.Documents)
		if keyword.Stats.FullDocumentScanFallbacks != 0 {
			t.Fatal("keyword scan fallback")
		}
		dense, err := svc.SearchDenseVector(ctx, name, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 1}, TopK: 10, EfSearch: 100, Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		verify(dense.Documents)
		if dense.Exact {
			t.Fatal("dense exact fallback")
		}
		hybrid, err := svc.SearchHybrid(ctx, name, HybridSearchRequest{Query: "content", QueryEmbedding: []float32{1, 1}, TopK: 10, TextCandidateLimit: 64, VectorCandidateLimit: 64, EfSearch: 100, Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		verify(hybrid.Documents)
	}
	check()
	closeOwners()
	if t.Failed() {
		return
	}
	open()
	check()
	closeOwners()
}
