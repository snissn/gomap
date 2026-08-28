package documentservice

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestServiceColumnGraphBufferedInsertCrashRecovery(t *testing.T) {
	const helper = "TREEDB_COLUMN_GRAPH_BUFFER_CRASH_HELPER"
	if dir := os.Getenv(helper); dir != "" {
		db, err := backenddb.Open(testBackendOptions(dir))
		if err != nil {
			t.Fatalf("open helper db: %v", err)
		}
		manager := collections.NewCollectionManager(db)
		svc := New(manager)
		ctx := context.Background()
		if _, err := svc.CreateIndex(ctx, CreateIndexRequest{
			Name:      "docs",
			Dimension: 2,
			VectorIndexOptions: &BenchmarkVectorIndexOptions{
				Strategy: collections.VectorIndexStrategyColumnGraph,
			},
		}); err != nil {
			t.Fatalf("CreateIndex: %v", err)
		}
		for _, doc := range []Document{
			{ID: "a", Content: "durable alpha", Embedding: []float32{1, 0}},
			{ID: "b", Content: "durable beta", Embedding: []float32{0, 1}},
		} {
			if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{doc}, DeferVectorIndexRebuild: true}); err != nil {
				t.Fatalf("UpsertDocuments(%s): %v", doc.ID, err)
			}
		}
		if stats := manager.StatsSnapshot(); stats.PendingRootRuns == 0 || stats.IndexedFlushCalls != 0 {
			t.Fatalf("helper writes were not buffered: %+v", stats)
		}
		os.Exit(0)
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestServiceColumnGraphBufferedInsertCrashRecovery$")
	cmd.Env = append(os.Environ(), helper+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()
	svc := New(collections.NewCollectionManager(db))
	defer func() {
		if err := svc.Close(); err != nil {
			t.Errorf("close service: %v", err)
		}
	}()
	result, err := svc.SearchKeyword(context.Background(), "docs", KeywordSearchRequest{Query: "durable", TopK: 2})
	if err != nil {
		t.Fatalf("SearchKeyword after crash recovery: %v", err)
	}
	if got := documentIDs(result.Documents); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("SearchKeyword after crash recovery ids=%v want [a b]", got)
	}
}

func TestServiceColumnGraphCoalescesBufferedInsertPublication(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Service, *collections.CollectionManager, *backenddb.DB) {
		db, err := backenddb.Open(testBackendOptions(dir))
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		manager := collections.NewCollectionManager(db)
		return New(manager), manager, db
	}

	ctx := context.Background()
	svc, manager, db := open()
	_, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name:      "docs",
		Dimension: 2,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if !col.Meta().Options.DisableBufferedIndexedAsyncFlush {
		t.Fatal("column_graph service collection did not select foreground buffered publication")
	}

	for _, doc := range []Document{
		{ID: "a", Content: "alpha first", Embedding: []float32{1, 0}},
		{ID: "b", Content: "alpha second", Embedding: []float32{0, 1}},
	} {
		if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{
			Documents:               []Document{doc},
			DeferVectorIndexRebuild: true,
		}); err != nil {
			t.Fatalf("UpsertDocuments(%s): %v", doc.ID, err)
		}
	}
	before := manager.StatsSnapshot()
	if before.PendingRootRuns == 0 || before.IndexedFlushCalls != 0 {
		t.Fatalf("before search pending_root_runs=%d indexed_flush_calls=%d, want pending and unpublished", before.PendingRootRuns, before.IndexedFlushCalls)
	}
	result, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "alpha", TopK: 2})
	if err != nil {
		t.Fatalf("SearchKeyword: %v", err)
	}
	if got := documentIDs(result.Documents); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("SearchKeyword ids=%v want [a b]", got)
	}
	after := manager.StatsSnapshot()
	if after.IndexedFlushCalls != 1 || after.IndexedFlushDocs != 2 || after.PendingRootRuns != 0 {
		t.Fatalf("after search flush_calls=%d flush_docs=%d pending_root_runs=%d, want 1/2/0", after.IndexedFlushCalls, after.IndexedFlushDocs, after.PendingRootRuns)
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	svc, _, db = open()
	defer func() {
		_ = svc.Close()
		_ = db.Close()
	}()
	reopened, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "alpha", TopK: 2})
	if err != nil {
		t.Fatalf("SearchKeyword after reopen: %v", err)
	}
	if got := documentIDs(reopened.Documents); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("SearchKeyword after reopen ids=%v want [a b]", got)
	}
}

func TestServiceColumnGraphCoalescesLegacyAsyncMetadata(t *testing.T) {
	db, err := backenddb.Open(testBackendOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	manager := collections.NewCollectionManager(db)
	col := createLegacyAsyncColumnGraphCollection(t, manager, 0)
	if !col.Meta().Options.BufferedIndexedAsyncFlush {
		t.Fatal("legacy metadata did not retain the historical async default")
	}

	svc := New(manager)
	for _, doc := range []Document{
		{ID: "a", Content: "alpha first", Embedding: []float32{1, 0}},
		{ID: "b", Content: "alpha second", Embedding: []float32{0, 1}},
	} {
		if _, err := svc.UpsertDocuments(context.Background(), "docs", UpsertDocumentsRequest{
			Documents:               []Document{doc},
			DeferVectorIndexRebuild: true,
		}); err != nil {
			t.Fatalf("UpsertDocuments(%s): %v", doc.ID, err)
		}
	}
	if stats := manager.StatsSnapshot(); stats.PendingRootRuns == 0 || stats.IndexedFlushCalls != 0 {
		t.Fatalf("legacy writes were not coalesced: %+v", stats)
	}
}

func TestServiceColumnGraphLegacyAsyncMetadataUsesForegroundThreshold(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	manager := collections.NewCollectionManager(db)
	createLegacyAsyncColumnGraphCollection(t, manager, 1)
	svc := New(manager)
	if _, err := svc.UpsertDocuments(context.Background(), "docs", UpsertDocumentsRequest{
		Documents:               []Document{{ID: "a", Content: "durable alpha", Embedding: []float32{1, 0}}},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	if err := manager.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if stats := manager.StatsSnapshot(); stats.IndexedAsyncFlushScheduled != 0 || stats.IndexedAsyncFlushErrors != 0 || stats.IndexedFlushDocs != 1 {
		t.Fatalf("legacy async stats=%+v, want one foreground flush", stats)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	db, err = backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()
	svc = New(collections.NewCollectionManager(db))
	defer func() {
		if err := svc.Close(); err != nil {
			t.Errorf("close service: %v", err)
		}
	}()
	result, err := svc.SearchKeyword(context.Background(), "docs", KeywordSearchRequest{Query: "durable", TopK: 1})
	if err != nil {
		t.Fatalf("SearchKeyword after reopen: %v", err)
	}
	if got := documentIDs(result.Documents); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("SearchKeyword after reopen ids=%v want [a]", got)
	}
}

func createLegacyAsyncColumnGraphCollection(t *testing.T, manager *collections.CollectionManager, maxDocuments int) *collections.Collection {
	t.Helper()
	_, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "docs",
		Options: collections.CollectionOptions{
			DocumentFormat:                   collections.DocumentFormatJSON,
			ColumnStore:                      serviceColumnStoreConfig(2),
			BufferedIndexedWriteMaxDocuments: maxDocuments,
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:             defaultVectorIndexName,
			Field:            defaultEmbeddingField,
			Metric:           collections.VectorMetricCosine,
			Dimensions:       2,
			Strategy:         collections.VectorIndexStrategyColumnGraph,
			SchemaGeneration: 1,
		}},
		TextIndexes: []collections.TextIndexDefinition{{
			Name:             defaultTextIndexName,
			Fields:           []collections.TextIndexField{{Field: defaultTextField}},
			Analyzer:         collections.TextAnalyzerSimple,
			StorePositions:   true,
			SchemaGeneration: 1,
		}},
	})
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}
