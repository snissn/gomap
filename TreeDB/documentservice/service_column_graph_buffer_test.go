package documentservice

import (
	"context"
	"encoding/json"
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
	result, err := svc.SearchKeyword(context.Background(), "docs", KeywordSearchRequest{Query: "durable", TopK: 2, ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("SearchKeyword after crash recovery: %v", err)
	}
	if got := documentIDs(result.Documents); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("SearchKeyword after crash recovery ids=%v want [a b]", got)
	}
	assertDocumentEmbeddings(t, result.Documents, map[string][]float32{"a": {1, 0}, "b": {0, 1}})
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
	if col.Meta().Options.DisableBufferedIndexedAsyncFlush {
		t.Fatal("column_graph service collection opted out of buffered async publication")
	}
	if !col.Meta().Options.BufferedIndexedAsyncFlush {
		t.Fatal("column_graph service collection did not enable buffered async publication")
	}
	if cfg := col.Meta().Options.ColumnStore; cfg == nil || cfg.RetainedPayload != collections.ColumnRetainedPayloadNonColumn || cfg.RetainedPayloadEncoding != collections.ColumnRetainedPayloadEncodingJSON {
		t.Fatalf("column_graph retained payload=%+v want non-column JSON", cfg)
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
	if before.PendingIndexedRawDocumentBytes == 0 {
		t.Fatal("before search read-your-writes overlay bytes=0 want positive")
	}
	if before.PendingIndexedFullDocumentRows != 2 {
		t.Fatalf("before search full-document overlay rows=%d want 2", before.PendingIndexedFullDocumentRows)
	}
	if before.PendingIndexedPublicationBytes == 0 {
		t.Fatal("before search retained compact publication bytes=0 want positive")
	}
	raw, err := col.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get buffered full document: %v", err)
	}
	var buffered Document
	if err := json.Unmarshal(raw, &buffered); err != nil {
		t.Fatalf("decode buffered full document: %v", err)
	}
	if buffered.ID != "a" || buffered.Content != "alpha first" || !reflect.DeepEqual(buffered.Embedding, []float32{1, 0}) {
		t.Fatalf("buffered full document=%+v", buffered)
	}

	result, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "alpha", TopK: 2})
	if err != nil {
		t.Fatalf("SearchKeyword: %v", err)
	}
	if got := documentIDs(result.Documents); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("SearchKeyword ids=%v want [a b]", got)
	}
	for _, doc := range result.Documents {
		if len(doc.Embedding) != 0 {
			t.Fatalf("SearchKeyword without embedding returned %q embedding=%v", doc.ID, doc.Embedding)
		}
	}
	after := manager.StatsSnapshot()
	if after.IndexedFlushCalls != 1 || after.IndexedFlushDocs != 2 || after.PendingRootRuns != 0 {
		t.Fatalf("after search flush_calls=%d flush_docs=%d pending_root_runs=%d, want 1/2/0", after.IndexedFlushCalls, after.IndexedFlushDocs, after.PendingRootRuns)
	}
	if after.PendingIndexedRawDocumentBytes != 0 || after.PendingIndexedPublicationBytes != 0 {
		t.Fatalf("after search retained raw/publication bytes=%d/%d want 0/0", after.PendingIndexedRawDocumentBytes, after.PendingIndexedPublicationBytes)
	}
	if after.PendingIndexedFullDocumentRows != 0 {
		t.Fatalf("after search full-document overlay rows=%d want 0", after.PendingIndexedFullDocumentRows)
	}
	withEmbedding, err := svc.SearchKeyword(ctx, "docs", KeywordSearchRequest{Query: "alpha", TopK: 2, ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("SearchKeyword with embedding: %v", err)
	}
	assertDocumentEmbeddings(t, withEmbedding.Documents, map[string][]float32{"a": {1, 0}, "b": {0, 1}})
	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(collections.ColumnRetainedPayloadCollectionAuditOptions{})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || audit.CheckedRows != 2 || audit.RetainedPayloadPolicy != collections.ColumnRetainedPayloadNonColumn || audit.RetainedPayloadEncoding != string(collections.ColumnRetainedPayloadEncodingJSON) || len(audit.Violations) != 0 || len(audit.Errors) != 0 {
		t.Fatalf("unexpected retained payload audit=%+v", audit)
	}
	replacement := Document{ID: "a", Content: "alpha replaced", Embedding: []float32{-1, 0}}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{replacement}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("replace document: %v", err)
	}
	if _, err := svc.DeleteDocuments(ctx, "docs", DeleteDocumentsRequest{IDs: []string{"b"}}); err != nil {
		t.Fatalf("delete document: %v", err)
	}
	if err := manager.FlushAll(); err != nil {
		t.Fatalf("FlushAll after replace/delete: %v", err)
	}
	mutated, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("FilterDocuments after replace/delete: %v", err)
	}
	if len(mutated.Documents) != 1 || mutated.Documents[0].ID != "a" || mutated.Documents[0].Content != replacement.Content || !reflect.DeepEqual(mutated.Documents[0].Embedding, replacement.Embedding) {
		t.Fatalf("FilterDocuments after replace/delete=%+v", mutated)
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
	reopened, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("FilterDocuments after reopen: %v", err)
	}
	if got := documentIDs(reopened.Documents); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("FilterDocuments after reopen ids=%v want [a]", got)
	}
	if reopened.Documents[0].Content != replacement.Content {
		t.Fatalf("FilterDocuments after reopen content=%q want %q", reopened.Documents[0].Content, replacement.Content)
	}
	assertDocumentEmbeddings(t, reopened.Documents, map[string][]float32{"a": {-1, 0}})
}

func TestServiceColumnGraphBufferedUpsertSameID(t *testing.T) {
	db, err := backenddb.Open(testBackendOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	manager := collections.NewCollectionManager(db)
	svc := New(manager)
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name:               "docs",
		Dimension:          2,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	for _, doc := range []Document{
		{ID: "a", Content: "first", Embedding: []float32{1, 0}},
		{ID: "a", Content: "replacement", Embedding: []float32{0, 1}},
	} {
		if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{doc}, DeferVectorIndexRebuild: true}); err != nil {
			t.Fatalf("UpsertDocuments(%q): %v", doc.Content, err)
		}
	}
	got, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("FilterDocuments: %v", err)
	}
	if len(got.Documents) != 1 || got.Documents[0].Content != "replacement" || !reflect.DeepEqual(got.Documents[0].Embedding, []float32{0, 1}) {
		t.Fatalf("FilterDocuments=%+v want latest full document", got)
	}
}

func assertDocumentEmbeddings(t *testing.T, docs []Document, want map[string][]float32) {
	t.Helper()
	for _, doc := range docs {
		if !reflect.DeepEqual(doc.Embedding, want[doc.ID]) {
			t.Fatalf("document %q embedding=%v want %v", doc.ID, doc.Embedding, want[doc.ID])
		}
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
	if got := col.Meta().Options.ColumnStore.RetainedPayload; got != collections.ColumnRetainedPayloadFull {
		t.Fatalf("legacy retained payload=%q want full", got)
	}

	svc := New(manager)
	if _, err := svc.CreateIndex(context.Background(), CreateIndexRequest{
		Name:               "docs",
		Dimension:          2,
		Metric:             MetricCosine,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph},
	}); err != nil {
		t.Fatalf("idempotent CreateIndex with legacy async metadata: %v", err)
	}
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

func TestServiceColumnGraphScalarIndexesRetainFullJSON(t *testing.T) {
	db, err := backenddb.Open(testBackendOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	manager := collections.NewCollectionManager(db)
	svc := New(manager)
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name:      "docs",
		Dimension: 2,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
		},
		ScalarFields: []ScalarFieldDeclaration{{Field: "meta.kind", ValueType: ScalarFieldString}},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if got := col.Meta().Options.ColumnStore.RetainedPayload; got != collections.ColumnRetainedPayloadFull {
		t.Fatalf("scalar-index retained payload=%q want full", got)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{
		Documents:               []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0}, Meta: map[string]any{"kind": "kept"}}},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	filtered, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{Filter: &Filter{Field: "meta.kind", Operator: "==", Value: "kept"}, ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("FilterDocuments: %v", err)
	}
	if len(filtered.Documents) != 1 || filtered.Documents[0].ID != "a" || !reflect.DeepEqual(filtered.Documents[0].Embedding, []float32{1, 0}) {
		t.Fatalf("FilterDocuments=%+v", filtered)
	}
}

func TestServiceColumnGraphCohere768RetainedPayload(t *testing.T) {
	db, err := backenddb.Open(testBackendOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	manager := collections.NewCollectionManager(db)
	svc := New(manager)
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name:               "docs",
		Dimension:          768,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	embedding := make([]float32, 768)
	for i := range embedding {
		embedding[i] = float32((i%31)-15) / 16
	}
	document := Document{ID: "cohere-0", Content: "cohere shaped retained payload", Embedding: embedding, Meta: map[string]any{"source": "test"}}
	fullJSON, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("marshal full document: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{document}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	if err := manager.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(collections.ColumnRetainedPayloadCollectionAuditOptions{})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || audit.CheckedRows != 1 || audit.RetainedPayloadBytes <= 0 || audit.RetainedPayloadBytes*5 >= int64(len(fullJSON)) {
		t.Fatalf("retained payload did not shrink by >80%%: retained=%d full=%d audit=%+v", audit.RetainedPayloadBytes, len(fullJSON), audit)
	}
	withoutEmbedding, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{})
	if err != nil {
		t.Fatalf("FilterDocuments without embedding: %v", err)
	}
	if len(withoutEmbedding.Documents) != 1 || len(withoutEmbedding.Documents[0].Embedding) != 0 || withoutEmbedding.Documents[0].Content != document.Content {
		t.Fatalf("FilterDocuments without embedding=%+v", withoutEmbedding)
	}
	withEmbedding, err := svc.FilterDocuments(ctx, "docs", FilterDocumentsRequest{ReturnEmbedding: true})
	if err != nil {
		t.Fatalf("FilterDocuments with embedding: %v", err)
	}
	if len(withEmbedding.Documents) != 1 || !reflect.DeepEqual(withEmbedding.Documents[0].Embedding, embedding) {
		t.Fatalf("FilterDocuments with embedding=%+v", withEmbedding)
	}
}

func TestServiceColumnGraphAsyncMetadataPublishesInBackground(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	manager := collections.NewCollectionManager(db)
	col := createExplicitAsyncColumnGraphCollection(t, manager, 1)
	svc := New(manager)
	want := Document{ID: "a", Content: "durable alpha", Embedding: []float32{1, 0}}
	if _, err := svc.UpsertDocuments(context.Background(), "docs", UpsertDocumentsRequest{
		Documents:               []Document{want},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	raw, err := col.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get while async publication may be active: %v", err)
	}
	var got Document
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode document while async publication may be active: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("document while async publication may be active=%+v want %+v", got, want)
	}
	if err := manager.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if stats := manager.StatsSnapshot(); stats.IndexedAsyncFlushScheduled == 0 || stats.IndexedAsyncFlushErrors != 0 || stats.IndexedFlushDocs != 1 {
		t.Fatalf("async stats=%+v, want one successful background flush", stats)
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
	return createColumnGraphCollection(t, manager, maxDocuments, false, collections.ColumnRetainedPayloadFull)
}

func createExplicitAsyncColumnGraphCollection(t *testing.T, manager *collections.CollectionManager, maxDocuments int) *collections.Collection {
	return createColumnGraphCollection(t, manager, maxDocuments, true, collections.ColumnRetainedPayloadNonColumn)
}

func createColumnGraphCollection(t *testing.T, manager *collections.CollectionManager, maxDocuments int, bufferedAsync bool, retainedPayload collections.ColumnRetainedPayloadPolicy) *collections.Collection {
	t.Helper()
	columnStore := serviceColumnStoreConfig(2)
	columnStore.RetainedPayload = retainedPayload
	_, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "docs",
		Options: collections.CollectionOptions{
			DocumentFormat:                   collections.DocumentFormatJSON,
			ColumnStore:                      columnStore,
			BufferedIndexedWriteMaxDocuments: maxDocuments,
			BufferedIndexedAsyncFlush:        bufferedAsync,
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
