package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
	"github.com/snissn/gomap/TreeDB/collections/embedding"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func openChunkTestCollectionWithText(t *testing.T, dir string) (*backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	meta := &CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Fields:         []TextIndexField{{Field: "body"}},
			Analyzer:       TextAnalyzerSimple,
			StorePositions: true,
		}},
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		_ = d.Close()
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("open collection: %v", err)
	}
	return d, mgr, col
}

func TestOmitParentTextFieldAvoidsDoubleIndexing(t *testing.T) {
	dir := t.TempDir()
	d, _, col := openChunkTestCollectionWithText(t, dir)
	defer func() { _ = d.Close() }()

	cfg := chunking.Config{
		Strategy: chunking.StrategyFixedWindow,
		SizeUnit: chunking.SizeUnitRunes,
		Size:     24,
		Overlap:  4,
	}
	sources := []SourceDocument{
		{
			ID:     []byte("doc-a"),
			Fields: map[string]any{"body": "quantum entanglement bell test alpha beta gamma delta"},
			Meta:   map[string]any{"repo": "physics", "ext": "md"},
		},
	}
	results, err := col.IngestChunkedDocuments(sources, cfg, ChunkedIngestOptions{
		TextField:           "body",
		OmitParentTextField: true,
	})
	if err != nil {
		t.Fatalf("IngestChunkedDocuments: %v", err)
	}
	if len(results) != 1 || len(results[0].ChildIDs) < 2 {
		t.Fatalf("expected >=2 children, got %+v", results)
	}

	// Parent document must still exist with metadata, but without "body".
	parentRaw, err := col.Get([]byte("doc-a"))
	if err != nil {
		t.Fatalf("Get parent: %v", err)
	}
	var parentObj map[string]any
	if err := json.Unmarshal(parentRaw, &parentObj); err != nil {
		t.Fatalf("unmarshal parent: %v", err)
	}
	if _, hasBody := parentObj["body"]; hasBody {
		t.Fatalf("parent document retained body when OmitParentTextField=true: %s", string(parentRaw))
	}
	if metaMap, ok := parentObj["meta"].(map[string]any); !ok || metaMap["repo"] != "physics" {
		t.Fatalf("parent meta missing: %s", string(parentRaw))
	}

	// Text search must return only chunk children (doc-a#N), never the parent ID "doc-a".
	resp, err := col.SearchText(TextSearchOptions{
		IndexName: "lexical",
		Query:     "quantum entanglement",
		TopK:      10,
	})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if len(resp.Results) == 0 {
		t.Fatalf("expected chunk hits for quantum entanglement")
	}
	for _, r := range resp.Results {
		if !strings.HasPrefix(string(r.DocumentID), "doc-a#") {
			t.Fatalf("expected only chunk children in SearchText, got %q", string(r.DocumentID))
		}
	}

	// Single-document IngestChunkedDocument must also honor OmitParentTextField.
	res2, err := col.IngestChunkedDocument(
		[]byte("doc-b"),
		[]byte(`{"body":"bohmian elastic electron carrier phase","meta":{"ext":"py"}}`),
		cfg,
		ChunkedIngestOptions{TextField: "body", OmitParentTextField: true},
	)
	if err != nil || len(res2.ChildIDs) < 2 {
		t.Fatalf("IngestChunkedDocument: res=%+v err=%v", res2, err)
	}
	p2Raw, err := col.Get([]byte("doc-b"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(p2Raw, []byte(`"body"`)) {
		t.Fatalf("single IngestChunkedDocument retained body: %s", string(p2Raw))
	}
}

func TestDeleteChunkedDocumentAndBatchReopen(t *testing.T) {
	dir := t.TempDir()
	d, _, col := openChunkTestCollectionWithText(t, dir)

	cfg := chunking.Config{
		Strategy: chunking.StrategyFixedWindow,
		SizeUnit: chunking.SizeUnitRunes,
		Size:     20,
		Overlap:  4,
	}
	sources := []SourceDocument{
		{ID: []byte("file-1"), Fields: map[string]any{"body": "alpha one alpha two alpha three alpha four"}},
		{ID: []byte("file-2"), Fields: map[string]any{"body": "beta one beta two beta three beta four"}},
		{ID: []byte("file-3"), Fields: map[string]any{"body": "gamma one gamma two gamma three gamma four"}},
	}
	res, err := col.IngestChunkedDocuments(sources, cfg, ChunkedIngestOptions{OmitParentTextField: true})
	if err != nil {
		t.Fatalf("IngestChunkedDocuments: %v", err)
	}
	f1Children := len(res[0].ChildIDs)
	f2Children := len(res[1].ChildIDs)

	// Invalid & duplicate parent IDs must fail closed without mutation.
	if _, err := col.DeleteChunkedDocument([]byte("bad#id")); err == nil {
		t.Fatal("expected invalid parent ID error")
	}
	if _, err := col.DeleteChunkedDocuments([][]byte{[]byte("file-1"), []byte("file-1")}); err == nil {
		t.Fatal("expected duplicate parent ID error")
	}

	// Delete file-1 via single API.
	deleted, err := col.DeleteChunkedDocument([]byte("file-1"))
	if err != nil {
		t.Fatalf("DeleteChunkedDocument: %v", err)
	}
	if deleted != f1Children+1 {
		t.Fatalf("deleted=%d want %d (parent + children)", deleted, f1Children+1)
	}
	// Deleting nonexistent parent is a clean 0 no-op.
	if n, err := col.DeleteChunkedDocument([]byte("nonexistent")); err != nil || n != 0 {
		t.Fatalf("nonexistent delete n=%d err=%v want 0,nil", n, err)
	}

	// Delete file-2 + nonexistent in one atomic batch.
	deletedBatch, err := col.DeleteChunkedDocuments([][]byte{[]byte("file-2"), []byte("does-not-exist")})
	if err != nil {
		t.Fatalf("DeleteChunkedDocuments: %v", err)
	}
	if deletedBatch != f2Children+1 {
		t.Fatalf("deletedBatch=%d want %d", deletedBatch, f2Children+1)
	}

	// Close and reopen to verify durable deletion across restart.
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d2, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()
	col2, err := NewCollectionManager(d2).OpenCollection("docs")
	if err != nil {
		t.Fatalf("reopen col: %v", err)
	}

	for _, gone := range []string{"file-1", "file-2"} {
		if raw, err := col2.Get([]byte(gone)); err != nil || len(raw) != 0 {
			t.Fatalf("expected %q gone after reopen, got len=%d err=%v", gone, len(raw), err)
		}
		if kids, err := col2.ChunkChildren([]byte(gone)); err != nil || len(kids) != 0 {
			t.Fatalf("expected %q children gone after reopen, got %d err=%v", gone, len(kids), err)
		}
	}
	if resp, err := col2.SearchText(TextSearchOptions{IndexName: "lexical", Query: "alpha", TopK: 5}); err != nil || len(resp.Results) != 0 {
		t.Fatalf("deleted file-1 still searchable: results=%+v err=%v", resp.Results, err)
	}
	if resp, err := col2.SearchText(TextSearchOptions{IndexName: "lexical", Query: "beta", TopK: 5}); err != nil || len(resp.Results) != 0 {
		t.Fatalf("deleted file-2 still searchable: results=%+v err=%v", resp.Results, err)
	}
	if resp, err := col2.SearchText(TextSearchOptions{IndexName: "lexical", Query: "gamma", TopK: 5}); err != nil || len(resp.Results) == 0 {
		t.Fatalf("surviving file-3 missing: results=%+v err=%v", resp.Results, err)
	}
}

func TestIngestSourcesOmitParentTextField(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "rag",
		TextIndexes: []TextIndexDefinition{{
			Name:     "lexical",
			Fields:   []TextIndexField{{Field: "body"}},
			Analyzer: TextAnalyzerSimple,
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "emb",
			Field:      "embedding",
			Dimensions: 8,
			Metric:     VectorMetricCosine,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("rag")
	if err != nil {
		t.Fatal(err)
	}
	res, err := col.IngestSources(context.Background(), []SourceDocument{{
		ID:     []byte("src-1"),
		Fields: map[string]any{"body": "one two three four five six seven eight nine ten"},
		Meta:   map[string]any{"kind": "doc"},
	}}, IngestSourcesConfig{
		Chunking: chunking.Config{
			Strategy: chunking.StrategyFixedWindow,
			SizeUnit: chunking.SizeUnitRunes,
			Size:     20,
			Overlap:  4,
		},
		Embedding:           embedding.Config{Provider: embedding.ProviderHashing, Dimensions: 8},
		VectorIndexName:     "emb",
		TextField:           "body",
		OmitParentTextField: true,
	})
	if err != nil || len(res.Ingested) != 1 {
		t.Fatalf("IngestSources: res=%+v err=%v", res, err)
	}
	parentRaw, err := col.Get([]byte("src-1"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(parentRaw, []byte(`"body"`)) {
		t.Fatalf("IngestSources parent retained body: %s", string(parentRaw))
	}
	resp, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "seven", TopK: 5})
	if err != nil || len(resp.Results) == 0 {
		t.Fatalf("SearchText: resp=%+v err=%v", resp, err)
	}
	for _, r := range resp.Results {
		if !strings.HasPrefix(string(r.DocumentID), "src-1#") {
			t.Fatalf("expected only chunk hit, got %q", string(r.DocumentID))
		}
	}
}

func BenchmarkDeleteChunkedDocument(b *testing.B) {
	dir := b.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:     "lexical",
			Fields:   []TextIndexField{{Field: "body"}},
			Analyzer: TextAnalyzerSimple,
		}},
	}); err != nil {
		b.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		b.Fatal(err)
	}
	cfg := chunking.Config{Strategy: chunking.StrategyFixedWindow, SizeUnit: chunking.SizeUnitRunes, Size: 32, Overlap: 4}
	src := SourceDocument{
		ID:     []byte("bench-doc"),
		Fields: map[string]any{"body": strings.Repeat("quantum bell state elastic carrier ", 8)},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		if _, err := col.IngestChunkedDocuments([]SourceDocument{src}, cfg, ChunkedIngestOptions{OmitParentTextField: true}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		n, err := col.DeleteChunkedDocument([]byte("bench-doc"))
		if err != nil || n < 2 {
			b.Fatalf("DeleteChunkedDocument n=%d err=%v", n, err)
		}
	}
}
