package documentservice

import (
	"bytes"
	"context"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
	"testing"
)

func TestTypedGetManyCapturedMetadataAndValues(t *testing.T) {
	s, db := newTestService(t)
	defer db.Close()
	defer s.Close()
	ctx := context.Background()
	info, err := s.CreateIndex(ctx, CreateIndexRequest{Name: "batch", Dimension: 2, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}})
	if err != nil {
		t.Fatal(err)
	}
	docs := []Document{{ID: "a", Content: "old", Embedding: []float32{1, 0}}}
	if _, err := s.UpsertDocuments(ctx, info.Name, UpsertDocumentsRequest{Documents: docs}); err != nil {
		t.Fatal(err)
	}
	col, _, err := s.openIndex(ctx, info.Name, info.Generation)
	if err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	meta, err := view.Meta()
	if err != nil {
		t.Fatal(err)
	}
	meta.Options.ColumnStore.Columns[0].Name = "caller mutation"
	if again, err := view.Meta(); err != nil || again.Options.ColumnStore.Columns[0].Name == "caller mutation" {
		t.Fatal("metadata aliases view")
	}
	docs[0].Content = "new"
	if _, err := s.UpsertDocuments(ctx, info.Name, UpsertDocumentsRequest{Documents: docs}); err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("a"), []byte("missing"), []byte("a")}
	held, err := fetchTypedDocumentsFromView(ctx, view, info.Generation, ids)
	if err != nil || len(held.Results) != 3 || !held.Results[0].Found || held.Results[1].Found || !bytes.Contains(held.Results[2].Document, []byte(`"old"`)) {
		t.Fatalf("held=%+v %v", held, err)
	}
	current, err := s.FetchTypedDocuments(ctx, info.Name, info.Generation, ids)
	if err != nil || !bytes.Contains(current.Results[0].Document, []byte(`"new"`)) {
		t.Fatalf("current=%+v %v", current, err)
	}
	// No graph Ensure/Build has occurred. Fresh ordinary service read views
	// reuse immutable offsets while retaining current locator visibility.
	beforeCache := workstats.Read().RowIndexCache
	for range 3 {
		again, err := s.FetchTypedDocuments(ctx, info.Name, info.Generation, ids)
		if err != nil || !bytes.Equal(again.Results[0].Document, current.Results[0].Document) {
			t.Fatalf("repeated fetch=%+v err=%v", again, err)
		}
	}
	afterCache := s.DiagnosticsSnapshot(nil).Work.RowIndexCache
	if afterCache.Builds != beforeCache.Builds || afterCache.Hits-beforeCache.Hits != 3 {
		t.Fatalf("fresh ordinary fetch before=%+v after=%+v", beforeCache, afterCache)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := view.Meta(); err == nil {
		t.Fatal("closed metadata accepted")
	}
	if !bytes.Contains(held.Results[0].Document, []byte(`"old"`)) {
		t.Fatal("owned response lost after close")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.FetchTypedDocuments(canceled, info.Name, info.Generation, ids); err == nil {
		t.Fatal("canceled fetch accepted")
	}
}

func TestTypedGetManyCapturedSchemaPublication(t *testing.T) {
	// Schema-only raw fixture: command-WAL deliberately rejects direct schema
	// operations without command frames. Mutation lifetime is covered above.
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := New(collections.NewCollectionManager(db))
	defer s.Close()
	ctx := context.Background()
	info, err := s.CreateIndex(ctx, CreateIndexRequest{Name: "schema", Dimension: 2, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}})
	if err != nil {
		t.Fatal(err)
	}
	col, _, err := s.openIndex(ctx, info.Name, 0)
	if err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	meta, err := view.Meta()
	if err != nil {
		t.Fatal(err)
	}
	def := meta.VectorIndexes[0]
	if _, err := col.DropVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FetchTypedDocuments(ctx, info.Name, info.Generation, nil); err == nil {
		t.Fatal("missing schema accepted")
	}
	if _, err := fetchTypedDocumentsFromView(ctx, view, info.Generation, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatal(err)
	}
	latest, err := indexInfoFromMeta(col.Meta())
	if err != nil {
		t.Fatal(err)
	}
	if latest.Generation == info.Generation {
		t.Fatal("generation did not advance")
	}
	if _, err := s.FetchTypedDocuments(ctx, info.Name, info.Generation, nil); ErrorCodeOf(err) != CodeIndexStale {
		t.Fatalf("stale generation accepted: %v", err)
	}
	if _, err := s.FetchTypedDocuments(ctx, info.Name, latest.Generation, nil); err != nil {
		t.Fatal(err)
	}
}
