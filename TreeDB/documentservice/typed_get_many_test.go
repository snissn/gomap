package documentservice

import (
	"bytes"
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
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
	beforeCurrentCache := workstats.Read().RowIndexCache
	beforeOutput := workstats.Output.GetMany.Read()
	beforeMaterialization := workstats.Output.Materialization.Read()
	current, err := s.FetchTypedDocuments(ctx, info.Name, info.Generation, ids)
	if err != nil || !bytes.Contains(current.Results[0].Document, []byte(`"new"`)) {
		t.Fatalf("current=%+v %v", current, err)
	}
	afterOutput := workstats.Output.GetMany.Read()
	afterMaterialization := workstats.Output.Materialization.Read()
	if afterOutput.Attempts-beforeOutput.Attempts != 1 || afterOutput.Completed-beforeOutput.Completed != 1 || afterOutput.Fetched-beforeOutput.Fetched != 2 || afterOutput.Missing-beforeOutput.Missing != 1 || afterOutput.Requested-beforeOutput.Requested != 3 || afterOutput.OutputBytes-beforeOutput.OutputBytes != current.Stats.OutputBytes || afterMaterialization.Fetched-beforeMaterialization.Fetched != 2 {
		t.Fatalf("GetMany work before=%+v after=%+v materialization=%+v", beforeOutput, afterOutput, afterMaterialization)
	}
	// No graph Ensure/Build has occurred. Fresh ordinary service read views
	// reuse immutable offsets while retaining current locator visibility.
	beforeCache := workstats.Read().RowIndexCache
	// The first fetch of this new row asset observes actual memo eligibility.
	// Collection controls independently check the opened file's identity.valid.
	cacheEligible := beforeCache.Hits > beforeCurrentCache.Hits || beforeCache.Misses > beforeCurrentCache.Misses
	if beforeCache.Builds-beforeCurrentCache.Builds != 1 || beforeCache.RowsVisited-beforeCurrentCache.RowsVisited != 1 {
		t.Fatalf("initial fetch before=%+v after=%+v", beforeCurrentCache, beforeCache)
	}
	for range 3 {
		again, err := s.FetchTypedDocuments(ctx, info.Name, info.Generation, ids)
		if err != nil || !bytes.Equal(again.Results[0].Document, current.Results[0].Document) {
			t.Fatalf("repeated fetch=%+v err=%v", again, err)
		}
	}
	afterCache := s.DiagnosticsSnapshot(nil).Work.RowIndexCache
	wantBuilds, wantHits := uint64(3), uint64(0)
	if cacheEligible {
		wantBuilds, wantHits = 0, 3
	}
	if afterCache.Builds-beforeCache.Builds != wantBuilds || afterCache.RowsVisited-beforeCache.RowsVisited != wantBuilds || afterCache.Hits-beforeCache.Hits != wantHits || afterCache.Misses != beforeCache.Misses {
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
	beforeError := workstats.Output.GetMany.Read()
	if _, err := s.FetchTypedDocuments(canceled, info.Name, info.Generation, ids); err == nil {
		t.Fatal("canceled fetch accepted")
	}
	afterError := workstats.Output.GetMany.Read()
	if afterError.Errors-beforeError.Errors != 1 || afterError.Completed != beforeError.Completed || afterError.Fetched != beforeError.Fetched {
		t.Fatalf("failed GetMany before=%+v after=%+v", beforeError, afterError)
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
