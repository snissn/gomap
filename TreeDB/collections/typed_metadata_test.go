package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestTypedMetadataBaseSuffixServingAndFold4769(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, typedGraphPublicTestOptions()); err != nil {
		t.Fatal(err)
	}
	// One ordinary full update supplies a scored suffix row beside seven base rows.
	suffix := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[1:2]}, {Name: "content", Strings: columns[1].Strings[1:2]}, {Name: "user", Strings: columns[2].Strings[1:2]}, {Name: "path", Strings: []string{"suffix"}}}
	if _, err := col.ReplaceTypedBatch(ids[1:2], retained[1:2], suffix); err != nil {
		t.Fatal(err)
	}
	query := VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 8, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeProduction}
	search := func(path string) (VectorIndexSearchResponse, *CollectionReadView) {
		t.Helper()
		q := query
		if path != "" {
			q.DeclaredScalarFilter = &HybridScalarFilter{IndexName: "path", Value: path}
		}
		var buffer VectorIndexSearchBuffer
		response, view, err := col.SearchVectorIndexWithBufferReadView(q, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		return response, view
	}
	before, pinned := search("")
	defer pinned.Close()
	warm, warmView := search("source")
	warmView.Close()
	if len(warm.Results) != 7 {
		t.Fatalf("warm=%+v", warm.Results)
	}
	state := col.typedGraphPublicationSnapshot()
	if len(state.rows) != 1 {
		t.Fatalf("suffix rows=%d", len(state.rows))
	}
	result, err := col.UpdateTypedMetadataByID(ids[:2], map[string]any{"meta.fpath": "allowed"}, nil, metadataGeneration4769(col))
	if err != nil || result != (TypedMetadataUpdateResult{2, 2}) {
		t.Fatalf("metadata=%+v err=%v", result, err)
	}
	next := col.typedGraphPublicationSnapshot()
	if len(next.rows) != 1 || &next.rows[0] != &state.rows[0] || (len(state.invNorms) > 0 && &next.invNorms[0] != &state.invNorms[0]) || next.lastMetadataGeneration == 0 {
		t.Fatal("metadata changed scoring state or missed frontier")
	}
	after, view := search("")
	view.Close()
	if !reflect.DeepEqual(before.Results, after.Results) {
		t.Fatalf("ranking changed: before=%+v after=%+v", before.Results, after.Results)
	}
	allowed, view := search("allowed")
	defer view.Close()
	if len(allowed.Results) != 2 {
		t.Fatalf("allowed=%+v", allowed.Results)
	}
	fetched, err := view.FetchDocumentsForVectorIndexSearchResults(allowed.Results, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range fetched.Results {
		if !bytes.Contains(row.Document, []byte(`"fpath":"allowed"`)) {
			t.Fatalf("stale fetch=%s", row.Document)
		}
	}
	old, err := pinned.FetchDocumentsByID(ids[:2], DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(old.Results[0].Document, []byte("allowed")) {
		t.Fatal("pinned reader changed")
	}
	view.Close()
	pinned.Close()
	if err := col.FoldColumnGraphServing(context.Background(), base.indexName); err != nil {
		t.Fatal(err)
	}
	if col.typedGraphPublicationSnapshot().lastMetadataGeneration != 0 {
		t.Fatal("fold retained metadata frontier")
	}
	allowed, view = search("allowed")
	view.Close()
	if len(allowed.Results) != 2 {
		t.Fatalf("fold filter=%+v", allowed.Results)
	}
	if _, err := col.UpdateTypedMetadataByID(ids[:2], map[string]any{"meta.fpath": "after-fold"}, nil, metadataGeneration4769(col)); err != nil {
		t.Fatal(err)
	}
	allowed, view = search("after-fold")
	view.Close()
	if len(allowed.Results) != 2 {
		t.Fatalf("after fold update=%+v", allowed.Results)
	}
}

func metadataGeneration4769(c *Collection) uint64 {
	var generation uint64
	meta := c.Meta()
	for _, d := range meta.VectorIndexes {
		generation = max(generation, d.SchemaGeneration)
	}
	for _, d := range meta.TextIndexes {
		generation = max(generation, d.SchemaGeneration)
	}
	return max(generation, 1)
}

func TestTypedMetadataUpdateNoVectorRewrite4769(t *testing.T) {
	dir, d, col := openTypedMinimaCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("a"), []byte("b")}
	retained := [][]byte{[]byte(`{"id":"a","meta":{"extra":{"flag":true}}}`), []byte(`{"id":"b","meta":{"extra":{"flag":true}}}`)}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"original a", "original b"}}, {Name: "user", Strings: []string{"old", "old"}}, {Name: "path", Strings: []string{"p", "p"}}}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	old, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	beforeRefs, err := old.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	generation := metadataGeneration4769(col)
	for _, user := range []string{"new", "newer"} {
		result, err := col.UpdateTypedMetadataByID(append(ids, []byte("missing")), map[string]any{"meta.user_id": user, "meta.extra.flag": false}, nil, generation)
		if err != nil || result != (TypedMetadataUpdateResult{2, 2}) {
			t.Fatalf("update=%+v err=%v", result, err)
		}
		view, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		_, err = view.visitDocumentScoringRowRefsByID(ids, func(id []byte, ref DocumentRowRef, found bool) error {
			i := 0
			if string(id) == "b" {
				i = 1
			}
			if !found || !reflect.DeepEqual(ref, beforeRefs.Results[i].RowRef) {
				t.Errorf("scoring authority changed: %+v vs %+v", ref, beforeRefs.Results[i].RowRef)
			}
			return nil
		})
		view.Close()
		if err != nil {
			t.Fatal(err)
		}
		for i, id := range ids {
			raw, err := col.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Content   string         `json:"content"`
				Embedding []float32      `json:"embedding"`
				Meta      map[string]any `json:"meta"`
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got.Content != columns[1].Strings[i] || !reflect.DeepEqual(got.Embedding, columns[0].Float32Vectors[i]) || got.Meta["user_id"] != user {
				t.Fatalf("changed nonmetadata or wrong metadata: %s", raw)
			}
		}
	}
	seq, root := dbCommitSeqAndSystemRoot(d)
	result, err := col.UpdateTypedMetadataByID(ids, map[string]any{"meta.user_id": "newer", "meta.extra.flag": false}, []string{"meta.absent"}, generation)
	if err != nil || result != (TypedMetadataUpdateResult{2, 0}) {
		t.Fatalf("noop=%+v err=%v", result, err)
	}
	if nextSeq, nextRoot := dbCommitSeqAndSystemRoot(d); nextSeq != seq || nextRoot != root {
		t.Fatal("no-op published")
	}
	oldResult, err := old.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range oldResult.Results {
		if !bytes.Contains(r.Document, []byte(`"user_id":"old"`)) {
			t.Fatalf("pinned metadata changed: %s", r.Document)
		}
	}
	old.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := col.Get(ids[0])
	if err != nil || !bytes.Contains(raw, []byte(`"user_id":"newer"`)) || !bytes.Contains(raw, []byte(`"original a"`)) {
		t.Fatalf("reopen=%s err=%v", raw, err)
	}
}
