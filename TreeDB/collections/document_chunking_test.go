package collections

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections/chunking"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func openChunkingTestCollection(t *testing.T) (string, *backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	// Close before TempDir cleanup: Windows cannot unlink files that are
	// still open, which fails the test harness with a stale index.db handle.
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{
			{
				Name:      "by_kind",
				Field:     chunking.MetaFieldKind,
				ValueType: IndexValueString,
			},
			{
				Name:      "by_tenant",
				Field:     "meta.tenant",
				ValueType: IndexValueString,
			},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV1,
		Fields:         []TextIndexField{{Field: "body"}},
		StorePositions: true,
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	return dir, d, col
}

func openChunkingColumnTestCollection(t *testing.T) (string, *backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	t.Cleanup(func() { _ = d.Close() })
	cfg := &ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{
			{Name: "body", Path: "body", ValueType: ColumnStoreValueString, Nullable: true},
			{Name: chunking.MetaFieldParent, Path: chunking.MetaFieldParent, ValueType: ColumnStoreValueString, Nullable: true},
			{Name: chunking.MetaFieldOrdinal, Path: chunking.MetaFieldOrdinal, ValueType: ColumnStoreValueInt64, Nullable: true},
			{Name: chunking.MetaFieldKind, Path: chunking.MetaFieldKind, ValueType: ColumnStoreValueString, Nullable: true},
		},
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "chunk_column_docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("chunk_column_docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return dir, d, col
}

func parentDoc(t *testing.T, title, body string, embedding []float64) []byte {
	t.Helper()
	return parentDocWithMeta(t, title, body, embedding, nil)
}

func parentDocWithMeta(t *testing.T, title, body string, embedding []float64, meta map[string]any) []byte {
	t.Helper()
	doc := map[string]any{"title": title, "body": body}
	if embedding != nil {
		doc["embedding"] = embedding
	}
	if meta != nil {
		doc[ingestSourceMetaField] = meta
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func assertChildMetadata(t *testing.T, col *Collection, childIDs [][]byte, want map[string]any) {
	t.Helper()
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal wanted metadata: %v", err)
	}
	for _, id := range childIDs {
		raw, err := col.Get(id)
		if err != nil || len(raw) == 0 {
			t.Fatalf("Get child %q: len=%d err=%v", id, len(raw), err)
		}
		var child struct {
			Meta json.RawMessage `json:"meta"`
		}
		if err := json.Unmarshal(raw, &child); err != nil {
			t.Fatalf("decode child %q: %v", id, err)
		}
		if string(child.Meta) != string(wantJSON) {
			t.Fatalf("child %q meta=%s want %s", id, child.Meta, wantJSON)
		}
	}
}

func childIDsFor(t *testing.T, col *Collection, parentID string) []string {
	t.Helper()
	children, err := col.ChunkChildren([]byte(parentID))
	if err != nil {
		t.Fatalf("ChunkChildren(%q): %v", parentID, err)
	}
	ids := make([]string, 0, len(children))
	for _, id := range children {
		ids = append(ids, string(id))
	}
	return ids
}

func fixedWindowCfg(size, overlap int) chunking.Config {
	return chunking.Config{Strategy: chunking.StrategyFixedWindow, SizeUnit: chunking.SizeUnitRunes, Size: size, Overlap: overlap}
}

func TestIngestChunkedDocumentCreatesLinkedChildren(t *testing.T) {
	_, _, col := openChunkingTestCollection(t)
	body := strings.Repeat("alpha beta gamma delta epsilon zeta eta theta. ", 20)
	parent := "src-1"
	res, err := col.IngestChunkedDocument([]byte(parent), parentDoc(t, "Source One", body, nil), fixedWindowCfg(120, 24), ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("IngestChunkedDocument: %v", err)
	}
	if len(res.ChildIDs) < 2 {
		t.Fatalf("len(res.ChildIDs)=%d want multiple children", len(res.ChildIDs))
	}
	if string(res.ParentID()) != parent || len(childIDsFor(t, col, parent)) != len(res.ChildIDs) {
		t.Fatalf("result=%+v live children=%v", res, childIDsFor(t, col, parent))
	}
	for i, id := range res.ChildIDs {
		want := chunking.ChildDocumentID(parent, i)
		if string(id) != want {
			t.Fatalf("child %d ID=%q want %q", i, id, want)
		}
		raw, err := col.Get(id)
		if err != nil || len(raw) == 0 {
			t.Fatalf("Get(%q) len=%d err=%v", id, len(raw), err)
		}
		var stored map[string]any
		if err := json.Unmarshal(raw, &stored); err != nil {
			t.Fatalf("unmarshal child %q: %v", id, err)
		}
		if stored[chunking.MetaFieldParent] != parent ||
			stored[chunking.MetaFieldOrdinal] != float64(i) ||
			stored[chunking.MetaFieldKind] != chunking.KindChunk {
			t.Fatalf("child %q metadata=%+v", id, stored)
		}
		if _, ok := stored[ingestSourceMetaField]; ok {
			t.Fatalf("metadata-free child %q unexpectedly has meta: %+v", id, stored)
		}
		wantRaw, err := json.Marshal(map[string]any{
			"body":                    stored["body"],
			chunking.MetaFieldParent:  parent,
			chunking.MetaFieldOrdinal: i,
			chunking.MetaFieldKind:    chunking.KindChunk,
		})
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != string(wantRaw) {
			t.Fatalf("metadata-free child %q bytes=%s want %s", id, raw, wantRaw)
		}
	}
}

func TestIngestChunkedDocumentsValidatesAndPublishesOneTextGeneration(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	cfg := fixedWindowCfg(12, 0)
	before, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("stats before: %v", err)
	}
	duplicate := []SourceDocument{
		{ID: []byte("new"), Fields: map[string]any{"body": "must not be written"}},
		{ID: []byte("new"), Fields: map[string]any{"body": "also invalid batch"}},
	}
	if _, err := col.IngestChunkedDocuments(duplicate, cfg, ChunkedIngestOptions{}); err == nil || !strings.Contains(err.Error(), "duplicate parent ID") {
		t.Fatalf("duplicate batch error=%v", err)
	}
	if got, err := col.Get([]byte("new")); err != nil || got != nil {
		t.Fatalf("invalid batch mutated collection: document=%s err=%v", got, err)
	}
	afterInvalid, err := col.TextIndexStorageStats("lexical")
	if err != nil || afterInvalid.V2RootGeneration != before.V2RootGeneration {
		t.Fatalf("invalid batch changed text generation: before=%+v after=%+v err=%v", before, afterInvalid, err)
	}

	sources := []SourceDocument{
		{ID: []byte("second"), Fields: map[string]any{"body": strings.Repeat("batchbeta ", 5)}},
		{ID: []byte("first"), Fields: map[string]any{"body": strings.Repeat("batchalpha ", 10)}},
	}
	results, err := col.IngestChunkedDocuments(sources, cfg, ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("IngestChunkedDocuments: %v", err)
	}
	if len(results) != len(sources) {
		t.Fatalf("results=%d want %d", len(results), len(sources))
	}
	for i, result := range results {
		if string(result.ParentID()) != string(sources[i].ID) || len(result.ChildIDs) == 0 {
			t.Fatalf("result %d=%+v want input parent %q and children", i, result, sources[i].ID)
		}
		for ordinal, id := range result.ChildIDs {
			if want := chunking.ChildDocumentID(string(sources[i].ID), ordinal); string(id) != want {
				t.Fatalf("result %d child %d=%q want %q", i, ordinal, id, want)
			}
		}
	}
	after, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("stats after: %v", err)
	}
	if got := after.V2RootGeneration - before.V2RootGeneration; got != 1 {
		t.Fatalf("batch text generations=%d want one durable batch publication (before=%d after=%d)", got, before.V2RootGeneration, after.V2RootGeneration)
	}

	// The batch path must produce the same live text IDs as the parent plus its
	// deterministic children, and score-only execution must not fetch documents.
	search, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "batchalpha", TopK: 100, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if search.Stats.DocumentsFetched != 0 {
		t.Fatalf("score-only fetched %d documents", search.Stats.DocumentsFetched)
	}
	got := make([]string, 0, len(search.Results))
	for _, result := range search.Results {
		got = append(got, string(result.DocumentID))
	}
	sort.Strings(got)
	// The fixed-width fixture puts the complete token only in ordinal zero;
	// the parent and that child are the exact expected lexical hits.
	want := []string{"first", string(results[1].ChildIDs[0])}
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("exact batch text IDs=%v want %v", got, want)
	}

	oldChildren := append([][]byte(nil), results[1].ChildIDs...)
	replacement, err := col.IngestChunkedDocuments([]SourceDocument{{ID: []byte("first"), Fields: map[string]any{"body": "batchfresh"}}}, cfg, ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("replacement batch: %v", err)
	}
	if len(replacement) != 1 || replacement[0].Replaced != len(oldChildren) || len(replacement[0].ChildIDs) != 1 {
		t.Fatalf("replacement=%+v old children=%d", replacement, len(oldChildren))
	}
	for _, id := range oldChildren[1:] {
		if raw, err := col.Get(id); err != nil || raw != nil {
			t.Fatalf("stale child %q remains after replacement: %s err=%v", id, raw, err)
		}
	}
	oldSearch, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "batchalpha", TopK: 100, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil || len(oldSearch.Results) != 0 || oldSearch.Stats.DocumentsFetched != 0 {
		t.Fatalf("stale text search results=%+v err=%v", oldSearch, err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d2, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()
	col2, err := NewCollectionManager(d2).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open after reopen: %v", err)
	}
	fresh, err := col2.SearchText(TextSearchOptions{IndexName: "lexical", Query: "batchfresh", TopK: 100, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil || fresh.Stats.DocumentsFetched != 0 || len(fresh.Results) != 2 {
		t.Fatalf("reopened fresh score-only results=%+v err=%v", fresh, err)
	}
}

func TestIngestChunkedDocumentCopiesMetadataWithoutLinkageOverride(t *testing.T) {
	_, _, col := openChunkingTestCollection(t)
	meta := map[string]any{
		"tenant":                  "alpha",
		"workspace":               "red",
		chunking.MetaFieldParent:  "spoof-parent",
		chunking.MetaFieldOrdinal: 99,
		chunking.MetaFieldKind:    "spoof-kind",
	}
	parent := parentDocWithMeta(t, "Metadata", strings.Repeat("metadata inheritance body ", 12), nil, meta)
	var parentFields map[string]any
	if err := json.Unmarshal(parent, &parentFields); err != nil {
		t.Fatal(err)
	}
	parentFields[chunking.MetaFieldParent] = "spoof-top-level-parent"
	parent, err := json.Marshal(parentFields)
	if err != nil {
		t.Fatal(err)
	}
	result, err := col.IngestChunkedDocument([]byte("meta-source"), parent, fixedWindowCfg(48, 8), ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("IngestChunkedDocument: %v", err)
	}
	for i := range parent {
		parent[i] = 'x'
	}
	assertChildMetadata(t, col, result.ChildIDs, meta)
	for i, id := range result.ChildIDs {
		raw, err := col.Get(id)
		if err != nil {
			t.Fatalf("Get child %q: %v", id, err)
		}
		var child map[string]any
		if err := json.Unmarshal(raw, &child); err != nil {
			t.Fatalf("decode child %q: %v", id, err)
		}
		if child[chunking.MetaFieldParent] != "meta-source" ||
			child[chunking.MetaFieldOrdinal] != float64(i) ||
			child[chunking.MetaFieldKind] != chunking.KindChunk {
			t.Fatalf("child %q authoritative linkage=%+v", id, child)
		}
	}
}

func TestIngestChunkedDocumentRejectsMalformedMetadataBeforeMutation(t *testing.T) {
	_, _, col := openChunkingTestCollection(t)
	const parent = "meta-fail-closed"
	good := parentDocWithMeta(t, "Good", strings.Repeat("stable old body ", 8), nil, map[string]any{"tenant": "old"})
	first, err := col.IngestChunkedDocument([]byte(parent), good, fixedWindowCfg(32, 4), ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("initial ingest: %v", err)
	}
	if _, err := col.IngestChunkedDocument(
		[]byte(parent),
		[]byte(`{"body":"replacement must not land","meta":"not-an-object"}`),
		fixedWindowCfg(32, 4),
		ChunkedIngestOptions{},
	); err == nil {
		t.Fatal("non-object metadata accepted")
	}
	storedParent, err := col.Get([]byte(parent))
	if err != nil || string(storedParent) != string(good) {
		t.Fatalf("parent mutated after metadata rejection: got=%s err=%v", storedParent, err)
	}
	children, err := col.ChunkChildren([]byte(parent))
	if err != nil {
		t.Fatalf("ChunkChildren: %v", err)
	}
	if len(children) != len(first.ChildIDs) {
		t.Fatalf("children=%d want unchanged %d", len(children), len(first.ChildIDs))
	}
	assertChildMetadata(t, col, children, map[string]any{"tenant": "old"})
}

func TestIngestChunkedDocumentFailClosedBeforeMutation(t *testing.T) {
	_, _, col := openChunkingTestCollection(t)
	parent := "src-bad"
	badCfg := fixedWindowCfg(100, 100) // overlap == size must fail closed
	if _, err := col.IngestChunkedDocument([]byte(parent), parentDoc(t, "t", "some body text here", nil), badCfg, ChunkedIngestOptions{}); err == nil {
		t.Fatal("invalid overlap config accepted")
	}
	// Fail-closed: nothing was written.
	if raw, err := col.Get([]byte(parent)); err == nil && len(raw) > 0 {
		t.Fatal("parent written despite invalid config")
	}
	if got := childIDsFor(t, col, parent); len(got) != 0 {
		t.Fatalf("children written despite invalid config: %v", got)
	}
}

func TestValidateChunkChildDocumentRejectsMalformedMetadata(t *testing.T) {
	rawGood, _ := json.Marshal(map[string]any{
		"body":                    "text",
		chunking.MetaFieldParent:  "p1",
		chunking.MetaFieldOrdinal: 0,
		chunking.MetaFieldKind:    chunking.KindChunk,
	})
	if err := ValidateChunkChildDocument([]byte("p1#0"), rawGood); err != nil {
		t.Fatalf("valid child rejected: %v", err)
	}
	rawBad, _ := json.Marshal(map[string]any{
		"body":                    "text",
		chunking.MetaFieldParent:  "p1",
		chunking.MetaFieldOrdinal: 5,
		chunking.MetaFieldKind:    chunking.KindChunk,
	})
	if err := ValidateChunkChildDocument([]byte("p1#0"), rawBad); err == nil {
		t.Fatal("mismatched child metadata accepted")
	}
	rawPartial, _ := json.Marshal(map[string]any{
		"body":                   "text",
		chunking.MetaFieldParent: "p1",
	})
	if err := ValidateChunkChildDocument([]byte("unrelated"), rawPartial); err == nil {
		t.Fatal("partial chunk metadata accepted")
	}
}

func TestRechunkReplacesChildrenAcrossIndexes(t *testing.T) {
	dir, d, col := openChunkingTestCollection(t)
	parent := "src-r"
	oldBody := "refund policy details for vintage lamps and shipping timelines. " +
		"Warranty coverage spans two years from purchase date with proof of receipt."
	v1, err := col.IngestChunkedDocument([]byte(parent), parentDocWithMeta(t, "Rechunk Me", oldBody, []float64{1, 0, 0}, map[string]any{"tenant": "old", "revision": 1}), fixedWindowCfg(60, 12), ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("ingest v1: %v", err)
	}
	if len(v1.ChildIDs) < 2 {
		t.Fatalf("v1 children=%d want multiple", len(v1.ChildIDs))
	}
	assertChildMetadata(t, col, v1.ChildIDs, map[string]any{"tenant": "old", "revision": 1})
	oldTenant, err := col.FindByIndexValue("by_tenant", "old")
	if err != nil || len(oldTenant) != len(v1.ChildIDs)+1 {
		t.Fatalf("old tenant index=%d want parent+%d children err=%v", len(oldTenant), len(v1.ChildIDs), err)
	}

	newBody := "Completely different content about orchard machinery, tractor maintenance, " +
		"and seasonal harvest scheduling across the valley cooperatives this autumn."
	cfgV2 := chunking.Config{Strategy: chunking.StrategyRecursive, SizeUnit: chunking.SizeUnitRunes, Size: 80, Overlap: 16, Separators: chunking.DefaultSeparators()}
	v2, err := col.IngestChunkedDocument([]byte(parent), parentDocWithMeta(t, "Rechunk Me", newBody, []float64{0, 1, 0}, map[string]any{"tenant": "new", "revision": 2}), cfgV2, ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("ingest v2: %v", err)
	}
	if len(v2.ChildIDs) == 0 {
		t.Fatal("no v2 children")
	}
	if v2.Replaced != len(v1.ChildIDs) {
		t.Fatalf("v2.Replaced=%d want %d", v2.Replaced, len(v1.ChildIDs))
	}
	assertChildMetadata(t, col, v2.ChildIDs, map[string]any{"tenant": "new", "revision": 2})

	live := map[string]bool{}
	for _, id := range v2.ChildIDs {
		live[string(id)] = true
	}
	// Old children are tombstoned; every surviving child carries v2 content.
	for _, id := range v1.ChildIDs {
		raw, err := col.Get(id)
		if err != nil {
			t.Fatalf("Get old child %q: %v", id, err)
		}
		if len(raw) > 0 && !live[string(id)] {
			t.Fatalf("old child %q still live after re-chunk", id)
		}
	}
	for _, id := range v2.ChildIDs {
		raw, err := col.Get(id)
		if err != nil || len(raw) == 0 {
			t.Fatalf("new child %q missing: err=%v", id, err)
		}
		var stored struct {
			Body string `json:"body"`
		}
		if err := json.Unmarshal(raw, &stored); err != nil {
			t.Fatalf("decode new child %q: %v", id, err)
		}
		if stored.Body == "" || !strings.Contains(newBody, stored.Body) {
			t.Fatalf("new child %q body %q is not a non-empty span of v2 source", id, stored.Body)
		}
	}

	// Text index resolves only live children.
	search, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "orchard OR refund OR warranty", TopK: 50})
	hitOrchard := false
	for _, r := range search.Results {
		id := string(r.DocumentID)
		if _, _, isChild := chunking.ParseChildID(id); isChild && strings.HasPrefix(id, parent+"#") {
			if !live[id] {
				t.Fatalf("stale child %q resolved by text index after re-chunk", id)
			}
		}
		for _, term := range r.MatchedTerms {
			if strings.EqualFold(term, "orchard") {
				hitOrchard = true
			}
		}
	}
	if !hitOrchard {
		t.Fatalf("text index did not resolve any v2 content: %+v", search.Results)
	}

	// Scalar index resolves exactly the live children.
	byKind, err := col.FindByIndexValue("by_kind", chunking.KindChunk)
	if err != nil {
		t.Fatalf("FindByIndexValue: %v", err)
	}
	if len(byKind) != len(v2.ChildIDs) {
		t.Fatalf("scalar index resolved %d docs want %d (live children)", len(byKind), len(v2.ChildIDs))
	}
	oldTenant, err = col.FindByIndexValue("by_tenant", "old")
	if err != nil {
		t.Fatalf("FindByIndexValue old tenant: %v", err)
	}
	if len(oldTenant) != 0 {
		t.Fatalf("stale tenant scalar rows survived replacement: %q", oldTenant)
	}
	newTenant, err := col.FindByIndexValue("by_tenant", "new")
	if err != nil || len(newTenant) != len(v2.ChildIDs)+1 {
		t.Fatalf("new tenant index=%d want parent+%d children err=%v", len(newTenant), len(v2.ChildIDs), err)
	}

	// Exact vector search over the embedding field resolves only live documents;
	// only the parent carries an embedding in this fixture.
	vecHits, err := col.SearchVectorsExact([]float32{0, 1, 0}, VectorSearchOptions{Field: "embedding", TopK: 10})
	if err != nil {
		t.Fatalf("SearchVectorsExact: %v", err)
	}
	if len(vecHits) == 0 || string(vecHits[0].DocumentID) != parent {
		t.Fatalf("vector hits=%+v want parent first", vecHits)
	}

	// Reopen: replaced state survives cleanly.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	after := childIDsFor(t, reopenedCol, parent)
	if len(after) != len(v2.ChildIDs) {
		t.Fatalf("reopened children=%d want %d", len(after), len(v2.ChildIDs))
	}
	byKindAfter, err := reopenedCol.FindByIndexValue("by_kind", chunking.KindChunk)
	if err != nil {
		t.Fatalf("reopen FindByIndexValue: %v", err)
	}
	if len(byKindAfter) != len(v2.ChildIDs) {
		t.Fatalf("reopened scalar index resolved %d docs want %d", len(byKindAfter), len(v2.ChildIDs))
	}
	assertChildMetadata(t, reopenedCol, v2.ChildIDs, map[string]any{"tenant": "new", "revision": 2})
	oldTenantAfter, err := reopenedCol.FindByIndexValue("by_tenant", "old")
	if err != nil || len(oldTenantAfter) != 0 {
		t.Fatalf("reopened stale tenant rows=%q err=%v", oldTenantAfter, err)
	}
	newTenantAfter, err := reopenedCol.FindByIndexValue("by_tenant", "new")
	if err != nil || len(newTenantAfter) != len(v2.ChildIDs)+1 {
		t.Fatalf("reopened new tenant index=%d want parent+%d children err=%v", len(newTenantAfter), len(v2.ChildIDs), err)
	}
	searchAfter, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 50})
	if err != nil {
		t.Fatalf("reopen SearchText: %v", err)
	}
	for _, r := range searchAfter.Results {
		t.Fatalf("stale term %q resolved after re-chunk + reopen: %+v", r.DocumentID, r)
	}
}

func TestPartialMultiChildIngestReopenLeavesNoTornParent(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	parent := "src-partial"
	body := strings.Repeat("sentence number one hundred. ", 30)
	cfg := fixedWindowCfg(90, 18)
	full, err := col.buildChunkPlan([]byte(parent), parentDoc(t, "Partial", body, nil), cfg, ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("buildChunkPlan: %v", err)
	}
	if len(full.children) < 4 {
		t.Fatalf("plan children=%d want >=4", len(full.children))
	}
	// Simulate a partial multi-child ingest interrupted mid-flight: persist only
	// a prefix of the plan, then close.
	prefix := full.children[:len(full.children)/2]
	if _, err := col.InsertBatch(chunkPlanIDs(prefix), chunkPlanDocs(prefix)); err != nil {
		t.Fatalf("partial insert: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close after partial ingest: %v", err)
	}

	// Reopen: exactly the prefix must be present, each row complete — no torn rows.
	d2, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	col2, err := NewCollectionManager(d2).OpenCollection("docs")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	got := childIDsFor(t, col2, parent)
	if len(got) != len(prefix) {
		t.Fatalf("reopened children=%d want partial prefix %d", len(got), len(prefix))
	}
	for i, id := range got {
		want := chunking.ChildDocumentID(parent, i)
		if id != want {
			t.Fatalf("child %d = %q want %q", i, id, want)
		}
		raw, err := col2.Get([]byte(id))
		if err != nil || len(raw) == 0 {
			t.Fatalf("reopened child %q incomplete: err=%v", id, err)
		}
	}
	// Completing the ingest converges to the full plan.
	res, err := col2.IngestChunkedDocument([]byte(parent), parentDoc(t, "Partial", body, nil), cfg, ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("complete ingest: %v", err)
	}
	if len(res.ChildIDs) != len(full.children) || res.Replaced != len(prefix) {
		t.Fatalf("completion result children=%d replaced=%d want %d/%d",
			len(res.ChildIDs), res.Replaced, len(full.children), len(prefix))
	}
	if err := d2.Close(); err != nil {
		t.Fatalf("final close: %v", err)
	}
}

func TestFullChunkedIngestReopenAtomic(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	parent := "src-atomic"
	body := strings.Repeat("atomic batch durability sentence for reopen verification. ", 25)
	res, err := col.IngestChunkedDocument([]byte(parent), parentDoc(t, "Atomic", body, nil), fixedWindowCfg(96, 16), ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d2, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()
	col2, err := NewCollectionManager(d2).OpenCollection("docs")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	after := childIDsFor(t, col2, parent)
	if len(after) != len(res.ChildIDs) {
		t.Fatalf("after reopen children=%d want %d (all-or-nothing batch atomicity)", len(after), len(res.ChildIDs))
	}
}

func TestChunkedIngestRejectsUnsafeParentIDsBeforeMutation(t *testing.T) {
	tests := []struct {
		name   string
		id     []byte
		reason chunking.ParentIDErrorReason
	}{
		{name: "child namespace separator", id: []byte("unsafe#parent"), reason: chunking.ParentIDReservedSeparator},
		{name: "invalid UTF-8", id: []byte{0xff, 'p'}, reason: chunking.ParentIDInvalidUTF8},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, col := openChunkingTestCollection(t)
			_, err := col.IngestChunkedDocument(tc.id, parentDoc(t, "unsafe", "body text", nil), fixedWindowCfg(8, 1), ChunkedIngestOptions{})
			var idErr *chunking.ParentIDError
			if !errors.As(err, &idErr) || idErr.Reason != tc.reason {
				t.Fatalf("IngestChunkedDocument(%x) error=%v typed=%+v want reason %q", tc.id, err, idErr, tc.reason)
			}
			if got, err := col.Get(tc.id); err != nil {
				t.Fatalf("Get rejected parent: %v", err)
			} else if got != nil {
				t.Fatalf("rejected parent mutated collection: %s", got)
			}
		})
	}
}

func TestChunkedIngestRejectsReservedTextFieldBeforeMutation(t *testing.T) {
	_, _, col := openChunkingTestCollection(t)
	parentID := []byte("reserved-text")
	raw, err := json.Marshal(map[string]any{chunking.MetaFieldParent: "text that must not be overwritten"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.IngestChunkedDocument(parentID, raw, fixedWindowCfg(8, 1), ChunkedIngestOptions{TextField: chunking.MetaFieldParent}); err == nil {
		t.Fatal("reserved linkage field accepted as chunk text destination")
	}
	if got, err := col.Get(parentID); err != nil {
		t.Fatalf("Get rejected parent: %v", err)
	} else if got != nil {
		t.Fatalf("rejected parent mutated collection: %s", got)
	}
}

func TestChunkedIngestRejectsCrossCallParentChildCollision(t *testing.T) {
	_, _, col := openChunkingTestCollection(t)
	cfg := fixedWindowCfg(8, 1)
	if _, err := col.IngestChunkedDocument([]byte("parent"), parentDoc(t, "parent", "first parent body", nil), cfg, ChunkedIngestOptions{}); err != nil {
		t.Fatalf("seed parent: %v", err)
	}
	before, err := col.Get([]byte("parent#0"))
	if err != nil || before == nil {
		t.Fatalf("seed child: document=%s err=%v", before, err)
	}
	if _, err := col.IngestChunkedDocument([]byte("parent#0"), parentDoc(t, "collision", "second parent body", nil), cfg, ChunkedIngestOptions{}); err == nil {
		t.Fatal("child ID accepted as a parent in a separate ingest call")
	}
	after, err := col.Get([]byte("parent#0"))
	if err != nil {
		t.Fatalf("Get original child: %v", err)
	}
	if string(after) != string(before) {
		t.Fatalf("collision mutated original child\nbefore=%s\nafter=%s", before, after)
	}
}

func TestChunkedIngestRejectsExistingChildNamespaceRowBeforeMutation(t *testing.T) {
	_, _, col := openChunkingTestCollection(t)
	collision := []byte(`{"body":"ordinary row"}`)
	if _, err := col.Insert([]byte("parent#0"), collision); err != nil {
		t.Fatalf("seed ordinary namespace row: %v", err)
	}
	if _, err := col.IngestChunkedDocument(
		[]byte("parent"),
		[]byte(`{"body":"new parent body"}`),
		fixedWindowCfg(8, 1),
		ChunkedIngestOptions{},
	); err == nil {
		t.Fatal("ordinary child-namespace row accepted")
	}
	if got, err := col.Get([]byte("parent")); err != nil {
		t.Fatalf("Get rejected parent: %v", err)
	} else if got != nil {
		t.Fatalf("parent mutated before namespace rejection: %s", got)
	}
	if got, err := col.Get([]byte("parent#0")); err != nil {
		t.Fatalf("Get collision row: %v", err)
	} else if string(got) != string(collision) {
		t.Fatalf("collision row mutated: %s", got)
	}
}

func TestChunkedIngestSerializesSameParentAcrossCollectionHandles(t *testing.T) {
	_, d, first := openChunkingTestCollection(t)
	second, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open second collection handle: %v", err)
	}
	parentID := []byte("same-parent")
	cfg := fixedWindowCfg(12, 2)
	if _, err := first.IngestChunkedDocument(parentID, parentDoc(t, "seed", "seed content long enough to chunk", nil), cfg, ChunkedIngestOptions{}); err != nil {
		t.Fatalf("seed parent: %v", err)
	}

	firstAfterDelete := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondAfterDelete := make(chan struct{})
	firstOpts := ChunkedIngestOptions{hooks: &chunkedIngestHooks{afterDelete: func() {
		close(firstAfterDelete)
		<-releaseFirst
	}}}
	secondOpts := ChunkedIngestOptions{hooks: &chunkedIngestHooks{afterDelete: func() {
		close(secondAfterDelete)
	}}}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := first.IngestChunkedDocument(parentID, parentDoc(t, "first", strings.Repeat("first ", 12), nil), cfg, firstOpts)
		errs <- err
	}()
	<-firstAfterDelete
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := second.IngestChunkedDocument(parentID, parentDoc(t, "second", strings.Repeat("second ", 12), nil), cfg, secondOpts)
		errs <- err
	}()

	interleaved := false
	select {
	case <-secondAfterDelete:
		interleaved = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ingest: %v", err)
		}
	}
	if interleaved {
		t.Fatal("same-parent lifecycle reached child insertion concurrently")
	}

	parentRaw, err := first.Get(parentID)
	if err != nil {
		t.Fatalf("Get parent: %v", err)
	}
	var parent map[string]any
	if err := json.Unmarshal(parentRaw, &parent); err != nil {
		t.Fatalf("decode parent: %v", err)
	}
	children, err := first.ChunkChildren(parentID)
	if err != nil {
		t.Fatalf("ChunkChildren: %v", err)
	}
	for _, id := range children {
		raw, err := first.Get(id)
		if err != nil {
			t.Fatalf("Get child %q: %v", id, err)
		}
		var child map[string]any
		if err := json.Unmarshal(raw, &child); err != nil {
			t.Fatalf("decode child %q: %v", id, err)
		}
		title := parent["title"].(string)
		body := child["body"].(string)
		other := map[string]string{"first": "second", "second": "first"}[title]
		if strings.Contains(body, other) {
			t.Fatalf("parent title=%q conflicts with child %q body=%q", title, id, body)
		}
	}
}

func TestChunkChildrenPrefixBoundedStructuralEvidence(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) (*backenddb.DB, *Collection)
	}{
		{name: "ordinary JSON", open: func(t *testing.T) (*backenddb.DB, *Collection) {
			_, d, col := openChunkingTestCollection(t)
			return d, col
		}},
		{name: "reconstructable column store", open: func(t *testing.T) (*backenddb.DB, *Collection) {
			_, d, col := openChunkingColumnTestCollection(t)
			return d, col
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, col := tc.open(t)
			const unrelated = 10_000
			ids := make([][]byte, unrelated)
			docs := make([][]byte, unrelated)
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("unrelated-%06d", i))
				docs[i] = []byte(`{"body":"unrelated"}`)
			}
			if _, err := col.InsertBatch(ids, docs); err != nil {
				t.Fatalf("InsertBatch unrelated documents: %v", err)
			}
			result, err := col.IngestChunkedDocument(
				[]byte("target"),
				[]byte(`{"body":"target text repeated target text repeated target text repeated"}`),
				fixedWindowCfg(16, 4),
				ChunkedIngestOptions{},
			)
			if err != nil {
				t.Fatalf("IngestChunkedDocument target: %v", err)
			}
			children, stats, err := col.ChunkChildrenWithStats([]byte("target"))
			if err != nil {
				t.Fatalf("ChunkChildrenWithStats: %v", err)
			}
			if len(children) != len(result.ChildIDs) {
				t.Fatalf("children=%d want %d", len(children), len(result.ChildIDs))
			}
			if stats.ScannedPrimaryRows != len(children) {
				t.Fatalf("stats=%+v children=%d unrelated=%d", stats, len(children), unrelated)
			}
			if tc.name == "reconstructable column store" &&
				(stats.ReconstructedDocuments != len(children) ||
					stats.RowLocatorLookups != len(children) ||
					stats.PointRowFetches != len(children)) {
				t.Fatalf("column stats=%+v want %d bounded row lookups", stats, len(children))
			}
			if tc.name == "ordinary JSON" &&
				(stats.ReconstructedDocuments != 0 || stats.RowLocatorLookups != 0 || stats.PointRowFetches != 0) {
				t.Fatalf("ordinary JSON stats=%+v", stats)
			}
		})
	}
}

func TestChunkChildrenColumnStoreReopenPrefixParity(t *testing.T) {
	dir, d, col := openChunkingColumnTestCollection(t)
	result, err := col.IngestChunkedDocument(
		[]byte("reopen-target"),
		[]byte(`{"body":"reopen target body repeated reopen target body repeated","meta":{"tenant":"column-retained","workspace":"red"}}`),
		fixedWindowCfg(14, 3),
		ChunkedIngestOptions{},
	)
	if err != nil {
		t.Fatalf("IngestChunkedDocument: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d2 := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d2.Close() }()
	col2, err := NewCollectionManager(d2).OpenCollection("chunk_column_docs")
	if err != nil {
		t.Fatalf("OpenCollection after reopen: %v", err)
	}
	children, stats, err := col2.ChunkChildrenWithStats([]byte("reopen-target"))
	if err != nil {
		t.Fatalf("ChunkChildrenWithStats after reopen: %v", err)
	}
	if len(children) != len(result.ChildIDs) ||
		stats.ScannedPrimaryRows != len(children) ||
		stats.ReconstructedDocuments != len(children) ||
		stats.RowLocatorLookups != len(children) ||
		stats.PointRowFetches != len(children) {
		t.Fatalf("children=%d want=%d stats=%+v", len(children), len(result.ChildIDs), stats)
	}
	for i := range children {
		if string(children[i]) != string(result.ChildIDs[i]) {
			t.Fatalf("child %d=%q want %q", i, children[i], result.ChildIDs[i])
		}
	}
	// "meta" is intentionally not a declared column: the existing
	// retained-non-column capability must preserve it without widening the
	// column schema.
	assertChildMetadata(t, col2, children, map[string]any{"tenant": "column-retained", "workspace": "red"})
}

func TestChunkChildrenPrefixScanTruncationAndOrdinalGapFailClosed(t *testing.T) {
	_, _, col := openChunkingTestCollection(t)
	result, err := col.IngestChunkedDocument(
		[]byte("target"),
		[]byte(`{"body":"one two three four five six seven eight nine ten"}`),
		fixedWindowCfg(10, 2),
		ChunkedIngestOptions{},
	)
	if err != nil {
		t.Fatalf("IngestChunkedDocument: %v", err)
	}
	if len(result.ChildIDs) < 3 {
		t.Fatalf("children=%d want at least 3", len(result.ChildIDs))
	}
	truncated, stats, err := col.scanChunkDocumentsByParentPrefix([]byte("target#"), 1, func(DocumentRecord) (bool, error) {
		return true, nil
	})
	if err != nil || !truncated || stats.ScannedPrimaryRows != 1 {
		t.Fatalf("bounded prefix scan truncated=%t stats=%+v err=%v", truncated, stats, err)
	}
	if err := col.Delete(result.ChildIDs[1]); err != nil {
		t.Fatalf("Delete ordinal 1: %v", err)
	}
	if _, err := col.ChunkChildren([]byte("target")); err == nil || !strings.Contains(err.Error(), "ordinal gap 1") {
		t.Fatalf("ChunkChildren gap error=%v", err)
	}
}

func BenchmarkChunkChildrenBoundedUnrelatedParents(b *testing.B) {
	for _, columnStore := range []bool{false, true} {
		layout := "json"
		if columnStore {
			layout = "column"
		}
		for _, unrelated := range []int{10_000, 100_000} {
			b.Run(fmt.Sprintf("%s/%d", layout, unrelated), func(b *testing.B) {
				dir := b.TempDir()
				if columnStore {
					if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
						b.Fatal(err)
					}
				}
				var d *backenddb.DB
				var err error
				if columnStore {
					d = openCollectionCommandWALDB(b, dir)
				} else {
					d, err = backenddb.Open(backenddb.Options{Dir: dir})
					if err != nil {
						b.Fatal(err)
					}
				}
				defer func() { _ = d.Close() }()
				meta := &CollectionMeta{Name: "chunk_bench"}
				if columnStore {
					meta.Options = CollectionOptions{
						DocumentFormat: DocumentFormatJSON,
						ColumnStore: &ColumnStoreConfig{
							Enabled:         true,
							RetainedPayload: ColumnRetainedPayloadNonColumn,
							Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
							Columns: []ColumnStoreColumn{
								{Name: "body", Path: "body", ValueType: ColumnStoreValueString, Nullable: true},
								{Name: chunking.MetaFieldParent, Path: chunking.MetaFieldParent, ValueType: ColumnStoreValueString, Nullable: true},
								{Name: chunking.MetaFieldOrdinal, Path: chunking.MetaFieldOrdinal, ValueType: ColumnStoreValueInt64, Nullable: true},
								{Name: chunking.MetaFieldKind, Path: chunking.MetaFieldKind, ValueType: ColumnStoreValueString, Nullable: true},
							},
						},
					}
				}
				mgr := NewCollectionManager(d)
				if _, err := mgr.CreateCollection(meta); err != nil {
					b.Fatal(err)
				}
				col, err := mgr.OpenCollection("chunk_bench")
				if err != nil {
					b.Fatal(err)
				}
				ids := make([][]byte, unrelated)
				docs := make([][]byte, unrelated)
				for i := range ids {
					ids[i] = []byte(fmt.Sprintf("unrelated-%06d", i))
					docs[i] = []byte(`{"body":"unrelated"}`)
				}
				if _, err := col.InsertBatch(ids, docs); err != nil {
					b.Fatal(err)
				}
				result, err := col.IngestChunkedDocument(
					[]byte("target"),
					[]byte(`{"body":"target text repeated target text repeated target text repeated"}`),
					fixedWindowCfg(16, 4),
					ChunkedIngestOptions{},
				)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					children, stats, err := col.ChunkChildrenWithStats([]byte("target"))
					if err != nil || len(children) != len(result.ChildIDs) || stats.ScannedPrimaryRows != len(children) {
						b.Fatalf("children=%d stats=%+v err=%v", len(children), stats, err)
					}
				}
				b.ReportMetric(float64(unrelated), "unrelated_parents/op")
				b.ReportMetric(float64(len(result.ChildIDs)), "scanned_rows/op")
			})
		}
	}
}
