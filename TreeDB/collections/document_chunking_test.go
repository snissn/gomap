package collections

import (
	"encoding/json"
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
		Indexes: []IndexDefinition{{
			Name:      "by_kind",
			Field:     chunking.MetaFieldKind,
			ValueType: IndexValueString,
		}},
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

func parentDoc(t *testing.T, title, body string, embedding []float64) []byte {
	t.Helper()
	doc := map[string]any{"title": title, "body": body}
	if embedding != nil {
		doc["embedding"] = embedding
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
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
	}
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
	v1, err := col.IngestChunkedDocument([]byte(parent), parentDoc(t, "Rechunk Me", oldBody, []float64{1, 0, 0}), fixedWindowCfg(60, 12), ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("ingest v1: %v", err)
	}
	if len(v1.ChildIDs) < 2 {
		t.Fatalf("v1 children=%d want multiple", len(v1.ChildIDs))
	}

	newBody := "Completely different content about orchard machinery, tractor maintenance, " +
		"and seasonal harvest scheduling across the valley cooperatives this autumn."
	cfgV2 := chunking.Config{Strategy: chunking.StrategyRecursive, SizeUnit: chunking.SizeUnitRunes, Size: 80, Overlap: 16, Separators: chunking.DefaultSeparators()}
	v2, err := col.IngestChunkedDocument([]byte(parent), parentDoc(t, "Rechunk Me", newBody, []float64{0, 1, 0}), cfgV2, ChunkedIngestOptions{})
	if err != nil {
		t.Fatalf("ingest v2: %v", err)
	}
	if len(v2.ChildIDs) == 0 {
		t.Fatal("no v2 children")
	}
	if v2.Replaced != len(v1.ChildIDs) {
		t.Fatalf("v2.Replaced=%d want %d", v2.Replaced, len(v1.ChildIDs))
	}

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
	v2Words := []string{"orchard", "tractor", "seasonal", "cooperatives"}
	for _, id := range v2.ChildIDs {
		raw, err := col.Get(id)
		if err != nil || len(raw) == 0 {
			t.Fatalf("new child %q missing: err=%v", id, err)
		}
		carriesNew := false
		for _, w := range v2Words {
			if strings.Contains(string(raw), w) {
				carriesNew = true
				break
			}
		}
		if !carriesNew {
			t.Fatalf("new child %q does not carry v2 content: %s", id, raw)
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
		name string
		id   []byte
	}{
		{name: "child namespace separator", id: []byte("unsafe#parent")},
		{name: "invalid UTF-8", id: []byte{0xff, 'p'}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, col := openChunkingTestCollection(t)
			if _, err := col.IngestChunkedDocument(tc.id, parentDoc(t, "unsafe", "body text", nil), fixedWindowCfg(8, 1), ChunkedIngestOptions{}); err == nil {
				t.Fatalf("IngestChunkedDocument(%x) succeeded", tc.id)
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
		if !strings.Contains(body, title) {
			t.Fatalf("parent title=%q does not match child %q body=%q", title, id, body)
		}
	}
}
