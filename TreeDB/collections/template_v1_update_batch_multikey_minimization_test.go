package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionTemplateV1UpdateBatchSkipsUnchangedMultikeySecondaryRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatTemplateV1,
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "tag", Field: "tags", ValueType: IndexValueString, MultiKey: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1")},
		[][]byte{templateV1UpdateBatchMultikeyDoc(t, []any{"b", nil, "a", "a"}, "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := templateV1UpdateBatchMultikeyRoots(t, d)
	templateV1UpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	templateV1UpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "b", "d1")

	sameTagsDoc := templateV1UpdateBatchMultikeyDoc(t, []any{"a", "b", "a", nil}, "same-effective-tags")
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("d1"),
		Update: func([]byte) ([]byte, bool, error) {
			return sameTagsDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch same effective tags: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("same effective results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same effective secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same effective index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("same effective index unchanged=%d want %d", got, want)
	}
	afterSame := templateV1UpdateBatchMultikeyRoots(t, d)
	for _, rootName := range []string{
		collectionTemplateRootName("docs"),
		collectionSecondaryRootName("docs", "tag"),
	} {
		if afterSame[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for template-v1 UpdateBatch with unchanged effective multikey values", rootName, before[rootName], afterSame[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("docs"); afterSame[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified template-v1 replacement", rootName)
	}

	changedTagsDoc := templateV1UpdateBatchMultikeyDoc(t, []any{"a", "c"}, "tag-changed")
	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("d1"),
		Update: func([]byte) ([]byte, bool, error) {
			return changedTagsDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch changed tags: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("changed tag results=%+v want one matched modified row", got)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("changed tag index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 0; got != want {
		t.Fatalf("changed tag index unchanged=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("changed tag secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "tag" || run.Deletes == 0 || run.Sets == 0 || run.KeyBytes == 0 {
		t.Fatalf("tag secondary run stats=%+v want tag delete/set work with key bytes", run)
	}
	afterChanged := templateV1UpdateBatchMultikeyRoots(t, d)
	if rootName := collectionSecondaryRootName("docs", "tag"); afterChanged[rootName] == afterSame[rootName] {
		t.Fatalf("tag root %q did not change for changed template-v1 multikey values", rootName)
	}
	if rootName := collectionTemplateRootName("docs"); afterChanged[rootName] != afterSame[rootName] {
		t.Fatalf("template root %q changed from %d to %d for same-shape tag update", rootName, afterSame[rootName], afterChanged[rootName])
	}
	templateV1UpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	templateV1UpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "b")
	templateV1UpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "c", "d1")
}

func templateV1UpdateBatchMultikeyDoc(tb *testing.T, tags []any, note string) []byte {
	tb.Helper()
	return mustTemplateV1Document(tb, []string{"tags", "note"}, []any{tags, note})
}

func templateV1UpdateBatchMultikeyRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		tb.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		tb.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("docs"),
		collectionTemplateRootName("docs"),
		collectionSecondaryRootName("docs", "tag"),
	}
	roots := make(map[string]uint64, len(names))
	for _, name := range names {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
		roots[name] = rootID
	}
	return roots
}

func templateV1UpdateBatchMultikeyRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	if len(ids) != len(want) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	for i := range want {
		if !bytes.Equal(ids[i], []byte(want[i])) {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}
