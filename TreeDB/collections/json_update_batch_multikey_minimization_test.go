package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionJSONUpdateBatchSkipsIndexStateAndSecondaryForUnchangedMultikeyValues(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
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
		[][]byte{[]byte(`{"tags":["b",null,"a","a"],"note":"seed"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := jsonUpdateBatchMultikeyRoots(t, d)
	jsonUpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	jsonUpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "b", "d1")

	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("d1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"tags":["a","b","a",null],"note":"same-effective-tags"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch same effective JSON tags: %v", err)
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
	afterSame := jsonUpdateBatchMultikeyRoots(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("docs"),
		collectionSecondaryRootName("docs", "tag"),
	} {
		if afterSame[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for JSON UpdateBatch with unchanged effective multikey values", rootName, before[rootName], afterSame[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("docs"); afterSame[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified JSON replacement", rootName)
	}

	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("d1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"tags":["a","c"],"note":"tag-changed"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch changed JSON tags: %v", err)
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
	afterChanged := jsonUpdateBatchMultikeyRoots(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("docs"),
		collectionSecondaryRootName("docs", "tag"),
	} {
		if afterChanged[rootName] == afterSame[rootName] {
			t.Fatalf("root %q did not change for JSON UpdateBatch multikey value update", rootName)
		}
	}
	jsonUpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	jsonUpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "b")
	jsonUpdateBatchMultikeyRequireIndexIDs(t, col, "tag", "c", "d1")
}

func jsonUpdateBatchMultikeyRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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
		collectionIndexStateRootName("docs"),
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

func jsonUpdateBatchMultikeyRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
