package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionJSONUpdateBatchSkipsUnchangedUniqueWhenNonUniqueChanges(t *testing.T) {
	const collectionName = "users"

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: collectionName,
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","note":"seed"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := jsonUpdateBatchUniqueSkipRoots(t, d, collectionName)
	jsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	jsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "hnl", "u1")

	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"sea","note":"city-changed"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch city-only JSON update: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("index unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("unique skips=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "city" || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("city secondary run stats=%+v want city delete+set with key bytes", run)
	}

	after := jsonUpdateBatchUniqueSkipRoots(t, d, collectionName)
	for _, rootName := range []string{
		collectionPrimaryRootName(collectionName),
		collectionIndexStateRootName(collectionName),
		collectionSecondaryRootName(collectionName, "city"),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for JSON city-only update", rootName)
		}
	}
	if rootName := collectionSecondaryRootName(collectionName, "email"); after[rootName] != before[rootName] {
		t.Fatalf("email root %q changed from %d to %d for unchanged unique email", rootName, before[rootName], after[rootName])
	}
	jsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	jsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "hnl")
	jsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "sea", "u1")
}

func jsonUpdateBatchUniqueSkipRoots(tb testing.TB, d *backenddb.DB, collectionName string) map[string]uint64 {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		tb.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		tb.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName(collectionName),
		collectionIndexStateRootName(collectionName),
		collectionSecondaryRootName(collectionName, "email"),
		collectionSecondaryRootName(collectionName, "city"),
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

func jsonUpdateBatchUniqueSkipRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	sort.Strings(got)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if len(got) != len(wantSorted) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}
