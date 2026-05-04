package collections

import (
	"sort"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionReplaceSkipsUnchangedSecondaryRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
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
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","seen":false}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}

	before := replaceMinimizationRoots(t, d)
	replaced, err := col.Replace([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl","seen":true}`))
	if err != nil {
		t.Fatalf("replace non-indexed field: %v", err)
	}
	if !replaced {
		t.Fatal("replace non-indexed field reported false")
	}
	afterSameIndexes := replaceMinimizationRoots(t, d)
	if rootName := collectionPrimaryRootName("users"); afterSameIndexes[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified replacement", rootName)
	}
	for _, rootName := range []string{
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if afterSameIndexes[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for replacement with unchanged indexed values", rootName, before[rootName], afterSameIndexes[rootName])
		}
	}
	replaceMinimizationRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	replaceMinimizationRequireIndexIDs(t, col, "city", "hnl", "u1")

	replaced, err = col.Replace([]byte("u1"), []byte(`{"email":"ada@example.com","city":"sea","seen":true}`))
	if err != nil {
		t.Fatalf("replace indexed city: %v", err)
	}
	if !replaced {
		t.Fatal("replace indexed city reported false")
	}
	afterCity := replaceMinimizationRoots(t, d)
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if afterCity[rootName] == afterSameIndexes[rootName] {
			t.Fatalf("root %q did not change for city replacement", rootName)
		}
	}
	if rootName := collectionSecondaryRootName("users", "email"); afterCity[rootName] != afterSameIndexes[rootName] {
		t.Fatalf("root %q changed from %d to %d for city-only replacement", rootName, afterSameIndexes[rootName], afterCity[rootName])
	}
	replaceMinimizationRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	replaceMinimizationRequireIndexIDs(t, col, "city", "hnl")
	replaceMinimizationRequireIndexIDs(t, col, "city", "sea", "u1")
}

func replaceMinimizationRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		tb.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		tb.Fatal("missing catalog")
	}
	names := []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	}
	out := make(map[string]uint64, len(names))
	for _, name := range names {
		rootID := catalog.rootID(name)
		if rootID == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
		out[name] = rootID
	}
	return out
}

func replaceMinimizationRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
	tb.Helper()
	ids, err := col.FindByIndexValue(indexName, value)
	if err != nil {
		tb.Fatalf("find index %s=%v: %v", indexName, value, err)
	}
	if len(ids) != len(want) {
		tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
	}
	got := make([]string, len(ids))
	for i := range ids {
		got[i] = string(ids[i])
	}
	wantSorted := append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(wantSorted)
	for i := range wantSorted {
		if got[i] != wantSorted[i] {
			tb.Fatalf("index %s=%v ids=%q want %q", indexName, value, ids, want)
		}
	}
}
