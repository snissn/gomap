package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionJSONUpdateSkipsIndexStateAndSecondaryForUnchangedMultikeyValues(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
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

	before := jsonMultikeyRoots(t, d)
	jsonMultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	jsonMultikeyRequireIndexIDs(t, col, "tag", "b", "d1")

	matched, modified, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"tags":["a","b","a",null],"note":"same-effective-tags"}`), true, nil
	})
	if err != nil {
		t.Fatalf("update same effective JSON tags: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("same effective update matched=%v modified=%v want true/true", matched, modified)
	}
	afterSame := jsonMultikeyRoots(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("docs"),
		collectionSecondaryRootName("docs", "tag"),
	} {
		if afterSame[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for JSON replacement with unchanged effective multikey values", rootName, before[rootName], afterSame[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("docs"); afterSame[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified JSON replacement", rootName)
	}
	jsonMultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	jsonMultikeyRequireIndexIDs(t, col, "tag", "b", "d1")

	matched, modified, err = col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"tags":["a","c"],"note":"tag-changed"}`), true, nil
	})
	if err != nil {
		t.Fatalf("update changed JSON tags: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("changed tag update matched=%v modified=%v want true/true", matched, modified)
	}
	afterChanged := jsonMultikeyRoots(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("docs"),
		collectionSecondaryRootName("docs", "tag"),
	} {
		if afterChanged[rootName] == afterSame[rootName] {
			t.Fatalf("root %q did not change for JSON multikey value update", rootName)
		}
	}
	jsonMultikeyRequireIndexIDs(t, col, "tag", "a", "d1")
	jsonMultikeyRequireIndexIDs(t, col, "tag", "b")
	jsonMultikeyRequireIndexIDs(t, col, "tag", "c", "d1")
}

func jsonMultikeyRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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

func jsonMultikeyRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
