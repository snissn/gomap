package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionJSONDirectUpdateSkipsUnchangedUniqueWhenNonUniqueChanges(t *testing.T) {
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
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","note":"seed"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := jsonDirectUniqueSkipRoots(t, d)
	jsonDirectUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	jsonDirectUniqueSkipRequireIndexIDs(t, col, "city", "hnl", "u1")

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"email":"ada@example.com","city":"sea","note":"city-changed"}`), true, nil
	})
	if err != nil {
		t.Fatalf("direct JSON city-only update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched=%v modified=%v want true/true", matched, modified)
	}
	after := jsonDirectUniqueSkipRoots(t, d)
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for direct JSON city-only update", rootName)
		}
	}
	if rootName := collectionSecondaryRootName("users", "email"); after[rootName] != before[rootName] {
		t.Fatalf("email root %q changed from %d to %d for unchanged unique email", rootName, before[rootName], after[rootName])
	}
	jsonDirectUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	jsonDirectUniqueSkipRequireIndexIDs(t, col, "city", "hnl")
	jsonDirectUniqueSkipRequireIndexIDs(t, col, "city", "sea", "u1")
}

func jsonDirectUniqueSkipRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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

func jsonDirectUniqueSkipRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
