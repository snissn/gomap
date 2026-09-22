package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionJSONUpdateTreatsMissingAndNullIndexValuesAsUnchanged(t *testing.T) {
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
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "deleted_at", Field: "deleted_at", ValueType: IndexValueString},
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
		[][]byte{[]byte(`{"city":"hnl","note":"seed"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := jsonNullMissingRoots(t, d)
	jsonNullMissingRequireIndexIDs(t, col, "city", "hnl", "u1")
	jsonNullMissingRequireIndexIDs(t, col, "deleted_at", "2026-05-03")

	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"city":"hnl","deleted_at":null,"note":"explicit-null"}`), true, nil
	})
	if err != nil {
		t.Fatalf("update missing deleted_at to null: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("missing-to-null update matched=%v modified=%v want true/true", matched, modified)
	}
	afterNull := jsonNullMissingRoots(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "city"),
		collectionSecondaryRootName("users", "deleted_at"),
	} {
		if afterNull[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for JSON missing-to-null index value update", rootName, before[rootName], afterNull[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterNull[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified JSON replacement", rootName)
	}
	jsonNullMissingRequireIndexIDs(t, col, "city", "hnl", "u1")
	jsonNullMissingRequireIndexIDs(t, col, "deleted_at", "2026-05-03")

	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"city":"hnl","deleted_at":"2026-05-03","note":"deleted-at-set"}`), true, nil
	})
	if err != nil {
		t.Fatalf("update deleted_at value: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("deleted_at value update matched=%v modified=%v want true/true", matched, modified)
	}
	afterValue := jsonNullMissingRoots(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "deleted_at"),
	} {
		if afterValue[rootName] == afterNull[rootName] {
			t.Fatalf("root %q did not change when JSON null became an indexed value", rootName)
		}
	}
	if rootName := collectionSecondaryRootName("users", "city"); afterValue[rootName] != afterNull[rootName] {
		t.Fatalf("city root %q changed from %d to %d for deleted_at-only JSON update", rootName, afterNull[rootName], afterValue[rootName])
	}
	jsonNullMissingRequireIndexIDs(t, col, "city", "hnl", "u1")
	jsonNullMissingRequireIndexIDs(t, col, "deleted_at", "2026-05-03", "u1")

	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"city":"hnl","note":"deleted-at-missing-again"}`), true, nil
	})
	if err != nil {
		t.Fatalf("update deleted_at value to missing: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("deleted_at missing update matched=%v modified=%v want true/true", matched, modified)
	}
	afterMissing := jsonNullMissingRoots(t, d)
	for _, rootName := range []string{
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "deleted_at"),
	} {
		if afterMissing[rootName] == afterValue[rootName] {
			t.Fatalf("root %q did not change when JSON indexed value became missing", rootName)
		}
	}
	if rootName := collectionSecondaryRootName("users", "city"); afterMissing[rootName] != afterValue[rootName] {
		t.Fatalf("city root %q changed from %d to %d for deleted_at removal", rootName, afterValue[rootName], afterMissing[rootName])
	}
	jsonNullMissingRequireIndexIDs(t, col, "city", "hnl", "u1")
	jsonNullMissingRequireIndexIDs(t, col, "deleted_at", "2026-05-03")
}

func jsonNullMissingRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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
		collectionSecondaryRootName("users", "city"),
		collectionSecondaryRootName("users", "deleted_at"),
	}
	roots := make(map[string]uint64, len(names))
	for _, name := range names {
		roots[name] = catalog.rootID(name)
	}
	for _, name := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if roots[name] == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
	}
	return roots
}

func jsonNullMissingRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
