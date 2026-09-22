package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONUpdateTreatsMissingAndNullIndexValuesAsUnchanged(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatBSON,
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
		[][]byte{bsonNullMissingDoc(t, "u1", "hnl", nil, false, "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := bsonNullMissingRoots(t, d)
	bsonNullMissingRequireIndexIDs(t, col, "city", "hnl", "u1")
	bsonNullMissingRequireIndexIDs(t, col, "deleted_at", "2026-05-03")

	nullDoc := bsonNullMissingDoc(t, "u1", "hnl", nil, true, "explicit-null")
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return nullDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update missing deleted_at to null: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("missing-to-null update matched=%v modified=%v want true/true", matched, modified)
	}
	afterNull := bsonNullMissingRoots(t, d)
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "city"),
		collectionSecondaryRootName("users", "deleted_at"),
	} {
		if afterNull[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for BSON missing-to-null index value update", rootName, before[rootName], afterNull[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterNull[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified BSON replacement", rootName)
	}
	bsonNullMissingRequireIndexIDs(t, col, "city", "hnl", "u1")
	bsonNullMissingRequireIndexIDs(t, col, "deleted_at", "2026-05-03")

	valueDoc := bsonNullMissingDoc(t, "u1", "hnl", "2026-05-03", true, "deleted-at-set")
	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return valueDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update deleted_at value: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("deleted_at value update matched=%v modified=%v want true/true", matched, modified)
	}
	afterValue := bsonNullMissingRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "deleted_at"); afterValue[rootName] == afterNull[rootName] {
		t.Fatalf("deleted_at root %q did not change when BSON null became an indexed value", rootName)
	}
	if rootName := collectionSecondaryRootName("users", "city"); afterValue[rootName] != afterNull[rootName] {
		t.Fatalf("city root %q changed from %d to %d for deleted_at-only BSON update", rootName, afterNull[rootName], afterValue[rootName])
	}
	bsonNullMissingRequireIndexIDs(t, col, "city", "hnl", "u1")
	bsonNullMissingRequireIndexIDs(t, col, "deleted_at", "2026-05-03", "u1")

	missingDoc := bsonNullMissingDoc(t, "u1", "hnl", nil, false, "deleted-at-missing-again")
	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return missingDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update deleted_at value to missing: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("deleted_at missing update matched=%v modified=%v want true/true", matched, modified)
	}
	afterMissing := bsonNullMissingRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "deleted_at"); afterMissing[rootName] == afterValue[rootName] {
		t.Fatalf("deleted_at root %q did not change when BSON indexed value became missing", rootName)
	}
	if rootName := collectionSecondaryRootName("users", "city"); afterMissing[rootName] != afterValue[rootName] {
		t.Fatalf("city root %q changed from %d to %d for deleted_at removal", rootName, afterValue[rootName], afterMissing[rootName])
	}
	bsonNullMissingRequireIndexIDs(t, col, "city", "hnl", "u1")
	bsonNullMissingRequireIndexIDs(t, col, "deleted_at", "2026-05-03")
}

func bsonNullMissingDoc(tb testing.TB, id, city string, deletedAt any, includeDeletedAt bool, note string) []byte {
	tb.Helper()
	doc := bson.D{
		{Key: "_id", Value: id},
		{Key: "city", Value: city},
		{Key: "note", Value: note},
	}
	if includeDeletedAt {
		doc = append(doc, bson.E{Key: "deleted_at", Value: deletedAt})
	}
	return mustBSONCollectionDocument(tb, doc)
}

func bsonNullMissingRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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
		collectionSecondaryRootName("users", "city"),
		collectionSecondaryRootName("users", "deleted_at"),
	}
	roots := make(map[string]uint64, len(names))
	for _, name := range names {
		roots[name] = catalog.rootID(name)
	}
	if roots[collectionPrimaryRootName("users")] == 0 {
		tb.Fatalf("primary root %q was not persisted", collectionPrimaryRootName("users"))
	}
	if roots[collectionSecondaryRootName("users", "city")] == 0 {
		tb.Fatalf("city root %q was not persisted", collectionSecondaryRootName("users", "city"))
	}
	return roots
}

func bsonNullMissingRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
