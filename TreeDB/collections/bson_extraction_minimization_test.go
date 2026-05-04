package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONUpdateSkipsUnchangedDottedAndArraySecondaryRoots(t *testing.T) {
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
			{Name: "email", Field: "profile.email", ValueType: IndexValueString, Unique: true},
			{Name: "tag", Field: "tags", ValueType: IndexValueString, MultiKey: true},
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
		[][]byte{bsonExtractionDoc(t, "u1", "ada@example.com", bson.A{"b", "a", "a"}, "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := bsonExtractionRoots(t, d)
	bsonExtractionRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	bsonExtractionRequireIndexIDs(t, col, "tag", "a", "u1")
	bsonExtractionRequireIndexIDs(t, col, "tag", "b", "u1")

	sameEffectiveDoc := bsonExtractionDoc(t, "u1", "ada@example.com", bson.A{"a", "b", "a"}, "same-effective-indexes")
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return sameEffectiveDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update same effective BSON indexes: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("same effective update matched=%v modified=%v want true/true", matched, modified)
	}
	afterSameEffective := bsonExtractionRoots(t, d)
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "tag"),
	} {
		if afterSameEffective[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for BSON replacement with unchanged effective index values", rootName, before[rootName], afterSameEffective[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterSameEffective[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified BSON replacement", rootName)
	}
	bsonExtractionRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	bsonExtractionRequireIndexIDs(t, col, "tag", "a", "u1")
	bsonExtractionRequireIndexIDs(t, col, "tag", "b", "u1")

	emailChangedDoc := bsonExtractionDoc(t, "u1", "grace@example.com", bson.A{"b", "a"}, "email-changed")
	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return emailChangedDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update dotted BSON email: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("email update matched=%v modified=%v want true/true", matched, modified)
	}
	afterEmail := bsonExtractionRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "email"); afterEmail[rootName] == afterSameEffective[rootName] {
		t.Fatalf("email root %q did not change for dotted-path indexed update", rootName)
	}
	if rootName := collectionSecondaryRootName("users", "tag"); afterEmail[rootName] != afterSameEffective[rootName] {
		t.Fatalf("tag root %q changed from %d to %d for email-only BSON update", rootName, afterSameEffective[rootName], afterEmail[rootName])
	}
	bsonExtractionRequireIndexIDs(t, col, "email", "grace@example.com", "u1")
	bsonExtractionRequireIndexIDs(t, col, "email", "ada@example.com")
	bsonExtractionRequireIndexIDs(t, col, "tag", "a", "u1")
	bsonExtractionRequireIndexIDs(t, col, "tag", "b", "u1")

	tagChangedDoc := bsonExtractionDoc(t, "u1", "grace@example.com", bson.A{"a", "c"}, "tag-changed")
	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return tagChangedDoc, true, nil
	})
	if err != nil {
		t.Fatalf("update BSON multikey tags: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("tag update matched=%v modified=%v want true/true", matched, modified)
	}
	afterTag := bsonExtractionRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "tag"); afterTag[rootName] == afterEmail[rootName] {
		t.Fatalf("tag root %q did not change for multikey indexed update", rootName)
	}
	if rootName := collectionSecondaryRootName("users", "email"); afterTag[rootName] != afterEmail[rootName] {
		t.Fatalf("email root %q changed from %d to %d for tag-only BSON update", rootName, afterEmail[rootName], afterTag[rootName])
	}
	bsonExtractionRequireIndexIDs(t, col, "email", "grace@example.com", "u1")
	bsonExtractionRequireIndexIDs(t, col, "tag", "a", "u1")
	bsonExtractionRequireIndexIDs(t, col, "tag", "b")
	bsonExtractionRequireIndexIDs(t, col, "tag", "c", "u1")
}

func bsonExtractionDoc(tb testing.TB, id, email string, tags bson.A, note string) []byte {
	tb.Helper()
	return mustBSONCollectionDocument(tb, bson.D{
		{Key: "_id", Value: id},
		{Key: "profile", Value: bson.D{{Key: "email", Value: email}}},
		{Key: "tags", Value: tags},
		{Key: "note", Value: note},
	})
}

func bsonExtractionRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "tag"),
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

func bsonExtractionRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
