package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONDirectUpdateSkipsUnchangedUniqueWhenNonUniqueChanges(t *testing.T) {
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
		[][]byte{bsonDirectUniqueSkipDoc(t, "u1", "ada@example.com", "hnl", "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := bsonDirectUniqueSkipRoots(t, d)
	bsonDirectUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	bsonDirectUniqueSkipRequireIndexIDs(t, col, "city", "hnl", "u1")

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return bsonDirectUniqueSkipDoc(t, "u1", "ada@example.com", "sea", "city-changed"), true, nil
	})
	if err != nil {
		t.Fatalf("direct BSON city-only update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched=%v modified=%v want true/true", matched, modified)
	}
	after := bsonDirectUniqueSkipRoots(t, d)
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for direct BSON city-only update", rootName)
		}
	}
	if rootName := collectionSecondaryRootName("users", "email"); after[rootName] != before[rootName] {
		t.Fatalf("email root %q changed from %d to %d for unchanged unique email", rootName, before[rootName], after[rootName])
	}
	bsonDirectUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	bsonDirectUniqueSkipRequireIndexIDs(t, col, "city", "hnl")
	bsonDirectUniqueSkipRequireIndexIDs(t, col, "city", "sea", "u1")
}

func bsonDirectUniqueSkipDoc(tb testing.TB, id, email, city, note string) []byte {
	tb.Helper()
	return mustBSONCollectionDocument(tb, bson.D{
		{Key: "_id", Value: id},
		{Key: "email", Value: email},
		{Key: "city", Value: city},
		{Key: "note", Value: note},
	})
}

func bsonDirectUniqueSkipRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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

func bsonDirectUniqueSkipRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
