package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONUpdateBatchSkipsUnchangedUniqueWhenNonUniqueChanges(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
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
		[][]byte{bsonUpdateBatchUniqueSkipDoc(t, "u1", "ada@example.com", "hnl", "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := bsonUpdateBatchUniqueSkipRoots(t, d)
	bsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	bsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "hnl", "u1")

	replacement := bsonUpdateBatchUniqueSkipDoc(t, "u1", "ada@example.com", "sea", "city-changed")
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return replacement, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch city-only BSON update: %v", err)
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
	after := bsonUpdateBatchUniqueSkipRoots(t, d)
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if after[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for BSON city-only update", rootName)
		}
	}
	if rootName := collectionSecondaryRootName("users", "email"); after[rootName] != before[rootName] {
		t.Fatalf("email root %q changed from %d to %d for unchanged unique email", rootName, before[rootName], after[rootName])
	}
	bsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	bsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "hnl")
	bsonUpdateBatchUniqueSkipRequireIndexIDs(t, col, "city", "sea", "u1")
}

func bsonUpdateBatchUniqueSkipDoc(tb testing.TB, id, email, city, note string) []byte {
	tb.Helper()
	return mustBSONCollectionDocument(tb, bson.D{
		{Key: "_id", Value: id},
		{Key: "email", Value: email},
		{Key: "city", Value: city},
		{Key: "note", Value: note},
	})
}

func bsonUpdateBatchUniqueSkipRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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

func bsonUpdateBatchUniqueSkipRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
