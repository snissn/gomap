package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONUpdateBatchTreatsMissingAndNullIndexValuesAsUnchanged(t *testing.T) {
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
		[][]byte{bsonUpdateBatchNullDoc(t, "u1", "hnl", nil, false, "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := bsonUpdateBatchNullRoots(t, d)
	bsonUpdateBatchNullRequireIndexIDs(t, col, "city", "hnl", "u1")
	bsonUpdateBatchNullRequireIndexIDs(t, col, "deleted_at", "2026-05-03")

	nullDoc := bsonUpdateBatchNullDoc(t, "u1", "hnl", nil, true, "explicit-null")
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return nullDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch missing deleted_at to null: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("missing-to-null results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("missing-to-null secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("missing-to-null index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 2; got != want {
		t.Fatalf("missing-to-null index unchanged=%d want %d", got, want)
	}
	afterNull := bsonUpdateBatchNullRoots(t, d)
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "city"),
		collectionSecondaryRootName("users", "deleted_at"),
	} {
		if afterNull[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for BSON UpdateBatch missing-to-null index value update", rootName, before[rootName], afterNull[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterNull[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified BSON replacement", rootName)
	}

	valueDoc := bsonUpdateBatchNullDoc(t, "u1", "hnl", "2026-05-03", true, "deleted-at-set")
	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return valueDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch deleted_at value: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("deleted_at value results=%+v want one matched modified row", got)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("deleted_at value index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("deleted_at value index unchanged=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("deleted_at value secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "deleted_at" || run.Deletes != 0 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("deleted_at secondary run stats=%+v want deleted_at set with key bytes", run)
	}
	afterValue := bsonUpdateBatchNullRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "deleted_at"); afterValue[rootName] == afterNull[rootName] {
		t.Fatalf("deleted_at root %q did not change when BSON null became an indexed value", rootName)
	}
	if rootName := collectionSecondaryRootName("users", "city"); afterValue[rootName] != afterNull[rootName] {
		t.Fatalf("city root %q changed from %d to %d for deleted_at-only BSON UpdateBatch", rootName, afterNull[rootName], afterValue[rootName])
	}
	bsonUpdateBatchNullRequireIndexIDs(t, col, "city", "hnl", "u1")
	bsonUpdateBatchNullRequireIndexIDs(t, col, "deleted_at", "2026-05-03", "u1")
}

func bsonUpdateBatchNullDoc(tb testing.TB, id, city string, deletedAt any, includeDeletedAt bool, note string) []byte {
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

func bsonUpdateBatchNullRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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
	for _, name := range []string{
		collectionPrimaryRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if roots[name] == 0 {
			tb.Fatalf("root %q was not persisted", name)
		}
	}
	return roots
}

func bsonUpdateBatchNullRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
