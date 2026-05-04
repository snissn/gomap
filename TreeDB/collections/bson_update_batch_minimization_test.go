package collections

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionBSONUpdateBatchSkipsUnchangedDottedAndMultikeyIndexes(t *testing.T) {
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
		[][]byte{bsonUpdateBatchMinDoc(t, "u1", "ada@example.com", bson.A{"b", nil, "a", "a"}, "seed")},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := bsonUpdateBatchMinRoots(t, d)
	bsonUpdateBatchMinRequireIndexIDs(t, col, "email", "ada@example.com", "u1")
	bsonUpdateBatchMinRequireIndexIDs(t, col, "tag", "a", "u1")
	bsonUpdateBatchMinRequireIndexIDs(t, col, "tag", "b", "u1")

	sameEffectiveDoc := bsonUpdateBatchMinDoc(t, "u1", "ada@example.com", bson.A{"a", "b", "a", nil}, "same-effective")
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return sameEffectiveDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch same effective BSON indexes: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("same effective results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same effective secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same effective changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 2; got != want {
		t.Fatalf("same effective unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("same effective unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("same effective unique skips=%d want %d", got, want)
	}
	afterSame := bsonUpdateBatchMinRoots(t, d)
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "tag"),
	} {
		if afterSame[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for BSON UpdateBatch with unchanged effective index values", rootName, before[rootName], afterSame[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterSame[rootName] == before[rootName] {
		t.Fatalf("primary root %q did not change for modified BSON replacement", rootName)
	}

	emailChangedDoc := bsonUpdateBatchMinDoc(t, "u1", "grace@example.com", bson.A{"a", "b"}, "email-changed")
	results, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return emailChangedDoc, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch changed BSON email: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("email change results=%+v want one matched modified row", got)
	}
	stats = col.LastUpdateStats()
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("email change index changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 1; got != want {
		t.Fatalf("email change index unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 1; got != want {
		t.Fatalf("email change unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 0; got != want {
		t.Fatalf("email change unique skips=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("email change secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "email" || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("email secondary run stats=%+v want email delete+set with key bytes", run)
	}
	afterEmail := bsonUpdateBatchMinRoots(t, d)
	if rootName := collectionSecondaryRootName("users", "email"); afterEmail[rootName] == afterSame[rootName] {
		t.Fatalf("email root %q did not change for BSON UpdateBatch dotted-path change", rootName)
	}
	if rootName := collectionSecondaryRootName("users", "tag"); afterEmail[rootName] != afterSame[rootName] {
		t.Fatalf("tag root %q changed from %d to %d for email-only BSON UpdateBatch", rootName, afterSame[rootName], afterEmail[rootName])
	}
	bsonUpdateBatchMinRequireIndexIDs(t, col, "email", "ada@example.com")
	bsonUpdateBatchMinRequireIndexIDs(t, col, "email", "grace@example.com", "u1")
	bsonUpdateBatchMinRequireIndexIDs(t, col, "tag", "a", "u1")
	bsonUpdateBatchMinRequireIndexIDs(t, col, "tag", "b", "u1")
}

func bsonUpdateBatchMinDoc(tb testing.TB, id, email string, tags bson.A, note string) []byte {
	tb.Helper()
	return mustBSONCollectionDocument(tb, bson.D{
		{Key: "_id", Value: id},
		{Key: "profile", Value: bson.D{{Key: "email", Value: email}}},
		{Key: "tags", Value: tags},
		{Key: "note", Value: note},
	})
}

func bsonUpdateBatchMinRoots(tb testing.TB, d *backenddb.DB) map[string]uint64 {
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

func bsonUpdateBatchMinRequireIndexIDs(tb testing.TB, col *Collection, indexName string, value any, want ...string) {
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
