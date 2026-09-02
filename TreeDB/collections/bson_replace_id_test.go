package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionReplaceBSONRejectsIDMutationBeforeRootWork(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}})
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	beforeState := d.State()
	beforeRoot := bsonReplacePrimaryRootIDForTest(t, d, "users")

	replacement := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(1)}})
	replaced, err := col.Replace([]byte("u1"), replacement)
	if !errors.Is(err, errBSONIDMutation) {
		t.Fatalf("Replace err=%v want _id mutation error", err)
	}
	if replaced {
		t.Fatalf("Replace replaced=%v want false on rejected _id mutation", replaced)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("rejected _id replace advanced commit seq by %d", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterRoot := bsonReplacePrimaryRootIDForTest(t, d, "users")
	if afterRoot != beforeRoot {
		t.Fatalf("primary root changed from %d to %d after rejected _id replace", beforeRoot, afterRoot)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("u1 after rejected _id replace=%x want original %x", got, doc)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("u2 after rejected _id replace=%x want nil", got)
	}
}

func TestCollectionReplaceBSONRejectsMissingIDBeforeRootWork(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}})
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	beforeState := d.State()
	beforeRoot := bsonReplacePrimaryRootIDForTest(t, d, "users")

	replacement := mustBSONCollectionDocument(t, bson.D{{Key: "score", Value: int32(1)}})
	replaced, err := col.Replace([]byte("u1"), replacement)
	if !errors.Is(err, errBSONIDMutation) {
		t.Fatalf("Replace err=%v want _id mutation error", err)
	}
	if replaced {
		t.Fatalf("Replace replaced=%v want false on rejected missing _id", replaced)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("rejected missing _id replace advanced commit seq by %d", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterRoot := bsonReplacePrimaryRootIDForTest(t, d, "users")
	if afterRoot != beforeRoot {
		t.Fatalf("primary root changed from %d to %d after rejected missing _id replace", beforeRoot, afterRoot)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("u1 after rejected missing _id replace=%x want original %x", got, doc)
	}
}

func bsonReplacePrimaryRootIDForTest(t *testing.T, d *backenddb.DB, collectionName string) uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	rootID := catalog.rootID(collectionPrimaryRootName(collectionName))
	if rootID == 0 {
		t.Fatalf("primary root for %q was not persisted", collectionName)
	}
	return rootID
}
