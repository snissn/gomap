package collections

import (
	"bytes"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionUpdateBSONRejectsIDMutationBeforeRootWork(t *testing.T) {
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
	beforeState := d.State()
	beforeRoot := collectionPrimaryRootIDForTest(t, d, "users")

	replacement := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(1)}})
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if err == nil || !strings.Contains(err.Error(), "cannot modify _id") {
		t.Fatalf("Update err=%v want _id mutation error", err)
	}
	if matched || modified {
		t.Fatalf("Update matched/modified=%v/%v want false/false on rejected _id mutation", matched, modified)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("rejected _id update advanced commit seq by %d", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterRoot := collectionPrimaryRootIDForTest(t, d, "users")
	if afterRoot != beforeRoot {
		t.Fatalf("primary root changed from %d to %d after rejected _id update", beforeRoot, afterRoot)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("u1 after rejected _id update=%x want original %x", got, doc)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("u2 after rejected _id update=%x want nil", got)
	}
}

func collectionPrimaryRootIDForTest(t *testing.T, d *backenddb.DB, collectionName string) uint64 {
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
