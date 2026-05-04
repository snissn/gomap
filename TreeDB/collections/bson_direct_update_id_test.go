package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionNoIndexUpdateBSONStagesValidIDPreservingReplacement(t *testing.T) {
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
	replacement := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(1)}})

	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if err := checkBSONInt32FieldForUpdateTest(current, "score", 0); err != nil {
			return nil, false, err
		}
		return replacement, true, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("Update matched/modified=%v/%v want true/true", matched, modified)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("staged BSON update commit seq=%d want %d", got, beforeState.CommitSeq)
	}
	if got := collectionPrimaryRootIDForTest(t, d, "users"); got != beforeRoot {
		t.Fatalf("staged BSON update primary root=%d want %d", got, beforeRoot)
	}
	if state := collectionNoIndexPendingStateForTest(t, col); state.count != 1 || state.tableLen < 1 {
		t.Fatalf("pending state after staged BSON update=%+v want one row", state)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get staged BSON u1: %v", err)
	}
	requireBSONStringFieldForUpdateTest(t, got, "_id", "u1")
	requireBSONInt32FieldForUpdateTest(t, got, "score", 1)

	if err := col.Flush(); err != nil {
		t.Fatalf("flush staged BSON update: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq+1 {
		t.Fatalf("flushed BSON update commit seq=%d want %d", got, beforeState.CommitSeq+1)
	}
	if got := collectionPrimaryRootIDForTest(t, d, "users"); got == beforeRoot {
		t.Fatalf("flushed BSON update primary root stayed at %d", got)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get flushed BSON u1: %v", err)
	}
	requireBSONStringFieldForUpdateTest(t, got, "_id", "u1")
	requireBSONInt32FieldForUpdateTest(t, got, "score", 1)
}

func TestCollectionUpdateBSONRejectsIDMutationBeforeStaging(t *testing.T) {
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
	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if !errors.Is(err, errBSONIDMutation) {
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
	if err := col.Flush(); err != nil {
		t.Fatalf("flush after rejected _id update: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("flush after rejected _id update commit seq=%d want %d", got, beforeState.CommitSeq)
	}
}

func TestCollectionUpdateBSONRejectsInPlaceIDMutationBeforeStaging(t *testing.T) {
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
	mutated := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(1)}})

	matched, modified, err := col.updateDirect([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		if len(mutated) != len(current) {
			t.Fatalf("mutated BSON length=%d current length=%d", len(mutated), len(current))
		}
		copy(current, mutated)
		return current, true, nil
	})
	if !errors.Is(err, errBSONIDMutation) {
		t.Fatalf("Update err=%v want _id mutation error", err)
	}
	if matched || modified {
		t.Fatalf("Update matched/modified=%v/%v want false/false on rejected in-place _id mutation", matched, modified)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("rejected in-place _id update advanced commit seq by %d", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterRoot := collectionPrimaryRootIDForTest(t, d, "users")
	if afterRoot != beforeRoot {
		t.Fatalf("primary root changed from %d to %d after rejected in-place _id update", beforeRoot, afterRoot)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("u1 after rejected in-place _id update=%x want original %x", got, doc)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("u2 after rejected in-place _id update=%x want nil", got)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush after rejected in-place _id update: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("flush after rejected in-place _id update commit seq=%d want %d", got, beforeState.CommitSeq)
	}
}

func TestCollectionUpdateBSONRejectsMissingIDBeforeStaging(t *testing.T) {
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
	replacement := mustBSONCollectionDocument(t, bson.D{{Key: "score", Value: int32(1)}})

	matched, modified, err := col.updateDirect([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if !errors.Is(err, errBSONIDMutation) {
		t.Fatalf("Update err=%v want _id mutation error", err)
	}
	if matched || modified {
		t.Fatalf("Update matched/modified=%v/%v want false/false on rejected missing _id", matched, modified)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("rejected missing _id update advanced commit seq by %d", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterRoot := collectionPrimaryRootIDForTest(t, d, "users")
	if afterRoot != beforeRoot {
		t.Fatalf("primary root changed from %d to %d after rejected missing _id update", beforeRoot, afterRoot)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("u1 after rejected missing _id update=%x want original %x", got, doc)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush after rejected missing _id update: %v", err)
	}
	if got := d.State().CommitSeq; got != beforeState.CommitSeq {
		t.Fatalf("flush after rejected missing _id update commit seq=%d want %d", got, beforeState.CommitSeq)
	}
}

func requireBSONStringFieldForUpdateTest(t *testing.T, raw []byte, field, want string) {
	t.Helper()
	value := bson.Raw(raw).Lookup(field)
	if value.Type != bson.TypeString {
		t.Fatalf("BSON field %q type=%s want string", field, value.Type)
	}
	if got := value.StringValue(); got != want {
		t.Fatalf("BSON field %q=%q want %q", field, got, want)
	}
}

func requireBSONInt32FieldForUpdateTest(t *testing.T, raw []byte, field string, want int32) {
	t.Helper()
	if err := checkBSONInt32FieldForUpdateTest(raw, field, want); err != nil {
		t.Fatal(err)
	}
}

func checkBSONInt32FieldForUpdateTest(raw []byte, field string, want int32) error {
	value := bson.Raw(raw).Lookup(field)
	if value.Type != bson.TypeInt32 {
		return fmt.Errorf("BSON field %q type=%s want int32", field, value.Type)
	}
	if got := value.Int32(); got != want {
		return fmt.Errorf("BSON field %q=%d want %d", field, got, want)
	}
	return nil
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
