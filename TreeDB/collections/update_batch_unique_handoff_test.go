package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionUpdateBatchAllowsDurableUniqueHandoff(t *testing.T) {
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"a@example.com","name":"ada"}`),
			[]byte(`{"email":"b@example.com","name":"grace"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONEmail("c@example.com")},
		{DocumentID: []byte("u2"), Update: setJSONEmail("a@example.com")},
	})
	if err != nil {
		t.Fatalf("unique handoff UpdateBatch: %v", err)
	}
	if len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		t.Fatalf("handoff results=%+v want two matched modified rows", results)
	}
	assertUniqueEmailOwnerForTest(t, col, "a@example.com", []byte("u2"))
	assertUniqueEmailOwnerForTest(t, col, "c@example.com", []byte("u1"))
	assertUniqueEmailMissingForTest(t, col, "b@example.com")
}

func TestCollectionUpdateBatchRejectsDuplicateFinalUniqueOwnersAfterHandoff(t *testing.T) {
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"email":"a@example.com","name":"ada"}`),
			[]byte(`{"email":"b@example.com","name":"grace"}`),
			[]byte(`{"email":"c@example.com","name":"katherine"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err = col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONEmail("d@example.com")},
		{DocumentID: []byte("u2"), Update: setJSONEmail("a@example.com")},
		{DocumentID: []byte("u3"), Update: setJSONEmail("a@example.com")},
	})
	if !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate final unique owners err=%v want ErrUniqueIndexConflict", err)
	}
	assertUniqueEmailOwnerForTest(t, col, "a@example.com", []byte("u1"))
	assertUniqueEmailOwnerForTest(t, col, "b@example.com", []byte("u2"))
	assertUniqueEmailOwnerForTest(t, col, "c@example.com", []byte("u3"))
}

func assertUniqueEmailOwnerForTest(t *testing.T, col *Collection, email string, want []byte) {
	t.Helper()
	ids, err := col.FindByIndexValue("email", email)
	if err != nil {
		t.Fatalf("find email %q: %v", email, err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], want) {
		t.Fatalf("email %q ids=%q want [%s]", email, ids, want)
	}
}

func assertUniqueEmailMissingForTest(t *testing.T, col *Collection, email string) {
	t.Helper()
	ids, err := col.FindByIndexValue("email", email)
	if err != nil {
		t.Fatalf("find email %q: %v", email, err)
	}
	if len(ids) != 0 {
		t.Fatalf("email %q ids=%q want none", email, ids)
	}
}
