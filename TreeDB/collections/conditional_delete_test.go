package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestDeleteDocumentIfContract(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"active":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := col.DeleteDocumentIf([]byte("u1"), nil); err == nil {
		t.Fatal("nil predicate accepted")
	}
	if deleted, err := col.DeleteDocumentIf([]byte("u1"), func([]byte) (bool, error) { return false, nil }); err != nil || deleted {
		t.Fatalf("false=%v,%v", deleted, err)
	}
	if got, _ := col.Get([]byte("u1")); got == nil {
		t.Fatal("false predicate deleted document")
	}
	want := errors.New("stop")
	if _, err := col.DeleteDocumentIf([]byte("u1"), func([]byte) (bool, error) { return false, want }); !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if deleted, err := col.DeleteDocumentIf([]byte("u1"), func(current []byte) (bool, error) { return string(current) == `{"active":true}`, nil }); err != nil || !deleted {
		t.Fatalf("delete=%v,%v", deleted, err)
	}
}
