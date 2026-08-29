package collections

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionDeleteBatchDeletesPrimaryAndSecondary(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{
		[]byte(`{"email":"ada@example.com"}`),
		[]byte(`{"email":"grace@example.com"}`),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	deleted, err := col.DeleteBatch([][]byte{[]byte("u1"), []byte("missing")})
	if err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d want 1", deleted)
	}
	if doc, err := col.Get([]byte("u1")); err != nil || doc != nil {
		t.Fatalf("u1 doc=%q err=%v want missing", doc, err)
	}
	if doc, err := col.Get([]byte("u2")); err != nil || doc == nil {
		t.Fatalf("u2 doc=%q err=%v want present", doc, err)
	}
	ids, err := col.FindByIndexValue("email", "ada@example.com")
	if err != nil {
		t.Fatalf("FindByIndexValue ada: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("ada index ids=%q want none", ids)
	}
}

func TestCollectionDeleteBatchPointerizesLargeSecondaryPostingUpdate(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "documents",
		Options: CollectionOptions{DocumentFormat: DocumentFormatJSON},
		Indexes: []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("documents")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	const rows = 3750
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		docs[i] = []byte(`{"tenant":"shared"}`)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if deleted, err := col.DeleteBatch(ids[:5]); err != nil || deleted != 5 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	got, err := col.FindByIndexValue("tenant", "shared")
	if err != nil || len(got) != rows-5 {
		t.Fatalf("FindByIndexValue rows=%d err=%v want=%d", len(got), err, rows-5)
	}
}

func TestCollectionDeleteBatchRejectsDuplicateIDs(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.DeleteBatch([][]byte{[]byte("u1"), []byte("u1")}); !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("DeleteBatch duplicate err=%v want ErrDuplicateDocumentID", err)
	}
}

func TestCollectionDeleteBatchRejectsEmptyIDs(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.DeleteBatch([][]byte{[]byte("")}); err == nil || !strings.Contains(err.Error(), "document id cannot be empty") {
		t.Fatalf("DeleteBatch empty id err=%v want empty-id rejection", err)
	}
}
