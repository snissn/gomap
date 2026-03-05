package collections

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestInsertThenUpdateIndexField_ReplacesPosting(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	id := []byte("u1")
	if _, err := col.Insert(id, []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert v1: %v", err)
	}
	if _, err := col.Insert(id, []byte(`{"city":"sea"}`)); err != nil {
		t.Fatalf("insert v2: %v", err)
	}

	hnl, err := col.FindByIndex("city_idx", "hnl")
	if err != nil {
		t.Fatalf("find hnl: %v", err)
	}
	if len(hnl) != 0 {
		t.Fatalf("expected old posting removed, got %#v", hnl)
	}
	sea, err := col.FindByIndex("city_idx", "sea")
	if err != nil {
		t.Fatalf("find sea: %v", err)
	}
	if len(sea) != 1 || !bytes.Equal(sea[0], id) {
		t.Fatalf("expected [u1] for sea, got %#v", sea)
	}
}

func TestDeleteDocument_RemovesFromAllIndexes(t *testing.T) {
	d, err := db.Open(db.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index email: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		t.Fatalf("create index city: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	id := []byte("u1")
	if _, err := col.Insert(id, []byte(`{"email":"a@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Delete(id); err != nil {
		t.Fatalf("delete: %v", err)
	}

	emailIDs, err := col.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 0 {
		t.Fatalf("expected email postings removed, got %#v", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city_idx", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 0 {
		t.Fatalf("expected city postings removed, got %#v", cityIDs)
	}
}
