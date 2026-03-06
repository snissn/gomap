package collections

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestSecondaryFindByKey_Unique_ReturnsSingleID(t *testing.T) {
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
		t.Fatalf("create index: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"a@example.com","name":"a"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ids, err := col.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("expected [u1], got %#v", ids)
	}
}

func TestSecondaryFindByKey_NonUnique_ReturnsSortedIDs(t *testing.T) {
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

	if _, err := col.Insert([]byte("u2"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}

	ids, err := col.FindByIndex("city_idx", "hnl")
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(ids) != 2 || !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("expected sorted [u1 u2], got %#v", ids)
	}
}

func TestSecondaryFindByKey_NonUnique_ReturnedIDsRemainStableAcrossLookups(t *testing.T) {
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

	if _, err := col.Insert([]byte("u2"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.Insert([]byte("u9"), []byte(`{"city":"sfo"}`)); err != nil {
		t.Fatalf("insert u9: %v", err)
	}

	ids, err := col.FindByIndex("city_idx", "hnl")
	if err != nil {
		t.Fatalf("find hnl: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}

	other, err := col.FindByIndex("city_idx", "sfo")
	if err != nil {
		t.Fatalf("find sfo: %v", err)
	}
	if len(other) != 1 || !bytes.Equal(other[0], []byte("u9")) {
		t.Fatalf("expected [u9], got %#v", other)
	}
	if !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("expected first lookup to remain [u1 u2], got %#v", ids)
	}
}

func TestSecondaryIndex_StoresNoValuePayload(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	indexDef, ok := col.indexByName("city_idx")
	if !ok {
		t.Fatalf("expected city_idx metadata")
	}
	rootDesc := mustLoadSecondaryRootDescriptor(t, d, indexDef.RootName)
	if rootDesc.RootPageID == 0 {
		t.Fatalf("expected secondary root page id after insert")
	}
	prefix, err := CollectionIndexPrefix(meta.Name, "city_idx")
	if err != nil {
		t.Fatalf("prefix: %v", err)
	}
	end := append(append([]byte{}, prefix...), 0xff)
	it, err := d.IteratorAtRootWithOptions(rootDesc.RootPageID, prefix, end, db.IteratorOptions{Mode: db.IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("expected index entry")
	}
	if len(it.UnsafeValue()) != 0 {
		t.Fatalf("expected no value payload for secondary index entry")
	}

	sharedIt, err := d.Iterator(prefix, end)
	if err != nil {
		t.Fatalf("shared iterator: %v", err)
	}
	defer sharedIt.Close()
	for ; sharedIt.Valid(); sharedIt.Next() {
		if !sharedIt.IsDeleted() && bytes.HasPrefix(sharedIt.UnsafeKey(), prefix) {
			t.Fatalf("expected no shared user-root secondary entries, found %q", sharedIt.UnsafeKey())
		}
	}
	if err := sharedIt.Error(); err != nil {
		t.Fatalf("shared iterator error: %v", err)
	}
}
