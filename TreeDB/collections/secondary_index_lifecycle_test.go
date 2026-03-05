package collections

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestCreateIndex_RegistersInSystemMetadata(t *testing.T) {
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

	def := IndexDefinition{Name: "email_idx", Field: "email", Unique: true}
	if _, err := mgr.CreateIndex(meta.Name, def); err != nil {
		t.Fatalf("create index: %v", err)
	}

	indexKey, err := SystemIndexKey(meta.Name, def.Name)
	if err != nil {
		t.Fatalf("system index key: %v", err)
	}
	raw, err := d.GetSystem(indexKey)
	if err != nil {
		t.Fatalf("get index definition: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("expected index definition persisted in system root")
	}

	openMeta, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if len(openMeta.meta.Indexes) != 1 {
		t.Fatalf("expected one index in collection metadata, got %d", len(openMeta.meta.Indexes))
	}
}

func TestDropIndex_CleansMetadataAndIndexEntries(t *testing.T) {
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
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"a@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := mgr.DropIndex(meta.Name, "email_idx"); err != nil {
		t.Fatalf("drop index: %v", err)
	}

	ids, err := col.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find after drop: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no results after drop, got %d", len(ids))
	}

	idxPrefix, err := CollectionIndexDataPrefix(meta.Name)
	if err != nil {
		t.Fatalf("index data prefix: %v", err)
	}
	it, err := d.Iterator(idxPrefix, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if bytes.HasPrefix(it.UnsafeKey(), idxPrefix) && !it.IsDeleted() {
			t.Fatalf("expected no remaining index keys, found %q", it.UnsafeKey())
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}

func TestCreateIndex_BackfillsExistingDocumentsFromDedicatedPrimaryRoot(t *testing.T) {
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
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"a@example.com"}`)); err != nil {
		t.Fatalf("insert existing document: %v", err)
	}

	if _, err := mgr.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	ids, err := col.FindByIndex("email_idx", "a@example.com")
	if err != nil {
		t.Fatalf("find after backfill: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("expected one backfilled result for u1, got %#v", ids)
	}
}
