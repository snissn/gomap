package collections

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestDeleteUnknownIDIsNoop(t *testing.T) {
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
	if err := col.Delete([]byte("does-not-exist")); err != nil {
		t.Fatalf("delete unknown id should be noop, got %v", err)
	}
}

func TestDropIndex_OpenCollectionHandleStopsIndexing(t *testing.T) {
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
	if err := mgr.DropIndex(meta.Name, "email_idx"); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"a@example.com"}`)); err != nil {
		t.Fatalf("insert after drop index: %v", err)
	}
	prefix, err := CollectionIndexDataPrefix(meta.Name)
	if err != nil {
		t.Fatalf("index prefix: %v", err)
	}
	it, err := d.Iterator(prefix, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if bytes.HasPrefix(it.UnsafeKey(), prefix) && !it.IsDeleted() {
			t.Fatalf("unexpected index entry after dropped index insert: %q", it.UnsafeKey())
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}

func TestCollectionConsistencyRandomized(t *testing.T) {
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

	type modelDoc struct {
		id    string
		email string
		doc   []byte
	}
	modelByID := make(map[string]modelDoc)
	modelByEmail := make(map[string]string)
	rng := rand.New(rand.NewSource(1337))
	for i := 0; i < 1000; i++ {
		op := rng.Intn(3)
		id := fmt.Sprintf("u-%d", rng.Intn(120))
		switch op {
		case 0: // upsert
			email := fmt.Sprintf("user-%d@example.com", rng.Intn(120))
			if owner, exists := modelByEmail[email]; exists && owner != id {
				// force unique conflicts deterministically
				if _, err := col.Insert([]byte(id), []byte(fmt.Sprintf(`{"email":%q}`, email))); err == nil {
					t.Fatalf("expected unique conflict for email=%q owner=%q id=%q", email, owner, id)
				}
				continue
			}
			doc := []byte(fmt.Sprintf(`{"email":%q}`, email))
			if _, err := col.Insert([]byte(id), doc); err != nil {
				t.Fatalf("insert id=%s email=%s: %v", id, email, err)
			}
			if old, ok := modelByID[id]; ok {
				delete(modelByEmail, old.email)
			}
			modelByID[id] = modelDoc{id: id, email: email, doc: doc}
			modelByEmail[email] = id
		case 1: // delete
			if err := col.Delete([]byte(id)); err != nil {
				t.Fatalf("delete id=%s: %v", id, err)
			}
			if old, ok := modelByID[id]; ok {
				delete(modelByID, id)
				delete(modelByEmail, old.email)
			}
		case 2: // verify point lookup
			got, err := col.Get([]byte(id))
			if err != nil {
				t.Fatalf("get id=%s: %v", id, err)
			}
			model, ok := modelByID[id]
			if !ok {
				if got != nil {
					t.Fatalf("expected missing id=%s", id)
				}
				continue
			}
			if !bytes.Equal(got, model.doc) {
				t.Fatalf("doc mismatch id=%s", id)
			}
		}
	}
	for email, id := range modelByEmail {
		ids, err := col.FindByIndex("email_idx", email)
		if err != nil {
			t.Fatalf("find index email=%s: %v", email, err)
		}
		if len(ids) != 1 || string(ids[0]) != id {
			t.Fatalf("index mismatch email=%s got=%#v want=%s", email, ids, id)
		}
	}
}
