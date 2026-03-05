package collections

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func FuzzCollectionConsistency(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte("abcdefghijklmnopqrstuvwxyz0123456789"))

	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) == 0 {
			t.Skip()
		}
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

		modelByID := map[string]string{}
		modelByEmail := map[string]string{}

		steps := len(script)
		if steps > 512 {
			steps = 512
		}
		for i := 0; i < steps; i++ {
			op := int(script[i] % 3)
			id := fmt.Sprintf("u-%d", script[i]%32)
			email := fmt.Sprintf("user-%d@example.com", script[(i+1)%len(script)]%32)
			switch op {
			case 0: // upsert
				if owner, exists := modelByEmail[email]; exists && owner != id {
					if _, err := col.Insert([]byte(id), []byte(fmt.Sprintf(`{"email":%q}`, email))); err == nil {
						t.Fatalf("expected unique conflict id=%s email=%s", id, email)
					}
					continue
				}
				doc := []byte(fmt.Sprintf(`{"email":%q}`, email))
				if _, err := col.Insert([]byte(id), doc); err != nil {
					t.Fatalf("insert id=%s email=%s: %v", id, email, err)
				}
				if old, ok := modelByID[id]; ok {
					delete(modelByEmail, old)
				}
				modelByID[id] = email
				modelByEmail[email] = id
			case 1: // delete
				if err := col.Delete([]byte(id)); err != nil {
					t.Fatalf("delete id=%s: %v", id, err)
				}
				if old, ok := modelByID[id]; ok {
					delete(modelByID, id)
					delete(modelByEmail, old)
				}
			default: // read/validate one id
				got, err := col.Get([]byte(id))
				if err != nil {
					t.Fatalf("get id=%s: %v", id, err)
				}
				email, ok := modelByID[id]
				if !ok {
					if got != nil {
						t.Fatalf("expected missing id=%s", id)
					}
					continue
				}
				want := []byte(fmt.Sprintf(`{"email":%q}`, email))
				if !bytes.Equal(got, want) {
					t.Fatalf("doc mismatch id=%s got=%q want=%q", id, got, want)
				}
			}
		}

		for email, id := range modelByEmail {
			ids, err := col.FindByIndex("email_idx", email)
			if err != nil {
				t.Fatalf("find by index email=%s: %v", email, err)
			}
			if len(ids) != 1 || string(ids[0]) != id {
				t.Fatalf("index mismatch email=%s got=%#v want=%s", email, ids, id)
			}
		}
	})
}
