package collections_test

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	dbpkg "github.com/snissn/gomap/TreeDB/db"
)

type collectionEngineFactory struct {
	name string
	open func(t *testing.T, dir string) (any, func())
	new  func(database any) *collections.CollectionManager
}

func collectionEngineFactories() []collectionEngineFactory {
	return []collectionEngineFactory{
		{
			name: "backend_direct",
			open: func(t *testing.T, dir string) (any, func()) {
				t.Helper()
				d, err := dbpkg.Open(dbpkg.Options{Dir: dir})
				if err != nil {
					t.Fatalf("open backend: %v", err)
				}
				return d, func() {
					if err := d.Close(); err != nil {
						t.Fatalf("close backend: %v", err)
					}
				}
			},
			new: func(database any) *collections.CollectionManager {
				return collections.NewCollectionManager(database.(*dbpkg.DB))
			},
		},
		{
			name: "cached",
			open: func(t *testing.T, dir string) (any, func()) {
				t.Helper()
				d, err := treedb.Open(treedb.Options{Dir: dir})
				if err != nil {
					t.Fatalf("open cached: %v", err)
				}
				return d, func() {
					if err := d.Close(); err != nil {
						t.Fatalf("close cached: %v", err)
					}
				}
			},
			new: func(database any) *collections.CollectionManager {
				return treedb.NewCollectionManager(database.(*treedb.DB))
			},
		},
	}
}

func TestCollectionEngineMatrix_BasicLifecycle(t *testing.T) {
	for _, tc := range collectionEngineFactories() {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			database, cleanup := tc.open(t, dir)
			defer cleanup()

			mgr := tc.new(database)
			meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
			if err != nil {
				t.Fatalf("create collection: %v", err)
			}
			col, err := mgr.OpenCollection(meta.Name)
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}

			doc := []byte(`{"name":"ada"}`)
			id, err := col.Insert([]byte("u1"), doc)
			if err != nil {
				t.Fatalf("insert: %v", err)
			}
			if !bytes.Equal(id, []byte("u1")) {
				t.Fatalf("unexpected inserted id: %q", id)
			}

			got, err := col.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !bytes.Equal(got, doc) {
				t.Fatalf("unexpected document: got=%q want=%q", got, doc)
			}

			if err := col.Delete([]byte("u1")); err != nil {
				t.Fatalf("delete: %v", err)
			}
			got, err = col.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("get after delete: %v", err)
			}
			if got != nil {
				t.Fatalf("expected nil after delete, got %q", got)
			}
		})
	}
}

func TestCollectionEngineMatrix_LiveIndexRoundTrip(t *testing.T) {
	for _, tc := range collectionEngineFactories() {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			database, cleanup := tc.open(t, dir)
			defer cleanup()

			mgr := tc.new(database)
			meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
			if err != nil {
				t.Fatalf("create collection: %v", err)
			}
			if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
				t.Fatalf("create index: %v", err)
			}
			col, err := mgr.OpenCollection(meta.Name)
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}
			if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
				t.Fatalf("insert: %v", err)
			}
			ids, err := col.FindByIndex("email_idx", "ada@example.com")
			if err != nil {
				t.Fatalf("find by index: %v", err)
			}
			if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
				t.Fatalf("unexpected ids: %#v", ids)
			}
		})
	}
}

func TestCollectionEngineMatrix_CachedMultiInsertRetainsNamedRootValueLogState(t *testing.T) {
	dir := t.TempDir()
	d, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Fatalf("close cached: %v", err)
		}
	}()

	mgr := treedb.NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	for i := 0; i < 30000; i++ {
		id := []byte(fmt.Sprintf("user-%08d", i))
		if _, err := col.Insert(id, []byte(`{"name":"ada"}`)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	got, err := col.Get([]byte("user-00000000"))
	if err != nil {
		t.Fatalf("get earliest doc: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"name":"ada"}`)) {
		t.Fatalf("unexpected earliest doc: %q", got)
	}
}
