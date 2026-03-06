package collections_test

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	dbpkg "github.com/snissn/gomap/TreeDB/db"
)

type collectionReopenEngineFactory struct {
	name   string
	open   func(t *testing.T, dir string) (any, func())
	reopen func(t *testing.T, dir string) (any, func())
	new    func(database any) *collections.CollectionManager
}

func collectionReopenEngineFactories() []collectionReopenEngineFactory {
	return []collectionReopenEngineFactory{
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
			reopen: func(t *testing.T, dir string) (any, func()) {
				t.Helper()
				d, err := dbpkg.Open(dbpkg.Options{Dir: dir})
				if err != nil {
					t.Fatalf("reopen backend: %v", err)
				}
				return d, func() {
					if err := d.Close(); err != nil {
						t.Fatalf("close reopened backend: %v", err)
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
			reopen: func(t *testing.T, dir string) (any, func()) {
				t.Helper()
				d, err := treedb.Open(treedb.Options{Dir: dir})
				if err != nil {
					t.Fatalf("reopen cached: %v", err)
				}
				return d, func() {
					if err := d.Close(); err != nil {
						t.Fatalf("close reopened cached: %v", err)
					}
				}
			},
			new: func(database any) *collections.CollectionManager {
				return treedb.NewCollectionManager(database.(*treedb.DB))
			},
		},
	}
}

func TestCollectionEngineMatrix_ReopenDurability(t *testing.T) {
	type scenario struct {
		name   string
		mutate func(t *testing.T, mgr *collections.CollectionManager)
		verify func(t *testing.T, mgr *collections.CollectionManager)
	}

	scenarios := []scenario{
		{
			name: "create_collection_only",
			mutate: func(t *testing.T, mgr *collections.CollectionManager) {
				t.Helper()
				if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"}); err != nil {
					t.Fatalf("create collection: %v", err)
				}
			},
			verify: func(t *testing.T, mgr *collections.CollectionManager) {
				t.Helper()
				col, err := mgr.OpenCollection("users")
				if err != nil {
					t.Fatalf("open collection after reopen: %v", err)
				}
				if col.Meta().PrimaryRoot == "" {
					t.Fatalf("expected primary root metadata after reopen")
				}
			},
		},
		{
			name: "create_collection_then_insert",
			mutate: func(t *testing.T, mgr *collections.CollectionManager) {
				t.Helper()
				meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
				if err != nil {
					t.Fatalf("create collection: %v", err)
				}
				col, err := mgr.OpenCollection(meta.Name)
				if err != nil {
					t.Fatalf("open collection: %v", err)
				}
				if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
					t.Fatalf("insert: %v", err)
				}
			},
			verify: func(t *testing.T, mgr *collections.CollectionManager) {
				t.Helper()
				col, err := mgr.OpenCollection("users")
				if err != nil {
					t.Fatalf("open collection after reopen: %v", err)
				}
				got, err := col.Get([]byte("u1"))
				if err != nil {
					t.Fatalf("get after reopen: %v", err)
				}
				if !bytes.Equal(got, []byte(`{"name":"ada"}`)) {
					t.Fatalf("unexpected doc after reopen: %q", got)
				}
			},
		},
		{
			name: "create_collection_then_index",
			mutate: func(t *testing.T, mgr *collections.CollectionManager) {
				t.Helper()
				meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
				if err != nil {
					t.Fatalf("create collection: %v", err)
				}
				if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
					t.Fatalf("create index: %v", err)
				}
			},
			verify: func(t *testing.T, mgr *collections.CollectionManager) {
				t.Helper()
				col, err := mgr.OpenCollection("users")
				if err != nil {
					t.Fatalf("open collection after reopen: %v", err)
				}
				found := false
				for _, idx := range col.Meta().Indexes {
					if idx.Name == "email_idx" {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("expected email_idx metadata after reopen")
				}
			},
		},
		{
			name: "create_collection_then_index_then_insert",
			mutate: func(t *testing.T, mgr *collections.CollectionManager) {
				t.Helper()
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
			},
			verify: func(t *testing.T, mgr *collections.CollectionManager) {
				t.Helper()
				col, err := mgr.OpenCollection("users")
				if err != nil {
					t.Fatalf("open collection after reopen: %v", err)
				}
				got, err := col.Get([]byte("u1"))
				if err != nil {
					t.Fatalf("get after reopen: %v", err)
				}
				if !bytes.Equal(got, []byte(`{"email":"ada@example.com"}`)) {
					t.Fatalf("unexpected doc after reopen: %q", got)
				}
				ids, err := col.FindByIndex("email_idx", "ada@example.com")
				if err != nil {
					t.Fatalf("find by index after reopen: %v", err)
				}
				if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
					t.Fatalf("unexpected ids after reopen: %#v", ids)
				}
			},
		},
	}

	for _, engine := range collectionReopenEngineFactories() {
		for _, scenario := range scenarios {
			t.Run(engine.name+"/"+scenario.name, func(t *testing.T) {
				dir := t.TempDir()
				database, cleanup := engine.open(t, dir)
				mgr := engine.new(database)
				scenario.mutate(t, mgr)
				cleanup()

				reopened, reopenCleanup := engine.reopen(t, dir)
				defer reopenCleanup()
				scenario.verify(t, engine.new(reopened))
			})
		}
	}
}
