package collections_test

import (
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	dbpkg "github.com/snissn/gomap/TreeDB/db"
)

type collectionBenchEngine struct {
	name string
	open func(tb testing.TB, dir string) (manager *collections.CollectionManager, cleanup func())
}

func collectionBenchEngines() []collectionBenchEngine {
	return []collectionBenchEngine{
		{
			name: "backend_direct",
			open: func(tb testing.TB, dir string) (*collections.CollectionManager, func()) {
				tb.Helper()
				d, err := dbpkg.Open(dbpkg.Options{Dir: dir})
				if err != nil {
					tb.Fatalf("open backend: %v", err)
				}
				return collections.NewCollectionManager(d), func() {
					if err := d.Close(); err != nil {
						tb.Fatalf("close backend: %v", err)
					}
				}
			},
		},
		{
			name: "cached",
			open: func(tb testing.TB, dir string) (*collections.CollectionManager, func()) {
				tb.Helper()
				d, err := treedb.Open(treedb.Options{Dir: dir})
				if err != nil {
					tb.Fatalf("open cached: %v", err)
				}
				return treedb.NewCollectionManager(d), func() {
					if err := d.Close(); err != nil {
						tb.Fatalf("close cached: %v", err)
					}
				}
			},
		},
	}
}

func BenchmarkCollectionEngineInsertProvidedID(b *testing.B) {
	for _, engine := range collectionBenchEngines() {
		b.Run(engine.name, func(b *testing.B) {
			manager, cleanup := engine.open(b, b.TempDir())
			defer cleanup()

			meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
			if err != nil {
				b.Fatalf("create collection: %v", err)
			}
			col, err := manager.OpenCollection(meta.Name)
			if err != nil {
				b.Fatalf("open collection: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := []byte(fmt.Sprintf("user-%08d", i))
				if _, err := col.Insert(id, []byte(`{"name":"ada"}`)); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
		})
	}
}

func BenchmarkCollectionEngineGetByID(b *testing.B) {
	for _, engine := range collectionBenchEngines() {
		b.Run(engine.name, func(b *testing.B) {
			manager, cleanup := engine.open(b, b.TempDir())
			defer cleanup()

			meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
			if err != nil {
				b.Fatalf("create collection: %v", err)
			}
			col, err := manager.OpenCollection(meta.Name)
			if err != nil {
				b.Fatalf("open collection: %v", err)
			}
			if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
				b.Fatalf("seed insert: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := col.Get([]byte("u1")); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}

func BenchmarkCollectionEngineFindByIndex(b *testing.B) {
	for _, engine := range collectionBenchEngines() {
		b.Run(engine.name, func(b *testing.B) {
			manager, cleanup := engine.open(b, b.TempDir())
			defer cleanup()

			meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
			if err != nil {
				b.Fatalf("create collection: %v", err)
			}
			if _, err := manager.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
				b.Fatalf("create index: %v", err)
			}
			col, err := manager.OpenCollection(meta.Name)
			if err != nil {
				b.Fatalf("open collection: %v", err)
			}
			if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
				b.Fatalf("seed insert: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ids, err := col.FindByIndex("email_idx", "ada@example.com")
				if err != nil {
					b.Fatalf("find by index: %v", err)
				}
				if len(ids) != 1 {
					b.Fatalf("unexpected ids: %#v", ids)
				}
			}
		})
	}
}
