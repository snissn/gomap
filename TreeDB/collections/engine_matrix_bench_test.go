package collections_test

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func BenchmarkCollectionEngineInsertProvidedID(b *testing.B) {
	for _, engine := range collectionBenchEngines() {
		b.Run(engine.name, func(b *testing.B) {
			manager, _, cleanup := engine.open(b, b.TempDir())
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
			manager, checkpoint, cleanup := engine.open(b, b.TempDir())
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
			checkpoint()

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
			manager, checkpoint, cleanup := engine.open(b, b.TempDir())
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
			checkpoint()

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
