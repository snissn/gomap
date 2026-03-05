package collections

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkCollectionCreate(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	manager := NewCollectionManager(database)
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		meta := &CollectionMeta{Name: fmt.Sprintf("bench_create_%d", idx)}
		if _, err := manager.CreateCollection(meta); err != nil {
			b.Fatalf("create collection: %v", err)
		}
	}
}

func BenchmarkCollectionInsertProvidedID(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{
		Name: "bench_insert_provided",
		Options: CollectionOptions{
			IDMode: idModeCallerProvided,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}

	ids := make([][]byte, b.N)
	docs := make([][]byte, b.N)
	for idx := 0; idx < b.N; idx++ {
		ids[idx] = []byte(fmt.Sprintf("doc-%d", idx))
		docs[idx] = []byte("payload")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.Insert(ids[idx], docs[idx]); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkCollectionInsertAutoID(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{
		Name: "bench_insert_auto",
		Options: CollectionOptions{
			IDMode: idModeAuto,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.Insert(nil, []byte("payload")); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkCollectionGetByID(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{
		Name: "bench_get",
		Options: CollectionOptions{
			IDMode: idModeCallerProvided,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}

	const docCount = 1024
	ids := make([][]byte, docCount)
	for idx := 0; idx < docCount; idx++ {
		id := []byte(fmt.Sprintf("doc-%d", idx))
		ids[idx] = id
		if _, err := collection.Insert(id, []byte("payload")); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		id := ids[idx%docCount]
		if _, err := collection.Get(id); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkCollectionDeleteByID(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{
		Name: "bench_delete",
		Options: CollectionOptions{
			IDMode: idModeCallerProvided,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}

	ids := make([][]byte, b.N)
	for idx := 0; idx < b.N; idx++ {
		id := []byte(fmt.Sprintf("doc-%d", idx))
		ids[idx] = id
		if _, err := collection.Insert(id, []byte("payload")); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if err := collection.Delete(ids[idx]); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkSecondaryLookupUnique(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()
	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{Name: "bench_secondary_unique"})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := manager.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}
	const count = 1024
	emails := make([]string, count)
	for idx := 0; idx < count; idx++ {
		email := fmt.Sprintf("user-%d@example.com", idx)
		emails[idx] = email
		doc := []byte(fmt.Sprintf(`{"email":%q}`, email))
		if _, err := collection.Insert([]byte(fmt.Sprintf("u-%d", idx)), doc); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.FindByIndex("email_idx", emails[idx%count]); err != nil {
			b.Fatalf("find: %v", err)
		}
	}
}

func BenchmarkSecondaryUpsertFieldChange(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()
	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{Name: "bench_secondary_upsert"})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := manager.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}
	id := []byte("u-1")
	if _, err := collection.Insert(id, []byte(`{"city":"hnl"}`)); err != nil {
		b.Fatal(err)
	}
	docs := [][]byte{
		[]byte(`{"city":"hnl"}`),
		[]byte(`{"city":"sea"}`),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.Insert(id, docs[idx%len(docs)]); err != nil {
			b.Fatalf("upsert: %v", err)
		}
	}
}

func BenchmarkCollectionStats(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{Name: "bench_stats"})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := manager.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		b.Fatal(err)
	}
	if _, err := manager.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}
	for idx := 0; idx < 1024; idx++ {
		doc := []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"hnl"}`, idx))
		if _, err := collection.Insert([]byte(fmt.Sprintf("u-%d", idx)), doc); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.Stats(); err != nil {
			b.Fatalf("stats: %v", err)
		}
	}
}

func BenchmarkCollectionCheckConsistency(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{Name: "bench_consistency"})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := manager.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		b.Fatal(err)
	}
	if _, err := manager.CreateIndex(meta.Name, IndexDefinition{Name: "city_idx", Field: "city"}); err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}
	for idx := 0; idx < 1024; idx++ {
		doc := []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"hnl"}`, idx))
		if _, err := collection.Insert([]byte(fmt.Sprintf("u-%d", idx)), doc); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.CheckConsistency(); err != nil {
			b.Fatalf("check consistency: %v", err)
		}
	}
}
