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

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		id := []byte(fmt.Sprintf("doc-%d", idx))
		if _, err := collection.Insert(id, []byte("payload")); err != nil {
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

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		id := []byte(fmt.Sprintf("doc-%d", idx))
		if _, err := collection.Insert(id, []byte("payload")); err != nil {
			b.Fatalf("insert before delete: %v", err)
		}
		if err := collection.Delete(id); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}
