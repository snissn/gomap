package collections

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func openFocusedBenchmarkCollection(b *testing.B, name string, indexes ...IndexDefinition) (*CollectionManager, *Collection) {
	b.Helper()

	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = database.Close()
	})

	manager := NewCollectionManager(database)
	meta, err := manager.CreateCollection(&CollectionMeta{
		Name: name,
		Options: CollectionOptions{
			IDMode: idModeCallerProvided,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, index := range indexes {
		if _, err := manager.CreateIndex(meta.Name, index); err != nil {
			b.Fatal(err)
		}
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		b.Fatal(err)
	}
	return manager, collection
}

func BenchmarkCollectionInsertWithSecondaryIndexes(b *testing.B) {
	_, collection := openFocusedBenchmarkCollection(
		b,
		"bench_insert_secondary_indexes",
		IndexDefinition{Name: "email_idx", Field: "email", Unique: true},
		IndexDefinition{Name: "city_idx", Field: "city"},
	)

	ids := make([][]byte, b.N)
	docs := make([][]byte, b.N)
	for idx := 0; idx < b.N; idx++ {
		ids[idx] = []byte(fmt.Sprintf("u-%d", idx))
		docs[idx] = []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"city-%d"}`, idx, idx%32))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.Insert(ids[idx], docs[idx]); err != nil {
			b.Fatalf("insert with secondary indexes: %v", err)
		}
	}
}

func BenchmarkCollectionDeleteWithSecondaryIndexes(b *testing.B) {
	_, collection := openFocusedBenchmarkCollection(
		b,
		"bench_delete_secondary_indexes",
		IndexDefinition{Name: "email_idx", Field: "email", Unique: true},
		IndexDefinition{Name: "city_idx", Field: "city"},
	)

	ids := make([][]byte, b.N)
	for idx := 0; idx < b.N; idx++ {
		id := []byte(fmt.Sprintf("u-%d", idx))
		ids[idx] = id
		doc := []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"city-%d"}`, idx, idx%32))
		if _, err := collection.Insert(id, doc); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if err := collection.Delete(ids[idx]); err != nil {
			b.Fatalf("delete with secondary indexes: %v", err)
		}
	}
}

func BenchmarkSecondaryLookupNonUnique(b *testing.B) {
	_, collection := openFocusedBenchmarkCollection(
		b,
		"bench_secondary_non_unique",
		IndexDefinition{Name: "city_idx", Field: "city"},
	)

	const (
		docCount  = 4096
		cityCount = 32
	)
	queries := make([]string, docCount)
	for idx := 0; idx < docCount; idx++ {
		city := fmt.Sprintf("city-%d", idx%cityCount)
		queries[idx] = city
		doc := []byte(fmt.Sprintf(`{"city":"%s","email":"user-%d@example.com"}`, city, idx))
		if _, err := collection.Insert([]byte(fmt.Sprintf("u-%d", idx)), doc); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.FindByIndex("city_idx", queries[idx%docCount]); err != nil {
			b.Fatalf("lookup non-unique: %v", err)
		}
	}
}

func BenchmarkCollectionCreateIndexBackfillExistingDocs(b *testing.B) {
	database, err := db.Open(db.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()

	manager := NewCollectionManager(database)
	const seededDocs = 1024
	ids := make([][]byte, seededDocs)
	docs := make([][]byte, seededDocs)
	for idx := 0; idx < seededDocs; idx++ {
		ids[idx] = []byte(fmt.Sprintf("u-%d", idx))
		docs[idx] = []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"city-%d"}`, idx, idx%32))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		b.StopTimer()
		name := fmt.Sprintf("bench_backfill_%d", idx)
		meta, err := manager.CreateCollection(&CollectionMeta{
			Name: name,
			Options: CollectionOptions{
				IDMode: idModeCallerProvided,
			},
		})
		if err != nil {
			b.Fatalf("create collection: %v", err)
		}
		collection, err := manager.OpenCollection(meta.Name)
		if err != nil {
			b.Fatalf("open collection: %v", err)
		}
		for docIdx := 0; docIdx < seededDocs; docIdx++ {
			if _, err := collection.Insert(ids[docIdx], docs[docIdx]); err != nil {
				b.Fatalf("seed insert: %v", err)
			}
		}
		b.StartTimer()
		if _, err := manager.CreateIndex(meta.Name, IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
			b.Fatalf("create index with backfill: %v", err)
		}
		b.StopTimer()
		if err := manager.DropCollection(meta.Name); err != nil {
			b.Fatalf("drop collection: %v", err)
		}
		b.StartTimer()
	}
}
