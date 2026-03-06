package collections_test

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func openFocusedBenchmarkCollection(b *testing.B, name string, indexes ...collections.IndexDefinition) (*collections.CollectionManager, *collections.Collection, func()) {
	b.Helper()

	manager, checkpoint, cleanup := openCollectionBenchmarkManager(b)
	b.Cleanup(cleanup)

	meta, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{
			IDMode: collections.IDModeCallerProvided,
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
	return manager, collection, checkpoint
}

func BenchmarkCollectionInsertWithSecondaryIndexes(b *testing.B) {
	_, collection, _ := openFocusedBenchmarkCollection(
		b,
		"bench_insert_secondary_indexes",
		collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true},
		collections.IndexDefinition{Name: "city_idx", Field: "city"},
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

func BenchmarkCollectionInsertBatchProvidedID(b *testing.B) {
	_, collection, _ := openFocusedBenchmarkCollection(
		b,
		"bench_insert_batch_provided",
	)

	const batchSize = 256
	ids := make([][]byte, b.N*batchSize)
	docs := make([][]byte, b.N*batchSize)
	for idx := range ids {
		ids[idx] = []byte(fmt.Sprintf("u-%d", idx))
		docs[idx] = []byte(`{"name":"ada"}`)
	}

	b.ReportAllocs()
	b.ReportMetric(batchSize, "docs/op")
	b.ResetTimer()
	for batchIdx := 0; batchIdx < b.N; batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if _, err := collection.InsertBatch(ids[start:end], docs[start:end]); err != nil {
			b.Fatalf("insert batch provided: %v", err)
		}
	}
}

func BenchmarkCollectionInsertBatchWithSecondaryIndexes(b *testing.B) {
	_, collection, _ := openFocusedBenchmarkCollection(
		b,
		"bench_insert_batch_secondary_indexes",
		collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true},
		collections.IndexDefinition{Name: "city_idx", Field: "city"},
	)

	const batchSize = 256
	ids := make([][]byte, b.N*batchSize)
	docs := make([][]byte, b.N*batchSize)
	for idx := range ids {
		ids[idx] = []byte(fmt.Sprintf("u-%d", idx))
		docs[idx] = []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"city-%d"}`, idx, idx%32))
	}

	b.ReportAllocs()
	b.ReportMetric(batchSize, "docs/op")
	b.ResetTimer()
	for batchIdx := 0; batchIdx < b.N; batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if _, err := collection.InsertBatch(ids[start:end], docs[start:end]); err != nil {
			b.Fatalf("insert batch secondary indexes: %v", err)
		}
	}
}

func BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	_, collection, checkpoint := openFocusedBenchmarkCollection(
		b,
		"bench_insert_batch_checkpoint_secondary_indexes",
		collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true},
		collections.IndexDefinition{Name: "city_idx", Field: "city"},
	)

	const batchSize = 256
	ids := make([][]byte, batchSize)
	docs := make([][]byte, batchSize)

	b.ReportAllocs()
	b.ReportMetric(batchSize, "docs/checkpoint")
	b.ResetTimer()
	for batchIdx := 0; batchIdx < b.N; batchIdx++ {
		base := batchIdx * batchSize
		for idx := range ids {
			docIdx := base + idx
			ids[idx] = []byte(fmt.Sprintf("u-%d", docIdx))
			docs[idx] = []byte(fmt.Sprintf(`{"email":"user-%d@example.com","city":"city-%d"}`, docIdx, docIdx%32))
		}
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert batch secondary indexes: %v", err)
		}
		checkpoint()
	}
}

func BenchmarkCollectionDeleteWithSecondaryIndexes(b *testing.B) {
	_, collection, checkpoint := openFocusedBenchmarkCollection(
		b,
		"bench_delete_secondary_indexes",
		collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true},
		collections.IndexDefinition{Name: "city_idx", Field: "city"},
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
	checkpoint()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if err := collection.Delete(ids[idx]); err != nil {
			b.Fatalf("delete with secondary indexes: %v", err)
		}
	}
}

func BenchmarkSecondaryLookupNonUnique(b *testing.B) {
	_, collection, checkpoint := openFocusedBenchmarkCollection(
		b,
		"bench_secondary_non_unique",
		collections.IndexDefinition{Name: "city_idx", Field: "city"},
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
	checkpoint()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if _, err := collection.FindByIndex("city_idx", queries[idx%docCount]); err != nil {
			b.Fatalf("lookup non-unique: %v", err)
		}
	}
}

func BenchmarkCollectionCreateIndexBackfillExistingDocs(b *testing.B) {
	manager, checkpoint, cleanup := openCollectionBenchmarkManager(b)
	defer cleanup()
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
		meta, err := manager.CreateCollection(&collections.CollectionMeta{
			Name: name,
			Options: collections.CollectionOptions{
				IDMode: collections.IDModeCallerProvided,
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
		checkpoint()
		b.StartTimer()
		if _, err := manager.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
			b.Fatalf("create index with backfill: %v", err)
		}
		b.StopTimer()
		checkpoint()
		if err := manager.DropCollection(meta.Name); err != nil {
			b.Fatalf("drop collection: %v", err)
		}
		b.StartTimer()
	}
}
