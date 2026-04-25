package collections_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	collectionBenchBatchSize = 256
	collectionBenchSeedDocs  = 4096
	collectionBenchCities    = 64
	collectionBenchBackfill  = 1024
)

var collectionBenchPayload = []byte(`{"name":"ada","city":"hnl","email":"ada@example.com","pad":"0123456789012345678901234567890123456789"}`)

func benchmarkTreeDBProfile(b *testing.B) treedb.Profile {
	b.Helper()

	switch strings.ToLower(strings.TrimSpace(os.Getenv("TREEDB_COLLECTION_BENCH_ENGINE"))) {
	case "", "backend_direct_fast", "backend_direct", "cached", "fast":
		return treedb.ProfileFast
	case "backend_direct_wal_on_fast", "wal_on_fast":
		return treedb.ProfileWALOnFast
	case "durable":
		return treedb.ProfileDurable
	case "bench":
		return treedb.ProfileBench
	default:
		b.Fatalf("unsupported TREEDB_COLLECTION_BENCH_ENGINE=%q", os.Getenv("TREEDB_COLLECTION_BENCH_ENGINE"))
		return treedb.ProfileFast
	}
}

func openBenchmarkBackend(b *testing.B, dir string) (*backenddb.DB, func() error) {
	b.Helper()

	opts := treedb.OptionsFor(benchmarkTreeDBProfile(b), dir)
	// Native collections currently run on the backend-root API, not the cached
	// public wrapper. Backend direct opens do not wire the cached leaf-page log
	// required by IndexOuterLeavesInValueLog, so keep the other fast-profile
	// knobs while using backend-native leaf storage.
	opts.IndexOuterLeavesInValueLog = false
	opts.IndexInternalBaseDelta = true
	backend, cleanup, err := treedb.OpenBackend(opts)
	if err != nil {
		b.Fatalf("open backend: %v", err)
	}
	return backend, cleanup
}

func openBenchmarkCollection(b *testing.B, name string, indexes ...collections.IndexDefinition) (*backenddb.DB, *collections.Collection) {
	b.Helper()

	backend, cleanup := openBenchmarkBackend(b, b.TempDir())
	b.Cleanup(func() {
		if err := cleanup(); err != nil {
			b.Errorf("close backend: %v", err)
		}
	})

	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name:    name,
		Indexes: indexes,
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection(name)
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	return backend, collection
}

func benchmarkSyncBoundary(b *testing.B, backend *backenddb.DB) {
	b.Helper()

	batch := backend.NewBatch()
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		b.Fatalf("sync boundary: %v", err)
	}
	if err := batch.Close(); err != nil {
		b.Fatalf("close sync boundary batch: %v", err)
	}
}

func benchmarkDocumentID(n int) []byte {
	return []byte(fmt.Sprintf("u-%09d", n))
}

func benchmarkIndexedDocument(n int) []byte {
	return []byte(fmt.Sprintf(
		`{"name":"user-%09d","email":"user-%09d@example.com","city":"city-%02d","pad":"01234567890123456789"}`,
		n,
		n,
		n%collectionBenchCities,
	))
}

func benchmarkDocumentBatch(start, count int, indexed bool) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		docNum := start + i
		ids[i] = benchmarkDocumentID(docNum)
		if indexed {
			docs[i] = benchmarkIndexedDocument(docNum)
		} else {
			docs[i] = collectionBenchPayload
		}
	}
	return ids, docs
}

func seedBenchmarkCollection(b *testing.B, collection *collections.Collection, start, count int, indexed bool) [][]byte {
	b.Helper()

	allIDs := make([][]byte, 0, count)
	for inserted := 0; inserted < count; inserted += collectionBenchBatchSize {
		batchSize := collectionBenchBatchSize
		if remaining := count - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(start+inserted, batchSize, indexed)
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("seed insert batch: %v", err)
		}
		allIDs = append(allIDs, ids...)
	}
	return allIDs
}

func secondaryIndexes() []collections.IndexDefinition {
	return []collections.IndexDefinition{
		{Name: "email_idx", Field: "email", Unique: true},
		{Name: "city_idx", Field: "city"},
	}
}

func BenchmarkCollectionInsertProvidedID(b *testing.B) {
	_, collection := openBenchmarkCollection(b, "bench_insert_provided")
	ids, docs := benchmarkDocumentBatch(0, b.N, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collection.Insert(ids[i], docs[i]); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkCollectionInsertBatchProvidedID(b *testing.B) {
	_, collection := openBenchmarkCollection(b, "bench_insert_batch_provided")

	b.ReportAllocs()
	b.ReportMetric(float64(collectionBenchBatchSize), "target_docs/batch")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := collectionBenchBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(inserted, batchSize, false)
		b.StartTimer()

		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert batch: %v", err)
		}
		inserted += batchSize
	}
}

func BenchmarkCollectionGetByID(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_get")
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, false)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collection.Get(ids[i%len(ids)]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkCollectionDeleteByID(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_delete")
	ids := seedBenchmarkCollection(b, collection, 0, b.N, false)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := collection.Delete(ids[i]); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkCollectionInsertWithSecondaryIndexes(b *testing.B) {
	_, collection := openBenchmarkCollection(b, "bench_insert_secondary", secondaryIndexes()...)
	ids, docs := benchmarkDocumentBatch(0, b.N, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := collection.Insert(ids[i], docs[i]); err != nil {
			b.Fatalf("insert with secondary indexes: %v", err)
		}
	}
}

func BenchmarkCollectionInsertBatchWithSecondaryIndexes(b *testing.B) {
	_, collection := openBenchmarkCollection(b, "bench_insert_batch_secondary", secondaryIndexes()...)

	b.ReportAllocs()
	b.ReportMetric(float64(collectionBenchBatchSize), "target_docs/batch")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := collectionBenchBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(inserted, batchSize, true)
		b.StartTimer()

		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert batch with secondary indexes: %v", err)
		}
		inserted += batchSize
	}
}

func BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_insert_batch_checkpoint_secondary", secondaryIndexes()...)

	b.ReportAllocs()
	b.ReportMetric(float64(collectionBenchBatchSize), "target_docs/checkpoint")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := collectionBenchBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(inserted, batchSize, true)
		b.StartTimer()

		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert batch with secondary indexes: %v", err)
		}
		benchmarkSyncBoundary(b, backend)
		inserted += batchSize
	}
}

func BenchmarkCollectionDeleteWithSecondaryIndexes(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_delete_secondary", secondaryIndexes()...)
	ids := seedBenchmarkCollection(b, collection, 0, b.N, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := collection.Delete(ids[i]); err != nil {
			b.Fatalf("delete with secondary indexes: %v", err)
		}
	}
}

func BenchmarkSecondaryLookupUnique(b *testing.B) {
	backend, collection := openBenchmarkCollection(
		b,
		"bench_secondary_unique",
		collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true},
	)
	seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		email := fmt.Sprintf("user-%09d@example.com", i%collectionBenchSeedDocs)
		if _, err := collection.FindByIndex("email_idx", email); err != nil {
			b.Fatalf("lookup unique: %v", err)
		}
	}
}

func BenchmarkSecondaryLookupNonUnique(b *testing.B) {
	backend, collection := openBenchmarkCollection(
		b,
		"bench_secondary_non_unique",
		collections.IndexDefinition{Name: "city_idx", Field: "city"},
	)
	seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		city := fmt.Sprintf("city-%02d", i%collectionBenchCities)
		if _, err := collection.FindByIndex("city_idx", city); err != nil {
			b.Fatalf("lookup non-unique: %v", err)
		}
	}
}

func BenchmarkCollectionCreateIndexBackfillExistingDocs(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, err := os.MkdirTemp("", "gomap_collections_backfill_*")
		if err != nil {
			b.Fatalf("create temp dir: %v", err)
		}
		backend, cleanup := openBenchmarkBackend(b, dir)
		manager := collections.NewCollectionManager(backend)
		if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "bench_backfill"}); err != nil {
			_ = cleanup()
			_ = os.RemoveAll(dir)
			b.Fatalf("create collection: %v", err)
		}
		collection, err := manager.OpenCollection("bench_backfill")
		if err != nil {
			_ = cleanup()
			_ = os.RemoveAll(dir)
			b.Fatalf("open collection: %v", err)
		}
		seedBenchmarkCollection(b, collection, 0, collectionBenchBackfill, true)
		benchmarkSyncBoundary(b, backend)
		b.StartTimer()

		if _, err := collection.CreateIndex(collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
			b.Fatalf("create index with backfill: %v", err)
		}

		b.StopTimer()
		if err := cleanup(); err != nil {
			_ = os.RemoveAll(dir)
			b.Fatalf("close backend: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatalf("remove temp dir: %v", err)
		}
		b.StartTimer()
	}
}
