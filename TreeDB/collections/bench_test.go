package collections_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	defaultCollectionBenchBatchSize = 8000
	collectionBenchSeedDocs         = 4096
	collectionBenchCities           = 64
	collectionBenchBackfill         = 1024
)

var collectionBenchPayload = []byte(`{"name":"ada","city":"hnl","email":"ada@example.com","pad":"0123456789012345678901234567890123456789"}`)

func benchmarkBatchSize(b *testing.B) int {
	b.Helper()

	raw := strings.TrimSpace(os.Getenv("TREEDB_COLLECTION_BENCH_BATCH_SIZE"))
	if raw == "" {
		return defaultCollectionBenchBatchSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		b.Fatalf("unsupported TREEDB_COLLECTION_BENCH_BATCH_SIZE=%q", raw)
	}
	return n
}

func benchmarkTreeDBProfile(b *testing.B) treedb.Profile {
	b.Helper()

	switch strings.ToLower(strings.TrimSpace(os.Getenv("TREEDB_COLLECTION_BENCH_ENGINE"))) {
	case "", "production_fast", "backend_direct_fast", "backend_direct", "cached", "fast":
		return treedb.ProfileFast
	case "production_wal_on_fast", "backend_direct_wal_on_fast", "wal_on_fast":
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

func benchmarkBoolEnv(tb testing.TB, name string, def bool) bool {
	tb.Helper()

	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		tb.Fatalf("unsupported %s=%q", name, raw)
	}
	return v
}

func benchmarkRootStoragePolicy(outerLeavesInVLog bool) collections.RootStoragePolicy {
	if outerLeavesInVLog {
		return collections.RootStorageCompressed
	}
	return collections.RootStorageFast
}

func benchmarkCollectionStoragePolicy(tb testing.TB) (dataOuter, indexOuter bool) {
	tb.Helper()

	dataOuter = benchmarkBoolEnv(tb, "TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG", true)
	indexOuter = benchmarkBoolEnv(tb, "TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG", false)
	return dataOuter, indexOuter
}

func benchmarkStatUint64(tb testing.TB, stats map[string]string, key string) uint64 {
	tb.Helper()

	raw, ok := stats[key]
	if !ok {
		tb.Fatalf("missing benchmark stat %q", key)
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		tb.Fatalf("parse benchmark stat %q=%q: %v", key, raw, err)
	}
	return n
}

func benchmarkNativeProbeFallbackCounters(tb testing.TB, backend *backenddb.DB) (key, prefix uint64) {
	tb.Helper()

	stats := backend.Stats()
	return benchmarkStatUint64(tb, stats, "treedb.native_fastpath.per_item_key_probe_fallback_count"),
		benchmarkStatUint64(tb, stats, "treedb.native_fastpath.per_item_prefix_probe_fallback_count")
}

func benchmarkReportNativeProbeFallbackDeltas(b *testing.B, backend *backenddb.DB, startKey, startPrefix uint64) {
	b.Helper()

	endKey, endPrefix := benchmarkNativeProbeFallbackCounters(b, backend)
	if endKey < startKey {
		b.Fatalf("per_item_key_probe_fallback_count moved backwards: start=%d end=%d", startKey, endKey)
	}
	if endPrefix < startPrefix {
		b.Fatalf("per_item_prefix_probe_fallback_count moved backwards: start=%d end=%d", startPrefix, endPrefix)
	}
	keyDelta := endKey - startKey
	prefixDelta := endPrefix - startPrefix
	b.ReportMetric(float64(keyDelta), "per_item_key_probe_fallback_count")
	b.ReportMetric(float64(prefixDelta), "per_item_prefix_probe_fallback_count")
	if keyDelta != 0 {
		b.Fatalf("per_item_key_probe_fallback_count=%d want 0", keyDelta)
	}
	if prefixDelta != 0 {
		b.Fatalf("per_item_prefix_probe_fallback_count=%d want 0", prefixDelta)
	}
}

func benchmarkReportCheckpointSplit(b *testing.B, docs int, insertElapsed, syncElapsed time.Duration) {
	b.Helper()

	if docs <= 0 {
		return
	}
	b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(docs), "insert_ns/doc")
	b.ReportMetric(float64(syncElapsed.Nanoseconds())/float64(docs), "sync_ns/doc")
}

func TestBenchmarkCollectionStoragePolicyDefaultsProductionMainline(t *testing.T) {
	t.Setenv("TREEDB_COLLECTION_DATA_OUTER_LEAVES_IN_VLOG", "")
	t.Setenv("TREEDB_COLLECTION_INDEX_OUTER_LEAVES_IN_VLOG", "")
	dataOuter, indexOuter := benchmarkCollectionStoragePolicy(t)
	if !dataOuter || indexOuter {
		t.Fatalf("benchmark storage defaults data_outer=%t index_outer=%t want production-mainline data_outer=true index_outer=false", dataOuter, indexOuter)
	}
}

func openBenchmarkBackend(b *testing.B, dir string) (*backenddb.DB, func() error) {
	b.Helper()

	dataOuter, indexOuter := benchmarkCollectionStoragePolicy(b)
	opts := treedb.OptionsFor(benchmarkTreeDBProfile(b), dir)
	opts.IndexOuterLeavesInValueLog = dataOuter || indexOuter
	opts.IndexInternalBaseDelta = !opts.IndexOuterLeavesInValueLog
	open := treedb.OpenBackend
	if opts.IndexOuterLeavesInValueLog {
		open = treedb.OpenBackendWithCachedLeafLog
	}
	backend, cleanup, err := open(opts)
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
	dataOuter, indexOuter := benchmarkCollectionStoragePolicy(b)
	for i := range indexes {
		indexes[i].StoragePolicy = benchmarkRootStoragePolicy(indexOuter)
	}
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{
			DataRootStoragePolicy:   benchmarkRootStoragePolicy(dataOuter),
			IndexStateStoragePolicy: benchmarkRootStoragePolicy(dataOuter),
		},
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

func appendZeroPaddedInt(dst []byte, n, width int) []byte {
	var scratch [20]byte
	pos := len(scratch)
	if n == 0 {
		pos--
		scratch[pos] = '0'
	} else {
		for n > 0 {
			pos--
			scratch[pos] = byte('0' + n%10)
			n /= 10
		}
	}
	for pad := width - (len(scratch) - pos); pad > 0; pad-- {
		dst = append(dst, '0')
	}
	return append(dst, scratch[pos:]...)
}

func benchmarkDocumentID(n int) []byte {
	out := make([]byte, 0, len("u-")+9)
	out = append(out, "u-"...)
	return appendZeroPaddedInt(out, n, 9)
}

func benchmarkIndexedDocument(n int) []byte {
	out := make([]byte, 0, 112)
	out = append(out, `{"name":"user-`...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, `","email":"user-`...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, `@example.com","city":"city-`...)
	out = appendZeroPaddedInt(out, n%collectionBenchCities, 2)
	out = append(out, `","pad":"01234567890123456789"}`...)
	return out
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

	targetBatchSize := benchmarkBatchSize(b)
	allIDs := make([][]byte, 0, count)
	for inserted := 0; inserted < count; inserted += targetBatchSize {
		batchSize := targetBatchSize
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
	backend, collection := openBenchmarkCollection(b, "bench_insert_batch_provided")
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)

	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
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
	b.StopTimer()
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
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

func BenchmarkCollectionGetByIDParallel(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_get_parallel")
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, false)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := collection.Get(ids[i%len(ids)]); err != nil {
				b.Errorf("parallel get: %v", err)
			}
			i++
		}
	})
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
	backend, collection := openBenchmarkCollection(b, "bench_insert_batch_secondary", secondaryIndexes()...)
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)

	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
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
	b.StopTimer()
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
}

func BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes(b *testing.B) {
	backend, collection := openBenchmarkCollection(b, "bench_insert_batch_checkpoint_secondary", secondaryIndexes()...)
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)
	var insertElapsed time.Duration
	var syncElapsed time.Duration

	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), "target_docs/checkpoint")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(inserted, batchSize, true)
		b.StartTimer()

		insertStart := time.Now()
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert batch with secondary indexes: %v", err)
		}
		insertElapsed += time.Since(insertStart)
		syncStart := time.Now()
		benchmarkSyncBoundary(b, backend)
		syncElapsed += time.Since(syncStart)
		inserted += batchSize
	}
	b.StopTimer()
	benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
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
		dataOuter, indexOuter := benchmarkCollectionStoragePolicy(b)
		if _, err := manager.CreateCollection(&collections.CollectionMeta{
			Name: "bench_backfill",
			Options: collections.CollectionOptions{
				DataRootStoragePolicy:   benchmarkRootStoragePolicy(dataOuter),
				IndexStateStoragePolicy: benchmarkRootStoragePolicy(dataOuter),
			},
		}); err != nil {
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

		if _, err := collection.CreateIndex(collections.IndexDefinition{
			Name:          "email_idx",
			Field:         "email",
			Unique:        true,
			StoragePolicy: benchmarkRootStoragePolicy(indexOuter),
		}); err != nil {
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
