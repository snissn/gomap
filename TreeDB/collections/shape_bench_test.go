package collections_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	defaultCollectionMixedSeedDocs       = 4096
	defaultCollectionMixedWriteBatchSize = 128
)

func addCollectionInsertStats(dst *collections.CollectionInsertStats, src collections.CollectionInsertStats) {
	dst.Documents += src.Documents
	dst.Indexes += src.Indexes
	dst.Runs += src.Runs
	dst.PrepareDocuments += src.PrepareDocuments
	dst.IndexStateExtraction += src.IndexStateExtraction
	dst.DuplicateDocumentPreflight += src.DuplicateDocumentPreflight
	dst.UniqueIndexPreflight += src.UniqueIndexPreflight
	dst.TemplateRunBuild += src.TemplateRunBuild
	dst.PrimaryRunBuild += src.PrimaryRunBuild
	dst.IndexStateRunBuild += src.IndexStateRunBuild
	dst.SecondaryRunBuild += src.SecondaryRunBuild
	dst.Publish += src.Publish
	dst.SecondaryEntries += src.SecondaryEntries
	dst.SecondaryKeyBytes += src.SecondaryKeyBytes
	dst.SecondarySortedRuns += src.SecondarySortedRuns
	dst.SecondaryUnsortedRuns += src.SecondaryUnsortedRuns
}

func benchmarkReportCollectionInsertStats(b *testing.B, docs, batches int, stats collections.CollectionInsertStats) {
	b.Helper()
	if docs <= 0 {
		return
	}
	reportDuration := func(name string, d time.Duration) {
		if d > 0 {
			b.ReportMetric(float64(d.Nanoseconds())/float64(docs), name)
		}
	}
	reportDuration("prepare_ns/doc", stats.PrepareDocuments)
	reportDuration("index_state_extract_ns/doc", stats.IndexStateExtraction)
	reportDuration("duplicate_preflight_ns/doc", stats.DuplicateDocumentPreflight)
	reportDuration("unique_preflight_ns/doc", stats.UniqueIndexPreflight)
	reportDuration("template_run_ns/doc", stats.TemplateRunBuild)
	reportDuration("primary_run_ns/doc", stats.PrimaryRunBuild)
	reportDuration("index_state_run_ns/doc", stats.IndexStateRunBuild)
	reportDuration("secondary_runs_ns/doc", stats.SecondaryRunBuild)
	reportDuration("publish_ns/doc", stats.Publish)
	if stats.SecondaryEntries > 0 {
		b.ReportMetric(float64(stats.SecondaryEntries)/float64(docs), "secondary_entries/doc")
	}
	if stats.SecondaryKeyBytes > 0 {
		b.ReportMetric(float64(stats.SecondaryKeyBytes)/float64(docs), "secondary_key_bytes/doc")
	}
	if batches > 0 {
		b.ReportMetric(float64(stats.Runs)/float64(batches), "roots/batch")
		b.ReportMetric(float64(stats.SecondarySortedRuns)/float64(batches), "secondary_sorted_runs/batch")
		b.ReportMetric(float64(stats.SecondaryUnsortedRuns)/float64(batches), "secondary_unsorted_runs/batch")
	}
}

func collectionShapeIndexes(indexCount int) []collections.IndexDefinition {
	switch indexCount {
	case 0:
		return nil
	case 1:
		return []collections.IndexDefinition{{Name: "email_idx", Field: "email", Unique: true}}
	case 2:
		return []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", Unique: true},
			{Name: "city_idx", Field: "city"},
		}
	case 3:
		return []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", Unique: true},
			{Name: "city_idx", Field: "city"},
			{Name: "name_idx", Field: "name"},
		}
	default:
		panic(fmt.Sprintf("unsupported collection benchmark index count %d", indexCount))
	}
}

func collectionSingleStringIndexes(indexCount int) []collections.IndexDefinition {
	switch indexCount {
	case 0:
		return nil
	case 1:
		return []collections.IndexDefinition{{Name: "value_idx", Field: "value", Unique: true}}
	default:
		panic(fmt.Sprintf("unsupported single-string benchmark index count %d", indexCount))
	}
}

func benchmarkSingleStringDocument(n int) []byte {
	out := make([]byte, 0, len(`{"value":"value-000000000"}`))
	out = append(out, `{"value":"value-`...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, `"}`...)
	return out
}

func benchmarkSingleStringDocumentBatch(tb testing.TB, start, count int) ([][]byte, [][]byte) {
	tb.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		docNum := start + i
		ids[i] = benchmarkDocumentID(docNum)
		docs[i] = benchmarkSingleStringDocument(docNum)
	}
	return ids, docs
}

func benchmarkCollectionShapeInsertBatch(b *testing.B, indexCount int, checkpoint bool) {
	backend, collection := openBenchmarkCollection(b, fmt.Sprintf("bench_shape_insert_%d", indexCount), collectionShapeIndexes(indexCount)...)
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)
	var insertElapsed time.Duration
	var syncElapsed time.Duration
	var insertStats collections.CollectionInsertStats
	batches := 0

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), metricName)
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkDocumentBatch(b, inserted, batchSize, true)
		b.StartTimer()

		insertStart := time.Now()
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("shape insert batch indexes=%d: %v", indexCount, err)
		}
		addCollectionInsertStats(&insertStats, collection.LastInsertStats())
		batches++
		insertElapsed += time.Since(insertStart)
		if checkpoint {
			syncStart := time.Now()
			benchmarkSyncBoundary(b, backend)
			syncElapsed += time.Since(syncStart)
		}
		inserted += batchSize
	}
	b.StopTimer()
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportCollectionInsertStats(b, b.N, batches, insertStats)
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
}

func BenchmarkCollectionShapeInsertBatch(b *testing.B) {
	for _, indexCount := range []int{0, 1, 2, 3} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeInsertBatch(b, indexCount, false)
		})
	}
}

func BenchmarkCollectionShapeInsertBatchCheckpoint(b *testing.B) {
	for _, indexCount := range []int{0, 1, 2, 3} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeInsertBatch(b, indexCount, true)
		})
	}
}

func benchmarkCollectionShapeSingleStringInsertBatch(b *testing.B, indexCount int, checkpoint bool) {
	if benchmarkCollectionDocumentFormat(b) != collections.DocumentFormatJSON {
		b.Skip("single-string shape benchmark uses JSON documents")
	}
	backend, collection := openBenchmarkCollection(b, fmt.Sprintf("bench_shape_single_string_%d", indexCount), collectionSingleStringIndexes(indexCount)...)
	targetBatchSize := benchmarkBatchSize(b)
	startKeyFallback, startPrefixFallback := benchmarkNativeProbeFallbackCounters(b, backend)
	var insertElapsed time.Duration
	var syncElapsed time.Duration
	var insertStats collections.CollectionInsertStats
	batches := 0

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
	b.ReportMetric(float64(targetBatchSize), metricName)
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids, docs := benchmarkSingleStringDocumentBatch(b, inserted, batchSize)
		b.StartTimer()

		insertStart := time.Now()
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			b.Fatalf("single-string insert batch indexes=%d: %v", indexCount, err)
		}
		addCollectionInsertStats(&insertStats, collection.LastInsertStats())
		batches++
		insertElapsed += time.Since(insertStart)
		if checkpoint {
			syncStart := time.Now()
			benchmarkSyncBoundary(b, backend)
			syncElapsed += time.Since(syncStart)
		}
		inserted += batchSize
	}
	b.StopTimer()
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportCollectionInsertStats(b, b.N, batches, insertStats)
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
}

func BenchmarkCollectionShapeInsertBatchSingleStringJSON(b *testing.B) {
	for _, indexCount := range []int{0, 1} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeSingleStringInsertBatch(b, indexCount, false)
		})
	}
}

func BenchmarkCollectionShapeInsertBatchCheckpointSingleStringJSON(b *testing.B) {
	for _, indexCount := range []int{0, 1} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeSingleStringInsertBatch(b, indexCount, true)
		})
	}
}

func benchmarkCollectionShapeReadPrimary(b *testing.B, indexCount int, parallel bool) {
	backend, collection := openBenchmarkCollection(b, fmt.Sprintf("bench_shape_read_%d", indexCount), collectionShapeIndexes(indexCount)...)
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if _, err := collection.Get(ids[i%len(ids)]); err != nil {
					b.Errorf("shape parallel primary read indexes=%d: %v", indexCount, err)
				}
				i++
			}
		})
		return
	}
	for i := 0; i < b.N; i++ {
		if _, err := collection.Get(ids[i%len(ids)]); err != nil {
			b.Fatalf("shape primary read indexes=%d: %v", indexCount, err)
		}
	}
}

func BenchmarkCollectionShapeReadPrimary(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeReadPrimary(b, indexCount, false)
		})
	}
}

func BenchmarkCollectionShapeReadPrimaryParallel(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeReadPrimary(b, indexCount, true)
		})
	}
}

func benchmarkCollectionMixedReadWrite(b *testing.B, secondaryRead bool) {
	indexes := collectionShapeIndexes(2)
	collectionName := "bench_shape_mixed_read_write"
	backend, seedCollection := openBenchmarkCollection(b, collectionName, indexes...)
	seedDocs := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_SEED_DOCS", defaultCollectionMixedSeedDocs)
	if seedDocs <= 0 {
		seedDocs = defaultCollectionMixedSeedDocs
	}
	ids := seedBenchmarkCollection(b, seedCollection, 0, seedDocs, true)
	benchmarkSyncBoundary(b, backend)

	manager := collections.NewCollectionManager(backend)
	readerCollection, err := manager.OpenCollection(collectionName)
	if err != nil {
		b.Fatalf("open mixed reader collection: %v", err)
	}
	writerCollection, err := manager.OpenCollection(collectionName)
	if err != nil {
		b.Fatalf("open mixed writer collection: %v", err)
	}

	writeBatchSize := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_WRITE_BATCH_SIZE", defaultCollectionMixedWriteBatchSize)
	if writeBatchSize <= 0 {
		writeBatchSize = defaultCollectionMixedWriteBatchSize
	}
	if maxBatch := benchmarkBatchSize(b); writeBatchSize > maxBatch {
		writeBatchSize = maxBatch
	}

	var stop atomic.Bool
	var writerDocs atomic.Uint64
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	documentFormat := benchmarkCollectionDocumentFormat(b)

	b.ReportAllocs()
	b.ReportMetric(float64(seedDocs), "seed_docs")
	b.ReportMetric(float64(writeBatchSize), "writer_docs/batch")
	b.ResetTimer()
	start := time.Now()

	go func() {
		defer wg.Done()
		var templateEncoder collections.TemplateV1Encoder
		for next := 1_000_000; !stop.Load(); next += writeBatchSize {
			ids := make([][]byte, writeBatchSize)
			docs := make([][]byte, writeBatchSize)
			for i := 0; i < writeBatchSize; i++ {
				docNum := next + i
				ids[i] = benchmarkDocumentID(docNum)
				if documentFormat == collections.DocumentFormatTemplateV1 {
					doc, err := templateEncoder.EncodeDocument(
						[]string{"name", "email", "city", "pad"},
						[]any{
							fmt.Sprintf("user-%09d", docNum),
							fmt.Sprintf("user-%09d@example.com", docNum),
							fmt.Sprintf("city-%02d", docNum%collectionBenchCities),
							collectionBenchIndexedPad,
						},
					)
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					docs[i] = doc
				} else {
					docs[i] = benchmarkIndexedDocument(docNum)
				}
			}
			if _, err := writerCollection.InsertBatch(ids, docs); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			writerDocs.Add(uint64(writeBatchSize))
		}
	}()

	readerStride := runtime.GOMAXPROCS(0)
	if readerStride <= 0 {
		readerStride = 1
	}
	var readerOffsets atomic.Uint64
	nextReaderStart := func(limit int) int {
		if limit <= 0 {
			return 0
		}
		spacing := limit / readerStride
		if spacing <= 0 {
			spacing = 1
		}
		readerID := int(readerOffsets.Add(1) - 1)
		return (readerID * spacing) % limit
	}

	if secondaryRead {
		b.RunParallel(func(pb *testing.PB) {
			i := nextReaderStart(seedDocs)
			for pb.Next() {
				email := fmt.Sprintf("user-%09d@example.com", i%seedDocs)
				if _, err := readerCollection.FindByIndex("email_idx", email); err != nil {
					b.Errorf("mixed secondary read: %v", err)
				}
				i += readerStride
			}
		})
	} else {
		b.RunParallel(func(pb *testing.PB) {
			i := nextReaderStart(len(ids))
			for pb.Next() {
				if _, err := readerCollection.Get(ids[i%len(ids)]); err != nil {
					b.Errorf("mixed primary read: %v", err)
				}
				i += readerStride
			}
		})
	}

	b.StopTimer()
	stop.Store(true)
	wg.Wait()
	end := time.Now()
	elapsed := end.Sub(start)
	select {
	case err := <-errCh:
		b.Fatalf("mixed writer: %v", err)
	default:
	}
	if elapsed > 0 {
		b.ReportMetric(float64(writerDocs.Load())/elapsed.Seconds(), "writer_docs/sec")
	}
}

func BenchmarkCollectionMixedReadWritePrimary(b *testing.B) {
	benchmarkCollectionMixedReadWrite(b, false)
}

func BenchmarkCollectionMixedReadWriteSecondaryUnique(b *testing.B) {
	benchmarkCollectionMixedReadWrite(b, true)
}
