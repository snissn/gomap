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

type collectionMixedScaleCase struct {
	readers int
	writers int
}

func addCollectionInsertStats(dst *collections.CollectionInsertStats, src collections.CollectionInsertStats) {
	dst.Documents += src.Documents
	dst.Indexes += src.Indexes
	dst.Runs += src.Runs
	dst.BufferedIndexedBatches += src.BufferedIndexedBatches
	dst.BufferedIndexedBypassBatches += src.BufferedIndexedBypassBatches
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
		if stats.BufferedIndexedBatches > 0 {
			b.ReportMetric(float64(stats.BufferedIndexedBatches), "buffered_indexed_batches")
		}
		if stats.BufferedIndexedBypassBatches > 0 {
			b.ReportMetric(float64(stats.BufferedIndexedBypassBatches), "buffered_indexed_bypass_batches")
		}
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
	var bufferedFlushElapsed time.Duration
	var syncElapsed time.Duration
	var insertStats collections.CollectionInsertStats
	batches := 0
	bufferedIndexedWrites := collection.Meta().Options.BufferedIndexedWrites

	metricName := "target_docs/batch"
	if checkpoint {
		metricName = "target_docs/checkpoint"
	}
	b.ReportAllocs()
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
		insertElapsed += time.Since(insertStart)
		b.StopTimer()
		addCollectionInsertStats(&insertStats, collection.LastInsertStats())
		batches++
		if checkpoint {
			b.StartTimer()
			if bufferedIndexedWrites {
				flushStart := time.Now()
				if err := collection.Flush(); err != nil {
					b.Fatalf("flush buffered indexed writes: %v", err)
				}
				bufferedFlushElapsed += time.Since(flushStart)
			}
			syncStart := time.Now()
			benchmarkSyncBoundary(b, backend)
			syncElapsed += time.Since(syncStart)
			b.StopTimer()
		}
		inserted += batchSize
	}
	if bufferedIndexedWrites && !checkpoint {
		b.StartTimer()
		flushStart := time.Now()
		if err := collection.Flush(); err != nil {
			b.Fatalf("flush buffered indexed writes: %v", err)
		}
		bufferedFlushElapsed += time.Since(flushStart)
		b.StopTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), metricName)
	b.ReportMetric(float64(indexCount), "indexes/doc")
	if bufferedIndexedWrites {
		meta := collection.Meta()
		b.ReportMetric(1, "buffered_indexed_writes")
		if meta.Options.BufferedIndexedWriteMaxDocuments > 0 {
			b.ReportMetric(float64(meta.Options.BufferedIndexedWriteMaxDocuments), "buffered_max_docs")
		}
		if meta.Options.BufferedIndexedWriteMaxBytes > 0 {
			b.ReportMetric(float64(meta.Options.BufferedIndexedWriteMaxBytes), "buffered_max_bytes")
		}
		if meta.Options.BufferedIndexedWriteMaxRootRuns > 0 {
			b.ReportMetric(float64(meta.Options.BufferedIndexedWriteMaxRootRuns), "buffered_max_root_runs")
		}
		if meta.Options.BufferedIndexedAsyncFlush {
			b.ReportMetric(1, "buffered_async_flush")
			if meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits > 0 {
				b.ReportMetric(float64(meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits), "buffered_async_max_units")
			}
		}
		if insertStats.BufferedIndexedBatches > 0 && insertStats.BufferedIndexedBypassBatches == 0 && insertElapsed > 0 {
			b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(b.N), "buffered_insert_ns/doc")
		}
		if insertStats.BufferedIndexedBatches > 0 && insertStats.BufferedIndexedBypassBatches == 0 && bufferedFlushElapsed > 0 {
			b.ReportMetric(float64(bufferedFlushElapsed.Nanoseconds())/float64(b.N), "buffered_flush_ns/doc")
		}
	}
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportCollectionInsertStats(b, b.N, batches, insertStats)
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
	benchmarkReportTreeDBDiskUsage(b, backend, b.N)
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
		insertElapsed += time.Since(insertStart)
		b.StopTimer()
		addCollectionInsertStats(&insertStats, collection.LastInsertStats())
		batches++
		if checkpoint {
			b.StartTimer()
			syncStart := time.Now()
			benchmarkSyncBoundary(b, backend)
			syncElapsed += time.Since(syncStart)
			b.StopTimer()
		}
		inserted += batchSize
	}
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), metricName)
	b.ReportMetric(float64(indexCount), "indexes/doc")
	if checkpoint {
		benchmarkReportCheckpointSplit(b, b.N, insertElapsed, syncElapsed)
	}
	benchmarkReportCollectionInsertStats(b, b.N, batches, insertStats)
	benchmarkReportNativeProbeFallbackDeltas(b, backend, startKeyFallback, startPrefixFallback)
	benchmarkReportTreeDBDiskUsage(b, backend, b.N)
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

func benchmarkCollectionShapeReadPrimaryInto(b *testing.B, indexCount int, parallel bool) {
	backend, collection := openBenchmarkCollection(b, fmt.Sprintf("bench_shape_read_into_%d", indexCount), collectionShapeIndexes(indexCount)...)
	ids := seedBenchmarkCollection(b, collection, 0, collectionBenchSeedDocs, true)
	benchmarkSyncBoundary(b, backend)

	b.ReportAllocs()
	b.ReportMetric(float64(indexCount), "indexes/doc")
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			buf := make([]byte, 0, 8<<10)
			for pb.Next() {
				got, found, err := collection.GetInto(ids[i%len(ids)], buf)
				if err != nil {
					b.Errorf("shape parallel primary read into indexes=%d: %v", indexCount, err)
				}
				if !found {
					b.Errorf("shape parallel primary read into indexes=%d: document not found", indexCount)
				}
				buf = got
				i++
			}
		})
		return
	}
	buf := make([]byte, 0, 8<<10)
	for i := 0; i < b.N; i++ {
		got, found, err := collection.GetInto(ids[i%len(ids)], buf)
		if err != nil {
			b.Fatalf("shape primary read into indexes=%d: %v", indexCount, err)
		}
		if !found {
			b.Fatalf("shape primary read into indexes=%d: document not found", indexCount)
		}
		buf = got
	}
}

func BenchmarkCollectionShapeReadPrimaryInto(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeReadPrimaryInto(b, indexCount, false)
		})
	}
}

func BenchmarkCollectionShapeReadPrimaryIntoParallel(b *testing.B) {
	for _, indexCount := range []int{0, 2} {
		b.Run(fmt.Sprintf("indexes_%d", indexCount), func(b *testing.B) {
			benchmarkCollectionShapeReadPrimaryInto(b, indexCount, true)
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
	if err := seedCollection.Flush(); err != nil {
		b.Fatalf("flush mixed seed collection: %v", err)
	}
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
				} else if documentFormat == collections.DocumentFormatBSON {
					docs[i] = benchmarkBSONDocument(b, docNum, true)
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

func benchmarkCollectionMixedReadWriteScaling(b *testing.B, readers, writers int, secondaryRead bool) {
	if readers <= 0 {
		readers = 1
	}
	if writers <= 0 {
		writers = 1
	}
	indexes := collectionShapeIndexes(2)
	collectionName := fmt.Sprintf("bench_shape_mixed_scaling_r%d_w%d", readers, writers)
	backend, manager, seedCollection := openBenchmarkCollectionWithManager(b, collectionName, indexes...)
	seedDocs := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_SEED_DOCS", defaultCollectionMixedSeedDocs)
	if seedDocs <= 0 {
		seedDocs = defaultCollectionMixedSeedDocs
	}
	ids := seedBenchmarkCollection(b, seedCollection, 0, seedDocs, true)
	if err := seedCollection.Flush(); err != nil {
		b.Fatalf("flush mixed scaling seed collection: %v", err)
	}
	benchmarkSyncBoundary(b, backend)

	readerCollections := make([]*collections.Collection, readers)
	for i := range readerCollections {
		var err error
		readerCollections[i], err = manager.OpenCollection(collectionName)
		if err != nil {
			b.Fatalf("open mixed scaling reader collection: %v", err)
		}
	}
	writerCollections := make([]*collections.Collection, writers)
	for i := range writerCollections {
		var err error
		writerCollections[i], err = manager.OpenCollection(collectionName)
		if err != nil {
			b.Fatalf("open mixed scaling writer collection: %v", err)
		}
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
	errCh := make(chan error, readers+writers)
	startCh := make(chan struct{})
	var readerWG sync.WaitGroup
	var writerWG sync.WaitGroup
	documentFormat := benchmarkCollectionDocumentFormat(b)

	writeDocumentBatch := func(start, count int, encoder *collections.TemplateV1Encoder) ([][]byte, [][]byte, error) {
		ids := make([][]byte, count)
		docs := make([][]byte, count)
		for i := 0; i < count; i++ {
			docNum := start + i
			ids[i] = benchmarkDocumentID(docNum)
			switch documentFormat {
			case collections.DocumentFormatTemplateV1:
				doc, err := encoder.EncodeDocument(
					[]string{"name", "email", "city", "pad"},
					[]any{
						fmt.Sprintf("user-%09d", docNum),
						fmt.Sprintf("user-%09d@example.com", docNum),
						fmt.Sprintf("city-%02d", docNum%collectionBenchCities),
						collectionBenchIndexedPad,
					},
				)
				if err != nil {
					return nil, nil, err
				}
				docs[i] = doc
			case collections.DocumentFormatBSON:
				docs[i] = benchmarkBSONDocument(b, docNum, true)
			default:
				docs[i] = benchmarkIndexedDocument(docNum)
			}
		}
		return ids, docs, nil
	}

	for writerID := 0; writerID < writers; writerID++ {
		writerID := writerID
		writerCollection := writerCollections[writerID]
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			<-startCh
			var templateEncoder collections.TemplateV1Encoder
			for next := 1_000_000 + writerID*100_000_000; !stop.Load(); next += writeBatchSize {
				ids, docs, err := writeDocumentBatch(next, writeBatchSize, &templateEncoder)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
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
	}

	readBase := b.N / readers
	readRemainder := b.N % readers
	readerStride := max(1, runtime.GOMAXPROCS(0))
	for readerID := 0; readerID < readers; readerID++ {
		readerID := readerID
		readOps := readBase
		if readerID < readRemainder {
			readOps++
		}
		readerCollection := readerCollections[readerID]
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			<-startCh
			if secondaryRead {
				i := (readerID * max(1, seedDocs/readers)) % seedDocs
				for op := 0; op < readOps; op++ {
					email := fmt.Sprintf("user-%09d@example.com", i%seedDocs)
					if _, err := readerCollection.FindByIndex("email_idx", email); err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					i += readerStride
				}
				return
			}
			i := (readerID * max(1, len(ids)/readers)) % len(ids)
			for op := 0; op < readOps; op++ {
				if _, err := readerCollection.Get(ids[i%len(ids)]); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				i += readerStride
			}
		}()
	}

	b.ReportAllocs()
	b.ReportMetric(float64(readers), "readers")
	b.ReportMetric(float64(writers), "writers")
	b.ReportMetric(float64(seedDocs), "seed_docs")
	b.ReportMetric(float64(writeBatchSize), "writer_docs/batch")
	b.ResetTimer()
	start := time.Now()
	close(startCh)

	readerWG.Wait()
	readerElapsed := time.Since(start)
	b.StopTimer()
	stop.Store(true)
	writerWG.Wait()
	writerElapsed := time.Since(start)
	flushStart := time.Now()
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush mixed scaling manager: %v", err)
	}
	flushElapsed := time.Since(flushStart)
	select {
	case err := <-errCh:
		b.Fatalf("mixed scaling benchmark: %v", err)
	default:
	}
	if readerElapsed > 0 {
		b.ReportMetric(float64(b.N)/readerElapsed.Seconds(), "reader_ops/sec")
	}
	if writerElapsed > 0 {
		b.ReportMetric(float64(writerDocs.Load())/writerElapsed.Seconds(), "writer_docs/sec")
	}
	if docs := writerDocs.Load(); docs > 0 && flushElapsed > 0 {
		b.ReportMetric(float64(flushElapsed.Nanoseconds())/float64(docs), "writer_flush_ns/doc")
	}
}

func BenchmarkCollectionMixedReadWriteScalingPrimary(b *testing.B) {
	for _, tc := range []collectionMixedScaleCase{
		{readers: 1, writers: 1},
		{readers: 4, writers: 1},
		{readers: 8, writers: 2},
	} {
		b.Run(fmt.Sprintf("readers_%d/writers_%d", tc.readers, tc.writers), func(b *testing.B) {
			benchmarkCollectionMixedReadWriteScaling(b, tc.readers, tc.writers, false)
		})
	}
}

func BenchmarkCollectionMixedReadWriteScalingSecondaryUnique(b *testing.B) {
	for _, tc := range []collectionMixedScaleCase{
		{readers: 1, writers: 1},
		{readers: 4, writers: 1},
		{readers: 8, writers: 2},
	} {
		b.Run(fmt.Sprintf("readers_%d/writers_%d", tc.readers, tc.writers), func(b *testing.B) {
			benchmarkCollectionMixedReadWriteScaling(b, tc.readers, tc.writers, true)
		})
	}
}
