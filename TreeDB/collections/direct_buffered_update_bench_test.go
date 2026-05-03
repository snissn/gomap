package collections

import (
	"fmt"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkCollectionUpdateBatchDirectBufferedTemplateV1NewShape(b *testing.B) {
	for _, batchSize := range []int{80, 16000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			benchmarkCollectionUpdateBatchDirectBufferedTemplateV1NewShape(b, batchSize)
		})
	}
}

func benchmarkCollectionUpdateBatchDirectBufferedTemplateV1NewShape(b *testing.B, batchSize int) {
	b.Helper()
	if batchSize <= 0 {
		b.Fatalf("invalid batch size %d", batchSize)
	}
	docs := b.N
	if docs <= 0 {
		docs = 1
	}
	d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "bench",
		Options: CollectionOptions{
			DocumentFormat:                          DocumentFormatTemplateV1,
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1 << 30,
			BufferedIndexedWriteMaxBytes:            1 << 40,
			BufferedIndexedWriteMaxRootRuns:         1 << 30,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 1 << 20,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("bench")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}

	ids := make([][]byte, docs)
	var preloadEncoder TemplateV1Encoder
	const preloadBatchSize = 16000
	for start := 0; start < docs; start += preloadBatchSize {
		n := preloadBatchSize
		if remaining := docs - start; remaining < n {
			n = remaining
		}
		batchIDs := make([][]byte, n)
		batchDocs := make([][]byte, n)
		for i := 0; i < n; i++ {
			docID := benchmarkTemplateV1UpdateDocID(start + i)
			ids[start+i] = docID
			batchIDs[i] = docID
			batchDocs[i] = benchmarkTemplateV1BaseUpdateDocument(b, &preloadEncoder, start+i)
		}
		if _, err := col.InsertBatch(batchIDs, batchDocs); err != nil {
			b.Fatalf("insert preload batch %d: %v", start/preloadBatchSize, err)
		}
	}
	if err := col.Flush(); err != nil {
		b.Fatalf("flush preload: %v", err)
	}

	updateDocs := make([][]byte, docs)
	for start := 0; start < docs; start += batchSize {
		shapeOrdinal := start / batchSize
		for i := start; i < start+batchSize && i < docs; i++ {
			updateDocs[i] = benchmarkTemplateV1NewShapeUpdateDocument(b, i, shapeOrdinal)
		}
	}

	batch := make([]UpdateBatchItem, batchSize)
	statsBefore := mgr.StatsSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	startTime := time.Now()
	for start := 0; start < docs; start += batchSize {
		n := batchSize
		if remaining := docs - start; remaining < n {
			n = remaining
		}
		for i := 0; i < n; i++ {
			batch[i] = UpdateBatchItem{
				DocumentID: ids[start+i],
				Update:     benchmarkTemplateV1ReplaceWith(updateDocs[start+i]),
			}
		}
		if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges(batch[:n]); err != nil {
			b.Fatalf("update batch %d: %v", start/batchSize, err)
		} else if !batched {
			b.Fatalf("update batch %d was declined", start/batchSize)
		}
	}
	if err := col.Flush(); err != nil {
		b.Fatalf("flush updates: %v", err)
	}
	elapsed := time.Since(startTime)
	b.StopTimer()

	stats := collectionManagerStatsBenchmarkDelta(mgr.StatsSnapshot(), statsBefore)
	b.ReportMetric(float64(docs)/elapsed.Seconds(), "docs/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
	reportCollectionUpdateStatsForBenchmark(b, stats, docs)
}

func benchmarkTemplateV1UpdateDocID(n int) []byte {
	return []byte(fmt.Sprintf("u-%09d", n))
}

func benchmarkTemplateV1BaseUpdateDocument(tb testing.TB, encoder *TemplateV1Encoder, n int) []byte {
	tb.Helper()
	doc, err := encoder.EncodeDocument(
		[]string{"name", "email", "city", "score"},
		[]any{
			fmt.Sprintf("user-%09d", n),
			fmt.Sprintf("user-%09d@example.com", n),
			fmt.Sprintf("city-%02d", n%64),
			int64(0),
		},
	)
	if err != nil {
		tb.Fatalf("encode base template-v1 doc %d: %v", n, err)
	}
	return doc
}

func benchmarkTemplateV1NewShapeUpdateDocument(tb testing.TB, n, shapeOrdinal int) []byte {
	tb.Helper()
	doc, err := EncodeTemplateV1Document(
		[]string{"name", "email", "city", "score", fmt.Sprintf("shape_%06d", shapeOrdinal)},
		[]any{
			fmt.Sprintf("user-%09d", n),
			fmt.Sprintf("user-%09d@example.com", n),
			fmt.Sprintf("updated-city-%02d", (n+shapeOrdinal)%64),
			int64(shapeOrdinal),
			"x",
		},
	)
	if err != nil {
		tb.Fatalf("encode new-shape template-v1 doc %d/%d: %v", n, shapeOrdinal, err)
	}
	return doc
}

func benchmarkTemplateV1ReplaceWith(raw []byte) func([]byte) ([]byte, bool, error) {
	return func([]byte) ([]byte, bool, error) {
		return raw, true, nil
	}
}

func collectionManagerStatsBenchmarkDelta(after, before CollectionManagerStats) CollectionManagerStats {
	return CollectionManagerStats{
		IndexedStageBatches:            after.IndexedStageBatches - before.IndexedStageBatches,
		IndexedStageDocs:               after.IndexedStageDocs - before.IndexedStageDocs,
		IndexedStageBytes:              after.IndexedStageBytes - before.IndexedStageBytes,
		IndexedStageRootRuns:           after.IndexedStageRootRuns - before.IndexedStageRootRuns,
		IndexedFlushCalls:              after.IndexedFlushCalls - before.IndexedFlushCalls,
		IndexedFlushErrors:             after.IndexedFlushErrors - before.IndexedFlushErrors,
		IndexedFlushDocs:               after.IndexedFlushDocs - before.IndexedFlushDocs,
		IndexedFlushBytes:              after.IndexedFlushBytes - before.IndexedFlushBytes,
		IndexedFlushRootRuns:           after.IndexedFlushRootRuns - before.IndexedFlushRootRuns,
		IndexedFlushRoots:              after.IndexedFlushRoots - before.IndexedFlushRoots,
		IndexedFlushDuration:           after.IndexedFlushDuration - before.IndexedFlushDuration,
		IndexedFlushMaterialize:        after.IndexedFlushMaterialize - before.IndexedFlushMaterialize,
		IndexedFlushPublish:            after.IndexedFlushPublish - before.IndexedFlushPublish,
		UpdateBatchCalls:               after.UpdateBatchCalls - before.UpdateBatchCalls,
		UpdateBatchItems:               after.UpdateBatchItems - before.UpdateBatchItems,
		UpdateBatchMatched:             after.UpdateBatchMatched - before.UpdateBatchMatched,
		UpdateBatchModified:            after.UpdateBatchModified - before.UpdateBatchModified,
		UpdateBatchRuns:                after.UpdateBatchRuns - before.UpdateBatchRuns,
		UpdateBatchBufferedBatches:     after.UpdateBatchBufferedBatches - before.UpdateBatchBufferedBatches,
		UpdateBatchCurrentRead:         after.UpdateBatchCurrentRead - before.UpdateBatchCurrentRead,
		UpdateBatchCallback:            after.UpdateBatchCallback - before.UpdateBatchCallback,
		UpdateBatchPrepareDocuments:    after.UpdateBatchPrepareDocuments - before.UpdateBatchPrepareDocuments,
		UpdateBatchIndexStateExtract:   after.UpdateBatchIndexStateExtract - before.UpdateBatchIndexStateExtract,
		UpdateBatchUniquePreflight:     after.UpdateBatchUniquePreflight - before.UpdateBatchUniquePreflight,
		UpdateBatchTemplateRunBuild:    after.UpdateBatchTemplateRunBuild - before.UpdateBatchTemplateRunBuild,
		UpdateBatchPrimaryRunBuild:     after.UpdateBatchPrimaryRunBuild - before.UpdateBatchPrimaryRunBuild,
		UpdateBatchSecondaryRunBuild:   after.UpdateBatchSecondaryRunBuild - before.UpdateBatchSecondaryRunBuild,
		UpdateBatchBufferStage:         after.UpdateBatchBufferStage - before.UpdateBatchBufferStage,
		UpdateBatchBufferLockWait:      after.UpdateBatchBufferLockWait - before.UpdateBatchBufferLockWait,
		UpdateBatchBufferLockHold:      after.UpdateBatchBufferLockHold - before.UpdateBatchBufferLockHold,
		UpdateBatchBufferRootAppend:    after.UpdateBatchBufferRootAppend - before.UpdateBatchBufferRootAppend,
		UpdateBatchPublish:             after.UpdateBatchPublish - before.UpdateBatchPublish,
		UpdateBatchSecondaryDeletes:    after.UpdateBatchSecondaryDeletes - before.UpdateBatchSecondaryDeletes,
		UpdateBatchSecondarySets:       after.UpdateBatchSecondarySets - before.UpdateBatchSecondarySets,
		UpdateBatchSecondaryKeyBytes:   after.UpdateBatchSecondaryKeyBytes - before.UpdateBatchSecondaryKeyBytes,
		UpdateBatchIndexValueChanges:   after.UpdateBatchIndexValueChanges - before.UpdateBatchIndexValueChanges,
		UpdateBatchIndexValueUnchanged: after.UpdateBatchIndexValueUnchanged - before.UpdateBatchIndexValueUnchanged,
		UpdateBatchUniqueChecks:        after.UpdateBatchUniqueChecks - before.UpdateBatchUniqueChecks,
		UpdateBatchUniqueCheckSkips:    after.UpdateBatchUniqueCheckSkips - before.UpdateBatchUniqueCheckSkips,
	}
}

func reportCollectionUpdateStatsForBenchmark(b *testing.B, stats CollectionManagerStats, docs int) {
	b.Helper()
	if docs <= 0 {
		return
	}
	reportUintPerDoc := func(value uint64, name string) {
		if value > 0 {
			b.ReportMetric(float64(value)/float64(docs), name)
		}
	}
	reportDurationPerDoc := func(value time.Duration, name string) {
		if value > 0 {
			b.ReportMetric(float64(value.Nanoseconds())/float64(docs), name)
		}
	}
	if stats.IndexedStageBatches > 0 {
		b.ReportMetric(float64(stats.IndexedStageBatches), "indexed_stage_batches")
		b.ReportMetric(float64(stats.IndexedStageDocs)/float64(stats.IndexedStageBatches), "indexed_stage_docs/batch")
	}
	reportUintPerDoc(stats.IndexedStageDocs, "indexed_stage_docs/doc")
	reportUintPerDoc(stats.IndexedStageBytes, "indexed_stage_bytes/doc")
	reportUintPerDoc(stats.IndexedStageRootRuns, "indexed_stage_root_runs/doc")
	if stats.IndexedFlushCalls > 0 {
		b.ReportMetric(float64(stats.IndexedFlushCalls), "indexed_flush_calls")
		b.ReportMetric(float64(stats.IndexedFlushDocs)/float64(stats.IndexedFlushCalls), "indexed_flush_docs/call")
		b.ReportMetric(float64(stats.IndexedFlushRootRuns)/float64(stats.IndexedFlushCalls), "indexed_flush_root_runs/call")
		b.ReportMetric(float64(stats.IndexedFlushRoots)/float64(stats.IndexedFlushCalls), "indexed_flush_roots/call")
	}
	reportUintPerDoc(stats.IndexedFlushDocs, "indexed_flush_docs/doc")
	reportUintPerDoc(stats.IndexedFlushBytes, "indexed_flush_bytes/doc")
	reportUintPerDoc(stats.IndexedFlushRootRuns, "indexed_flush_root_runs/doc")
	reportUintPerDoc(stats.IndexedFlushRoots, "indexed_flush_roots/doc")
	reportDurationPerDoc(stats.IndexedFlushDuration, "indexed_flush_ns/doc")
	reportDurationPerDoc(stats.IndexedFlushMaterialize, "indexed_flush_materialize_ns/doc")
	reportDurationPerDoc(stats.IndexedFlushPublish, "indexed_flush_publish_ns/doc")
	if stats.UpdateBatchCalls > 0 {
		b.ReportMetric(float64(stats.UpdateBatchCalls), "update_batches")
		b.ReportMetric(float64(stats.UpdateBatchItems)/float64(stats.UpdateBatchCalls), "update_items/batch")
		b.ReportMetric(float64(stats.UpdateBatchRuns)/float64(stats.UpdateBatchCalls), "update_roots/batch")
	}
	reportUintPerDoc(stats.UpdateBatchMatched, "update_matched/doc")
	reportUintPerDoc(stats.UpdateBatchModified, "update_modified/doc")
	reportUintPerDoc(stats.UpdateBatchSecondaryDeletes, "update_secondary_deletes/doc")
	reportUintPerDoc(stats.UpdateBatchSecondarySets, "update_secondary_sets/doc")
	reportUintPerDoc(stats.UpdateBatchSecondaryKeyBytes, "update_secondary_key_bytes/doc")
	reportUintPerDoc(stats.UpdateBatchIndexValueChanges, "update_index_value_changes/doc")
	reportUintPerDoc(stats.UpdateBatchIndexValueUnchanged, "update_index_value_unchanged/doc")
	reportUintPerDoc(stats.UpdateBatchUniqueChecks, "update_unique_checks/doc")
	reportUintPerDoc(stats.UpdateBatchUniqueCheckSkips, "update_unique_check_skips/doc")
	reportDurationPerDoc(stats.UpdateBatchCurrentRead, "update_current_read_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchCallback, "update_callback_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchPrepareDocuments, "update_prepare_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchIndexStateExtract, "update_index_state_extract_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchUniquePreflight, "update_unique_preflight_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchTemplateRunBuild, "update_template_run_build_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchPrimaryRunBuild, "update_primary_run_build_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchSecondaryRunBuild, "update_secondary_run_build_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchBufferStage, "update_buffer_stage_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchBufferLockWait, "update_buffer_lock_wait_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchBufferLockHold, "update_buffer_lock_hold_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchBufferRootAppend, "update_buffer_root_append_ns/doc")
	reportDurationPerDoc(stats.UpdateBatchPublish, "update_publish_ns/doc")
}
