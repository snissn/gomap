package collections

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type collectionFlushAfterStagingShape string

const (
	collectionFlushRepeatedSameIDIndexedChangeBack collectionFlushAfterStagingShape = "repeated_same_id_indexed_change_back"
	collectionFlushRepeatedSameIDNonIndexedUpdate  collectionFlushAfterStagingShape = "repeated_same_id_non_indexed_update"
	collectionFlushManyIDsIndexedChanges           collectionFlushAfterStagingShape = "many_ids_indexed_changes"
	collectionFlushManyIDsNonIndexedChanges        collectionFlushAfterStagingShape = "many_ids_non_indexed_changes"
)

func BenchmarkCollectionFlushAfterStaging(b *testing.B) {
	shapes := []collectionFlushAfterStagingShape{
		collectionFlushRepeatedSameIDIndexedChangeBack,
		collectionFlushRepeatedSameIDNonIndexedUpdate,
		collectionFlushManyIDsIndexedChanges,
		collectionFlushManyIDsNonIndexedChanges,
	}
	for _, docs := range []int{64, 512, 5000} {
		for _, shape := range shapes {
			b.Run(fmt.Sprintf("%s/docs_%d", shape, docs), func(b *testing.B) {
				benchmarkCollectionFlushAfterStaging(b, docs, shape)
			})
		}
	}
}

func benchmarkCollectionFlushAfterStaging(b *testing.B, docs int, shape collectionFlushAfterStagingShape) {
	b.Helper()
	if docs <= 0 {
		b.Fatalf("invalid docs %d", docs)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	manager := NewCollectionManager(db)
	manager.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := manager.CreateCollection(&CollectionMeta{
		Name: "bench",
		Options: CollectionOptions{
			DocumentFormat:                          DocumentFormatJSON,
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1 << 30,
			BufferedIndexedWriteMaxBytes:            1 << 40,
			BufferedIndexedWriteMaxRootRuns:         1 << 30,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 1 << 20,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "score", Field: "score", ValueType: IndexValueInt64},
		},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	col, err := manager.OpenCollection("bench")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}

	ids := make([][]byte, docs)
	documents := make([][]byte, docs)
	for i := 0; i < docs; i++ {
		ids[i] = []byte(collectionFlushBenchDocID(i))
		documents[i] = collectionFlushBenchJSON(i, "base-city", 0, 0)
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		b.Fatalf("insert preload: %v", err)
	}
	if err := manager.FlushAll(); err != nil {
		b.Fatalf("flush preload: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatalf("checkpoint preload: %v", err)
	}

	statsBefore := manager.StatsSnapshot()
	dbStatsBefore := db.Stats()
	var flushElapsed time.Duration
	totalDocs := uint64(0)

	b.ReportAllocs()
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		b.StopTimer()
		stagedDocs := stageCollectionFlushAfterStagingWorkload(b, col, docs, iter, shape)
		totalDocs += uint64(stagedDocs)
		b.StartTimer()
		start := time.Now()
		if err := manager.FlushAll(); err != nil {
			b.Fatalf("flush staged workload: %v", err)
		}
		flushElapsed += time.Since(start)
		b.StopTimer()
	}

	stats := collectionManagerStatsBenchmarkDelta(manager.StatsSnapshot(), statsBefore)
	dbStatsDelta := collectionFlushBenchStatsDelta(db.Stats(), dbStatsBefore)
	if totalDocs > 0 {
		docsFloat := float64(totalDocs)
		b.ReportMetric(float64(flushElapsed.Nanoseconds())/docsFloat, "ns/doc")
		reportCollectionFlushShapeStatsForBenchmark(b, stats, int(totalDocs))
		collectionFlushBenchReportUintPerDoc(b, dbStatsDelta["treedb.publish.ordered_root_delta_group.root_apply_ns_total"], totalDocs, "root_apply_ns/doc")
		collectionFlushBenchReportUintPerDoc(b, dbStatsDelta["treedb.publish.ordered_root_delta_group.root_apply_calls_total"], totalDocs, "root_apply_calls/doc")
	}
}

func stageCollectionFlushAfterStagingWorkload(b *testing.B, col *Collection, docs, iter int, shape collectionFlushAfterStagingShape) int {
	b.Helper()
	switch shape {
	case collectionFlushRepeatedSameIDIndexedChangeBack:
		id := []byte(collectionFlushBenchDocID(0))
		for i := 0; i < docs; i++ {
			city := "base-city"
			if i%2 == 0 {
				city = "staged-city"
			}
			item := UpdateBatchItem{
				DocumentID: id,
				Update:     collectionFlushBenchReplace(collectionFlushBenchJSON(0, city, 0, 0)),
			}
			if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{item}); err != nil {
				b.Fatalf("stage repeated indexed update %d: %v", i, err)
			} else if !batched {
				b.Fatalf("stage repeated indexed update %d declined", i)
			}
		}
		return docs
	case collectionFlushRepeatedSameIDNonIndexedUpdate:
		id := []byte(collectionFlushBenchDocID(0))
		for i := 0; i < docs; i++ {
			counter := iter*docs + i + 1
			item := UpdateBatchItem{
				DocumentID: id,
				Update:     collectionFlushBenchReplace(collectionFlushBenchJSON(0, "base-city", 0, counter)),
			}
			if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{item}); err != nil {
				b.Fatalf("stage repeated non-indexed update %d: %v", i, err)
			} else if !batched {
				b.Fatalf("stage repeated non-indexed update %d declined", i)
			}
		}
		return docs
	case collectionFlushManyIDsIndexedChanges:
		items := make([]UpdateBatchItem, docs)
		city := "city-even"
		if iter%2 != 0 {
			city = "city-odd"
		}
		for i := 0; i < docs; i++ {
			items[i] = UpdateBatchItem{
				DocumentID: []byte(collectionFlushBenchDocID(i)),
				Update:     collectionFlushBenchReplace(collectionFlushBenchJSON(i, city, i%97, iter)),
			}
		}
		if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges(items); err != nil {
			b.Fatalf("stage many-id indexed updates: %v", err)
		} else if !batched {
			b.Fatalf("stage many-id indexed updates declined")
		}
		return docs
	case collectionFlushManyIDsNonIndexedChanges:
		items := make([]UpdateBatchItem, docs)
		for i := 0; i < docs; i++ {
			items[i] = UpdateBatchItem{
				DocumentID: []byte(collectionFlushBenchDocID(i)),
				Update:     collectionFlushBenchReplace(collectionFlushBenchJSON(i, "base-city", 0, iter+1)),
			}
		}
		if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges(items); err != nil {
			b.Fatalf("stage many-id non-indexed updates: %v", err)
		} else if !batched {
			b.Fatalf("stage many-id non-indexed updates declined")
		}
		return docs
	default:
		b.Fatalf("unknown collection flush shape %q", shape)
		return 0
	}
}

func reportCollectionFlushShapeStatsForBenchmark(b *testing.B, stats CollectionManagerStats, docs int) {
	b.Helper()
	if docs <= 0 {
		return
	}
	reportDurationPerDoc := func(value time.Duration, name string) {
		if value > 0 {
			b.ReportMetric(float64(value.Nanoseconds())/float64(docs), name)
		}
	}
	if stats.IndexedFlushCalls > 0 {
		b.ReportMetric(float64(stats.IndexedFlushCalls), "indexed_flush_calls")
		b.ReportMetric(float64(stats.IndexedFlushDocs)/float64(stats.IndexedFlushCalls), "indexed_flush_docs/call")
	}
	reportDurationPerDoc(stats.IndexedFlushRotate, "indexed_flush_rotate_ns/doc")
	reportDurationPerDoc(stats.IndexedFlushMaterialize, "indexed_flush_materialize_ns/doc")
	reportDurationPerDoc(stats.IndexedFlushPublish, "indexed_flush_publish_ns/doc")
	reportDurationPerDoc(stats.IndexedFlushDuration, "indexed_flush_ns/doc")
	reportCollectionRootDeltaShapeStatsForBenchmark(b, stats, docs)
}

func collectionFlushBenchDocID(n int) string {
	return "u-" + strconv.FormatInt(int64(n), 10)
}

func collectionFlushBenchJSON(n int, city string, score, counter int) []byte {
	return []byte(fmt.Sprintf(`{"name":"user-%d","email":"user-%d@example.com","city":%q,"score":%d,"counter":%d}`, n, n, city, score, counter))
}

func collectionFlushBenchReplace(raw []byte) func([]byte) ([]byte, bool, error) {
	return func([]byte) ([]byte, bool, error) {
		return raw, true, nil
	}
}

func collectionFlushBenchStatsDelta(after, before map[string]string) map[string]uint64 {
	out := make(map[string]uint64, len(after))
	for key, afterValue := range after {
		afterUint, err := strconv.ParseUint(afterValue, 10, 64)
		if err != nil {
			continue
		}
		beforeUint, _ := strconv.ParseUint(before[key], 10, 64)
		if afterUint >= beforeUint {
			out[key] = afterUint - beforeUint
		}
	}
	return out
}

func collectionFlushBenchReportUintPerDoc(b *testing.B, value uint64, docs uint64, name string) {
	b.Helper()
	if value == 0 || docs == 0 {
		return
	}
	b.ReportMetric(float64(value)/float64(docs), name)
}
