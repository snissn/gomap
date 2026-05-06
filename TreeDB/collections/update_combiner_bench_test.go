package collections

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkCollectionUpdateCombinerTemplateV1NewShape(b *testing.B) {
	for _, parallelism := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("parallel_%d", parallelism), func(b *testing.B) {
			benchmarkCollectionUpdateCombinerTemplateV1NewShape(b, parallelism)
		})
	}
}

func benchmarkCollectionUpdateCombinerTemplateV1NewShape(b *testing.B, parallelism int) {
	b.Helper()
	if parallelism <= 0 {
		b.Fatalf("invalid parallelism %d", parallelism)
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
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "bench",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatTemplateV1,
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1 << 30,
			BufferedIndexedWriteMaxBytes:     1 << 40,
			BufferedIndexedWriteMaxRootRuns:  1 << 30,
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
	for start := 0; start < docs; start += preloadBatchSize {
		shapeOrdinal := start / preloadBatchSize
		for i := start; i < start+preloadBatchSize && i < docs; i++ {
			updateDocs[i] = benchmarkTemplateV1NewShapeUpdateDocument(b, i, shapeOrdinal)
		}
	}

	statsBefore := mgr.StatsSnapshot()
	b.ReportAllocs()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	startTime := time.Now()
	var next atomic.Uint64
	var failed atomic.Bool
	var benchErr atomic.Value
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if failed.Load() {
				return
			}
			i := int(next.Add(1) - 1)
			matched, modified, err := col.Update(ids[i], benchmarkTemplateV1ReplaceWith(updateDocs[i]))
			if err != nil {
				if failed.CompareAndSwap(false, true) {
					benchErr.Store(fmt.Sprintf("update %d: %v", i, err))
				}
				return
			}
			if !matched || !modified {
				if failed.CompareAndSwap(false, true) {
					benchErr.Store(fmt.Sprintf("update %d matched=%v modified=%v want true/true", i, matched, modified))
				}
				return
			}
		}
	})
	if v := benchErr.Load(); v != nil {
		b.Fatal(v)
	}
	if err := col.Flush(); err != nil {
		b.Fatalf("flush updates: %v", err)
	}
	elapsed := time.Since(startTime)
	b.StopTimer()

	statsAfter := mgr.StatsSnapshot()
	combineRequests := statsAfter.UpdateCombineRequests - statsBefore.UpdateCombineRequests
	combineBatches := statsAfter.UpdateCombineBatches - statsBefore.UpdateCombineBatches
	combineBatchedRequests := statsAfter.UpdateCombineBatchedRequests - statsBefore.UpdateCombineBatchedRequests
	combineFallbackRequests := statsAfter.UpdateCombineFallbackRequests - statsBefore.UpdateCombineFallbackRequests
	combineQueueDepthMax := statsAfter.UpdateCombineQueueDepthMax
	if statsBefore.UpdateCombineQueueDepthMax > combineQueueDepthMax {
		combineQueueDepthMax = 0
	}
	b.ReportMetric(float64(docs)/elapsed.Seconds(), "docs/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(docs), "ns/doc")
	b.ReportMetric(float64(combineRequests), "update_combine_requests")
	b.ReportMetric(float64(combineBatches), "update_combine_batches")
	if combineBatches > 0 {
		b.ReportMetric(float64(combineBatchedRequests)/float64(combineBatches), "update_combine_requests/batch")
	}
	b.ReportMetric(float64(combineFallbackRequests), "update_combine_fallback_requests")
	b.ReportMetric(float64(combineQueueDepthMax), "update_combine_queue_depth_max")
}
