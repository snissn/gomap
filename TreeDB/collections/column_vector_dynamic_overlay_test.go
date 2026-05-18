package collections

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestColumnVectorDynamicGraphSearchTombstonesAndOverlay(t *testing.T) {
	base, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(64, 16, 63, true))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	graph, err := NewColumnVectorDynamicGraph(base)
	if err != nil {
		t.Fatalf("NewColumnVectorDynamicGraph: %v", err)
	}
	query, ok := base.VectorAt(nil, 10)
	if !ok {
		t.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 5, EfSearch: 64}
	var scratch ColumnVectorDynamicGraphSearchScratch
	results, _, err := graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		t.Fatalf("initial SearchCosine: %v", err)
	}
	if len(results) == 0 || !bytes.Equal(results[0].DocumentID, []byte("doc-000010")) {
		t.Fatalf("initial top result=%+v want doc-000010", results)
	}

	if _, err := graph.ApplyBatch([]ColumnVectorDynamicMutation{
		{Kind: ColumnVectorDynamicMutationDelete, DocumentID: []byte("doc-000010")},
	}); err != nil {
		t.Fatalf("delete base doc: %v", err)
	}
	results, trace, err := graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		t.Fatalf("after delete SearchCosine: %v", err)
	}
	if containsColumnVectorDynamicResult(results, []byte("doc-000010")) {
		t.Fatalf("deleted base doc leaked into results: %+v", results)
	}
	if trace.BaseTombstoned == 0 {
		t.Fatalf("trace=%+v want at least one tombstoned base candidate", trace)
	}

	if _, err := graph.ApplyBatch([]ColumnVectorDynamicMutation{
		{Kind: ColumnVectorDynamicMutationInsert, DocumentID: []byte("aaa-dynamic"), Vector: query},
	}); err != nil {
		t.Fatalf("insert overlay doc: %v", err)
	}
	results, trace, err = graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		t.Fatalf("after insert SearchCosine: %v", err)
	}
	if len(results) == 0 || !bytes.Equal(results[0].DocumentID, []byte("aaa-dynamic")) {
		t.Fatalf("overlay insert top result=%+v want aaa-dynamic", results)
	}
	if trace.OverlayScanned != 1 || trace.OverlayCandidates != 1 {
		t.Fatalf("trace=%+v want one scanned overlay candidate", trace)
	}

	if _, err := graph.ApplyBatch([]ColumnVectorDynamicMutation{
		{Kind: ColumnVectorDynamicMutationDelete, DocumentID: []byte("aaa-dynamic")},
		{Kind: ColumnVectorDynamicMutationUpdate, DocumentID: []byte("doc-000011"), Vector: query},
	}); err != nil {
		t.Fatalf("delete overlay and update base doc: %v", err)
	}
	results, trace, err = graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		t.Fatalf("after update SearchCosine: %v", err)
	}
	if len(results) == 0 || !bytes.Equal(results[0].DocumentID, []byte("doc-000011")) {
		t.Fatalf("updated base doc top result=%+v want doc-000011", results)
	}
	if containsColumnVectorDynamicResult(results, []byte("aaa-dynamic")) {
		t.Fatalf("deleted overlay doc leaked into results: %+v", results)
	}
	if trace.BaseTombstoned < 2 {
		t.Fatalf("trace=%+v want both deleted and updated base docs tombstoned", trace)
	}

	if _, err := graph.ApplyBatch([]ColumnVectorDynamicMutation{
		{Kind: ColumnVectorDynamicMutationDelete, DocumentID: []byte("doc-000011")},
	}); err != nil {
		t.Fatalf("delete updated base doc: %v", err)
	}
	results, _, err = graph.SearchCosine(query, opts, &scratch)
	if err != nil {
		t.Fatalf("after final delete SearchCosine: %v", err)
	}
	if containsColumnVectorDynamicResult(results, []byte("doc-000011")) {
		t.Fatalf("deleted updated doc leaked into results: %+v", results)
	}
}

func TestColumnVectorDynamicGraphApplyBatchDoesNotReportUnpublishedCounts(t *testing.T) {
	base, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(32, 16, 8, false))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	graph, err := NewColumnVectorDynamicGraph(base)
	if err != nil {
		t.Fatalf("NewColumnVectorDynamicGraph: %v", err)
	}
	query, ok := base.VectorAt(nil, 7)
	if !ok {
		t.Fatal("missing query vector")
	}
	stats, err := graph.ApplyBatch([]ColumnVectorDynamicMutation{
		{Kind: ColumnVectorDynamicMutationInsert, DocumentID: []byte("dyn-before-error"), Vector: query},
		{Kind: ColumnVectorDynamicMutationInsert, DocumentID: []byte("doc-000001"), Vector: query},
	})
	if err == nil {
		t.Fatal("ApplyBatch succeeded with duplicate base document insert")
	}
	if stats.Inserted != 0 || stats.Updated != 0 || stats.Deleted != 0 || stats.OverlayGeneration != 0 {
		t.Fatalf("stats after failed batch=%+v want zero unpublished counts", stats)
	}
	snapshot := graph.Snapshot()
	if snapshot.OverlayGeneration() != 0 || snapshot.Overlay().Rows() != 0 || snapshot.Overlay().LiveRows() != 0 {
		t.Fatalf("snapshot after failed batch generation=%d rows=%d live=%d, want unchanged empty overlay", snapshot.OverlayGeneration(), snapshot.Overlay().Rows(), snapshot.Overlay().LiveRows())
	}
}

func TestColumnVectorDynamicGraphSameBatchDeleteInsertUsesWriterTombstones(t *testing.T) {
	base, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(64, 16, 63, true))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	graph, err := NewColumnVectorDynamicGraph(base)
	if err != nil {
		t.Fatalf("NewColumnVectorDynamicGraph: %v", err)
	}
	if _, err := graph.ApplyBatch([]ColumnVectorDynamicMutation{
		{Kind: ColumnVectorDynamicMutationDelete, DocumentID: []byte("doc-000020")},
		{Kind: ColumnVectorDynamicMutationDelete, DocumentID: []byte("doc-000030")},
	}); err != nil {
		t.Fatalf("seed tombstones: %v", err)
	}
	query, ok := base.VectorAt(nil, 10)
	if !ok {
		t.Fatal("missing query vector")
	}
	if _, err := graph.ApplyBatch([]ColumnVectorDynamicMutation{
		{Kind: ColumnVectorDynamicMutationDelete, DocumentID: []byte("doc-000010")},
		{Kind: ColumnVectorDynamicMutationInsert, DocumentID: []byte("doc-000010"), Vector: query},
	}); err != nil {
		t.Fatalf("same-batch delete+insert should see writer tombstone: %v", err)
	}
	results, trace, err := graph.SearchCosine(query, ColumnVectorGraphSearchOptions{TopK: 5, EfSearch: 64}, &ColumnVectorDynamicGraphSearchScratch{})
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) == 0 || !bytes.Equal(results[0].DocumentID, []byte("doc-000010")) {
		t.Fatalf("top result=%+v want reinserted doc-000010", results)
	}
	if trace.BaseTombstoned == 0 {
		t.Fatalf("trace=%+v want tombstoned base candidate", trace)
	}
}

func TestColumnVectorDynamicGraphSearchAllocs(t *testing.T) {
	base, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(1024, 64, 16, false))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	graph, err := NewColumnVectorDynamicGraph(base)
	if err != nil {
		t.Fatalf("NewColumnVectorDynamicGraph: %v", err)
	}
	query, ok := base.VectorAt(nil, 511)
	if !ok {
		t.Fatal("missing query vector")
	}
	if _, err := graph.ApplyBatch([]ColumnVectorDynamicMutation{
		{Kind: ColumnVectorDynamicMutationUpdate, DocumentID: []byte("doc-000123"), Vector: query},
		{Kind: ColumnVectorDynamicMutationInsert, DocumentID: []byte("dyn-alloc"), Vector: query},
		{Kind: ColumnVectorDynamicMutationDelete, DocumentID: []byte("doc-000511")},
	}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
	var scratch ColumnVectorDynamicGraphSearchScratch
	if results, _, err := graph.SearchCosine(query, opts, &scratch); err != nil {
		t.Fatalf("warm SearchCosine: %v", err)
	} else if len(results) != opts.TopK {
		t.Fatalf("warm results=%d want %d", len(results), opts.TopK)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		results, trace, err := graph.SearchCosine(query, opts, &scratch)
		if err != nil {
			panic(err)
		}
		if len(results) != opts.TopK {
			panic("unexpected dynamic column vector graph result count")
		}
		columnVectorGraphBenchSink += int64(len(results[0].DocumentID) + trace.CandidatesExamined + trace.EdgesVisited)
	})
	if allocs != 0 {
		t.Fatalf("hot dynamic SearchCosine allocs/run=%g want 0", allocs)
	}
}

func TestColumnVectorDynamicGraphConcurrentReadersAndWriter(t *testing.T) {
	base, err := NewColumnVectorGraphFromColumns(columnVectorGraphTestColumns(1024, 32, 16, false))
	if err != nil {
		t.Fatalf("NewColumnVectorGraphFromColumns: %v", err)
	}
	graph, err := NewColumnVectorDynamicGraph(base)
	if err != nil {
		t.Fatalf("NewColumnVectorDynamicGraph: %v", err)
	}
	query, ok := base.VectorAt(nil, 257)
	if !ok {
		t.Fatal("missing query vector")
	}
	opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
	var stop uint32
	errs := make(chan error, 16)
	var readers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		readers.Add(1)
		go func(worker int) {
			defer readers.Done()
			var scratch ColumnVectorDynamicGraphSearchScratch
			if results, _, err := graph.SearchCosine(query, opts, &scratch); err != nil {
				errs <- fmt.Errorf("worker %d warm SearchCosine: %w", worker, err)
				return
			} else if len(results) != opts.TopK {
				errs <- fmt.Errorf("worker %d warm results=%d want %d", worker, len(results), opts.TopK)
				return
			}
			for atomic.LoadUint32(&stop) == 0 {
				results, trace, err := graph.SearchCosine(query, opts, &scratch)
				if err != nil {
					errs <- fmt.Errorf("worker %d SearchCosine: %w", worker, err)
					return
				}
				if len(results) != opts.TopK {
					errs <- fmt.Errorf("worker %d results=%d want %d", worker, len(results), opts.TopK)
					return
				}
				if trace.BaseGeneration == 0 {
					errs <- fmt.Errorf("worker %d trace has zero base generation: %+v", worker, trace)
					return
				}
			}
		}(worker)
	}

	for i := 0; i < 64; i++ {
		mutations := []ColumnVectorDynamicMutation{
			{
				Kind:       ColumnVectorDynamicMutationUpdate,
				DocumentID: []byte(fmt.Sprintf("doc-%06d", 100+i%32)),
				Vector:     vectorBenchmarkEmbedding(10_000+i, base.Dims()),
			},
			{
				Kind:       ColumnVectorDynamicMutationInsert,
				DocumentID: []byte(fmt.Sprintf("dyn-%06d", i)),
				Vector:     vectorBenchmarkEmbedding(20_000+i, base.Dims()),
			},
		}
		if i >= 8 {
			mutations = append(mutations, ColumnVectorDynamicMutation{
				Kind:       ColumnVectorDynamicMutationDelete,
				DocumentID: []byte(fmt.Sprintf("dyn-%06d", i-8)),
			})
		}
		if _, err := graph.ApplyBatch(mutations); err != nil {
			atomic.StoreUint32(&stop, 1)
			t.Fatalf("ApplyBatch %d: %v", i, err)
		}
	}
	atomic.StoreUint32(&stop, 1)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func BenchmarkColumnVectorDynamicGraphSearchCosineScale(b *testing.B) {
	cases := []struct {
		name   string
		rows   int
		dims   int
		degree int
	}{
		{name: "rows_100k_dims_128_degree_16", rows: 100_000, dims: 128, degree: 16},
		{name: "rows_1m_dims_128_degree_16", rows: 1_000_000, dims: 128, degree: 16},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			if tc.rows >= 1_000_000 && testing.Short() {
				b.Skip("skipping 1M-row dynamic scale benchmark in -short mode")
			}
			base, query := openColumnVectorGraphScaleBenchmark(b, tc.rows, tc.dims, tc.degree)
			opts := ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128}
			b.Run("parallel_read_only", func(b *testing.B) {
				graph := openColumnVectorDynamicGraphBenchmark(b, base, query, 256)
				benchmarkColumnVectorDynamicGraphSearchCosineParallelReadOnly(b, graph, query, opts)
			})
			b.Run("parallel_read_write", func(b *testing.B) {
				graph := openColumnVectorDynamicGraphBenchmark(b, base, query, 256)
				benchmarkColumnVectorDynamicGraphSearchCosineParallelReadWrite(b, graph, query, opts, tc.rows, tc.dims)
			})
		})
	}
}

func benchmarkColumnVectorDynamicGraphSearchCosineParallelReadOnly(b *testing.B, graph *ColumnVectorDynamicGraph, query []float32, opts ColumnVectorGraphSearchOptions) {
	b.Helper()
	b.SetParallelism(1)
	workers := runtime.GOMAXPROCS(0)
	scratches := make([]*ColumnVectorDynamicGraphSearchScratch, workers)
	var warmTrace ColumnVectorDynamicGraphSearchTrace
	for i := 0; i < workers; i++ {
		scratch := new(ColumnVectorDynamicGraphSearchScratch)
		warm, trace, err := graph.SearchCosine(query, opts, scratch)
		if err != nil {
			b.Fatalf("warm SearchCosine worker %d: %v", i, err)
		}
		if len(warm) != opts.TopK {
			b.Fatalf("warm worker %d results=%d want %d", i, len(warm), opts.TopK)
		}
		warmTrace = trace
		scratches[i] = scratch
	}
	b.ReportAllocs()
	b.SetBytes(int64(warmTrace.CandidatesExamined * graph.Snapshot().Base().Dims() * 4))
	b.ResetTimer()
	started := time.Now()
	var nextWorker uint64
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(atomic.AddUint64(&nextWorker, 1)) - 1
		if workerID >= len(scratches) {
			b.Errorf("RunParallel spawned worker %d, but only %d scratches were prewarmed", workerID+1, len(scratches))
			return
		}
		scratch := scratches[workerID]
		var localSink int64
		for pb.Next() {
			results, trace, err := graph.SearchCosine(query, opts, scratch)
			if err != nil {
				panic(err)
			}
			if len(results) != opts.TopK {
				panic("unexpected dynamic column vector graph result count")
			}
			localSink += int64(len(results[0].DocumentID) + trace.CandidatesExamined + trace.EdgesVisited)
		}
		atomic.AddInt64(&columnVectorGraphBenchSink, localSink)
	})
	elapsed := time.Since(started)
	b.StopTimer()
	reportColumnVectorDynamicGraphMetrics(b, graph.Snapshot(), warmTrace)
	reportColumnVectorDynamicReadMetrics(b, elapsed, 0, 0, 0)
}

func benchmarkColumnVectorDynamicGraphSearchCosineParallelReadWrite(b *testing.B, graph *ColumnVectorDynamicGraph, query []float32, opts ColumnVectorGraphSearchOptions, rows int, dims int) {
	b.Helper()
	b.SetParallelism(1)
	workers := runtime.GOMAXPROCS(0)
	scratches := make([]*ColumnVectorDynamicGraphSearchScratch, workers)
	for i := 0; i < workers; i++ {
		scratch := new(ColumnVectorDynamicGraphSearchScratch)
		warm, _, err := graph.SearchCosine(query, opts, scratch)
		if err != nil {
			b.Fatalf("warm SearchCosine worker %d: %v", i, err)
		}
		if len(warm) != opts.TopK {
			b.Fatalf("warm worker %d results=%d want %d", i, len(warm), opts.TopK)
		}
		scratches[i] = scratch
	}
	batches := columnVectorDynamicBenchmarkMutationBatches(b, rows, dims, 4096)
	var publishedBatches atomic.Int64
	var publishedMutations atomic.Int64
	var publishNanos atomic.Int64
	errs := make(chan error, 1)
	stop := make(chan struct{})
	done := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		defer close(done)
		for seq := 0; ; seq++ {
			select {
			case <-stop:
				return
			default:
			}
			batch := batches[seq%len(batches)]
			stats, err := graph.ApplyBatch(batch)
			if err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
			publishedBatches.Add(1)
			publishedMutations.Add(int64(len(batch)))
			publishNanos.Add(int64(stats.PublishDuration))
		}
	}()
	started := time.Now()
	var nextWorker uint64
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(atomic.AddUint64(&nextWorker, 1)) - 1
		if workerID >= len(scratches) {
			b.Errorf("RunParallel spawned worker %d, but only %d scratches were prewarmed", workerID+1, len(scratches))
			return
		}
		scratch := scratches[workerID]
		var localSink int64
		for pb.Next() {
			results, trace, err := graph.SearchCosine(query, opts, scratch)
			if err != nil {
				panic(err)
			}
			if len(results) != opts.TopK {
				panic("unexpected dynamic column vector graph result count")
			}
			localSink += int64(len(results[0].DocumentID) + trace.CandidatesExamined + trace.EdgesVisited)
		}
		atomic.AddInt64(&columnVectorGraphBenchSink, localSink)
	})
	elapsed := time.Since(started)
	b.StopTimer()
	close(stop)
	<-done
	select {
	case err := <-errs:
		b.Fatalf("writer ApplyBatch: %v", err)
	default:
	}
	var finalScratch ColumnVectorDynamicGraphSearchScratch
	finalResults, finalTrace, err := graph.SearchCosine(query, opts, &finalScratch)
	if err != nil {
		b.Fatalf("final SearchCosine: %v", err)
	}
	if len(finalResults) != opts.TopK {
		b.Fatalf("final results=%d want %d", len(finalResults), opts.TopK)
	}
	reportColumnVectorDynamicGraphMetrics(b, graph.Snapshot(), finalTrace)
	reportColumnVectorDynamicReadMetrics(b, elapsed, publishedBatches.Load(), publishedMutations.Load(), publishNanos.Load())
}

func openColumnVectorDynamicGraphBenchmark(b *testing.B, base *ColumnVectorGraph, query []float32, overlayRows int) *ColumnVectorDynamicGraph {
	b.Helper()
	graph, err := NewColumnVectorDynamicGraph(base)
	if err != nil {
		b.Fatalf("NewColumnVectorDynamicGraph: %v", err)
	}
	mutations := make([]ColumnVectorDynamicMutation, 0, overlayRows)
	for i := 0; i < overlayRows; i++ {
		vector := make([]float32, len(query))
		fillVectorBenchmarkEmbedding(vector, 1_000_000+i)
		mutations = append(mutations, ColumnVectorDynamicMutation{
			Kind:       ColumnVectorDynamicMutationInsert,
			DocumentID: []byte(fmt.Sprintf("dyn-seed-%09d", i)),
			Vector:     vector,
		})
	}
	if _, err := graph.ApplyBatch(mutations); err != nil {
		b.Fatalf("seed dynamic overlay: %v", err)
	}
	return graph
}

func columnVectorDynamicBenchmarkMutationBatches(tb testing.TB, rows int, dims int, batchCount int) [][]ColumnVectorDynamicMutation {
	tb.Helper()
	batches := make([][]ColumnVectorDynamicMutation, batchCount)
	for seq := 0; seq < batchCount; seq++ {
		updateVector := make([]float32, dims)
		insertVector := make([]float32, dims)
		fillVectorBenchmarkEmbedding(updateVector, 2_000_000+seq)
		fillVectorBenchmarkEmbedding(insertVector, 3_000_000+seq)
		mutations := []ColumnVectorDynamicMutation{
			{
				Kind:       ColumnVectorDynamicMutationUpdate,
				DocumentID: columnVectorDynamicScaleDocumentID(seq % rows),
				Vector:     updateVector,
			},
			{
				Kind:       ColumnVectorDynamicMutationInsert,
				DocumentID: []byte(fmt.Sprintf("dyn-write-%09d", seq)),
				Vector:     insertVector,
			},
		}
		if seq >= 64 {
			mutations = append(mutations, ColumnVectorDynamicMutation{
				Kind:       ColumnVectorDynamicMutationDelete,
				DocumentID: []byte(fmt.Sprintf("dyn-write-%09d", seq-64)),
			})
		}
		batches[seq] = mutations
	}
	return batches
}

func columnVectorDynamicScaleDocumentID(ordinal int) []byte {
	documentID := make([]byte, len("doc-000000000"))
	fillColumnVectorGraphOrdinalID(documentID, ordinal)
	return documentID
}

func reportColumnVectorDynamicGraphMetrics(b *testing.B, snapshot ColumnVectorDynamicGraphSnapshot, trace ColumnVectorDynamicGraphSearchTrace) {
	b.Helper()
	base := snapshot.Base()
	overlay := snapshot.Overlay()
	if base == nil || overlay == nil {
		return
	}
	baseRows := base.Rows()
	graphBytes := columnVectorGraphPayloadBytes(base)
	overlayBytes := columnVectorDynamicOverlayPayloadBytes(overlay)
	b.ReportMetric(float64(baseRows), "base_rows")
	b.ReportMetric(float64(base.Dims()), "dims")
	if baseRows > 0 {
		b.ReportMetric(float64(base.Edges())/float64(baseRows), "edges/node")
	} else {
		b.ReportMetric(0, "edges/node")
	}
	b.ReportMetric(float64(trace.BaseTrace.CandidatesExamined), "base_candidates/search")
	b.ReportMetric(float64(trace.BaseTombstoned), "base_tombstoned/search")
	b.ReportMetric(float64(trace.OverlayScanned), "overlay_scanned/search")
	b.ReportMetric(float64(trace.MergeCandidates), "merge_candidates/search")
	b.ReportMetric(float64(trace.CandidatesExamined), "total_candidates/search")
	b.ReportMetric(float64(trace.BaseTrace.EdgesVisited), "edges/search")
	b.ReportMetric(float64(overlay.Rows()), "overlay_rows")
	b.ReportMetric(float64(overlay.LiveRows()), "overlay_live_rows")
	b.ReportMetric(float64(overlay.Tombstones()), "overlay_tombstones")
	b.ReportMetric(float64(graphBytes), "base_payload_bytes")
	b.ReportMetric(float64(overlayBytes), "overlay_payload_bytes")
}

func reportColumnVectorDynamicReadMetrics(b *testing.B, elapsed time.Duration, publishedBatches int64, publishedMutations int64, publishNanos int64) {
	b.Helper()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "read_qps")
	}
	if publishedBatches > 0 && elapsed > 0 {
		b.ReportMetric(float64(publishedBatches)/elapsed.Seconds(), "publishes/s")
		b.ReportMetric(float64(publishedMutations)/elapsed.Seconds(), "mutations/s")
		b.ReportMetric(float64(publishNanos)/float64(publishedBatches), "publish_ns/op")
	}
}

func columnVectorDynamicOverlayPayloadBytes(overlay *ColumnVectorDynamicOverlaySnapshot) int64 {
	if overlay == nil {
		return 0
	}
	var tombstoneBytes int
	for _, documentID := range overlay.tombstoneDocIDs {
		tombstoneBytes += len(documentID)
	}
	return int64(len(overlay.vectors)*4 +
		len(overlay.invNorms)*4 +
		len(overlay.idArena) +
		len(overlay.idOffsets)*4 +
		len(overlay.live) +
		len(overlay.tombstoneDocIDs)*24 +
		tombstoneBytes)
}

func containsColumnVectorDynamicResult(results []VectorSearchResult, documentID []byte) bool {
	for _, result := range results {
		if bytes.Equal(result.DocumentID, documentID) {
			return true
		}
	}
	return false
}
