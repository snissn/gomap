package collections

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestColumnVectorGraphNativeSearchCosineUsesPhysicalRowsV3(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{1, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()
	query := []float32{0, 0.2, 1}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	want := exactColumnGraphTopKForTest(t, rows, query, 2)
	assertColumnGraphNativeSearchResultsV3(t, got, want)
	if stats.Candidates != uint64(len(rows)) {
		t.Fatalf("Candidates=%d want %d", stats.Candidates, len(rows))
	}
	if stats.Edges == 0 || stats.CandidateFetches != uint64(len(rows)) || stats.ExpansionFetches == 0 || stats.ResultFetches != uint64(len(got)) {
		t.Fatalf("stats=%+v want graph traversal row fetch accounting", stats)
	}
	if readerStats := reader.Stats(); readerStats.BatchFetches == 0 {
		t.Fatalf("reader stats=%+v want batched top-k result fetch", readerStats)
	}
}

func TestColumnVectorGraphNativeSearchUsesBestFirstFrontierV3(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-start"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{1, 2}},
		{ID: []byte("doc-low"), Vector: []float32{-1, 0, 0}, InvNorm: 1, Adjacency: []uint32{3}},
		{ID: []byte("doc-bridge"), Vector: []float32{0.8, 0.6, 0}, InvNorm: 1, Adjacency: []uint32{4}},
		{ID: []byte("doc-mid"), Vector: []float32{0.5, 0.8660254, 0}, InvNorm: 1},
		{ID: []byte("doc-best"), Vector: []float32{1, 0, 0}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()

	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 4}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(got) != 1 || got[0].Ordinal != 4 || string(got[0].ID) != "doc-best" {
		t.Fatalf("results=%+v want best-first traversal to reach doc-best before lower-score branch", got)
	}
	if stats.Candidates != 4 || stats.CandidateFetches != 4 || stats.ExpansionFetches == 0 {
		t.Fatalf("stats=%+v want four scored candidates with expansion fetch accounting", stats)
	}
}

func TestColumnVectorGraphNativeSearchKeepsExpansionAdjacencyStableV3(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-start"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{1, 2}},
		{ID: []byte("doc-low"), Vector: []float32{-1, 0, 0}, InvNorm: 1, Adjacency: []uint32{4, 4}},
		{ID: []byte("doc-best"), Vector: []float32{1, 0, 0}, InvNorm: 1},
		{ID: []byte("doc-unused"), Vector: []float32{0, 0, 1}, InvNorm: 1},
		{ID: []byte("doc-bad"), Vector: []float32{-0.9, 0, 0}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()

	var scratch columnVectorGraphNativeSearchScratch
	got, _, err := reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 3}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(got) != 1 || got[0].Ordinal != 2 || string(got[0].ID) != "doc-best" {
		t.Fatalf("results=%+v want scoring fetches not to mutate expansion adjacency", got)
	}
}

func TestColumnVectorGraphNativeSearchRejectsBadQueryV3(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()
	var scratch columnVectorGraphNativeSearchScratch
	_, _, err = reader.SearchCosine([]float32{1, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1}, &scratch)
	if err == nil || !strings.Contains(err.Error(), "query dims=2 want 3") {
		t.Fatalf("SearchCosine dim err=%v want dimension failure", err)
	}
	_, _, err = reader.SearchCosine([]float32{0, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1}, &scratch)
	if err == nil || !strings.Contains(err.Error(), "query norm") {
		t.Fatalf("SearchCosine zero err=%v want norm failure", err)
	}
	_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: -1}, &scratch)
	if err == nil || !strings.Contains(err.Error(), "ef_search cannot be negative") {
		t.Fatalf("SearchCosine ef_search err=%v want negative ef_search failure", err)
	}
}

func TestColumnVectorGraphNativeSearchRequiresScratchV3(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()

	got, stats, err := reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 0, EfSearch: 1}, nil)
	if err != nil {
		t.Fatalf("SearchCosine zero top_k with nil scratch: %v", err)
	}
	if len(got) != 0 || stats != (columnVectorGraphNativeSearchStats{}) {
		t.Fatalf("zero top_k results=%v stats=%+v want empty", got, stats)
	}

	_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "requires caller-owned scratch") {
		t.Fatalf("SearchCosine err=%v want caller-owned scratch failure", err)
	}
}

func TestColumnVectorGraphNativeCosineScoreRejectsMalformedRowV3(t *testing.T) {
	_, err := columnVectorGraphNativeCosineScore([]float32{1, 0}, 1, columnVectorGraphPhysicalRow{
		Ordinal: 7,
		Vector:  []float32{1},
		InvNorm: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "ordinal=7 vector dims=1 want 2") {
		t.Fatalf("columnVectorGraphNativeCosineScore err=%v want dimension failure", err)
	}
}

func TestColumnVectorGraphNativeSearchEmptyAndTopKClampV3(t *testing.T) {
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, nil)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex empty collection: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 10}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine empty: %v", err)
	}
	if len(got) != 0 || stats != (columnVectorGraphNativeSearchStats{}) {
		t.Fatalf("empty search results=%v stats=%+v want empty", got, stats)
	}
	got, stats, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 0, EfSearch: 10}, nil)
	if err != nil {
		t.Fatalf("SearchCosine zero top_k with nil scratch: %v", err)
	}
	if len(got) != 0 || stats != (columnVectorGraphNativeSearchStats{}) {
		t.Fatalf("zero top_k results=%v stats=%+v want empty", got, stats)
	}
}

func TestColumnVectorGraphNativeSearchScratchVisitMarksShrinkV3(t *testing.T) {
	var scratch columnVectorGraphNativeSearchScratch
	if err := scratch.prepare(10, 3, 2, 1, 4); err != nil {
		t.Fatalf("prepare large: %v", err)
	}
	if len(scratch.visitMarks) != 10 {
		t.Fatalf("large visitMarks len=%d want 10", len(scratch.visitMarks))
	}
	if cap(scratch.frontier) > 4 {
		t.Fatalf("frontier cap=%d want bounded by ef_search", cap(scratch.frontier))
	}
	if err := scratch.prepare(1, 3, 2, 1, 1); err != nil {
		t.Fatalf("prepare small: %v", err)
	}
	if len(scratch.visitMarks) != 1 {
		t.Fatalf("small visitMarks len=%d want 1", len(scratch.visitMarks))
	}
	if scratch.markVisited(5) {
		t.Fatalf("markVisited accepted stale ordinal outside current row count")
	}
}

func TestColumnVectorGraphNativeSearchDoesNotFetchDocumentsV3(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()
	var scratch columnVectorGraphNativeSearchScratch
	got, _, err := reader.SearchCosine([]float32{0, 0, 1}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(got) != 1 || string(got[0].ID) != "doc-c" {
		t.Fatalf("results=%+v want doc-c", got)
	}
	// This kernel has no collection/document handle and returns only IDs,
	// ordinals, and scores from physical graph rows. Document materialization
	// belongs above this layer after top-k selection.
}

func TestColumnVectorGraphNativeSearchContinuesAcrossDisconnectedComponentsV3(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1},
		{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine([]float32{0, 0, 1}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(got) != 1 || string(got[0].ID) != "doc-c" {
		t.Fatalf("results=%+v want doc-c after disconnected traversal", got)
	}
	if stats.Candidates != uint64(len(rows)) || stats.Edges != 0 {
		t.Fatalf("stats=%+v want all disconnected rows searched without edges", stats)
	}
}

func TestColumnVectorGraphNativeSearchWarmScratchZeroAllocsV3(t *testing.T) {
	const (
		rows = 32
		dims = 16
		m    = 8
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dims, m, input)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()
	query := append([]float32(nil), input[7].vector...)
	var scratch columnVectorGraphNativeSearchScratch
	if _, _, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 16}, &scratch); err != nil {
		t.Fatalf("warm SearchCosine: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		got, _, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 16}, &scratch)
		if err != nil {
			t.Fatalf("SearchCosine: %v", err)
		}
		columnPhysicalScanBenchSum += int64(got[0].Ordinal)
	})
	if allocs != 0 {
		t.Fatalf("hot SearchCosine allocs=%v want zero", allocs)
	}
}

func TestColumnVectorGraphNativeSearchParallelReadersV3(t *testing.T) {
	const (
		rows = 128
		dims = 32
		m    = 16
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dims, m, input)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	query := append([]float32(nil), input[17].vector...)
	workers := runtime.GOMAXPROCS(0)
	readers := make([]*columnVectorGraphPhysicalRowReader, workers)
	for i := range readers {
		reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("open reader %d: %v", i, err)
		}
		defer reader.Close()
		readers[i] = reader
	}
	want, _, err := readers[0].SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64}, &columnVectorGraphNativeSearchScratch{})
	if err != nil {
		t.Fatalf("baseline SearchCosine: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan string, len(readers))
	for worker := range readers {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			var scratch columnVectorGraphNativeSearchScratch
			for i := 0; i < 100; i++ {
				got, _, err := readers[worker].SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64}, &scratch)
				if err != nil {
					errs <- fmt.Sprintf("worker %d SearchCosine: %v", worker, err)
					return
				}
				if mismatch := columnGraphNativeSearchResultsMismatchV3(got, want); mismatch != "" {
					errs <- fmt.Sprintf("worker %d iteration %d: %s", worker, i, mismatch)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func BenchmarkColumnVectorGraphNativeSearchCosineV3(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(b, status, def.Name)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		b.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()
	query := append([]float32(nil), input[37].vector...)
	var scratch columnVectorGraphNativeSearchScratch
	if _, _, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: topK, EfSearch: efSearch}, &scratch); err != nil {
		b.Fatalf("warm SearchCosine: %v", err)
	}
	baseStats := reader.Stats()
	var searchStats columnVectorGraphNativeSearchStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: topK, EfSearch: efSearch}, &scratch)
		if err != nil {
			b.Fatalf("SearchCosine: %v", err)
		}
		columnPhysicalScanBenchSum += int64(got[0].Ordinal)
		searchStats.Candidates += stats.Candidates
		searchStats.Edges += stats.Edges
		searchStats.CandidateFetches += stats.CandidateFetches
		searchStats.ExpansionFetches += stats.ExpansionFetches
		searchStats.ResultFetches += stats.ResultFetches
	}
	b.StopTimer()
	stats := reader.Stats()
	reportColumnGraphNativeSearchBenchMetricsV3(b, b.N, baseStats, stats, searchStats)
}

func BenchmarkColumnVectorGraphNativeSearchCosineParallelV3(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(b, status, def.Name)
	type searchWorker struct {
		reader  *columnVectorGraphPhysicalRowReader
		scratch columnVectorGraphNativeSearchScratch
	}
	workers := runtime.GOMAXPROCS(0)
	benchWorkers := make([]*searchWorker, workers)
	query := append([]float32(nil), input[37].vector...)
	for i := range benchWorkers {
		reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("open reader %d: %v", i, err)
		}
		defer reader.Close()
		worker := &searchWorker{reader: reader}
		if _, _, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: topK, EfSearch: efSearch}, &worker.scratch); err != nil {
			b.Fatalf("warm reader %d SearchCosine: %v", i, err)
		}
		benchWorkers[i] = worker
	}
	var baseStats columnPhysicalRowReaderStats
	for _, worker := range benchWorkers {
		baseStats = addColumnPhysicalRowReaderStatsV3(baseStats, worker.reader.Stats())
	}

	var sink atomic.Int64
	var firstErr atomic.Value
	var failed atomic.Bool
	recordParallelErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	var totalCandidates atomic.Uint64
	var totalEdges atomic.Uint64
	var totalCandidateFetches atomic.Uint64
	var totalExpansionFetches atomic.Uint64
	var totalResultFetches atomic.Uint64
	var nextWorker atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerIndex := int(nextWorker.Add(1)) - 1
		if workerIndex < 0 || workerIndex >= len(benchWorkers) {
			recordParallelErr("parallel worker requested more than %d prewarmed readers/scratches", workers)
			for pb.Next() {
			}
			return
		}
		worker := benchWorkers[workerIndex]
		for pb.Next() {
			if failed.Load() {
				continue
			}
			got, stats, err := worker.reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: topK, EfSearch: efSearch}, &worker.scratch)
			if err != nil {
				recordParallelErr("SearchCosine: %v", err)
				continue
			}
			sink.Add(int64(got[0].Ordinal))
			totalCandidates.Add(stats.Candidates)
			totalEdges.Add(stats.Edges)
			totalCandidateFetches.Add(stats.CandidateFetches)
			totalExpansionFetches.Add(stats.ExpansionFetches)
			totalResultFetches.Add(stats.ResultFetches)
		}
	})
	b.StopTimer()
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	columnPhysicalScanBenchSum += sink.Load()
	var stats columnPhysicalRowReaderStats
	for _, worker := range benchWorkers {
		stats = addColumnPhysicalRowReaderStatsV3(stats, worker.reader.Stats())
	}
	reportColumnGraphNativeSearchBenchMetricsV3(b, b.N, baseStats, stats, columnVectorGraphNativeSearchStats{
		Candidates:       totalCandidates.Load(),
		Edges:            totalEdges.Load(),
		CandidateFetches: totalCandidateFetches.Load(),
		ExpansionFetches: totalExpansionFetches.Load(),
		ResultFetches:    totalResultFetches.Load(),
	})
}

func exactColumnGraphTopKForTest(tb testing.TB, rows []columnGraphRebuildInputRowV2A, query []float32, topK int) []columnVectorGraphNativeSearchResult {
	tb.Helper()
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNorm query: %v", err)
	}
	var top []columnVectorGraphSearchCandidate
	for ordinal, row := range rows {
		invNorm, err := columnVectorGraphInvNorm(row.vector)
		if err != nil {
			tb.Fatalf("columnVectorGraphInvNorm row %d: %v", ordinal, err)
		}
		var dot float64
		for i, v := range query {
			dot += float64(v) * float64(row.vector[i])
		}
		score := dot * float64(queryInvNorm) * float64(invNorm)
		top = insertColumnGraphTopForTest(top, topK, columnVectorGraphSearchCandidate{ordinal: ordinal, score: score})
	}
	out := make([]columnVectorGraphNativeSearchResult, len(top))
	for i, candidate := range top {
		out[i] = columnVectorGraphNativeSearchResult{
			Ordinal: candidate.ordinal,
			ID:      []byte(rows[candidate.ordinal].id),
			Score:   candidate.score,
		}
	}
	return out
}

func insertColumnGraphTopForTest(top []columnVectorGraphSearchCandidate, limit int, candidate columnVectorGraphSearchCandidate) []columnVectorGraphSearchCandidate {
	pos := len(top)
	for pos > 0 && columnVectorGraphSearchCandidateLess(candidate, top[pos-1]) {
		pos--
	}
	if pos >= limit {
		return top
	}
	if len(top) < limit {
		top = append(top, columnVectorGraphSearchCandidate{})
	}
	copy(top[pos+1:], top[pos:len(top)-1])
	top[pos] = candidate
	return top
}

func assertColumnGraphNativeSearchResultsV3(tb testing.TB, got, want []columnVectorGraphNativeSearchResult) {
	tb.Helper()
	if mismatch := columnGraphNativeSearchResultsMismatchV3(got, want); mismatch != "" {
		tb.Fatal(mismatch)
	}
}

func columnGraphNativeSearchResultsMismatchV3(got, want []columnVectorGraphNativeSearchResult) string {
	if len(got) != len(want) {
		return fmt.Sprintf("results len=%d want %d got=%+v want=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Ordinal != want[i].Ordinal || string(got[i].ID) != string(want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			return fmt.Sprintf("result[%d]=%s want %s", i, columnGraphNativeSearchResultStringV3(got[i]), columnGraphNativeSearchResultStringV3(want[i]))
		}
	}
	return ""
}

func columnGraphNativeSearchResultStringV3(result columnVectorGraphNativeSearchResult) string {
	return fmt.Sprintf("{ordinal:%d id:%q score:%.9f}", result.Ordinal, string(result.ID), result.Score)
}

func reportColumnGraphNativeSearchBenchMetricsV3(b *testing.B, n int, baseStats, stats columnPhysicalRowReaderStats, searchStats columnVectorGraphNativeSearchStats) {
	b.Helper()
	if n <= 0 {
		return
	}
	b.ReportMetric(float64(searchStats.Candidates)/float64(n), "candidates/search")
	b.ReportMetric(float64(searchStats.Edges)/float64(n), "edges/search")
	if searchStats.Candidates > 0 {
		b.ReportMetric(float64(searchStats.Edges)/float64(searchStats.Candidates), "edges/node")
	}
	b.ReportMetric(float64(searchStats.CandidateFetches)/float64(n), "candidate_fetches/search")
	b.ReportMetric(float64(searchStats.ExpansionFetches)/float64(n), "expansion_fetches/search")
	b.ReportMetric(float64(searchStats.ResultFetches)/float64(n), "result_fetches/search")
	b.ReportMetric(float64(stats.CacheHits-baseStats.CacheHits)/float64(n), "cache_hits/search")
	b.ReportMetric(float64(stats.CacheMisses-baseStats.CacheMisses)/float64(n), "cache_misses/search")
	b.ReportMetric(float64(stats.DecodedBlocks-baseStats.DecodedBlocks)/float64(n), "decoded_blocks/search")
	b.ReportMetric(float64(stats.GranulesTouched-baseStats.GranulesTouched)/float64(n), "granules_touched/search")
	b.ReportMetric(float64(stats.PhysicalBytesRead-baseStats.PhysicalBytesRead)/float64(n), "physical_B/search")
	b.ReportMetric(float64(stats.MaxResidentBytes), "max_resident_B")
	if stats.Rows > 0 {
		b.ReportMetric(float64(stats.OpenPhysicalBytesRead)/float64(stats.Rows), "asset_B/row")
	}
}

func addColumnPhysicalRowReaderStatsV3(left, right columnPhysicalRowReaderStats) columnPhysicalRowReaderStats {
	left.Rows += right.Rows
	left.Granules += right.Granules
	left.OpenGranulesRead += right.OpenGranulesRead
	left.OpenPhysicalBytesRead += right.OpenPhysicalBytesRead
	left.OpenSegmentCacheHits += right.OpenSegmentCacheHits
	left.OpenSegmentCacheMisses += right.OpenSegmentCacheMisses
	left.RowFetches += right.RowFetches
	left.BatchFetches += right.BatchFetches
	left.RowsFetched += right.RowsFetched
	left.CacheHits += right.CacheHits
	left.CacheMisses += right.CacheMisses
	left.DecodedBlocks += right.DecodedBlocks
	left.BlockEvictions += right.BlockEvictions
	left.GranulesTouched += right.GranulesTouched
	left.PhysicalBytesRead += right.PhysicalBytesRead
	left.ResidentBytes += right.ResidentBytes
	if right.MaxResidentBytes > left.MaxResidentBytes {
		left.MaxResidentBytes = right.MaxResidentBytes
	}
	left.SegmentFileCacheHits += right.SegmentFileCacheHits
	left.SegmentFileCacheMisses += right.SegmentFileCacheMisses
	return left
}
