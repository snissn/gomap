package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 = 8

type columnVectorGraphNativeSearchBenchShapeV3 struct {
	rows                      int
	dims                      int
	m                         int
	topK                      int
	efSearch                  int
	queryOrdinal              int
	directPhysicalAsset       bool
	typedColumnVector         bool
	omitResultMaterialization bool
	scoreBatchMode            columnVectorGraphScoreBatchMode
	statsMode                 columnVectorGraphNativeSearchStatsMode
}

func TestColumnVectorGraphAdjacencySourceStatsSkipEmptyNoopV3(t *testing.T) {
	var stats columnVectorGraphNativeSearchStats
	recordColumnVectorGraphAdjacencySourceStats(&stats, 0, false)
	if stats.AdjacencyBytesRead != 0 || stats.AdjacencyDirectViews != 0 || stats.AdjacencyMmapDirectViews != 0 || stats.AdjacencyHeapCopyTypedViews != 0 || stats.AdjacencyScratchDecodes != 0 {
		t.Fatalf("empty adjacency stats=%+v want no direct/scratch decode counts", stats)
	}
	recordColumnVectorGraphAdjacencySourceStats(&stats, 2, false)
	if stats.AdjacencyBytesRead != 8 || stats.AdjacencyDirectViews != 0 || stats.AdjacencyMmapDirectViews != 0 || stats.AdjacencyHeapCopyTypedViews != 0 || stats.AdjacencyScratchDecodes != 1 {
		t.Fatalf("scratch adjacency stats=%+v want bytes=8 scratch=1", stats)
	}
	recordColumnVectorGraphAdjacencySourceStats(&stats, 3, true)
	if stats.AdjacencyBytesRead != 20 || stats.AdjacencyDirectViews != 1 || stats.AdjacencyMmapDirectViews != 0 || stats.AdjacencyHeapCopyTypedViews != 0 || stats.AdjacencyScratchDecodes != 1 {
		t.Fatalf("row-asset direct adjacency stats=%+v want cumulative bytes=20 direct=1 scratch=1 without layer-0 mmap attribution", stats)
	}
	recordColumnVectorGraphAdjacencySourceOutcomeStats(&stats, 0, columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect)
	if stats.AdjacencyBytesRead != 20 || stats.AdjacencyDirectViews != 2 || stats.AdjacencyMmapDirectViews != 1 || stats.AdjacencyHeapCopyTypedViews != 0 || stats.AdjacencyScratchDecodes != 1 {
		t.Fatalf("empty layer-0 direct adjacency stats=%+v want mmap outcome counted without bytes", stats)
	}
}

func TestColumnVectorGraphNativeSearchFrontierHeapFanout2272(t *testing.T) {
	if columnVectorGraphNativeFrontierHeapFanout != 4 {
		t.Fatalf("frontier heap fanout=%d; update unrolled frontierSiftDown child scan before changing fanout", columnVectorGraphNativeFrontierHeapFanout)
	}
}

func TestColumnVectorGraphNativeSearchVisitedEpochReuseGrowthWrap2273(t *testing.T) {
	var scratch columnVectorGraphNativeSearchScratch
	prepare := func(rows int) {
		t.Helper()
		if err := scratch.prepare(rows, 4, 2, 2, 4, 4, 0); err != nil {
			t.Fatalf("prepare rows=%d: %v", rows, err)
		}
	}

	prepare(3)
	firstEpoch := scratch.visitEpoch
	if firstEpoch == 0 || len(scratch.visitMarks) != 3 {
		t.Fatalf("first prepare epoch=%d marks=%d", firstEpoch, len(scratch.visitMarks))
	}
	if !scratch.markVisited(1) || scratch.markVisited(1) {
		t.Fatalf("markVisited did not report first mark then duplicate for epoch=%d marks=%v", scratch.visitEpoch, scratch.visitMarks)
	}
	if seed, ok := columnVectorGraphNextCandidateSeed(1, 3, typedcolumn.RowSelection{}, false, scratch.visitMarks, scratch.visitEpoch); !ok || seed != 2 {
		t.Fatalf("seed after visited mark=(%d,%v), want (2,true)", seed, ok)
	}

	prepare(3)
	if scratch.visitEpoch != firstEpoch+1 {
		t.Fatalf("reuse prepare epoch=%d want %d", scratch.visitEpoch, firstEpoch+1)
	}
	if !scratch.markVisited(1) {
		t.Fatalf("reused scratch treated prior epoch mark as current: epoch=%d marks=%v", scratch.visitEpoch, scratch.visitMarks)
	}

	prepare(5)
	if len(scratch.visitMarks) != 5 {
		t.Fatalf("grown marks len=%d want 5", len(scratch.visitMarks))
	}
	if !scratch.markVisited(4) || !scratch.markVisited(2) {
		t.Fatalf("grown scratch did not accept first marks: epoch=%d marks=%v", scratch.visitEpoch, scratch.visitMarks)
	}

	for i := range scratch.visitMarks {
		scratch.visitMarks[i] = math.MaxUint64
	}
	scratch.visitEpoch = math.MaxUint64
	prepare(5)
	if scratch.visitEpoch != 1 {
		t.Fatalf("wrap prepare epoch=%d want 1", scratch.visitEpoch)
	}
	for i, mark := range scratch.visitMarks {
		if mark != 0 {
			t.Fatalf("wrap mark[%d]=%d want cleared 0; marks=%v", i, mark, scratch.visitMarks)
		}
	}
	if !scratch.markVisited(3) || scratch.markVisited(3) {
		t.Fatalf("markVisited after wrap did not report first mark then duplicate: epoch=%d marks=%v", scratch.visitEpoch, scratch.visitMarks)
	}
}

func TestColumnVectorGraphNativeSearchFrontierHeapOrder1980(t *testing.T) {
	candidates := []columnVectorGraphSearchCandidate{
		{ordinal: 7, score: 0.70},
		{ordinal: 3, score: 0.70},
		{ordinal: 11, score: -0.10},
		{ordinal: 1, score: 0.95},
		{ordinal: 9, score: 0.20},
		{ordinal: 5, score: 0.95},
		{ordinal: 4, score: 0.20},
		{ordinal: 2, score: -0.10},
	}
	want := make([]columnVectorGraphSearchCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		want = insertColumnGraphTopForTest(want, len(candidates), candidate)
	}

	var scratch columnVectorGraphNativeSearchScratch
	for _, candidate := range candidates {
		scratch.pushFrontier(candidate)
	}
	assertColumnVectorGraphFrontierPopOrder1980(t, &scratch, want)

	var interleavedScratch columnVectorGraphNativeSearchScratch
	interleavedWant := make([]columnVectorGraphSearchCandidate, 0, len(candidates))
	for i, candidate := range candidates {
		interleavedScratch.pushFrontier(candidate)
		interleavedWant = insertColumnGraphTopForTest(interleavedWant, len(candidates), candidate)
		if i%3 == 2 {
			got, ok := interleavedScratch.popFrontier()
			if !ok {
				t.Fatalf("interleaved frontier pop after push %d missing", i)
			}
			if got != interleavedWant[0] {
				t.Fatalf("interleaved frontier pop after push %d=%+v want %+v", i, got, interleavedWant[0])
			}
			interleavedWant = interleavedWant[1:]
		}
	}
	assertColumnVectorGraphFrontierPopOrder1980(t, &interleavedScratch, interleavedWant)

	var debugScratch columnVectorGraphNativeSearchScratch
	var stats columnVectorGraphNativeSearchStats
	debugCounters := &columnVectorGraphNativeSearchDebugCounters{stats: &stats}
	for _, candidate := range candidates {
		debugScratch.pushFrontierDebug(candidate, debugCounters)
	}
	if stats.FrontierPushes != uint64(len(candidates)) {
		t.Fatalf("debug frontier pushes=%d want %d", stats.FrontierPushes, len(candidates))
	}
	assertColumnVectorGraphFrontierPopOrderDebug1980(t, &debugScratch, debugCounters, want)
	if stats.FrontierPops != uint64(len(candidates)) || stats.FrontierPopMisses != 1 {
		t.Fatalf("debug frontier pop stats=%+v want pops=%d misses=1", stats, len(candidates))
	}
	if stats.FrontierSiftUpSteps == 0 || stats.FrontierSiftDownSteps == 0 {
		t.Fatalf("debug frontier sift stats=%+v want non-zero up/down steps", stats)
	}
}

func assertColumnVectorGraphFrontierPopOrder1980(t *testing.T, scratch *columnVectorGraphNativeSearchScratch, want []columnVectorGraphSearchCandidate) {
	t.Helper()
	for i, wantCandidate := range want {
		got, ok := scratch.popFrontier()
		if !ok {
			t.Fatalf("frontier pop[%d] missing want %+v", i, wantCandidate)
		}
		if got != wantCandidate {
			t.Fatalf("frontier pop[%d]=%+v want %+v", i, got, wantCandidate)
		}
	}
	if got, ok := scratch.popFrontier(); ok {
		t.Fatalf("frontier pop after drain=%+v, want empty", got)
	}
}

func assertColumnVectorGraphFrontierPopOrderDebug1980(t *testing.T, scratch *columnVectorGraphNativeSearchScratch, debugCounters *columnVectorGraphNativeSearchDebugCounters, want []columnVectorGraphSearchCandidate) {
	t.Helper()
	for i, wantCandidate := range want {
		got, ok := scratch.popFrontierDebug(debugCounters)
		if !ok {
			t.Fatalf("debug frontier pop[%d] missing want %+v", i, wantCandidate)
		}
		if got != wantCandidate {
			t.Fatalf("debug frontier pop[%d]=%+v want %+v", i, got, wantCandidate)
		}
	}
	if got, ok := scratch.popFrontierDebug(debugCounters); ok {
		t.Fatalf("debug frontier pop after drain=%+v, want empty", got)
	}
}

func TestColumnVectorGraphNativeSearchTopInsertOrder2272(t *testing.T) {
	candidates := []columnVectorGraphSearchCandidate{
		{ordinal: 7, score: 0.70},
		{ordinal: 3, score: 0.70},
		{ordinal: 11, score: -0.10},
		{ordinal: 1, score: 0.95},
		{ordinal: 9, score: 0.20},
		{ordinal: 5, score: 0.95},
		{ordinal: 4, score: 0.20},
		{ordinal: 2, score: -0.10},
	}
	const limit = 4
	var scratch columnVectorGraphNativeSearchScratch
	var debugScratch columnVectorGraphNativeSearchScratch
	var stats columnVectorGraphNativeSearchStats
	debugCounters := &columnVectorGraphNativeSearchDebugCounters{stats: &stats}
	var want []columnVectorGraphSearchCandidate
	var successes uint64
	for i, candidate := range candidates {
		before := append([]columnVectorGraphSearchCandidate(nil), want...)
		want = insertColumnGraphTopForTest(want, limit, candidate)
		wantAccepted := !columnVectorGraphCandidateSlicesEqual2272(before, want)
		if gotAccepted := scratch.insertTop(limit, candidate); gotAccepted != wantAccepted {
			t.Fatalf("insertTop[%d] accepted=%v want %v", i, gotAccepted, wantAccepted)
		}
		if got := sortedColumnGraphTopForTest4136(scratch.top); !columnVectorGraphCandidateSlicesEqual2272(got, want) {
			t.Fatalf("insertTop[%d] top=%+v want %+v", i, scratch.top, want)
		}
		if gotAccepted := debugScratch.insertTopDebug(limit, candidate, debugCounters); gotAccepted != wantAccepted {
			t.Fatalf("debug insertTop[%d] accepted=%v want %v", i, gotAccepted, wantAccepted)
		}
		if got := sortedColumnGraphTopForTest4136(debugScratch.top); !columnVectorGraphCandidateSlicesEqual2272(got, want) {
			t.Fatalf("debug insertTop[%d] top=%+v want %+v", i, debugScratch.top, want)
		}
		if wantAccepted {
			successes++
		}
	}
	if stats.TopKInsertAttempts != uint64(len(candidates)) || stats.TopKInsertSuccesses != successes || stats.TopKInsertRejections != uint64(len(candidates))-successes {
		t.Fatalf("debug top-k stats=%+v want attempts=%d successes=%d", stats, len(candidates), successes)
	}
	if stats.TopKComparisons == 0 || stats.TopKHeapSiftSteps == 0 {
		t.Fatalf("debug top-k comparison/sift stats=%+v want non-zero", stats)
	}
	scratch.sortTopBestFirst()
	debugScratch.sortTopBestFirst()
	if !columnVectorGraphCandidateSlicesEqual2272(scratch.top, want) || !columnVectorGraphCandidateSlicesEqual2272(debugScratch.top, want) {
		t.Fatalf("final ordered top=%+v debug=%+v want=%+v", scratch.top, debugScratch.top, want)
	}
}

func TestColumnVectorGraphNativeSearchTopRetentionCharacterization4136(t *testing.T) {
	for _, limit := range []int{1, 2, 10, 32, 128, 256} {
		scratch := columnVectorGraphNativeSearchScratch{frontier: make([]columnVectorGraphSearchCandidate, 0, limit)}
		var want []columnVectorGraphSearchCandidate
		state := uint64(0x4136_9e37_79b9_7f4a)
		for i := 0; i < 4096; i++ {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			candidate := columnVectorGraphSearchCandidate{ordinal: int(state % 769), score: float64(int64(state>>32)%31) / 8}
			want = insertColumnGraphTopForTest(want, limit, candidate)
			scratch.insertTop(limit, candidate)
			got := sortedColumnGraphTopForTest4136(scratch.top)
			if !columnVectorGraphCandidateSlicesEqual2272(got, want) {
				t.Fatalf("limit=%d insert=%d got=%+v want=%+v", limit, i, got, want)
			}
		}
		retained := append([]columnVectorGraphSearchCandidate(nil), scratch.top...)
		for _, outputLimit := range []int{1, 10, 32, 33, 100, 128} {
			if outputLimit > limit {
				continue
			}
			scratch.top = append(scratch.top[:0], retained...)
			scratch.retainTopBestFirst(outputLimit)
			if !columnVectorGraphCandidateSlicesEqual2272(scratch.top, want[:outputLimit]) {
				t.Fatalf("limit=%d output_limit=%d retained=%+v want=%+v", limit, outputLimit, scratch.top, want[:outputLimit])
			}
		}
		scratch.top = scratch.top[:0]
		if !scratch.insertTop(limit, columnVectorGraphSearchCandidate{ordinal: 1, score: 1}) || len(scratch.top) != 1 {
			t.Fatalf("limit=%d reset/reuse top=%+v", limit, scratch.top)
		}
	}
}

func sortedColumnGraphTopForTest4136(top []columnVectorGraphSearchCandidate) []columnVectorGraphSearchCandidate {
	got := append([]columnVectorGraphSearchCandidate(nil), top...)
	sort.Slice(got, func(i, j int) bool { return columnVectorGraphSearchCandidateBetter(got[i], got[j]) })
	return got
}

func columnVectorGraphCandidateSlicesEqual2272(left, right []columnVectorGraphSearchCandidate) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func columnVectorGraphNativeSearchSmallBenchShapeV3() columnVectorGraphNativeSearchBenchShapeV3 {
	return columnVectorGraphNativeSearchBenchShapeV3{
		rows:         1024,
		dims:         128,
		m:            16,
		topK:         10,
		efSearch:     128,
		queryOrdinal: 37,
	}
}

func columnVectorGraphNativeSearchProduction8192BenchShapeV3() columnVectorGraphNativeSearchBenchShapeV3 {
	return columnVectorGraphNativeSearchBenchShapeV3{
		rows:         8192,
		dims:         128,
		m:            16,
		topK:         10,
		efSearch:     128,
		queryOrdinal: 4096,
		// Publish the physical graph asset directly so repeated benchmark counts
		// measure the native search loop, not O(rows^2) rebuild adjacency setup.
		directPhysicalAsset: true,
	}
}

func columnVectorGraphNativeSearchProductionSweepBenchShapesV3() []columnVectorGraphNativeSearchBenchShapeV3 {
	dims := []int{128, 384, 768, 1024, 1536}
	shapes := make([]columnVectorGraphNativeSearchBenchShapeV3, 0, len(dims)+1)
	shapes = append(shapes, columnVectorGraphNativeSearchBenchShapeV3{
		rows:                1024,
		dims:                128,
		m:                   16,
		topK:                10,
		efSearch:            128,
		queryOrdinal:        512,
		directPhysicalAsset: true,
	})
	for _, dim := range dims {
		shapes = append(shapes, columnVectorGraphNativeSearchBenchShapeV3{
			rows:                8192,
			dims:                dim,
			m:                   16,
			topK:                10,
			efSearch:            128,
			queryOrdinal:        4096,
			directPhysicalAsset: true,
		})
	}
	return shapes
}

func columnVectorGraphNativeSearchBenchShapeNameV3(shape columnVectorGraphNativeSearchBenchShapeV3) string {
	name := fmt.Sprintf("rows=%d/dims=%d/degree=%d/topK=%d/efSearch=%d", shape.rows, shape.dims, shape.m, shape.topK, shape.efSearch)
	if shape.statsMode != columnVectorGraphNativeSearchStatsModeDefault {
		name += "/stats=" + shape.statsMode.String()
	}
	return name
}

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
	if stats.Edges == 0 || stats.CandidateFetches < uint64(len(rows)) || stats.ScoreBatches < uint64(len(rows)) || stats.ExpansionFetches != stats.AdjacencyExpansions || stats.ResultFetches != uint64(len(got)) {
		t.Fatalf("stats=%+v want at least %d candidate/score batches, exact result fetches, and matching lazy expansion fetches", stats, len(rows))
	}
	readerStats := reader.Stats()
	if readerStats.RowFetches != 0 || readerStats.BatchFetches != 0 || readerStats.RowsFetched != 0 {
		t.Fatalf("reader stats=%+v want no generic row fetches for scoring or result IDs", readerStats)
	}
}

func TestColumnVectorGraphNativeSearchCountersReportPayloadBytesC3(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{1, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var scratch columnVectorGraphNativeSearchScratch
	_, stats, err := reader.SearchCosine([]float32{0, 0.2, 1}, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if stats.CandidateRows != uint64(len(rows)) || stats.VisitedNodes < stats.Candidates || stats.VisitedEdges != stats.Edges {
		t.Fatalf("stats=%+v want candidate-row and non-undercounting visited graph counters", stats)
	}
	if got, want := stats.VectorBytesRead, stats.CandidateFetches*uint64(def.Dimensions)*4; got != want {
		t.Fatalf("vector bytes read=%d want candidate_fetches*dims*4=%d stats=%+v", got, want, stats)
	}
	if stats.AdjacencyBytesRead == 0 || stats.AdjacencyMmapDirectViews+stats.AdjacencyHeapCopyTypedViews+stats.AdjacencyScratchDecodes == 0 {
		t.Fatalf("stats=%+v want adjacency bytes and a classified adjacency source outcome", stats)
	}
	if stats.AdjacencyScratchDecodes != 0 {
		t.Fatalf("stats=%+v want certified adjacency sources to avoid scratch decodes", stats)
	}
}

func TestColumnVectorGraphNativeSearchCountsUpperLayerVisitedNodesC3(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{0, 1}, InvNorm: 1, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 1, 2, 1, 1}},
		{ID: []byte("doc-upper-best"), Vector: []float32{1, 0}, InvNorm: 1},
		{ID: []byte("doc-base-neighbor"), Vector: []float32{0.2, 0.8}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(t, 2, 2, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine([]float32{1, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(got) != 1 || got[0].Ordinal != 1 {
		t.Fatalf("results=%+v want upper-layer greedy entry ordinal 1", got)
	}
	if stats.Candidates != 1 || stats.VisitedNodes <= stats.Candidates || stats.CandidateFetches <= stats.Candidates {
		t.Fatalf("stats=%+v want upper-layer scoring counted as visited node fetches", stats)
	}
}

func TestColumnVectorGraphNativeSearchComposesCandidateSelectionC3(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-outside-best"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1, 2, 3}},
		{ID: []byte("doc-selected-low"), Vector: []float32{0, 1, 0}, InvNorm: 1},
		{ID: []byte("doc-outside-mid"), Vector: []float32{0.8, 0.2, 0}, InvNorm: 1},
		{ID: []byte("doc-selected-best"), Vector: []float32{0.9, 0.1, 0}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	predicate, err := typedcolumn.NewSparseRowSelection(len(rows), []int{0, 1, 3})
	if err != nil {
		t.Fatalf("NewSparseRowSelection predicate: %v", err)
	}
	visibility, err := typedcolumn.NewSparseRowSelection(len(rows), []int{1, 2, 3})
	if err != nil {
		t.Fatalf("NewSparseRowSelection visibility: %v", err)
	}
	var selectionScratch typedcolumn.RowSelectionScratch
	candidateRows, hasCandidateRows, err := composeColumnVectorGraphCandidateRowSelection(len(rows), &predicate, &visibility, &selectionScratch)
	if err != nil {
		t.Fatalf("composeColumnVectorGraphCandidateRowSelection: %v", err)
	}
	if !hasCandidateRows || candidateRows.Count() != 2 || candidateRows.Contains(0) || !candidateRows.Contains(1) || !candidateRows.Contains(3) {
		t.Fatalf("candidate selection=%+v rows=%v want visible predicate intersection {1,3}", candidateRows.Shape(), candidateRows.AppendRows(nil))
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows), CandidateRows: candidateRows, HasCandidateRows: hasCandidateRows}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine filtered: %v", err)
	}
	if len(got) != 2 || got[0].Ordinal != 3 || got[1].Ordinal != 1 {
		t.Fatalf("filtered results=%+v want selected ordinals [3 1] only", got)
	}
	if stats.CandidateRows != 2 || stats.Candidates != 2 || stats.VectorBytesRead != stats.CandidateFetches*uint64(def.Dimensions)*4 {
		t.Fatalf("filtered stats=%+v want two candidate rows and vector-byte accounting", stats)
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
	if stats.Candidates != 5 || stats.CandidateFetches != 5 || stats.ScoreBatches != 5 || stats.ExpansionFetches != stats.AdjacencyExpansions {
		t.Fatalf("stats=%+v want five scored candidates plus lazy adjacency expansion fetches", stats)
	}
	if readerStats := reader.Stats(); readerStats.RowFetches != 0 || readerStats.BatchFetches != 0 || readerStats.RowsFetched != 0 {
		t.Fatalf("reader stats=%+v want no generic row fetches for scoring or result IDs stats=%+v", readerStats, stats)
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

func TestColumnVectorGraphNativeSearchCandidateCarriesNoAdjacencyV3(t *testing.T) {
	candidate := columnVectorGraphSearchCandidate{ordinal: 1, score: 1}
	if got, want := unsafe.Sizeof(candidate), uintptr(16); got != want {
		t.Fatalf("candidate size=%d want compact ordinal+score size=%d", got, want)
	}
}

func TestColumnVectorGraphNativeSearchRejectsBadAdjacencyOrdinalV3(t *testing.T) {
	d := openCollectionCommandWALDB(t, t.TempDir())
	defer func() { _ = d.Close() }()
	ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
	bad := writeUncheckedColumnVectorIndexStateAdjacencyAsset1987(t, d, *ctx.cfg, ctx.def, ctx.identity.Generation, 500, 0, typedcolumn.Uint32List{Rows: 2, Offsets: []uint64{0, 1, 1}, Values: []uint32{2}}, nil)
	ctx.state.Assets[0] = bad
	ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
	publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(ctx.def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err == nil {
		_ = reader.Close()
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=nil want typed adjacency state ordinal validation failure")
	}
	if !errors.Is(err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds) {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want adjacency bounds sentinel", err)
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
	if !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("SearchCosine dim err=%v want dimension failure", err)
	}
	_, _, err = reader.SearchCosine([]float32{0, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1}, &scratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchQueryNormInvalid) {
		t.Fatalf("SearchCosine zero err=%v want norm failure", err)
	}
	if !errors.Is(err, errColumnVectorGraphInvNormNormInvalid) {
		t.Fatalf("SearchCosine zero err=%v want underlying inv_norm failure", err)
	}
	_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: -1, EfSearch: 1}, &scratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchTopKNegative) {
		t.Fatalf("SearchCosine top_k err=%v want negative top_k failure", err)
	}
	_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: -1}, &scratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchEfSearchNegative) {
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
	_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 0, EfSearch: -1}, nil)
	if !errors.Is(err, errColumnVectorGraphNativeSearchEfSearchNegative) {
		t.Fatalf("SearchCosine zero top_k ef_search err=%v want negative ef_search failure", err)
	}

	_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1}, nil)
	if !errors.Is(err, errColumnVectorGraphNativeSearchScratchRequired) {
		t.Fatalf("SearchCosine err=%v want caller-owned scratch failure", err)
	}
}

func TestColumnVectorGraphNativeCosineScoreRejectsMalformedRowV3(t *testing.T) {
	_, err := columnVectorGraphNativeCosineScore([]float32{1, 0}, 1, columnVectorGraphPhysicalRow{
		Ordinal: 7,
		Vector:  []float32{1},
		InvNorm: 1,
	})
	if !errors.Is(err, errColumnVectorGraphNativeSearchCandidateDimensionMismatch) {
		t.Fatalf("columnVectorGraphNativeCosineScore err=%v want dimension failure", err)
	}
}

func TestColumnVectorGraphNativeCosineScoreKeepsFiniteFloat32ZeroV3(t *testing.T) {
	tiny := float32(math.SmallestNonzeroFloat32)
	got, err := columnVectorGraphNativeCosineScore([]float32{tiny}, 1, columnVectorGraphPhysicalRow{
		Ordinal: 7,
		Vector:  []float32{tiny},
		InvNorm: 1,
	})
	if err != nil {
		t.Fatalf("columnVectorGraphNativeCosineScore: %v", err)
	}
	if got != 0 {
		t.Fatalf("columnVectorGraphNativeCosineScore=%g want finite float32 zero without float64 fallback", got)
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
	_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: -1}, &scratch)
	if !errors.Is(err, errColumnVectorGraphNativeSearchEfSearchNegative) {
		t.Fatalf("SearchCosine empty ef_search err=%v want negative ef_search failure", err)
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
	if err := scratch.prepare(64, 3, 2, 64, 64, 2, 0); err != nil {
		t.Fatalf("prepare large: %v", err)
	}
	if len(scratch.visitMarks) != 64 {
		t.Fatalf("large visitMarks len=%d want 64", len(scratch.visitMarks))
	}
	if cap(scratch.frontier) < 64 || cap(scratch.top) < 64 || cap(scratch.results) < 64 || cap(scratch.idBuffers) < 64 || cap(scratch.resultOrder) < 64 || cap(scratch.resultOrdinals) < 64 {
		t.Fatalf("large scratch caps frontier=%d top=%d results=%d idBuffers=%d resultOrder=%d resultOrdinals=%d want at least 64", cap(scratch.frontier), cap(scratch.top), cap(scratch.results), cap(scratch.idBuffers), cap(scratch.resultOrder), cap(scratch.resultOrdinals))
	}
	if err := scratch.prepare(1, 3, 2, 1, 1, 2, 0); err != nil {
		t.Fatalf("prepare small: %v", err)
	}
	if len(scratch.visitMarks) != 1 {
		t.Fatalf("small visitMarks len=%d want 1", len(scratch.visitMarks))
	}
	if cap(scratch.visitMarks) > 1+columnVectorGraphNativeScratchOversizeSlack {
		t.Fatalf("small visitMarks retained oversized cap=%d", cap(scratch.visitMarks))
	}
	if scratch.markVisited(5) {
		t.Fatalf("markVisited accepted stale ordinal outside current row count")
	}
	if cap(scratch.frontier) > 1+columnVectorGraphNativeScratchOversizeSlack ||
		cap(scratch.top) > 1+columnVectorGraphNativeScratchOversizeSlack ||
		cap(scratch.results) > 1+columnVectorGraphNativeScratchOversizeSlack ||
		cap(scratch.idBuffers) > 1+columnVectorGraphNativeScratchOversizeSlack ||
		cap(scratch.resultOrder) > 1+columnVectorGraphNativeScratchOversizeSlack ||
		cap(scratch.resultOrdinals) > 1+columnVectorGraphNativeScratchOversizeSlack {
		t.Fatalf("small scratch retained oversized caps frontier=%d top=%d results=%d idBuffers=%d resultOrder=%d resultOrdinals=%d", cap(scratch.frontier), cap(scratch.top), cap(scratch.results), cap(scratch.idBuffers), cap(scratch.resultOrder), cap(scratch.resultOrdinals))
	}
}

func TestColumnVectorGraphNativeSearchScratchFrontierCapacityPolicyV3(t *testing.T) {
	const (
		rowCount = 4096
		degree   = 16
		topK     = 10
		efSearch = 128
	)
	var scratch columnVectorGraphNativeSearchScratch
	if err := scratch.prepare(rowCount, 32, degree, topK, efSearch, degree, 0); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	wantFrontierCap := columnVectorGraphNativeSearchFrontierCapacity(rowCount, degree, topK, efSearch)
	if wantFrontierCap != efSearch*degree {
		t.Fatalf("frontier policy target=%d want %d", wantFrontierCap, efSearch*degree)
	}
	if cap(scratch.frontier) != wantFrontierCap {
		t.Fatalf("frontier cap=%d want %d", cap(scratch.frontier), wantFrontierCap)
	}
	if cap(scratch.top) != efSearch {
		t.Fatalf("top cap=%d want efSearch cap %d unchanged", cap(scratch.top), efSearch)
	}
	if !scratch.markVisited(7) {
		t.Fatal("initial markVisited failed")
	}
	scratch.frontier = scratch.frontier[:cap(scratch.frontier)]
	frontierBacking := columnVectorGraphCandidateBackingPtrForTest(scratch.frontier)
	visitEpoch := scratch.visitEpoch
	if err := scratch.prepare(rowCount, 32, degree, topK, efSearch, degree, 0); err != nil {
		t.Fatalf("prepare repeated: %v", err)
	}
	if len(scratch.frontier) != 0 {
		t.Fatalf("repeated prepare frontier len=%d want 0", len(scratch.frontier))
	}
	if cap(scratch.frontier) != wantFrontierCap || columnVectorGraphCandidateBackingPtrForTest(scratch.frontier) != frontierBacking {
		t.Fatalf("repeated prepare frontier cap=%d ptr=%#x want cap=%d ptr=%#x", cap(scratch.frontier), columnVectorGraphCandidateBackingPtrForTest(scratch.frontier), wantFrontierCap, frontierBacking)
	}
	if scratch.visitEpoch != visitEpoch+1 {
		t.Fatalf("visitEpoch=%d want %d", scratch.visitEpoch, visitEpoch+1)
	}
	if !scratch.markVisited(7) {
		t.Fatal("markVisited after prepare should see prior epoch as reset")
	}

	scratch.frontier = make([]columnVectorGraphSearchCandidate, 0, rowCount)
	if err := scratch.prepare(8, 32, degree, 1, efSearch, degree, 0); err != nil {
		t.Fatalf("prepare small rowCount: %v", err)
	}
	if cap(scratch.frontier) > 8+columnVectorGraphNativeScratchOversizeSlack {
		t.Fatalf("small rowCount frontier cap=%d want bounded by row count/slack", cap(scratch.frontier))
	}
	if got := columnVectorGraphNativeSearchFrontierCapacity(8, degree, 32, efSearch); got != 8 {
		t.Fatalf("frontier target with topK above rowCount=%d want rowCount cap 8", got)
	}
}

func TestColumnVectorGraphNativeSearchRepeatedSearchRetainsFrontierCapacityV3(t *testing.T) {
	const (
		rows     = 256
		dims     = 16
		m        = 16
		topK     = 10
		efSearch = 16
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

	query := append([]float32(nil), input[17].vector...)
	opts := columnVectorGraphNativeSearchOptions{TopK: topK, EfSearch: efSearch}
	var scratch columnVectorGraphNativeSearchScratch
	first, _, err := reader.SearchCosine(query, opts, &scratch)
	if err != nil {
		t.Fatalf("first SearchCosine: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("first SearchCosine returned no results")
	}
	firstOrdinals := make([]int, len(first))
	firstScores := make([]float64, len(first))
	for i := range first {
		firstOrdinals[i] = first[i].Ordinal
		firstScores[i] = first[i].Score
	}
	wantFrontierCap := columnVectorGraphNativeSearchFrontierCapacity(reader.RowCount(), reader.def.M, topK, efSearch)
	if cap(scratch.frontier) < wantFrontierCap || cap(scratch.frontier) > reader.RowCount() {
		t.Fatalf("frontier cap after first search=%d want [%d,%d]", cap(scratch.frontier), wantFrontierCap, reader.RowCount())
	}
	frontierCap := cap(scratch.frontier)
	frontierBacking := columnVectorGraphCandidateBackingPtrForTest(scratch.frontier)
	visitEpoch := scratch.visitEpoch

	second, _, err := reader.SearchCosine(query, opts, &scratch)
	if err != nil {
		t.Fatalf("second SearchCosine: %v", err)
	}
	if len(second) != len(first) {
		t.Fatalf("second results len=%d want %d", len(second), len(first))
	}
	for i := range firstOrdinals {
		if firstOrdinals[i] != second[i].Ordinal || firstScores[i] != second[i].Score {
			t.Fatalf("result[%d]=%+v want ordinal=%d score=%v", i, second[i], firstOrdinals[i], firstScores[i])
		}
	}
	if cap(scratch.frontier) != frontierCap || columnVectorGraphCandidateBackingPtrForTest(scratch.frontier) != frontierBacking {
		t.Fatalf("frontier reallocated across repeated search: cap=%d ptr=%#x want cap=%d ptr=%#x", cap(scratch.frontier), columnVectorGraphCandidateBackingPtrForTest(scratch.frontier), frontierCap, frontierBacking)
	}
	if scratch.visitEpoch != visitEpoch+1 {
		t.Fatalf("visitEpoch after repeated search=%d want %d", scratch.visitEpoch, visitEpoch+1)
	}
}

func columnVectorGraphCandidateBackingPtrForTest(candidates []columnVectorGraphSearchCandidate) uintptr {
	if cap(candidates) == 0 {
		return 0
	}
	backing := candidates[:cap(candidates)]
	return uintptr(unsafe.Pointer(&backing[0]))
}

func TestColumnVectorGraphNativeScratchCapOversizedOverflowSafeV3(t *testing.T) {
	if !columnVectorGraphNativeScratchCapOversized(64, 1) {
		t.Fatalf("expected oversized scratch cap for small target")
	}
	if columnVectorGraphNativeScratchCapOversized(math.MaxInt, math.MaxInt/2+1) {
		t.Fatalf("overflow-sized target must not wrap oversized threshold")
	}
}

func TestColumnVectorGraphNativeSearchScratchClearsResultAliasesV3(t *testing.T) {
	var scratch columnVectorGraphNativeSearchScratch
	oldID := []byte("old-doc")
	scratch.results = append(scratch.results,
		columnVectorGraphNativeSearchResult{Ordinal: 7, ID: oldID, Score: 1},
		columnVectorGraphNativeSearchResult{Ordinal: 8, ID: oldID, Score: 0.5},
	)
	oldResults := scratch.results
	if err := scratch.prepare(1, 3, 2, 1, 1, 2, 0); err != nil {
		t.Fatalf("prepare small: %v", err)
	}
	if len(scratch.results) != 0 {
		t.Fatalf("prepared results len=%d want zero", len(scratch.results))
	}
	for i, result := range oldResults {
		if result.ID != nil {
			t.Fatalf("old result[%d] retained ID alias %q", i, string(result.ID))
		}
	}
	scratch.results = append(scratch.results,
		columnVectorGraphNativeSearchResult{Ordinal: 9, ID: oldID, Score: 1},
		columnVectorGraphNativeSearchResult{Ordinal: 10, ID: oldID, Score: 0.5},
	)
	oldResults = scratch.results
	if err := scratch.prepare(2, 3, 2, 2, 2, 2, 0); err != nil {
		t.Fatalf("prepare same target: %v", err)
	}
	if len(scratch.results) != 0 {
		t.Fatalf("prepared same-target results len=%d want zero", len(scratch.results))
	}
	for i, result := range oldResults {
		if result.ID != nil {
			t.Fatalf("same-target old result[%d] retained ID alias %q", i, string(result.ID))
		}
	}
	scratch.results = make([]columnVectorGraphNativeSearchResult, 2, 64)
	scratch.results[0].ID = oldID
	scratch.results[1].ID = oldID
	oldResults = scratch.results
	if err := scratch.prepare(1, 3, 2, 1, 1, 2, 0); err != nil {
		t.Fatalf("prepare oversized: %v", err)
	}
	if cap(scratch.results) > 1+columnVectorGraphNativeScratchOversizeSlack {
		t.Fatalf("oversized prepared results cap=%d want bounded", cap(scratch.results))
	}
	for i, result := range oldResults {
		if result.ID != nil {
			t.Fatalf("oversized old result[%d] retained ID alias %q", i, string(result.ID))
		}
	}
}

func TestColumnVectorGraphNativeSearchScratchClearsRowValueAliasesV3(t *testing.T) {
	var scratch columnVectorGraphNativeSearchScratch
	staleBytes := []byte("stale-bytes")
	staleVector := []float32{1, 2, 3}
	staleAdjacency := []uint32{4, 5}
	scratch.scoreScratch.Values = make([]columnDeclaredValue, 1, columnVectorGraphPhysicalRowValueCount)
	scratch.scoreScratch.Values[0] = columnDeclaredValue{
		Type:          ColumnStoreValueString,
		Present:       true,
		String:        "stale-string",
		StringBytes:   staleBytes,
		Float32Vector: staleVector,
		AdjacencyList: staleAdjacency,
	}
	oldValues := scratch.scoreScratch.Values
	if err := scratch.prepare(1, 3, 2, 1, 1, 2, 0); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if len(scratch.scoreScratch.Values) != 0 {
		t.Fatalf("prepared row values len=%d want zero", len(scratch.scoreScratch.Values))
	}
	for i, value := range oldValues {
		if value.String != "" || value.StringBytes != nil || value.Float32Vector != nil || value.AdjacencyList != nil {
			t.Fatalf("old row value[%d] retained aliases: string=%q bytes=%v vector=%v adjacency=%v", i, value.String, value.StringBytes, value.Float32Vector, value.AdjacencyList)
		}
	}
}

func TestColumnVectorGraphNativeSearchScratchClearsIDBufferGrowthAliasesV3(t *testing.T) {
	stale := []byte("stale-doc")
	dst := make([][]byte, 1, 4)
	expanded := dst[:4]
	expanded[2] = stale
	dst = expanded[:1]

	dst = resizeColumnVectorGraphNativeIDBuffersScratch(dst, 3)
	if len(dst) != 3 {
		t.Fatalf("id buffers len=%d want 3", len(dst))
	}
	for i := 1; i < len(dst); i++ {
		if dst[i] != nil {
			t.Fatalf("newly exposed id buffer[%d] retained alias %q", i, string(dst[i]))
		}
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
	if collectionsRaceEnabled {
		t.Skip("AllocsPerRun is not stable under -race")
	}
	if !enterIsolatedVectorAllocationGate(t, "native-search-warm-scratch") {
		return
	}
	var hotErr error
	allocs := testing.AllocsPerRun(1000, func() {
		if hotErr != nil {
			return
		}
		got, _, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 16}, &scratch)
		if err != nil {
			hotErr = err
			return
		}
		if len(got) == 0 {
			hotErr = errors.New("SearchCosine returned no results")
			return
		}
		columnPhysicalScanBenchSum += int64(got[0].Ordinal)
	})
	if hotErr != nil {
		t.Fatalf("SearchCosine: %v", hotErr)
	}
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
	if workers < 2 {
		workers = 2
	}
	if workers > columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 {
		workers = columnVectorGraphNativeSearchParallelBenchMaxWorkersV3
	}
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
	benchmarkColumnVectorGraphNativeSearchCosineV3(b, columnVectorGraphNativeSearchSmallBenchShapeV3())
}

func BenchmarkColumnVectorGraphNativeSearchCosineProduction8192V3(b *testing.B) {
	benchmarkColumnVectorGraphNativeSearchCosineV3(b, columnVectorGraphNativeSearchProduction8192BenchShapeV3())
}

func BenchmarkColumnVectorGraphNativeSearchCosineProductionSweepV3(b *testing.B) {
	for _, shape := range columnVectorGraphNativeSearchProductionSweepBenchShapesV3() {
		shape := shape
		b.Run(columnVectorGraphNativeSearchBenchShapeNameV3(shape), func(b *testing.B) {
			benchmarkColumnVectorGraphNativeSearchCosineV3(b, shape)
		})
	}
}

func BenchmarkColumnVectorGraphNativeSearchCosineRebuildProduction8192V3(b *testing.B) {
	shape := columnVectorGraphNativeSearchProduction8192BenchShapeV3()
	shape.directPhysicalAsset = false
	benchmarkColumnVectorGraphNativeSearchCosineV3(b, shape)
}

func BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3(b *testing.B) {
	shape := columnVectorGraphNativeSearchSmallBenchShapeV3()
	shape.typedColumnVector = true
	benchmarkColumnVectorGraphNativeSearchCosineV3(b, shape)
}

func BenchmarkVectorSearchCoreGraphSerialTypedColumn1961(b *testing.B) {
	BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3(b)
}

func BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnProduction8192V3(b *testing.B) {
	shape := columnVectorGraphNativeSearchProduction8192BenchShapeV3()
	shape.typedColumnVector = true
	benchmarkColumnVectorGraphNativeSearchCosineV3(b, shape)
}

func BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnEFScale4136(b *testing.B) {
	for _, efSearch := range []int{32, 64, 128, 256} {
		shape := columnVectorGraphNativeSearchProduction8192BenchShapeV3()
		shape.typedColumnVector = true
		shape.efSearch = efSearch
		b.Run(fmt.Sprintf("ef=%d", efSearch), func(b *testing.B) {
			benchmarkColumnVectorGraphNativeSearchCosineV3(b, shape)
		})
	}
}

func BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnStatsMode2042(b *testing.B) {
	for _, tc := range []struct {
		name string
		mode columnVectorGraphNativeSearchStatsMode
	}{
		{name: "full_diagnostics", mode: columnVectorGraphNativeSearchStatsModeFullDiagnostics},
		{name: "minimal", mode: columnVectorGraphNativeSearchStatsModeMinimal},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			shape := columnVectorGraphNativeSearchProduction8192BenchShapeV3()
			shape.typedColumnVector = true
			shape.omitResultMaterialization = true
			shape.statsMode = tc.mode
			benchmarkColumnVectorGraphNativeSearchCosineV3(b, shape)
		})
	}
}

func TestColumnVectorGraphAdjacencyAccessApplesToApples2043(t *testing.T) {
	if !columnVectorGraphLayer0AdjacencyMmapExpectedOnPlatform1919() {
		t.Skip("adjacency access apples-to-apples evidence requires mmap_direct prepared CSR support")
	}
	closeFn, reader, ordinals := openColumnVectorGraphAdjacencyAccessFixture2043(t)
	defer closeFn()
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		t.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	var preparedStats, graphRowStats columnVectorGraphAdjacencyAccessMicroStats2043
	preparedSum, err := columnVectorGraphPreparedCSRAdjacencyAccessOnce2043(reader, ordinals, &preparedStats)
	if err != nil {
		t.Fatalf("prepared CSR adjacency access: %v", err)
	}
	graphRowSum, err := columnVectorGraphGraphRowAdjacencyAccessOnce2043(reader, plan, ordinals, &scratch, &graphRowStats)
	if err != nil {
		t.Fatalf("graph-row adjacency access: %v", err)
	}
	if preparedStats.Expansions != graphRowStats.Expansions || preparedStats.Edges != graphRowStats.Edges || preparedStats.AdjacencyBytesRead != graphRowStats.AdjacencyBytesRead || preparedSum != graphRowSum {
		t.Fatalf("prepared stats=%+v checksum=%d graph-row stats=%+v checksum=%d want identical topology/ordinals/edges", preparedStats, preparedSum, graphRowStats, graphRowSum)
	}
	if preparedStats.Expansions != uint64(len(ordinals)) || preparedStats.Edges == 0 {
		t.Fatalf("prepared stats=%+v ordinals=%d want non-zero fixed expansion set", preparedStats, len(ordinals))
	}
	if preparedStats.PreparedCSRMmapDirectViews != preparedStats.Expansions || preparedStats.ScratchDecodes != 0 || preparedStats.LegacyGraphRowDecodes != 0 {
		t.Fatalf("prepared stats=%+v want prepared CSR mmap/direct only", preparedStats)
	}
	if graphRowStats.PreparedCSRMmapDirectViews != 0 || graphRowStats.LegacyGraphRowDecodes != graphRowStats.Expansions || graphRowStats.ScratchDecodes != graphRowStats.Expansions {
		t.Fatalf("graph-row stats=%+v want graph-row compatibility scratch decodes only", graphRowStats)
	}
}

func BenchmarkColumnVectorGraphAdjacencyAccessApplesToApples2043(b *testing.B) {
	if !columnVectorGraphLayer0AdjacencyMmapExpectedOnPlatform1919() {
		b.Skip("adjacency access apples-to-apples evidence requires mmap_direct prepared CSR support")
	}
	for _, tc := range []struct {
		name  string
		graph bool
	}{
		{name: "state_prepared_csr_adjacency"},
		{name: "graph_row_adjacency_decode", graph: true},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkColumnVectorGraphAdjacencyAccessApplesToApples2043(b, tc.graph)
		})
	}
}

type columnVectorGraphAdjacencyAccessMicroStats2043 struct {
	Expansions                 uint64
	Edges                      uint64
	AdjacencyBytesRead         uint64
	PreparedCSRDirectViews     uint64
	PreparedCSRMmapDirectViews uint64
	LegacyGraphRowDecodes      uint64
	ScratchDecodes             uint64
	DirectViews                uint64
	SourceFallbacks            uint64
}

func benchmarkColumnVectorGraphAdjacencyAccessApplesToApples2043(b *testing.B, graphRow bool) {
	b.Helper()
	closeFn, reader, ordinals := openColumnVectorGraphAdjacencyAccessFixture2043(b)
	defer closeFn()
	plan, err := newColumnVectorGraphSearchPlan(reader)
	if err != nil {
		b.Fatalf("newColumnVectorGraphSearchPlan: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	var warmStats columnVectorGraphAdjacencyAccessMicroStats2043
	if graphRow {
		if _, err := columnVectorGraphGraphRowAdjacencyAccessOnce2043(reader, plan, ordinals, &scratch, &warmStats); err != nil {
			b.Fatalf("warm graph-row adjacency access: %v", err)
		}
	} else if _, err := columnVectorGraphPreparedCSRAdjacencyAccessOnce2043(reader, ordinals, &warmStats); err != nil {
		b.Fatalf("warm prepared CSR adjacency access: %v", err)
	}
	assertColumnVectorGraphAdjacencyAccessMicroStats2043(b, warmStats, uint64(len(ordinals)), graphRow)

	var stats columnVectorGraphAdjacencyAccessMicroStats2043
	var checksum uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum uint64
		var err error
		if graphRow {
			sum, err = columnVectorGraphGraphRowAdjacencyAccessOnce2043(reader, plan, ordinals, &scratch, &stats)
		} else {
			sum, err = columnVectorGraphPreparedCSRAdjacencyAccessOnce2043(reader, ordinals, &stats)
		}
		if err != nil {
			b.Fatalf("adjacency access: %v", err)
		}
		checksum += sum
	}
	b.StopTimer()
	columnPhysicalScanBenchSum += int64(checksum & uint64(math.MaxInt64))
	assertColumnVectorGraphAdjacencyAccessMicroStats2043(b, stats, uint64(len(ordinals))*uint64(max(b.N, 1)), graphRow)
	reportColumnVectorGraphAdjacencyAccessMicroMetrics2043(b, b.N, len(ordinals), stats, graphRow)
}

func openColumnVectorGraphAdjacencyAccessFixture2043(tb testing.TB) (func(), *columnVectorGraphPhysicalRowReader, []int) {
	tb.Helper()
	shape := columnVectorGraphNativeSearchProduction8192BenchShapeV3()
	shape.omitResultMaterialization = true
	closeFixture, col, def, _ := openColumnVectorGraphNativeSearchBenchFixtureV3(tb, shape)
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		closeFixture()
		tb.Fatalf("open reader: %v", err)
	}
	closeFn := func() {
		_ = reader.Close()
		closeFixture()
	}
	if err := validateColumnVectorGraphPreparedSearchAdjacency(reader.adjacencyLayerSources, reader.RowCount()); err != nil {
		closeFn()
		tb.Fatalf("prepared adjacency source: %v", err)
	}
	ordinals := columnVectorGraphAdjacencyAccessOrdinals2043(reader.RowCount())
	if len(ordinals) == 0 {
		closeFn()
		tb.Fatalf("no adjacency access ordinals for rows=%d", reader.RowCount())
	}
	return closeFn, reader, ordinals
}

func columnVectorGraphAdjacencyAccessOrdinals2043(rows int) []int {
	if rows <= 0 {
		return nil
	}
	count := 128
	if rows < count {
		count = rows
	}
	ordinals := make([]int, count)
	for i := range ordinals {
		ordinals[i] = (rows/2 + i*61) % rows
	}
	return ordinals
}

func columnVectorGraphPreparedCSRAdjacencyAccessOnce2043(reader *columnVectorGraphPhysicalRowReader, ordinals []int, stats *columnVectorGraphAdjacencyAccessMicroStats2043) (uint64, error) {
	if reader == nil || reader.adjacencyLayerSources == nil {
		return 0, fmt.Errorf("prepared CSR adjacency source unavailable")
	}
	var checksum uint64
	for _, ordinal := range ordinals {
		neighbors, outcome, reason, ok := reader.adjacencyLayerSources.Neighbors(0, ordinal)
		if !ok || reason != "" {
			return 0, fmt.Errorf("prepared CSR adjacency ordinal=%d ok=%t reason=%s", ordinal, ok, reason)
		}
		if outcome != columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect {
			return 0, fmt.Errorf("prepared CSR adjacency ordinal=%d outcome=%s want prepared_csr_mmap_direct", ordinal, outcome)
		}
		if stats != nil {
			stats.Expansions++
			stats.Edges += uint64(len(neighbors))
			stats.AdjacencyBytesRead += uint64(len(neighbors)) * 4
			stats.PreparedCSRDirectViews++
			stats.PreparedCSRMmapDirectViews++
			stats.DirectViews++
		}
		checksum = columnVectorGraphAdjacencyAccessChecksum2043(checksum, ordinal, neighbors)
	}
	return checksum, nil
}

func columnVectorGraphGraphRowAdjacencyAccessOnce2043(reader *columnVectorGraphPhysicalRowReader, plan *columnVectorGraphSearchPlan, ordinals []int, scratch *columnVectorGraphNativeSearchScratch, stats *columnVectorGraphAdjacencyAccessMicroStats2043) (uint64, error) {
	if reader == nil || plan == nil || plan.physicalReader == nil {
		return 0, fmt.Errorf("graph-row adjacency source unavailable")
	}
	if scratch == nil {
		return 0, errColumnVectorGraphNativeSearchScratchRequired
	}
	var checksum uint64
	for _, ordinal := range ordinals {
		adjacency, direct, err := reader.rawCandidateAdjacencyWithDirectView(plan, nil, ordinal, scratch)
		if err != nil {
			return 0, err
		}
		neighbors, err := columnVectorGraphAdjacencyLayer(adjacency, 0)
		if err != nil {
			return 0, fmt.Errorf("graph-row adjacency ordinal=%d layer 0: %w", ordinal, err)
		}
		if stats != nil {
			stats.Expansions++
			stats.Edges += uint64(len(neighbors))
			stats.AdjacencyBytesRead += uint64(len(adjacency)) * 4
			stats.LegacyGraphRowDecodes++
			if direct {
				stats.DirectViews++
			} else if len(adjacency) > 0 {
				stats.ScratchDecodes++
			}
		}
		checksum = columnVectorGraphAdjacencyAccessChecksum2043(checksum, ordinal, neighbors)
	}
	return checksum, nil
}

func columnVectorGraphAdjacencyAccessChecksum2043(seed uint64, ordinal int, neighbors []uint32) uint64 {
	sum := seed + uint64(ordinal+1)*1315423911 + uint64(len(neighbors))*2654435761
	for i, neighbor := range neighbors {
		sum ^= (uint64(neighbor) + 1) * uint64(i+1) * 1099511628211
	}
	return sum
}

func assertColumnVectorGraphAdjacencyAccessMicroStats2043(tb testing.TB, stats columnVectorGraphAdjacencyAccessMicroStats2043, wantExpansions uint64, graphRow bool) {
	tb.Helper()
	if stats.Expansions != wantExpansions || stats.Edges == 0 || stats.AdjacencyBytesRead == 0 {
		tb.Fatalf("adjacency access stats=%+v want expansions=%d and non-zero edges/bytes", stats, wantExpansions)
	}
	if graphRow {
		if stats.PreparedCSRMmapDirectViews != 0 || stats.LegacyGraphRowDecodes != stats.Expansions || stats.ScratchDecodes != stats.Expansions || stats.SourceFallbacks != 0 {
			tb.Fatalf("graph-row adjacency stats=%+v want scratch-decoded graph-row access only", stats)
		}
		return
	}
	if stats.PreparedCSRMmapDirectViews != stats.Expansions || stats.PreparedCSRDirectViews != stats.Expansions || stats.DirectViews != stats.Expansions || stats.LegacyGraphRowDecodes != 0 || stats.ScratchDecodes != 0 || stats.SourceFallbacks != 0 {
		tb.Fatalf("prepared CSR adjacency stats=%+v want prepared CSR mmap/direct access only", stats)
	}
}

func reportColumnVectorGraphAdjacencyAccessMicroMetrics2043(b *testing.B, n int, ordinalCount int, stats columnVectorGraphAdjacencyAccessMicroStats2043, graphRow bool) {
	b.Helper()
	if n <= 0 {
		return
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(n)/elapsed.Seconds(), "ops/sec")
	}
	b.ReportMetric(2043, "adjacency_micro_issue")
	if graphRow {
		b.ReportMetric(1, "adjacency_source_graph_row_decode")
	} else {
		b.ReportMetric(1, "adjacency_source_state_prepared_csr")
	}
	b.ReportMetric(float64(ordinalCount), "ordinals/op")
	b.ReportMetric(float64(stats.Expansions)/float64(n), "adjacency_expansions/op")
	b.ReportMetric(float64(stats.Edges)/float64(n), "edges/op")
	if stats.Expansions > 0 {
		b.ReportMetric(float64(stats.Edges)/float64(stats.Expansions), "edges/expansion")
	}
	b.ReportMetric(float64(stats.AdjacencyBytesRead)/float64(n), "adjacency_B/op")
	b.ReportMetric(float64(stats.PreparedCSRDirectViews)/float64(n), "adjacency_prepared_csr_direct_views/op")
	b.ReportMetric(float64(stats.PreparedCSRMmapDirectViews)/float64(n), "adjacency_prepared_csr_mmap_direct/op")
	b.ReportMetric(float64(stats.LegacyGraphRowDecodes)/float64(n), "adjacency_legacy_graph_row_decodes/op")
	b.ReportMetric(float64(stats.ScratchDecodes)/float64(n), "adjacency_scratch_decodes/op")
	b.ReportMetric(float64(stats.DirectViews)/float64(n), "adjacency_direct_views/op")
	b.ReportMetric(float64(stats.SourceFallbacks)/float64(n), "adjacency_source_fallbacks/op")
}

func BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnMinimalStats2042(b *testing.B) {
	shape := columnVectorGraphNativeSearchProduction8192BenchShapeV3()
	shape.typedColumnVector = true
	shape.omitResultMaterialization = true
	shape.statsMode = columnVectorGraphNativeSearchStatsModeMinimal
	benchmarkColumnVectorGraphNativeSearchCosineParallelV3(b, shape)
}

func BenchmarkColumnVectorGraphNativeSearchCosineIndexedScoring1969(b *testing.B) {
	for _, tc := range []struct {
		name string
		mode columnVectorGraphScoreBatchMode
	}{
		{name: "scalar", mode: columnVectorGraphScoreBatchModeScalar},
		{name: "indexed", mode: columnVectorGraphScoreBatchModeIndexed},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			shape := columnVectorGraphNativeSearchSmallBenchShapeV3()
			shape.typedColumnVector = true
			shape.scoreBatchMode = tc.mode
			benchmarkColumnVectorGraphNativeSearchCosineV3(b, shape)
		})
	}
}

func benchmarkColumnVectorGraphNativeSearchCosineV3(b *testing.B, shape columnVectorGraphNativeSearchBenchShapeV3) {
	b.Helper()
	closeFn, col, def, query := openColumnVectorGraphNativeSearchBenchFixtureV3(b, shape)
	defer closeFn()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		b.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer reader.Close()
	var scratch columnVectorGraphNativeSearchScratch
	opts := columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch, ScoreBatchMode: shape.scoreBatchMode, StatsMode: shape.statsMode, OmitResultMaterialization: shape.omitResultMaterialization}
	if _, _, err := reader.SearchCosine(query, opts, &scratch); err != nil {
		b.Fatalf("warm SearchCosine: %v", err)
	}
	baseStats := reader.Stats()
	var searchStats columnVectorGraphNativeSearchStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, stats, err := reader.SearchCosine(query, opts, &scratch)
		if err != nil {
			b.Fatalf("SearchCosine: %v", err)
		}
		if len(got) == 0 {
			b.Fatalf("SearchCosine returned no results")
		}
		columnPhysicalScanBenchSum += int64(got[0].Ordinal)
		searchStats.CandidateRows += stats.CandidateRows
		searchStats.Candidates += stats.Candidates
		searchStats.Edges += stats.Edges
		searchStats.VisitedNodes += stats.VisitedNodes
		searchStats.VisitedEdges += stats.VisitedEdges
		searchStats.VectorBytesRead += stats.VectorBytesRead
		searchStats.NormBytesRead += stats.NormBytesRead
		searchStats.AdjacencyBytesRead += stats.AdjacencyBytesRead
		searchStats.CandidateFetches += stats.CandidateFetches
		searchStats.ExpansionFetches += stats.ExpansionFetches
		searchStats.ResultFetches += stats.ResultFetches
		searchStats.ScoreBatches += stats.ScoreBatches
		searchStats.OrdinalsGrouped += stats.OrdinalsGrouped
		searchStats.ScoreBatchCalls += stats.ScoreBatchCalls
		searchStats.ScoreBatchCandidates += stats.ScoreBatchCandidates
		if stats.ScoreBatchMaxTileSize > searchStats.ScoreBatchMaxTileSize {
			searchStats.ScoreBatchMaxTileSize = stats.ScoreBatchMaxTileSize
		}
		searchStats.ScoreBatchOptimizedCalls += stats.ScoreBatchOptimizedCalls
		searchStats.ScoreBatchScalarFallbackCalls += stats.ScoreBatchScalarFallbackCalls
		searchStats.PreparedScoreCalls += stats.PreparedScoreCalls
		searchStats.ScoreFloat64Fallbacks += stats.ScoreFloat64Fallbacks
		searchStats.BlockViewHits += stats.BlockViewHits
		searchStats.BlockViewMisses += stats.BlockViewMisses
		searchStats.BlockViewBuilds += stats.BlockViewBuilds
		searchStats.AdjacencyExpansions += stats.AdjacencyExpansions
		searchStats.AdjacencyScratchDecodes += stats.AdjacencyScratchDecodes
		searchStats.AdjacencyDirectViews += stats.AdjacencyDirectViews
		searchStats.AdjacencyMmapDirectViews += stats.AdjacencyMmapDirectViews
		searchStats.AdjacencyHeapCopyTypedViews += stats.AdjacencyHeapCopyTypedViews
		searchStats.AdjacencyPreparedCSRDirectViews += stats.AdjacencyPreparedCSRDirectViews
		searchStats.AdjacencyPreparedCSRMmapDirectViews += stats.AdjacencyPreparedCSRMmapDirectViews
		searchStats.AdjacencyTypedListDirectViews += stats.AdjacencyTypedListDirectViews
		searchStats.AdjacencyTypedListMmapDirectViews += stats.AdjacencyTypedListMmapDirectViews
		searchStats.AdjacencyTypedListHeapCopyTypedViews += stats.AdjacencyTypedListHeapCopyTypedViews
		searchStats.AdjacencyTypedListScratchDecodes += stats.AdjacencyTypedListScratchDecodes
		searchStats.AdjacencyLegacyFallbacks += stats.AdjacencyLegacyFallbacks
		searchStats.AdjacencySourceUnavailable += stats.AdjacencySourceUnavailable
		searchStats.AdjacencySourceFallbacks += stats.AdjacencySourceFallbacks
		searchStats.AdjacencyCertificationFailures += stats.AdjacencyCertificationFailures
		searchStats.AdjacencyValidationFailures += stats.AdjacencyValidationFailures
		searchStats.AdjacencyAbsoluteOffsetUnaligned += stats.AdjacencyAbsoluteOffsetUnaligned
		searchStats.AdjacencyActualPointerUnaligned += stats.AdjacencyActualPointerUnaligned
		searchStats.AdjacencyStaleHandles += stats.AdjacencyStaleHandles
		searchStats.NormDirectViews += stats.NormDirectViews
		searchStats.NormMmapDirectViews += stats.NormMmapDirectViews
		searchStats.NormHeapCopyTypedViews += stats.NormHeapCopyTypedViews
		searchStats.NormScratchDecodes += stats.NormScratchDecodes
		searchStats.NormPreparedDirectViews += stats.NormPreparedDirectViews
		searchStats.NormSourceUnavailable += stats.NormSourceUnavailable
		searchStats.NormSourceFallbacks += stats.NormSourceFallbacks
		searchStats.NormValidationFailures += stats.NormValidationFailures
		searchStats.NormAbsoluteOffsetUnaligned += stats.NormAbsoluteOffsetUnaligned
		searchStats.NormActualPointerUnaligned += stats.NormActualPointerUnaligned
		searchStats.NormStaleHandles += stats.NormStaleHandles
		searchStats.NormMappedBytes = stats.NormMappedBytes
		searchStats.NormHeapCopyBytes = stats.NormHeapCopyBytes
		searchStats.NormDecodedBytes = stats.NormDecodedBytes
		searchStats.NormActiveHandles = stats.NormActiveHandles
		searchStats.NormDeniedResources = stats.NormDeniedResources
		searchStats.VectorDirectViews += stats.VectorDirectViews
		searchStats.VectorMmapDirectViews += stats.VectorMmapDirectViews
		searchStats.VectorHeapCopyTypedViews += stats.VectorHeapCopyTypedViews
		searchStats.VectorScratchDecodes += stats.VectorScratchDecodes
		searchStats.VectorPreparedDirectViews += stats.VectorPreparedDirectViews
		searchStats.VectorPreparedIdentityMappings += stats.VectorPreparedIdentityMappings
		searchStats.VectorPreparedRowRefMappings += stats.VectorPreparedRowRefMappings
		searchStats.VectorCertificationFailures += stats.VectorCertificationFailures
		searchStats.VectorAbsoluteOffsetUnaligned += stats.VectorAbsoluteOffsetUnaligned
		searchStats.VectorActualPointerUnaligned += stats.VectorActualPointerUnaligned
		searchStats.VectorStaleHandles += stats.VectorStaleHandles
		searchStats.TypedColumnMappedBytes = stats.TypedColumnMappedBytes
		searchStats.TypedColumnHeapCopyBytes = stats.TypedColumnHeapCopyBytes
		searchStats.TypedColumnDecodedBytes = stats.TypedColumnDecodedBytes
		searchStats.TypedColumnActiveHandles = stats.TypedColumnActiveHandles
		searchStats.TypedColumnDeniedResources = stats.TypedColumnDeniedResources
		searchStats.TypedColumnFallbacks += stats.TypedColumnFallbacks
		searchStats.RowRefVectorSourceState += stats.RowRefVectorSourceState
		searchStats.RowRefVectorSourceLegacyGraphIDs += stats.RowRefVectorSourceLegacyGraphIDs
		searchStats.RowRefStatePreparedViews += stats.RowRefStatePreparedViews
		searchStats.RowRefStateMmapDirectFields += stats.RowRefStateMmapDirectFields
		searchStats.RowRefStateResultRefs += stats.RowRefStateResultRefs
		searchStats.RowRefStateSourceUnavailable += stats.RowRefStateSourceUnavailable
		searchStats.RowRefStateSourceFallbacks += stats.RowRefStateSourceFallbacks
		searchStats.ResultIDPreparedBytesViews += stats.ResultIDPreparedBytesViews
		searchStats.ResultIDTypedBytesState += stats.ResultIDTypedBytesState
		searchStats.ResultIDGraphFallbacks += stats.ResultIDGraphFallbacks
		searchStats.ResultIDStateValidationFailures += stats.ResultIDStateValidationFailures
		searchStats.PreparedGraphSearchViews += stats.PreparedGraphSearchViews
		searchStats.GraphRowFallbacks += stats.GraphRowFallbacks
		searchStats.WavefrontSearches += stats.WavefrontSearches
		if stats.WavefrontWidth > searchStats.WavefrontWidth {
			searchStats.WavefrontWidth = stats.WavefrontWidth
		}
		searchStats.WavefrontRounds += stats.WavefrontRounds
		searchStats.WavefrontCandidatePops += stats.WavefrontCandidatePops
		searchStats.WavefrontStagedNeighbors += stats.WavefrontStagedNeighbors
		if stats.WavefrontMaxTileSize > searchStats.WavefrontMaxTileSize {
			searchStats.WavefrontMaxTileSize = stats.WavefrontMaxTileSize
		}
		addColumnVectorGraphNativeSearchDebugStats1979(&searchStats, stats)
	}
	b.StopTimer()
	stats := reader.Stats()
	reportColumnGraphNativeSearchBenchShapeMetricsV3(b, shape)
	reportColumnGraphNativeSearchBenchMetricsV3(b, b.N, baseStats, stats, searchStats)
}

func BenchmarkColumnVectorGraphNativeSearchCosineParallelV3(b *testing.B) {
	benchmarkColumnVectorGraphNativeSearchCosineParallelV3(b, columnVectorGraphNativeSearchSmallBenchShapeV3())
}

func BenchmarkColumnVectorGraphNativeSearchCosineParallelProduction8192V3(b *testing.B) {
	benchmarkColumnVectorGraphNativeSearchCosineParallelV3(b, columnVectorGraphNativeSearchProduction8192BenchShapeV3())
}

func BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnV3(b *testing.B) {
	shape := columnVectorGraphNativeSearchSmallBenchShapeV3()
	shape.typedColumnVector = true
	benchmarkColumnVectorGraphNativeSearchCosineParallelV3(b, shape)
}

func BenchmarkVectorSearchCoreGraphParallelTypedColumn1961(b *testing.B) {
	BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnV3(b)
}

func BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnProduction8192V3(b *testing.B) {
	shape := columnVectorGraphNativeSearchProduction8192BenchShapeV3()
	shape.typedColumnVector = true
	benchmarkColumnVectorGraphNativeSearchCosineParallelV3(b, shape)
}

func BenchmarkColumnVectorGraphNativeSearchCosineParallelProductionSweepV3(b *testing.B) {
	for _, shape := range columnVectorGraphNativeSearchProductionSweepBenchShapesV3() {
		shape := shape
		b.Run(columnVectorGraphNativeSearchBenchShapeNameV3(shape), func(b *testing.B) {
			benchmarkColumnVectorGraphNativeSearchCosineParallelV3(b, shape)
		})
	}
}

func benchmarkColumnVectorGraphNativeSearchCosineParallelV3(b *testing.B, shape columnVectorGraphNativeSearchBenchShapeV3) {
	b.Helper()
	closeFn, col, def, query := openColumnVectorGraphNativeSearchBenchFixtureV3(b, shape)
	defer closeFn()
	type searchWorker struct {
		reader  *columnVectorGraphPhysicalRowReader
		scratch columnVectorGraphNativeSearchScratch
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 {
		workers = columnVectorGraphNativeSearchParallelBenchMaxWorkersV3
	}
	previousGOMAXPROCS := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(previousGOMAXPROCS)
	benchWorkers := make([]*searchWorker, workers)
	opts := columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch, ScoreBatchMode: shape.scoreBatchMode, StatsMode: shape.statsMode, OmitResultMaterialization: shape.omitResultMaterialization}
	for i := range benchWorkers {
		reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("open reader %d: %v", i, err)
		}
		defer reader.Close()
		worker := &searchWorker{reader: reader}
		if _, _, err := reader.SearchCosine(query, opts, &worker.scratch); err != nil {
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
	var totalCandidateRows atomic.Uint64
	var totalCandidates atomic.Uint64
	var totalEdges atomic.Uint64
	var totalVisitedNodes atomic.Uint64
	var totalVisitedEdges atomic.Uint64
	var totalVectorBytesRead atomic.Uint64
	var totalNormBytesRead atomic.Uint64
	var totalAdjacencyBytesRead atomic.Uint64
	var totalCandidateFetches atomic.Uint64
	var totalExpansionFetches atomic.Uint64
	var totalResultFetches atomic.Uint64
	var totalScoreBatches atomic.Uint64
	var totalOrdinalsGrouped atomic.Uint64
	var totalScoreBatchCalls atomic.Uint64
	var totalScoreBatchCandidates atomic.Uint64
	var totalScoreBatchMaxTileSize atomic.Uint64
	var totalScoreBatchOptimizedCalls atomic.Uint64
	var totalScoreBatchScalarFallbackCalls atomic.Uint64
	var totalPreparedScoreCalls atomic.Uint64
	var totalScoreFloat64Fallbacks atomic.Uint64
	var totalBlockViewHits atomic.Uint64
	var totalBlockViewMisses atomic.Uint64
	var totalBlockViewBuilds atomic.Uint64
	var totalAdjacencyExpansions atomic.Uint64
	var totalAdjacencyScratchDecodes atomic.Uint64
	var totalAdjacencyDirectViews atomic.Uint64
	var totalAdjacencyMmapDirectViews atomic.Uint64
	var totalAdjacencyHeapCopyTypedViews atomic.Uint64
	var totalAdjacencyPreparedCSRDirectViews atomic.Uint64
	var totalAdjacencyPreparedCSRMmapDirectViews atomic.Uint64
	var totalAdjacencyTypedListDirectViews atomic.Uint64
	var totalAdjacencyTypedListMmapDirectViews atomic.Uint64
	var totalAdjacencyTypedListHeapCopyTypedViews atomic.Uint64
	var totalAdjacencyTypedListScratchDecodes atomic.Uint64
	var totalAdjacencyLegacyFallbacks atomic.Uint64
	var totalAdjacencySourceUnavailable atomic.Uint64
	var totalAdjacencySourceFallbacks atomic.Uint64
	var totalAdjacencyCertificationFailures atomic.Uint64
	var totalAdjacencyValidationFailures atomic.Uint64
	var totalAdjacencyAbsoluteOffsetUnaligned atomic.Uint64
	var totalAdjacencyActualPointerUnaligned atomic.Uint64
	var totalAdjacencyStaleHandles atomic.Uint64
	var totalNormDirectViews atomic.Uint64
	var totalNormMmapDirectViews atomic.Uint64
	var totalNormHeapCopyTypedViews atomic.Uint64
	var totalNormScratchDecodes atomic.Uint64
	var totalNormPreparedDirectViews atomic.Uint64
	var totalNormSourceUnavailable atomic.Uint64
	var totalNormSourceFallbacks atomic.Uint64
	var totalNormValidationFailures atomic.Uint64
	var totalNormAbsoluteOffsetUnaligned atomic.Uint64
	var totalNormActualPointerUnaligned atomic.Uint64
	var totalNormStaleHandles atomic.Uint64
	var totalNormMappedBytes atomic.Uint64
	var totalNormHeapCopyBytes atomic.Uint64
	var totalNormDecodedBytes atomic.Uint64
	var totalNormActiveHandles atomic.Int64
	var totalNormDeniedResources atomic.Uint64
	var totalVectorDirectViews atomic.Uint64
	var totalVectorMmapDirectViews atomic.Uint64
	var totalVectorHeapCopyTypedViews atomic.Uint64
	var totalVectorScratchDecodes atomic.Uint64
	var totalVectorPreparedDirectViews atomic.Uint64
	var totalVectorPreparedIdentityMappings atomic.Uint64
	var totalVectorPreparedRowRefMappings atomic.Uint64
	var totalVectorCertificationFailures atomic.Uint64
	var totalVectorAbsoluteOffsetUnaligned atomic.Uint64
	var totalVectorActualPointerUnaligned atomic.Uint64
	var totalVectorStaleHandles atomic.Uint64
	var totalTypedColumnMappedBytes atomic.Uint64
	var totalTypedColumnHeapCopyBytes atomic.Uint64
	var totalTypedColumnDecodedBytes atomic.Uint64
	var totalTypedColumnActiveHandles atomic.Int64
	var totalTypedColumnDeniedResources atomic.Uint64
	var totalTypedColumnFallbacks atomic.Uint64
	var totalRowRefVectorSourceState atomic.Uint64
	var totalRowRefVectorSourceLegacyGraphIDs atomic.Uint64
	var totalRowRefStatePreparedViews atomic.Uint64
	var totalRowRefStateMmapDirectFields atomic.Uint64
	var totalRowRefStateResultRefs atomic.Uint64
	var totalRowRefStateSourceUnavailable atomic.Uint64
	var totalRowRefStateSourceFallbacks atomic.Uint64
	var totalResultIDPreparedBytesViews atomic.Uint64
	var totalResultIDTypedBytesState atomic.Uint64
	var totalResultIDGraphFallbacks atomic.Uint64
	var totalResultIDStateValidationFailures atomic.Uint64
	var totalPreparedGraphSearchViews atomic.Uint64
	var totalGraphRowFallbacks atomic.Uint64
	var nextWorker atomic.Uint64
	b.SetParallelism(1) // Keep one prewarmed reader/scratch per RunParallel worker.
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
		var localSink int64
		var localStats columnVectorGraphNativeSearchStats
		for pb.Next() {
			if failed.Load() {
				continue
			}
			got, stats, err := worker.reader.SearchCosine(query, opts, &worker.scratch)
			if err != nil {
				recordParallelErr("SearchCosine: %v", err)
				continue
			}
			if len(got) == 0 {
				recordParallelErr("SearchCosine returned no results")
				continue
			}
			localSink += int64(got[0].Ordinal)
			localStats.CandidateRows += stats.CandidateRows
			localStats.Candidates += stats.Candidates
			localStats.Edges += stats.Edges
			localStats.VisitedNodes += stats.VisitedNodes
			localStats.VisitedEdges += stats.VisitedEdges
			localStats.VectorBytesRead += stats.VectorBytesRead
			localStats.NormBytesRead += stats.NormBytesRead
			localStats.AdjacencyBytesRead += stats.AdjacencyBytesRead
			localStats.CandidateFetches += stats.CandidateFetches
			localStats.ExpansionFetches += stats.ExpansionFetches
			localStats.ResultFetches += stats.ResultFetches
			localStats.ScoreBatches += stats.ScoreBatches
			localStats.OrdinalsGrouped += stats.OrdinalsGrouped
			localStats.ScoreBatchCalls += stats.ScoreBatchCalls
			localStats.ScoreBatchCandidates += stats.ScoreBatchCandidates
			if stats.ScoreBatchMaxTileSize > localStats.ScoreBatchMaxTileSize {
				localStats.ScoreBatchMaxTileSize = stats.ScoreBatchMaxTileSize
			}
			localStats.ScoreBatchOptimizedCalls += stats.ScoreBatchOptimizedCalls
			localStats.ScoreBatchScalarFallbackCalls += stats.ScoreBatchScalarFallbackCalls
			localStats.PreparedScoreCalls += stats.PreparedScoreCalls
			localStats.ScoreFloat64Fallbacks += stats.ScoreFloat64Fallbacks
			localStats.BlockViewHits += stats.BlockViewHits
			localStats.BlockViewMisses += stats.BlockViewMisses
			localStats.BlockViewBuilds += stats.BlockViewBuilds
			localStats.AdjacencyExpansions += stats.AdjacencyExpansions
			localStats.AdjacencyScratchDecodes += stats.AdjacencyScratchDecodes
			localStats.AdjacencyDirectViews += stats.AdjacencyDirectViews
			localStats.AdjacencyMmapDirectViews += stats.AdjacencyMmapDirectViews
			localStats.AdjacencyHeapCopyTypedViews += stats.AdjacencyHeapCopyTypedViews
			localStats.AdjacencyPreparedCSRDirectViews += stats.AdjacencyPreparedCSRDirectViews
			localStats.AdjacencyPreparedCSRMmapDirectViews += stats.AdjacencyPreparedCSRMmapDirectViews
			localStats.AdjacencyTypedListDirectViews += stats.AdjacencyTypedListDirectViews
			localStats.AdjacencyTypedListMmapDirectViews += stats.AdjacencyTypedListMmapDirectViews
			localStats.AdjacencyTypedListHeapCopyTypedViews += stats.AdjacencyTypedListHeapCopyTypedViews
			localStats.AdjacencyTypedListScratchDecodes += stats.AdjacencyTypedListScratchDecodes
			localStats.AdjacencyLegacyFallbacks += stats.AdjacencyLegacyFallbacks
			localStats.AdjacencySourceUnavailable += stats.AdjacencySourceUnavailable
			localStats.AdjacencySourceFallbacks += stats.AdjacencySourceFallbacks
			localStats.AdjacencyCertificationFailures += stats.AdjacencyCertificationFailures
			localStats.AdjacencyValidationFailures += stats.AdjacencyValidationFailures
			localStats.AdjacencyAbsoluteOffsetUnaligned += stats.AdjacencyAbsoluteOffsetUnaligned
			localStats.AdjacencyActualPointerUnaligned += stats.AdjacencyActualPointerUnaligned
			localStats.AdjacencyStaleHandles += stats.AdjacencyStaleHandles
			localStats.NormDirectViews += stats.NormDirectViews
			localStats.NormMmapDirectViews += stats.NormMmapDirectViews
			localStats.NormHeapCopyTypedViews += stats.NormHeapCopyTypedViews
			localStats.NormScratchDecodes += stats.NormScratchDecodes
			localStats.NormPreparedDirectViews += stats.NormPreparedDirectViews
			localStats.NormSourceUnavailable += stats.NormSourceUnavailable
			localStats.NormSourceFallbacks += stats.NormSourceFallbacks
			localStats.NormValidationFailures += stats.NormValidationFailures
			localStats.NormAbsoluteOffsetUnaligned += stats.NormAbsoluteOffsetUnaligned
			localStats.NormActualPointerUnaligned += stats.NormActualPointerUnaligned
			localStats.NormStaleHandles += stats.NormStaleHandles
			localStats.NormMappedBytes = stats.NormMappedBytes
			localStats.NormHeapCopyBytes = stats.NormHeapCopyBytes
			localStats.NormDecodedBytes = stats.NormDecodedBytes
			localStats.NormActiveHandles = stats.NormActiveHandles
			localStats.NormDeniedResources = stats.NormDeniedResources
			localStats.VectorDirectViews += stats.VectorDirectViews
			localStats.VectorMmapDirectViews += stats.VectorMmapDirectViews
			localStats.VectorHeapCopyTypedViews += stats.VectorHeapCopyTypedViews
			localStats.VectorScratchDecodes += stats.VectorScratchDecodes
			localStats.VectorPreparedDirectViews += stats.VectorPreparedDirectViews
			localStats.VectorPreparedIdentityMappings += stats.VectorPreparedIdentityMappings
			localStats.VectorPreparedRowRefMappings += stats.VectorPreparedRowRefMappings
			localStats.VectorCertificationFailures += stats.VectorCertificationFailures
			localStats.VectorAbsoluteOffsetUnaligned += stats.VectorAbsoluteOffsetUnaligned
			localStats.VectorActualPointerUnaligned += stats.VectorActualPointerUnaligned
			localStats.VectorStaleHandles += stats.VectorStaleHandles
			localStats.TypedColumnMappedBytes = stats.TypedColumnMappedBytes
			localStats.TypedColumnHeapCopyBytes = stats.TypedColumnHeapCopyBytes
			localStats.TypedColumnDecodedBytes = stats.TypedColumnDecodedBytes
			localStats.TypedColumnActiveHandles = stats.TypedColumnActiveHandles
			localStats.TypedColumnDeniedResources = stats.TypedColumnDeniedResources
			localStats.TypedColumnFallbacks += stats.TypedColumnFallbacks
			localStats.RowRefVectorSourceState += stats.RowRefVectorSourceState
			localStats.RowRefVectorSourceLegacyGraphIDs += stats.RowRefVectorSourceLegacyGraphIDs
			localStats.RowRefStatePreparedViews += stats.RowRefStatePreparedViews
			localStats.RowRefStateMmapDirectFields += stats.RowRefStateMmapDirectFields
			localStats.RowRefStateResultRefs += stats.RowRefStateResultRefs
			localStats.RowRefStateSourceUnavailable += stats.RowRefStateSourceUnavailable
			localStats.RowRefStateSourceFallbacks += stats.RowRefStateSourceFallbacks
			localStats.ResultIDPreparedBytesViews += stats.ResultIDPreparedBytesViews
			localStats.ResultIDTypedBytesState += stats.ResultIDTypedBytesState
			localStats.ResultIDGraphFallbacks += stats.ResultIDGraphFallbacks
			localStats.ResultIDStateValidationFailures += stats.ResultIDStateValidationFailures
			localStats.PreparedGraphSearchViews += stats.PreparedGraphSearchViews
			localStats.GraphRowFallbacks += stats.GraphRowFallbacks
		}
		sink.Add(localSink)
		totalCandidateRows.Add(localStats.CandidateRows)
		totalCandidates.Add(localStats.Candidates)
		totalEdges.Add(localStats.Edges)
		totalVisitedNodes.Add(localStats.VisitedNodes)
		totalVisitedEdges.Add(localStats.VisitedEdges)
		totalVectorBytesRead.Add(localStats.VectorBytesRead)
		totalNormBytesRead.Add(localStats.NormBytesRead)
		totalAdjacencyBytesRead.Add(localStats.AdjacencyBytesRead)
		totalCandidateFetches.Add(localStats.CandidateFetches)
		totalExpansionFetches.Add(localStats.ExpansionFetches)
		totalResultFetches.Add(localStats.ResultFetches)
		totalScoreBatches.Add(localStats.ScoreBatches)
		totalOrdinalsGrouped.Add(localStats.OrdinalsGrouped)
		totalScoreBatchCalls.Add(localStats.ScoreBatchCalls)
		totalScoreBatchCandidates.Add(localStats.ScoreBatchCandidates)
		for {
			current := totalScoreBatchMaxTileSize.Load()
			if localStats.ScoreBatchMaxTileSize <= current || totalScoreBatchMaxTileSize.CompareAndSwap(current, localStats.ScoreBatchMaxTileSize) {
				break
			}
		}
		totalScoreBatchOptimizedCalls.Add(localStats.ScoreBatchOptimizedCalls)
		totalScoreBatchScalarFallbackCalls.Add(localStats.ScoreBatchScalarFallbackCalls)
		totalPreparedScoreCalls.Add(localStats.PreparedScoreCalls)
		totalScoreFloat64Fallbacks.Add(localStats.ScoreFloat64Fallbacks)
		totalBlockViewHits.Add(localStats.BlockViewHits)
		totalBlockViewMisses.Add(localStats.BlockViewMisses)
		totalBlockViewBuilds.Add(localStats.BlockViewBuilds)
		totalAdjacencyExpansions.Add(localStats.AdjacencyExpansions)
		totalAdjacencyScratchDecodes.Add(localStats.AdjacencyScratchDecodes)
		totalAdjacencyDirectViews.Add(localStats.AdjacencyDirectViews)
		totalAdjacencyMmapDirectViews.Add(localStats.AdjacencyMmapDirectViews)
		totalAdjacencyHeapCopyTypedViews.Add(localStats.AdjacencyHeapCopyTypedViews)
		totalAdjacencyPreparedCSRDirectViews.Add(localStats.AdjacencyPreparedCSRDirectViews)
		totalAdjacencyPreparedCSRMmapDirectViews.Add(localStats.AdjacencyPreparedCSRMmapDirectViews)
		totalAdjacencyTypedListDirectViews.Add(localStats.AdjacencyTypedListDirectViews)
		totalAdjacencyTypedListMmapDirectViews.Add(localStats.AdjacencyTypedListMmapDirectViews)
		totalAdjacencyTypedListHeapCopyTypedViews.Add(localStats.AdjacencyTypedListHeapCopyTypedViews)
		totalAdjacencyTypedListScratchDecodes.Add(localStats.AdjacencyTypedListScratchDecodes)
		totalAdjacencyLegacyFallbacks.Add(localStats.AdjacencyLegacyFallbacks)
		totalAdjacencySourceUnavailable.Add(localStats.AdjacencySourceUnavailable)
		totalAdjacencySourceFallbacks.Add(localStats.AdjacencySourceFallbacks)
		totalAdjacencyCertificationFailures.Add(localStats.AdjacencyCertificationFailures)
		totalAdjacencyValidationFailures.Add(localStats.AdjacencyValidationFailures)
		totalAdjacencyAbsoluteOffsetUnaligned.Add(localStats.AdjacencyAbsoluteOffsetUnaligned)
		totalAdjacencyActualPointerUnaligned.Add(localStats.AdjacencyActualPointerUnaligned)
		totalAdjacencyStaleHandles.Add(localStats.AdjacencyStaleHandles)
		totalNormDirectViews.Add(localStats.NormDirectViews)
		totalNormMmapDirectViews.Add(localStats.NormMmapDirectViews)
		totalNormHeapCopyTypedViews.Add(localStats.NormHeapCopyTypedViews)
		totalNormScratchDecodes.Add(localStats.NormScratchDecodes)
		totalNormPreparedDirectViews.Add(localStats.NormPreparedDirectViews)
		totalNormSourceUnavailable.Add(localStats.NormSourceUnavailable)
		totalNormSourceFallbacks.Add(localStats.NormSourceFallbacks)
		totalNormValidationFailures.Add(localStats.NormValidationFailures)
		totalNormAbsoluteOffsetUnaligned.Add(localStats.NormAbsoluteOffsetUnaligned)
		totalNormActualPointerUnaligned.Add(localStats.NormActualPointerUnaligned)
		totalNormStaleHandles.Add(localStats.NormStaleHandles)
		totalNormMappedBytes.Add(localStats.NormMappedBytes)
		totalNormHeapCopyBytes.Add(localStats.NormHeapCopyBytes)
		totalNormDecodedBytes.Add(localStats.NormDecodedBytes)
		totalNormActiveHandles.Add(localStats.NormActiveHandles)
		totalNormDeniedResources.Add(localStats.NormDeniedResources)
		totalVectorDirectViews.Add(localStats.VectorDirectViews)
		totalVectorMmapDirectViews.Add(localStats.VectorMmapDirectViews)
		totalVectorHeapCopyTypedViews.Add(localStats.VectorHeapCopyTypedViews)
		totalVectorScratchDecodes.Add(localStats.VectorScratchDecodes)
		totalVectorPreparedDirectViews.Add(localStats.VectorPreparedDirectViews)
		totalVectorPreparedIdentityMappings.Add(localStats.VectorPreparedIdentityMappings)
		totalVectorPreparedRowRefMappings.Add(localStats.VectorPreparedRowRefMappings)
		totalVectorCertificationFailures.Add(localStats.VectorCertificationFailures)
		totalVectorAbsoluteOffsetUnaligned.Add(localStats.VectorAbsoluteOffsetUnaligned)
		totalVectorActualPointerUnaligned.Add(localStats.VectorActualPointerUnaligned)
		totalVectorStaleHandles.Add(localStats.VectorStaleHandles)
		totalTypedColumnMappedBytes.Add(localStats.TypedColumnMappedBytes)
		totalTypedColumnHeapCopyBytes.Add(localStats.TypedColumnHeapCopyBytes)
		totalTypedColumnDecodedBytes.Add(localStats.TypedColumnDecodedBytes)
		totalTypedColumnActiveHandles.Add(localStats.TypedColumnActiveHandles)
		totalTypedColumnDeniedResources.Add(localStats.TypedColumnDeniedResources)
		totalTypedColumnFallbacks.Add(localStats.TypedColumnFallbacks)
		totalRowRefVectorSourceState.Add(localStats.RowRefVectorSourceState)
		totalRowRefVectorSourceLegacyGraphIDs.Add(localStats.RowRefVectorSourceLegacyGraphIDs)
		totalRowRefStatePreparedViews.Add(localStats.RowRefStatePreparedViews)
		totalRowRefStateMmapDirectFields.Add(localStats.RowRefStateMmapDirectFields)
		totalRowRefStateResultRefs.Add(localStats.RowRefStateResultRefs)
		totalRowRefStateSourceUnavailable.Add(localStats.RowRefStateSourceUnavailable)
		totalRowRefStateSourceFallbacks.Add(localStats.RowRefStateSourceFallbacks)
		totalResultIDPreparedBytesViews.Add(localStats.ResultIDPreparedBytesViews)
		totalResultIDTypedBytesState.Add(localStats.ResultIDTypedBytesState)
		totalResultIDGraphFallbacks.Add(localStats.ResultIDGraphFallbacks)
		totalResultIDStateValidationFailures.Add(localStats.ResultIDStateValidationFailures)
		totalPreparedGraphSearchViews.Add(localStats.PreparedGraphSearchViews)
		totalGraphRowFallbacks.Add(localStats.GraphRowFallbacks)
	})
	b.StopTimer()
	reportColumnGraphNativeSearchBenchShapeMetricsV3(b, shape)
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	b.ReportMetric(float64(workers), "parallel_workers")
	columnPhysicalScanBenchSum += sink.Load()
	var stats columnPhysicalRowReaderStats
	for _, worker := range benchWorkers {
		stats = addColumnPhysicalRowReaderStatsV3(stats, worker.reader.Stats())
	}
	reportColumnGraphNativeSearchBenchMetricsV3(b, b.N, baseStats, stats, columnVectorGraphNativeSearchStats{
		CandidateRows:                        totalCandidateRows.Load(),
		Candidates:                           totalCandidates.Load(),
		Edges:                                totalEdges.Load(),
		VisitedNodes:                         totalVisitedNodes.Load(),
		VisitedEdges:                         totalVisitedEdges.Load(),
		VectorBytesRead:                      totalVectorBytesRead.Load(),
		NormBytesRead:                        totalNormBytesRead.Load(),
		AdjacencyBytesRead:                   totalAdjacencyBytesRead.Load(),
		CandidateFetches:                     totalCandidateFetches.Load(),
		ExpansionFetches:                     totalExpansionFetches.Load(),
		ResultFetches:                        totalResultFetches.Load(),
		ScoreBatches:                         totalScoreBatches.Load(),
		OrdinalsGrouped:                      totalOrdinalsGrouped.Load(),
		ScoreBatchCalls:                      totalScoreBatchCalls.Load(),
		ScoreBatchCandidates:                 totalScoreBatchCandidates.Load(),
		ScoreBatchMaxTileSize:                totalScoreBatchMaxTileSize.Load(),
		ScoreBatchOptimizedCalls:             totalScoreBatchOptimizedCalls.Load(),
		ScoreBatchScalarFallbackCalls:        totalScoreBatchScalarFallbackCalls.Load(),
		PreparedScoreCalls:                   totalPreparedScoreCalls.Load(),
		ScoreFloat64Fallbacks:                totalScoreFloat64Fallbacks.Load(),
		BlockViewHits:                        totalBlockViewHits.Load(),
		BlockViewMisses:                      totalBlockViewMisses.Load(),
		BlockViewBuilds:                      totalBlockViewBuilds.Load(),
		AdjacencyExpansions:                  totalAdjacencyExpansions.Load(),
		AdjacencyScratchDecodes:              totalAdjacencyScratchDecodes.Load(),
		AdjacencyDirectViews:                 totalAdjacencyDirectViews.Load(),
		AdjacencyMmapDirectViews:             totalAdjacencyMmapDirectViews.Load(),
		AdjacencyHeapCopyTypedViews:          totalAdjacencyHeapCopyTypedViews.Load(),
		AdjacencyPreparedCSRDirectViews:      totalAdjacencyPreparedCSRDirectViews.Load(),
		AdjacencyPreparedCSRMmapDirectViews:  totalAdjacencyPreparedCSRMmapDirectViews.Load(),
		AdjacencyTypedListDirectViews:        totalAdjacencyTypedListDirectViews.Load(),
		AdjacencyTypedListMmapDirectViews:    totalAdjacencyTypedListMmapDirectViews.Load(),
		AdjacencyTypedListHeapCopyTypedViews: totalAdjacencyTypedListHeapCopyTypedViews.Load(),
		AdjacencyTypedListScratchDecodes:     totalAdjacencyTypedListScratchDecodes.Load(),
		AdjacencyLegacyFallbacks:             totalAdjacencyLegacyFallbacks.Load(),
		AdjacencySourceUnavailable:           totalAdjacencySourceUnavailable.Load(),
		AdjacencySourceFallbacks:             totalAdjacencySourceFallbacks.Load(),
		AdjacencyCertificationFailures:       totalAdjacencyCertificationFailures.Load(),
		AdjacencyValidationFailures:          totalAdjacencyValidationFailures.Load(),
		AdjacencyAbsoluteOffsetUnaligned:     totalAdjacencyAbsoluteOffsetUnaligned.Load(),
		AdjacencyActualPointerUnaligned:      totalAdjacencyActualPointerUnaligned.Load(),
		AdjacencyStaleHandles:                totalAdjacencyStaleHandles.Load(),
		NormDirectViews:                      totalNormDirectViews.Load(),
		NormMmapDirectViews:                  totalNormMmapDirectViews.Load(),
		NormHeapCopyTypedViews:               totalNormHeapCopyTypedViews.Load(),
		NormScratchDecodes:                   totalNormScratchDecodes.Load(),
		NormPreparedDirectViews:              totalNormPreparedDirectViews.Load(),
		NormSourceUnavailable:                totalNormSourceUnavailable.Load(),
		NormSourceFallbacks:                  totalNormSourceFallbacks.Load(),
		NormValidationFailures:               totalNormValidationFailures.Load(),
		NormAbsoluteOffsetUnaligned:          totalNormAbsoluteOffsetUnaligned.Load(),
		NormActualPointerUnaligned:           totalNormActualPointerUnaligned.Load(),
		NormStaleHandles:                     totalNormStaleHandles.Load(),
		NormMappedBytes:                      totalNormMappedBytes.Load(),
		NormHeapCopyBytes:                    totalNormHeapCopyBytes.Load(),
		NormDecodedBytes:                     totalNormDecodedBytes.Load(),
		NormActiveHandles:                    totalNormActiveHandles.Load(),
		NormDeniedResources:                  totalNormDeniedResources.Load(),
		VectorDirectViews:                    totalVectorDirectViews.Load(),
		VectorMmapDirectViews:                totalVectorMmapDirectViews.Load(),
		VectorHeapCopyTypedViews:             totalVectorHeapCopyTypedViews.Load(),
		VectorScratchDecodes:                 totalVectorScratchDecodes.Load(),
		VectorPreparedDirectViews:            totalVectorPreparedDirectViews.Load(),
		VectorPreparedIdentityMappings:       totalVectorPreparedIdentityMappings.Load(),
		VectorPreparedRowRefMappings:         totalVectorPreparedRowRefMappings.Load(),
		VectorCertificationFailures:          totalVectorCertificationFailures.Load(),
		VectorAbsoluteOffsetUnaligned:        totalVectorAbsoluteOffsetUnaligned.Load(),
		VectorActualPointerUnaligned:         totalVectorActualPointerUnaligned.Load(),
		VectorStaleHandles:                   totalVectorStaleHandles.Load(),
		TypedColumnMappedBytes:               totalTypedColumnMappedBytes.Load(),
		TypedColumnHeapCopyBytes:             totalTypedColumnHeapCopyBytes.Load(),
		TypedColumnDecodedBytes:              totalTypedColumnDecodedBytes.Load(),
		TypedColumnActiveHandles:             totalTypedColumnActiveHandles.Load(),
		TypedColumnDeniedResources:           totalTypedColumnDeniedResources.Load(),
		TypedColumnFallbacks:                 totalTypedColumnFallbacks.Load(),
		RowRefVectorSourceState:              totalRowRefVectorSourceState.Load(),
		RowRefVectorSourceLegacyGraphIDs:     totalRowRefVectorSourceLegacyGraphIDs.Load(),
		RowRefStatePreparedViews:             totalRowRefStatePreparedViews.Load(),
		RowRefStateMmapDirectFields:          totalRowRefStateMmapDirectFields.Load(),
		RowRefStateResultRefs:                totalRowRefStateResultRefs.Load(),
		RowRefStateSourceUnavailable:         totalRowRefStateSourceUnavailable.Load(),
		RowRefStateSourceFallbacks:           totalRowRefStateSourceFallbacks.Load(),
		ResultIDPreparedBytesViews:           totalResultIDPreparedBytesViews.Load(),
		ResultIDTypedBytesState:              totalResultIDTypedBytesState.Load(),
		ResultIDGraphFallbacks:               totalResultIDGraphFallbacks.Load(),
		ResultIDStateValidationFailures:      totalResultIDStateValidationFailures.Load(),
		PreparedGraphSearchViews:             totalPreparedGraphSearchViews.Load(),
		GraphRowFallbacks:                    totalGraphRowFallbacks.Load(),
	})
}

func reportColumnGraphNativeSearchBenchShapeMetricsV3(b *testing.B, shape columnVectorGraphNativeSearchBenchShapeV3) {
	b.Helper()
	b.ReportMetric(float64(shape.rows), "rows")
	b.ReportMetric(float64(shape.dims), "dims")
	b.ReportMetric(float64(shape.m), "degree")
	b.ReportMetric(float64(shape.topK), "top_k")
	b.ReportMetric(float64(shape.efSearch), "ef_search")
	if shape.typedColumnVector {
		b.ReportMetric(1, "typed_column_vector")
	}
	if shape.omitResultMaterialization {
		b.ReportMetric(1, "graph_only_no_result_materialization")
	}
	if shape.scoreBatchMode != columnVectorGraphScoreBatchModeDefault {
		if shape.scoreBatchMode.indexedEnabled() {
			b.ReportMetric(1, "score_batch_mode_indexed")
		} else {
			b.ReportMetric(1, "score_batch_mode_scalar")
		}
	}
	switch shape.statsMode.normalized() {
	case columnVectorGraphNativeSearchStatsModeMinimal:
		b.ReportMetric(1, "stats_mode_minimal")
	case columnVectorGraphNativeSearchStatsModeBenchmarkDebug:
		b.ReportMetric(1, "stats_mode_benchmark_debug")
	default:
		b.ReportMetric(1, "stats_mode_full_diagnostics")
	}
}

func openColumnVectorGraphNativeSearchBenchFixtureV3(b testing.TB, shape columnVectorGraphNativeSearchBenchShapeV3) (func(), *Collection, VectorIndexDefinition, []float32) {
	b.Helper()
	if shape.queryOrdinal < 0 || shape.queryOrdinal >= shape.rows {
		b.Fatalf("query ordinal=%d out of range rows=%d", shape.queryOrdinal, shape.rows)
	}
	if shape.directPhysicalAsset && !shape.typedColumnVector {
		rows := columnVectorGraphNativeSearchBenchAssetRowsV3(b, shape.rows, shape.dims, shape.m)
		d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeAndAdjacencyState1989(b, shape.dims, shape.m, rows)
		query := append([]float32(nil), rows[shape.queryOrdinal].Vector...)
		return func() { _ = d.Close() }, col, def, query
	}
	input := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	var d *backenddb.DB
	var col *Collection
	var def VectorIndexDefinition
	if shape.typedColumnVector {
		_, d, col, def = openColumnGraphTypedColumnVectorTestCollection1782(b, shape.dims, shape.m, input)
	} else {
		_, d, col, def = openColumnGraphRebuildTestCollectionV2A(b, shape.dims, shape.m, input)
	}
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(b, status, def.Name)
	query := append([]float32(nil), input[shape.queryOrdinal].vector...)
	return func() { _ = d.Close() }, col, def, query
}

func publishColumnVectorGraphPhysicalReaderTestAssetWithShapeAndAdjacencyState1989(tb testing.TB, dims, m int, rows []columnVectorGraphAssetRow) (*backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	baseCfg, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(dims))
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(dims, m)
	const generation = uint64(2)
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, generation, 1, 1, rows)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("docs", *baseCfg, def)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: generation, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	graph := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: generation,
		BaseManifestChecksum:   identity.Checksum,
		BaseSchemaHash:         baseCfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               prepared.RowCount,
		AssetRef:               prepared.Ref,
		AssetBytes:             prepared.Bytes,
	}
	records, manifestIdentity := testColumnGraphManifestRecordsFromSnapshot1920(tb, *baseCfg, graph, identity)
	graph = graphManifestFromRecords1918(tb, records, def)
	stateRows := columnVectorGraphStateRowsForTest1987(rows, graph.RowCount, def.Dimensions, columnVectorGraphExpectedAdjacencyLayerCountFromAssetRows1989(tb, rows))
	records, manifestIdentity = appendCompleteVectorIndexStateForGraphTest1987(tb, d, "docs", *baseCfg, def, graph, records, manifestIdentity, columnVectorIndexStateChecksumInput1986(*baseCfg), stateRows)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    baseCfg,
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	publishColumnGraphCatalogForTestV2A(tb, d, meta, manifestIdentity, records)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	return d, col, def
}

func columnVectorGraphNativeSearchBenchAssetRowsV3(tb testing.TB, rows, dims, degree int) []columnVectorGraphAssetRow {
	tb.Helper()
	out := make([]columnVectorGraphAssetRow, rows)
	for row := range out {
		vector := columnVectorGraphNativeSearchBenchEmbeddingV3(row, dims)
		invNorm, err := columnVectorGraphInvNorm(vector)
		if err != nil {
			tb.Fatalf("columnVectorGraphInvNorm row %d: %v", row, err)
		}
		adjacency := make([]uint32, 0, degree)
		for edge := 0; edge < degree; edge++ {
			step := edge/2 + 1
			neighbor := row + step
			if edge%2 == 1 {
				neighbor = row - step
			}
			neighbor %= rows
			if neighbor < 0 {
				neighbor += rows
			}
			adjacency = append(adjacency, uint32(neighbor))
		}
		out[row] = columnVectorGraphAssetRow{
			ID:        []byte(fmt.Sprintf("doc-%06d", row)),
			Vector:    vector,
			InvNorm:   invNorm,
			Adjacency: adjacency,
		}
	}
	return out
}

func columnVectorGraphNativeSearchBenchEmbeddingV3(id, dims int) []float32 {
	out := make([]float32, dims)
	var norm float64
	x := float64(id + 1)
	for i := range out {
		d := float64(i + 1)
		value := math.Sin(x*d*0.013) + math.Cos((x+17)*d*0.007) + math.Sin(float64((id%31)+1)*d*0.019)
		out[i] = float32(value)
		norm += value * value
	}
	scale := 1 / math.Sqrt(norm)
	for i := range out {
		out[i] = float32(float64(out[i]) * scale)
	}
	return out
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
	for pos > 0 && columnVectorGraphSearchCandidateBetter(candidate, top[pos-1]) {
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
		if !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
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
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(n)/elapsed.Seconds(), "ops/sec")
	}
	b.ReportMetric(float64(stats.Rows), "graph_rows")
	b.ReportMetric(float64(stats.Granules), "graph_granules")
	b.ReportMetric(float64(searchStats.CandidateRows)/float64(n), "candidate_rows/search")
	b.ReportMetric(float64(searchStats.Candidates)/float64(n), "candidates/search")
	b.ReportMetric(float64(searchStats.Edges)/float64(n), "edges/search")
	b.ReportMetric(float64(searchStats.VisitedNodes)/float64(n), "visited_nodes/search")
	b.ReportMetric(float64(searchStats.VisitedEdges)/float64(n), "visited_edges/search")
	b.ReportMetric(float64(searchStats.VectorBytesRead)/float64(n), "vector_B/search")
	b.ReportMetric(float64(searchStats.NormBytesRead)/float64(n), "norm_B/search")
	b.ReportMetric(float64(searchStats.AdjacencyBytesRead)/float64(n), "adjacency_B/search")
	if searchStats.Candidates > 0 {
		b.ReportMetric(float64(searchStats.Edges)/float64(searchStats.Candidates), "edges/node")
	}
	b.ReportMetric(float64(searchStats.CandidateFetches)/float64(n), "candidate_fetches/search")
	b.ReportMetric(float64(searchStats.ScoreBatches)/float64(n), "score_batches/search")
	b.ReportMetric(float64(searchStats.OrdinalsGrouped)/float64(n), "ordinals_grouped/search")
	b.ReportMetric(float64(searchStats.ScoreBatchCalls)/float64(n), "score_batch_calls/search")
	b.ReportMetric(float64(searchStats.ScoreBatchCandidates)/float64(n), "score_batch_candidates/search")
	b.ReportMetric(float64(searchStats.ScoreBatchMaxTileSize), "score_batch_max_tile_size")
	b.ReportMetric(float64(searchStats.ScoreBatchOptimizedCalls)/float64(n), "score_batch_optimized/search")
	b.ReportMetric(float64(searchStats.ScoreBatchScalarFallbackCalls)/float64(n), "score_batch_fallback/search")
	b.ReportMetric(float64(searchStats.PreparedScoreCalls)/float64(n), "prepared_score_calls/search")
	b.ReportMetric(float64(searchStats.ScoreFloat64Fallbacks)/float64(n), "score_float64_fallbacks/search")
	if searchStats.ScoreBatchCalls > 0 {
		b.ReportMetric(float64(searchStats.ScoreBatchCandidates)/float64(searchStats.ScoreBatchCalls), "score_batch_avg_tile_size")
	}
	reportColumnVectorGraphNativeSearchDebugMetrics1979(b, n, searchStats)
	b.ReportMetric(float64(searchStats.BlockViewHits)/float64(n), "block_view_hits/search")
	b.ReportMetric(float64(searchStats.BlockViewMisses)/float64(n), "block_view_misses/search")
	b.ReportMetric(float64(searchStats.BlockViewBuilds)/float64(n), "block_view_builds/search")
	b.ReportMetric(float64(searchStats.AdjacencyExpansions)/float64(n), "adjacency_expansions/search")
	b.ReportMetric(float64(searchStats.AdjacencyScratchDecodes)/float64(n), "adjacency_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.AdjacencyDirectViews)/float64(n), "adjacency_direct_views/search")
	b.ReportMetric(float64(searchStats.AdjacencyMmapDirectViews)/float64(n), "adjacency_mmap_direct/search")
	b.ReportMetric(float64(searchStats.AdjacencyHeapCopyTypedViews)/float64(n), "adjacency_heap_copy_typed_view/search")
	b.ReportMetric(float64(searchStats.AdjacencyPreparedCSRDirectViews)/float64(n), "adjacency_prepared_csr_direct_views/search")
	b.ReportMetric(float64(searchStats.AdjacencyPreparedCSRMmapDirectViews)/float64(n), "adjacency_prepared_csr_mmap_direct/search")
	b.ReportMetric(float64(searchStats.AdjacencyTypedListDirectViews)/float64(n), "adjacency_typed_list_direct_views/search")
	b.ReportMetric(float64(searchStats.AdjacencyTypedListMmapDirectViews)/float64(n), "adjacency_typed_list_mmap_direct/search")
	b.ReportMetric(float64(searchStats.AdjacencyTypedListHeapCopyTypedViews)/float64(n), "adjacency_typed_list_heap_copy_typed_view/search")
	b.ReportMetric(float64(searchStats.AdjacencyTypedListScratchDecodes)/float64(n), "adjacency_typed_list_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.AdjacencyLegacyFallbacks)/float64(n), "adjacency_legacy_fallbacks/search")
	b.ReportMetric(float64(searchStats.AdjacencySourceUnavailable)/float64(n), "adjacency_source_unavailable/search")
	b.ReportMetric(float64(searchStats.AdjacencySourceFallbacks)/float64(n), "adjacency_source_fallbacks/search")
	b.ReportMetric(float64(searchStats.AdjacencyCertificationFailures)/float64(n), "adjacency_certification_failures/search")
	b.ReportMetric(float64(searchStats.AdjacencyValidationFailures)/float64(n), "adjacency_validation_failures/search")
	b.ReportMetric(float64(searchStats.AdjacencyAbsoluteOffsetUnaligned)/float64(n), "adjacency_absolute_offset_unaligned/search")
	b.ReportMetric(float64(searchStats.AdjacencyActualPointerUnaligned)/float64(n), "adjacency_actual_pointer_unaligned/search")
	b.ReportMetric(float64(searchStats.AdjacencyStaleHandles)/float64(n), "adjacency_stale_handles/search")
	b.ReportMetric(float64(searchStats.NormDirectViews)/float64(n), "norm_direct_views/search")
	b.ReportMetric(float64(searchStats.NormMmapDirectViews)/float64(n), "norm_mmap_direct/search")
	b.ReportMetric(float64(searchStats.NormHeapCopyTypedViews)/float64(n), "norm_heap_copy_typed_view/search")
	b.ReportMetric(float64(searchStats.NormScratchDecodes)/float64(n), "norm_scratch_decode/search")
	b.ReportMetric(float64(searchStats.NormScratchDecodes)/float64(n), "norm_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.NormPreparedDirectViews)/float64(n), "norm_prepared_direct/search")
	b.ReportMetric(float64(searchStats.NormSourceUnavailable)/float64(n), "norm_source_unavailable/search")
	b.ReportMetric(float64(searchStats.NormSourceFallbacks)/float64(n), "norm_source_fallbacks/search")
	b.ReportMetric(float64(searchStats.NormValidationFailures)/float64(n), "norm_validation_failures/search")
	b.ReportMetric(float64(searchStats.NormAbsoluteOffsetUnaligned)/float64(n), "norm_absolute_offset_unaligned/search")
	b.ReportMetric(float64(searchStats.NormActualPointerUnaligned)/float64(n), "norm_actual_pointer_unaligned/search")
	b.ReportMetric(float64(searchStats.NormStaleHandles)/float64(n), "norm_stale_handles/search")
	b.ReportMetric(float64(searchStats.NormMappedBytes), "norm_mapped_B")
	b.ReportMetric(float64(searchStats.NormHeapCopyBytes), "norm_heap_copy_B")
	b.ReportMetric(float64(searchStats.NormDecodedBytes), "norm_decoded_B")
	b.ReportMetric(float64(searchStats.NormActiveHandles), "norm_active_handles")
	b.ReportMetric(float64(searchStats.NormDeniedResources), "norm_denied_resources")
	b.ReportMetric(float64(searchStats.VectorDirectViews)/float64(n), "vector_direct_views/search")
	b.ReportMetric(float64(searchStats.VectorMmapDirectViews)/float64(n), "vector_mmap_direct/search")
	b.ReportMetric(float64(searchStats.VectorHeapCopyTypedViews)/float64(n), "vector_heap_copy_typed_view/search")
	b.ReportMetric(float64(searchStats.VectorScratchDecodes)/float64(n), "vector_scratch_decode/search")
	b.ReportMetric(float64(searchStats.VectorScratchDecodes)/float64(n), "vector_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.VectorPreparedDirectViews)/float64(n), "vector_prepared_direct/search")
	b.ReportMetric(float64(searchStats.VectorPreparedIdentityMappings)/float64(n), "vector_prepared_identity_mapping/search")
	b.ReportMetric(float64(searchStats.VectorPreparedRowRefMappings)/float64(n), "vector_prepared_row_ref_mapping/search")
	b.ReportMetric(float64(searchStats.VectorCertificationFailures)/float64(n), "vector_certification_failures/search")
	b.ReportMetric(float64(searchStats.VectorAbsoluteOffsetUnaligned)/float64(n), "vector_absolute_offset_unaligned/search")
	b.ReportMetric(float64(searchStats.VectorActualPointerUnaligned)/float64(n), "vector_actual_pointer_unaligned/search")
	b.ReportMetric(float64(searchStats.VectorStaleHandles)/float64(n), "vector_stale_handles/search")
	if searchStats.VectorMmapDirectViews > 0 {
		b.ReportMetric(1, "typed_column_vector_source_mmap")
	}
	if searchStats.VectorHeapCopyTypedViews > 0 {
		b.ReportMetric(1, "typed_column_vector_source_heap_copy")
	}
	if searchStats.VectorScratchDecodes > 0 && searchStats.TypedColumnDecodedBytes > 0 {
		b.ReportMetric(1, "typed_column_vector_source_scratch")
	}
	if searchStats.TypedColumnFallbacks > 0 {
		b.ReportMetric(1, "typed_column_vector_source_fallback")
	}
	b.ReportMetric(float64(searchStats.TypedColumnFallbacks)/float64(n), "typed_column_vector_fallbacks/search")
	b.ReportMetric(float64(searchStats.RowRefVectorSourceState)/float64(n), "row_ref_vector_source_state/search")
	b.ReportMetric(float64(searchStats.RowRefVectorSourceLegacyGraphIDs)/float64(n), "row_ref_vector_source_legacy_graph_ids/search")
	b.ReportMetric(float64(searchStats.RowRefStatePreparedViews)/float64(n), "row_ref_state_prepared_views/search")
	b.ReportMetric(float64(searchStats.RowRefStateMmapDirectFields)/float64(n), "row_ref_state_mmap_direct_fields/search")
	b.ReportMetric(float64(searchStats.RowRefStateResultRefs)/float64(n), "row_ref_state_result_refs/search")
	b.ReportMetric(float64(searchStats.RowRefStateSourceUnavailable)/float64(n), "row_ref_state_source_unavailable/search")
	b.ReportMetric(float64(searchStats.RowRefStateSourceFallbacks)/float64(n), "row_ref_state_source_fallbacks/search")
	b.ReportMetric(float64(searchStats.ResultIDPreparedBytesViews)/float64(n), "result_id_prepared_bytes_views/search")
	b.ReportMetric(float64(searchStats.ResultIDTypedBytesState)/float64(n), "result_id_typed_bytes_state/search")
	b.ReportMetric(float64(searchStats.ResultIDGraphFallbacks)/float64(n), "result_id_graph_fallbacks/search")
	b.ReportMetric(float64(searchStats.ResultIDStateValidationFailures)/float64(n), "result_id_state_validation_failures/search")
	b.ReportMetric(float64(searchStats.PreparedGraphSearchViews)/float64(n), "prepared_graph_search_views/search")
	b.ReportMetric(float64(searchStats.GraphRowFallbacks)/float64(n), "graph_row_fallbacks/search")
	b.ReportMetric(float64(searchStats.WavefrontSearches)/float64(n), "wavefront_searches/search")
	b.ReportMetric(float64(searchStats.WavefrontWidth), "wavefront_width")
	b.ReportMetric(float64(searchStats.WavefrontRounds)/float64(n), "wavefront_rounds/search")
	b.ReportMetric(float64(searchStats.WavefrontCandidatePops)/float64(n), "wavefront_candidate_pops/search")
	b.ReportMetric(float64(searchStats.WavefrontStagedNeighbors)/float64(n), "wavefront_staged_neighbors/search")
	b.ReportMetric(float64(searchStats.WavefrontMaxTileSize), "wavefront_max_tile_size")
	if searchStats.WavefrontRounds > 0 {
		b.ReportMetric(float64(searchStats.WavefrontStagedNeighbors)/float64(searchStats.WavefrontRounds), "wavefront_avg_tile_size")
	}
	b.ReportMetric(float64(searchStats.TypedColumnMappedBytes), "mapped_B")
	b.ReportMetric(float64(searchStats.TypedColumnHeapCopyBytes), "heap_copy_B")
	b.ReportMetric(float64(searchStats.TypedColumnDecodedBytes), "decoded_derived_B")
	b.ReportMetric(float64(searchStats.TypedColumnActiveHandles), "active_handles")
	b.ReportMetric(float64(searchStats.TypedColumnDeniedResources), "denied_resources")
	b.ReportMetric(float64(searchStats.ExpansionFetches)/float64(n), "expansion_fetches/search")
	b.ReportMetric(float64(searchStats.ResultFetches)/float64(n), "result_fetches/search")
	// Lower-level native search never materializes documents; keep these zero
	// labels present so graph-only truth-matrix rows have the same report columns
	// as public result/document boundary rows.
	b.ReportMetric(0, "docs_fetched/search")
	b.ReportMetric(0, "doc_fetch_ns/search")
	b.ReportMetric(0, "doc_row_ref_state_fetches/search")
	b.ReportMetric(0, "doc_row_ref_lookup_fallbacks/search")
	b.ReportMetric(float64(deltaColumnGraphNativeBenchCounterV3(stats.CacheHits, baseStats.CacheHits))/float64(n), "cache_hits/search")
	b.ReportMetric(float64(deltaColumnGraphNativeBenchCounterV3(stats.CacheMisses, baseStats.CacheMisses))/float64(n), "cache_misses/search")
	b.ReportMetric(float64(deltaColumnGraphNativeBenchCounterV3(stats.DecodedBlocks, baseStats.DecodedBlocks))/float64(n), "decoded_blocks/search")
	b.ReportMetric(float64(deltaColumnGraphNativeBenchCounterV3(stats.GranulesTouched, baseStats.GranulesTouched))/float64(n), "granules_touched/search")
	b.ReportMetric(float64(deltaColumnGraphNativeBenchBytesV3(stats.PhysicalBytesRead, baseStats.PhysicalBytesRead))/float64(n), "physical_B/search")
	b.ReportMetric(float64(stats.MaxResidentBytes), "max_resident_B")
	if stats.Rows > 0 {
		b.ReportMetric(float64(stats.OpenPhysicalBytesRead)/float64(stats.Rows), "asset_B/row")
	}
}

func deltaColumnGraphNativeBenchCounterV3(current, base uint64) uint64 {
	if current < base {
		return 0
	}
	return current - base
}

func deltaColumnGraphNativeBenchBytesV3(current, base int64) int64 {
	if current < base {
		return 0
	}
	return current - base
}

func addColumnPhysicalRowReaderStatsV3(left, right columnPhysicalRowReaderStats) columnPhysicalRowReaderStats {
	// Rows/granules and open bytes describe one bound graph asset/reader shape.
	// Parallel benchmarks open one reader per worker, but reporting worker-summed
	// graph_rows makes the fixture look larger than it is. Keep absolute shape
	// counters at the per-reader maximum and sum only mutable search counters.
	if right.Rows > left.Rows {
		left.Rows = right.Rows
	}
	if right.Granules > left.Granules {
		left.Granules = right.Granules
	}
	if right.OpenGranulesRead > left.OpenGranulesRead {
		left.OpenGranulesRead = right.OpenGranulesRead
	}
	if right.OpenPhysicalBytesRead > left.OpenPhysicalBytesRead {
		left.OpenPhysicalBytesRead = right.OpenPhysicalBytesRead
	}
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
