package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 = 8

type columnVectorGraphNativeSearchBenchShapeV3 struct {
	rows                int
	dims                int
	m                   int
	topK                int
	efSearch            int
	queryOrdinal        int
	directPhysicalAsset bool
	typedColumnVector   bool
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
	return fmt.Sprintf("rows=%d/dims=%d/degree=%d/topK=%d/efSearch=%d", shape.rows, shape.dims, shape.m, shape.topK, shape.efSearch)
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
	if stats.Candidates != 4 || stats.CandidateFetches != 4 || stats.ScoreBatches != 4 || stats.ExpansionFetches != stats.AdjacencyExpansions {
		t.Fatalf("stats=%+v want four scored candidates plus lazy adjacency expansion fetches", stats)
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
	if err := scratch.prepare(64, 3, 2, 64, 64); err != nil {
		t.Fatalf("prepare large: %v", err)
	}
	if len(scratch.visitMarks) != 64 {
		t.Fatalf("large visitMarks len=%d want 64", len(scratch.visitMarks))
	}
	if cap(scratch.frontier) < 64 || cap(scratch.top) < 64 || cap(scratch.results) < 64 || cap(scratch.idBuffers) < 64 || cap(scratch.resultOrder) < 64 || cap(scratch.resultOrdinals) < 64 {
		t.Fatalf("large scratch caps frontier=%d top=%d results=%d idBuffers=%d resultOrder=%d resultOrdinals=%d want at least 64", cap(scratch.frontier), cap(scratch.top), cap(scratch.results), cap(scratch.idBuffers), cap(scratch.resultOrder), cap(scratch.resultOrdinals))
	}
	if err := scratch.prepare(1, 3, 2, 1, 1); err != nil {
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
	if err := scratch.prepare(1, 3, 2, 1, 1); err != nil {
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
	if err := scratch.prepare(2, 3, 2, 2, 2); err != nil {
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
	if err := scratch.prepare(1, 3, 2, 1, 1); err != nil {
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
	if err := scratch.prepare(1, 3, 2, 1, 1); err != nil {
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
	opts := columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch}
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
		searchStats.BlockViewHits += stats.BlockViewHits
		searchStats.BlockViewMisses += stats.BlockViewMisses
		searchStats.BlockViewBuilds += stats.BlockViewBuilds
		searchStats.AdjacencyExpansions += stats.AdjacencyExpansions
		searchStats.AdjacencyScratchDecodes += stats.AdjacencyScratchDecodes
		searchStats.AdjacencyDirectViews += stats.AdjacencyDirectViews
		searchStats.AdjacencyMmapDirectViews += stats.AdjacencyMmapDirectViews
		searchStats.AdjacencyHeapCopyTypedViews += stats.AdjacencyHeapCopyTypedViews
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
	opts := columnVectorGraphNativeSearchOptions{TopK: shape.topK, EfSearch: shape.efSearch}
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
	var totalBlockViewHits atomic.Uint64
	var totalBlockViewMisses atomic.Uint64
	var totalBlockViewBuilds atomic.Uint64
	var totalAdjacencyExpansions atomic.Uint64
	var totalAdjacencyScratchDecodes atomic.Uint64
	var totalAdjacencyDirectViews atomic.Uint64
	var totalAdjacencyMmapDirectViews atomic.Uint64
	var totalAdjacencyHeapCopyTypedViews atomic.Uint64
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
			localStats.BlockViewHits += stats.BlockViewHits
			localStats.BlockViewMisses += stats.BlockViewMisses
			localStats.BlockViewBuilds += stats.BlockViewBuilds
			localStats.AdjacencyExpansions += stats.AdjacencyExpansions
			localStats.AdjacencyScratchDecodes += stats.AdjacencyScratchDecodes
			localStats.AdjacencyDirectViews += stats.AdjacencyDirectViews
			localStats.AdjacencyMmapDirectViews += stats.AdjacencyMmapDirectViews
			localStats.AdjacencyHeapCopyTypedViews += stats.AdjacencyHeapCopyTypedViews
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
		totalBlockViewHits.Add(localStats.BlockViewHits)
		totalBlockViewMisses.Add(localStats.BlockViewMisses)
		totalBlockViewBuilds.Add(localStats.BlockViewBuilds)
		totalAdjacencyExpansions.Add(localStats.AdjacencyExpansions)
		totalAdjacencyScratchDecodes.Add(localStats.AdjacencyScratchDecodes)
		totalAdjacencyDirectViews.Add(localStats.AdjacencyDirectViews)
		totalAdjacencyMmapDirectViews.Add(localStats.AdjacencyMmapDirectViews)
		totalAdjacencyHeapCopyTypedViews.Add(localStats.AdjacencyHeapCopyTypedViews)
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
		BlockViewHits:                        totalBlockViewHits.Load(),
		BlockViewMisses:                      totalBlockViewMisses.Load(),
		BlockViewBuilds:                      totalBlockViewBuilds.Load(),
		AdjacencyExpansions:                  totalAdjacencyExpansions.Load(),
		AdjacencyScratchDecodes:              totalAdjacencyScratchDecodes.Load(),
		AdjacencyDirectViews:                 totalAdjacencyDirectViews.Load(),
		AdjacencyMmapDirectViews:             totalAdjacencyMmapDirectViews.Load(),
		AdjacencyHeapCopyTypedViews:          totalAdjacencyHeapCopyTypedViews.Load(),
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
}

func openColumnVectorGraphNativeSearchBenchFixtureV3(b *testing.B, shape columnVectorGraphNativeSearchBenchShapeV3) (func(), *Collection, VectorIndexDefinition, []float32) {
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
		BaseSchemaHash:         baseCfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               prepared.RowCount,
		AssetRef:               prepared.Ref,
		AssetBytes:             prepared.Bytes,
	}
	identity := ColumnManifestIdentity{Generation: generation, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, manifestIdentity := testColumnGraphManifestRecordsFromSnapshot1920(tb, *baseCfg, graph, identity)
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
	b.ReportMetric(float64(searchStats.BlockViewHits)/float64(n), "block_view_hits/search")
	b.ReportMetric(float64(searchStats.BlockViewMisses)/float64(n), "block_view_misses/search")
	b.ReportMetric(float64(searchStats.BlockViewBuilds)/float64(n), "block_view_builds/search")
	b.ReportMetric(float64(searchStats.AdjacencyExpansions)/float64(n), "adjacency_expansions/search")
	b.ReportMetric(float64(searchStats.AdjacencyScratchDecodes)/float64(n), "adjacency_scratch_decodes/search")
	b.ReportMetric(float64(searchStats.AdjacencyDirectViews)/float64(n), "adjacency_direct_views/search")
	b.ReportMetric(float64(searchStats.AdjacencyMmapDirectViews)/float64(n), "adjacency_mmap_direct/search")
	b.ReportMetric(float64(searchStats.AdjacencyHeapCopyTypedViews)/float64(n), "adjacency_heap_copy_typed_view/search")
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
	b.ReportMetric(float64(searchStats.TypedColumnMappedBytes), "mapped_B")
	b.ReportMetric(float64(searchStats.TypedColumnHeapCopyBytes), "heap_copy_B")
	b.ReportMetric(float64(searchStats.TypedColumnDecodedBytes), "decoded_derived_B")
	b.ReportMetric(float64(searchStats.TypedColumnActiveHandles), "active_handles")
	b.ReportMetric(float64(searchStats.TypedColumnDeniedResources), "denied_resources")
	b.ReportMetric(float64(searchStats.ExpansionFetches)/float64(n), "expansion_fetches/search")
	b.ReportMetric(float64(searchStats.ResultFetches)/float64(n), "result_fetches/search")
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
