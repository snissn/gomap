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
	if stats.Edges == 0 || stats.CandidateFetches != uint64(len(rows)) || stats.ScoreBatches != uint64(len(rows)) || stats.ExpansionFetches != 0 || stats.ResultFetches != uint64(len(got)) {
		t.Fatalf("stats=%+v want candidate/result fetches without duplicate expansion row fetches", stats)
	}
	readerStats := reader.Stats()
	if readerStats.RowFetches != 0 || readerStats.BatchFetches != 0 || readerStats.RowsFetched != 0 {
		t.Fatalf("reader stats=%+v want no generic row fetches for scoring or result IDs", readerStats)
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
	if stats.Candidates != 4 || stats.CandidateFetches != 4 || stats.ScoreBatches != 4 || stats.ExpansionFetches != 0 {
		t.Fatalf("stats=%+v want four scored candidates without duplicate expansion row fetches", stats)
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

func TestColumnVectorGraphNativeSearchScratchClearsCandidateAdjacencyRefsV3(t *testing.T) {
	var scratch columnVectorGraphNativeSearchScratch
	scratch.frontier = append(scratch.frontier, columnVectorGraphSearchCandidate{
		ordinal:   1,
		score:     1,
		adjacency: []uint32{1, 2},
	})
	scratch.top = append(scratch.top, columnVectorGraphSearchCandidate{
		ordinal:   2,
		score:     2,
		adjacency: []uint32{3, 4},
	})
	if err := scratch.prepare(4, 3, 2, 1, 2); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	assertColumnVectorGraphCandidateScratchNoAdjacencyRefsV3(t, "frontier after prepare", scratch.frontier)
	assertColumnVectorGraphCandidateScratchNoAdjacencyRefsV3(t, "top after prepare", scratch.top)

	scratch.frontier = append(scratch.frontier,
		columnVectorGraphSearchCandidate{ordinal: 1, score: 1, adjacency: []uint32{1}},
		columnVectorGraphSearchCandidate{ordinal: 2, score: 2, adjacency: []uint32{2}},
	)
	if _, ok := scratch.popFrontier(); !ok {
		t.Fatalf("popFrontier returned ok=false")
	}
	backing := scratch.frontier[:cap(scratch.frontier)]
	for i := len(scratch.frontier); i < len(backing); i++ {
		if backing[i].adjacency != nil {
			t.Fatalf("frontier backing slot %d retained adjacency %v after pop", i, backing[i].adjacency)
		}
	}
}

func assertColumnVectorGraphCandidateScratchNoAdjacencyRefsV3(t *testing.T, label string, candidates []columnVectorGraphSearchCandidate) {
	t.Helper()
	backing := candidates[:cap(candidates)]
	for i, candidate := range backing {
		if candidate.adjacency != nil {
			t.Fatalf("%s backing slot %d retained adjacency %v", label, i, candidate.adjacency)
		}
	}
}

func TestColumnVectorGraphNativeSearchRejectsBadAdjacencyOrdinalV3(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-start"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{2}},
		{ID: []byte("doc-other"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var scratch columnVectorGraphNativeSearchScratch
	_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 2}, &scratch)
	if !errors.Is(err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds) {
		t.Fatalf("SearchCosine err=%v want adjacency bounds sentinel", err)
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

func TestColumnVectorGraphNativeCosineScoreFallsBackWhenFloat32DotUnderflowsV3(t *testing.T) {
	tiny := float32(math.SmallestNonzeroFloat32)
	got, err := columnVectorGraphNativeCosineScore([]float32{tiny}, 1, columnVectorGraphPhysicalRow{
		Ordinal: 7,
		Vector:  []float32{tiny},
		InvNorm: 1,
	})
	if err != nil {
		t.Fatalf("columnVectorGraphNativeCosineScore: %v", err)
	}
	if got <= 0 {
		t.Fatalf("columnVectorGraphNativeCosineScore=%g want positive float64 fallback score", got)
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
		searchStats.Candidates += stats.Candidates
		searchStats.Edges += stats.Edges
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
	var totalCandidates atomic.Uint64
	var totalEdges atomic.Uint64
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
			localStats.Candidates += stats.Candidates
			localStats.Edges += stats.Edges
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
		}
		sink.Add(localSink)
		totalCandidates.Add(localStats.Candidates)
		totalEdges.Add(localStats.Edges)
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
		Candidates:              totalCandidates.Load(),
		Edges:                   totalEdges.Load(),
		CandidateFetches:        totalCandidateFetches.Load(),
		ExpansionFetches:        totalExpansionFetches.Load(),
		ResultFetches:           totalResultFetches.Load(),
		ScoreBatches:            totalScoreBatches.Load(),
		OrdinalsGrouped:         totalOrdinalsGrouped.Load(),
		BlockViewHits:           totalBlockViewHits.Load(),
		BlockViewMisses:         totalBlockViewMisses.Load(),
		BlockViewBuilds:         totalBlockViewBuilds.Load(),
		AdjacencyExpansions:     totalAdjacencyExpansions.Load(),
		AdjacencyScratchDecodes: totalAdjacencyScratchDecodes.Load(),
		AdjacencyDirectViews:    totalAdjacencyDirectViews.Load(),
	})
}

func reportColumnGraphNativeSearchBenchShapeMetricsV3(b *testing.B, shape columnVectorGraphNativeSearchBenchShapeV3) {
	b.Helper()
	b.ReportMetric(float64(shape.rows), "rows")
	b.ReportMetric(float64(shape.dims), "dims")
	b.ReportMetric(float64(shape.m), "degree")
	b.ReportMetric(float64(shape.topK), "top_k")
	b.ReportMetric(float64(shape.efSearch), "ef_search")
}

func openColumnVectorGraphNativeSearchBenchFixtureV3(b *testing.B, shape columnVectorGraphNativeSearchBenchShapeV3) (func(), *Collection, VectorIndexDefinition, []float32) {
	b.Helper()
	if shape.queryOrdinal < 0 || shape.queryOrdinal >= shape.rows {
		b.Fatalf("query ordinal=%d out of range rows=%d", shape.queryOrdinal, shape.rows)
	}
	if shape.directPhysicalAsset {
		rows := columnVectorGraphNativeSearchBenchAssetRowsV3(b, shape.rows, shape.dims, shape.m)
		d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(b, shape.dims, shape.m, rows)
		query := append([]float32(nil), rows[shape.queryOrdinal].Vector...)
		return func() { _ = d.Close() }, col, def, query
	}
	input := columnGraphRebuildSyntheticRowsV2A(shape.rows, shape.dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, shape.dims, shape.m, input)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = d.Close()
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(b, status, def.Name)
	query := append([]float32(nil), input[shape.queryOrdinal].vector...)
	return func() { _ = d.Close() }, col, def, query
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
		if got[i].Ordinal != want[i].Ordinal || !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
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
	b.ReportMetric(float64(searchStats.Candidates)/float64(n), "candidates/search")
	b.ReportMetric(float64(searchStats.Edges)/float64(n), "edges/search")
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
