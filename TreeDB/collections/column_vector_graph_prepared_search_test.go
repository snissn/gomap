package collections

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestColumnVectorGraphPreparedSearchSelectedAndEquivalent2045(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("combined prepared graph-search view requires mmap_direct test support")
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0, 0}},
		{id: "doc-b", vector: []float32{0.7, 0.3, 0, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1, 0}},
		{id: "doc-e", vector: []float32{0, 0, 0, 1}},
		{id: "doc-f", vector: []float32{0.4, 0.1, 0.5, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 4, 4, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.preparedSearch == nil || !reader.preparedSearch.ready() {
		t.Fatalf("preparedSearch=%v ready=%v want combined prepared view selected", reader.preparedSearch != nil, reader.preparedSearch != nil && reader.preparedSearch.ready())
	}
	query := []float32{1, 0, 0, 0}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 3, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	assertColumnGraphNativeSearchResultsV3(t, got, exactColumnGraphTopKForTest(t, rows, query, 3))
	assertColumnVectorGraphPreparedSearchStats2045(t, stats, len(got))
	if readerStats := reader.Stats(); readerStats.Rows != 0 || readerStats.RowFetches != 0 || readerStats.RowsFetched != 0 || readerStats.PhysicalBytesRead != 0 {
		t.Fatalf("reader stats=%+v want no graph-row reader residency or reads", readerStats)
	}
}

func TestColumnVectorGraphPreparedSearchMinimalStats2042(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("combined prepared graph-search view requires mmap_direct test support")
	}
	rows := columnGraphRebuildSyntheticRowsV2A(48, 12)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 12, 6, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.preparedSearch == nil || !reader.preparedSearch.ready() {
		t.Fatalf("preparedSearch=%v ready=%v want combined prepared view selected", reader.preparedSearch != nil, reader.preparedSearch != nil && reader.preparedSearch.ready())
	}
	query := append([]float32(nil), rows[7].vector...)
	var fullScratch columnVectorGraphNativeSearchScratch
	fullResults, fullStats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 6, EfSearch: 32, StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics}, &fullScratch)
	if err != nil {
		t.Fatalf("full SearchCosine: %v", err)
	}
	var minimalScratch columnVectorGraphNativeSearchScratch
	minimalResults, minimalStats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 6, EfSearch: 32, StatsMode: columnVectorGraphNativeSearchStatsModeMinimal}, &minimalScratch)
	if err != nil {
		t.Fatalf("minimal SearchCosine: %v", err)
	}
	if mismatch := columnGraphNativeSearchResultsMismatchV3(minimalResults, fullResults); mismatch != "" {
		t.Fatal(mismatch)
	}
	assertColumnVectorGraphPreparedSearchStats2045(t, fullStats, len(fullResults))
	assertColumnVectorGraphFullDiagnosticsStats2126(t, fullStats)
	assertColumnVectorGraphPreparedMinimalStats2042(t, minimalStats, fullStats, len(minimalResults))

	var debugReferenceScratch columnVectorGraphNativeSearchScratch
	debugReferenceResults, _, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 6, EfSearch: len(rows), StatsMode: columnVectorGraphNativeSearchStatsModeFullDiagnostics}, &debugReferenceScratch)
	if err != nil {
		t.Fatalf("benchmark-debug reference SearchCosine: %v", err)
	}
	var debugScratch columnVectorGraphNativeSearchScratch
	debugResults, debugStats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 6, EfSearch: len(rows), StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug}, &debugScratch)
	if err != nil {
		t.Fatalf("benchmark-debug SearchCosine: %v", err)
	}
	if mismatch := columnGraphNativeSearchResultsMismatchV3(debugResults, debugReferenceResults); mismatch != "" {
		t.Fatalf("benchmark-debug %s", mismatch)
	}
	assertColumnVectorGraphBenchmarkDebugStats2126(t, debugStats)

	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	publicMinimal, err := searcher.Search(VectorIndexSearcherSearchOptions{Query: query, TopK: 6, EfSearch: 32, StatsMode: VectorIndexSearchStatsModeMinimal})
	if err != nil {
		t.Fatalf("public minimal Search: %v", err)
	}
	assertColumnVectorGraphPreparedPublicMinimalStats2042(t, publicMinimal.Stats, len(publicMinimal.Results))
	publicProduction, err := searcher.Search(VectorIndexSearcherSearchOptions{Query: query, TopK: 6, EfSearch: 32, StatsMode: VectorIndexSearchStatsModeProduction})
	if err != nil {
		t.Fatalf("public production Search: %v", err)
	}
	if mismatch := vectorIndexSearchResultsMismatch1969(publicProduction.Results, publicMinimal.Results); mismatch != "" {
		t.Fatalf("public production %s", mismatch)
	}
	assertColumnVectorGraphPreparedPublicMinimalStats2042(t, publicProduction.Stats, len(publicProduction.Results))
}

func TestColumnVectorGraphPreparedSearchPublicDocumentsReopen2045(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("combined prepared graph-search view requires mmap_direct test support")
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0, 0}},
		{id: "doc-b", vector: []float32{0.8, 0.2, 0, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1, 0}},
	}
	dir, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 4, 3, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := []float32{1, 0, 0, 0}
	before, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: 2, EfSearch: len(rows), IncludeDocuments: true, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("SearchVectorIndex before reopen: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, before, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, before.Results, exactColumnGraphTopKForTest(t, rows, query, 2), true)
	assertColumnVectorGraphPreparedPublicStats2045(t, before.Stats, len(before.Results))
	assertVectorIndexSearchDocumentDIDV4(t, before.Results[0].Document, string(before.Results[0].ID))
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	after, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: 2, EfSearch: len(rows), IncludeDocuments: true, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("SearchVectorIndex after reopen: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, after, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, after.Results, exactColumnGraphTopKForTest(t, rows, query, 2), true)
	assertColumnVectorGraphPreparedPublicStats2045(t, after.Stats, len(after.Results))
	if len(after.Results) != len(before.Results) {
		t.Fatalf("reopen results len=%d want %d", len(after.Results), len(before.Results))
	}
	for i := range after.Results {
		if string(after.Results[i].ID) != string(before.Results[i].ID) || after.Results[i].Ordinal != before.Results[i].Ordinal || string(after.Results[i].Document) != string(before.Results[i].Document) {
			t.Fatalf("result[%d] after=%+v before=%+v", i, after.Results[i], before.Results[i])
		}
	}
}

func TestColumnVectorGraphPreparedSearchNonMmapCompatibility2045(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(24, 8)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 8, 4, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	forceColumnVectorGraphPreparedSearchNonMmapCompatibility2045(reader)
	if err := maybePrepareColumnVectorGraphPreparedSearchView(reader); err != nil {
		t.Fatalf("maybePrepareColumnVectorGraphPreparedSearchView: %v", err)
	}
	if reader.preparedSearch != nil {
		t.Fatalf("preparedSearch=%v want nil compatibility/source route when mmap-direct prerequisites are unavailable", reader.preparedSearch)
	}
	query := append([]float32(nil), rows[5].vector...)
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: len(rows), StatsMode: columnVectorGraphNativeSearchStatsModeMinimal}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	assertColumnGraphNativeSearchResultsV3(t, got, exactColumnGraphTopKForTest(t, rows, query, 5))
	if stats.PreparedGraphSearchViews != 0 || stats.GraphRowFallbacks != 0 || stats.ResultIDGraphFallbacks != 0 {
		t.Fatalf("stats=%+v want source compatibility route without graph-row fallback", stats)
	}
	if stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 || stats.NormHeapCopyTypedViews+stats.NormScratchDecodes == 0 || stats.AdjacencyTypedListHeapCopyTypedViews+stats.AdjacencyTypedListScratchDecodes == 0 {
		t.Fatalf("stats=%+v want non-mmap vector/norm/adjacency compatibility counters", stats)
	}
	if stats.Candidates == 0 || stats.Edges == 0 || stats.VisitedEdges != stats.Edges {
		t.Fatalf("stats=%+v want fallback traversal counters preserved in minimal mode", stats)
	}
	assertColumnVectorGraphDebugCountersZero2126(t, stats)
}

func TestColumnVectorGraphPreparedSearchStaleStateFailsClosed2045(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("combined prepared graph-search view requires mmap_direct test support")
	}
	rows := columnGraphRebuildSyntheticRowsV2A(12, 6)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 6, 4, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.preparedSearch == nil {
		t.Fatal("preparedSearch is nil")
	}
	if err := reader.documentIDSource.Close(); err != nil {
		t.Fatalf("close document ID source: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	_, _, err = reader.SearchCosine(rows[0].vector, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: len(rows)}, &scratch)
	if err == nil || !strings.Contains(err.Error(), "combined prepared graph-search view is stale") {
		t.Fatalf("SearchCosine err=%v want stale combined prepared fail-closed", err)
	}
}

func TestColumnVectorGraphPreparedSearchSinglePartRowMapFailsClosed2127(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("combined prepared graph-search view requires mmap_direct test support")
	}
	rows := columnGraphRebuildSyntheticRowsV2A(16, 6)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 6, 4, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.preparedSearch == nil || reader.preparedSearch.vector.singlePart == nil {
		t.Fatalf("preparedSearch=%v singlePart=%v want single-part prepared view", reader.preparedSearch != nil, reader.preparedSearch != nil && reader.preparedSearch.vector.singlePart != nil)
	}
	rowIndexByOrdinal := make([]uint32, len(rows)-1)
	for ordinal := range rowIndexByOrdinal {
		rowIndexByOrdinal[ordinal] = uint32(ordinal)
	}
	reader.preparedSearch.vector.rowIndexByOrdinal = rowIndexByOrdinal
	reader.preparedSearch.vectorIdentityMapping = false

	var scratch columnVectorGraphNativeSearchScratch
	_, _, err = reader.SearchCosine(rows[0].vector, columnVectorGraphNativeSearchOptions{TopK: 4, EfSearch: len(rows)}, &scratch)
	if err == nil || !strings.Contains(err.Error(), "combined prepared graph-search view is stale") || !strings.Contains(err.Error(), "row map") {
		t.Fatalf("SearchCosine err=%v want fail-closed stale prepared single-part row map", err)
	}
}

func TestColumnVectorGraphPreparedSearchParallelScratchSafety2045(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("combined prepared graph-search view requires mmap_direct test support")
	}
	rows := columnGraphRebuildSyntheticRowsV2A(96, 16)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 16, 8, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.preparedSearch == nil || !reader.preparedSearch.ready() {
		t.Fatalf("preparedSearch=%v ready=%v want shared immutable combined prepared view", reader.preparedSearch != nil, reader.preparedSearch != nil && reader.preparedSearch.ready())
	}
	const workers = 4
	query := append([]float32(nil), rows[11].vector...)
	baseline, _, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 8, EfSearch: 64}, &columnVectorGraphNativeSearchScratch{})
	if err != nil {
		t.Fatalf("baseline SearchCosine: %v", err)
	}
	baseline = cloneColumnVectorGraphPreparedResults2045(baseline)
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			var scratch columnVectorGraphNativeSearchScratch
			for i := 0; i < 25; i++ {
				got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 8, EfSearch: 64, StatsMode: columnVectorGraphNativeSearchStatsModeMinimal}, &scratch)
				if err != nil {
					errs <- fmt.Sprintf("worker %d iteration %d SearchCosine: %v", worker, i, err)
					return
				}
				if mismatch := columnGraphNativeSearchResultsMismatchV3(got, baseline); mismatch != "" {
					errs <- fmt.Sprintf("worker %d iteration %d: %s", worker, i, mismatch)
					return
				}
				if stats.PreparedGraphSearchViews != 1 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.ResultIDGraphFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 {
					errs <- fmt.Sprintf("worker %d iteration %d stats=%+v want combined prepared without fallback", worker, i, stats)
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

func assertColumnVectorGraphPreparedSearchStats2045(tb testing.TB, stats columnVectorGraphNativeSearchStats, results int) {
	tb.Helper()
	if stats.PreparedGraphSearchViews != 1 || stats.GraphRowFallbacks != 0 {
		tb.Fatalf("stats=%+v want one combined prepared view and zero graph-row fallback", stats)
	}
	if stats.TypedColumnFallbacks != 0 || stats.NormSourceFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencySourceFallbacks != 0 || stats.ResultIDGraphFallbacks != 0 {
		tb.Fatalf("stats=%+v want zero compatibility/source fallbacks", stats)
	}
	if stats.PreparedScoreCalls == 0 || stats.PreparedScoreCalls != stats.CandidateFetches || stats.VectorPreparedDirectViews != stats.CandidateFetches || stats.NormPreparedDirectViews != stats.CandidateFetches {
		tb.Fatalf("stats=%+v want prepared scoring to cover all candidate fetches", stats)
	}
	if stats.AdjacencyPreparedCSRMmapDirectViews == 0 || stats.AdjacencyTypedListMmapDirectViews != 0 || stats.AdjacencyTypedListScratchDecodes != 0 {
		tb.Fatalf("stats=%+v want prepared CSR adjacency only", stats)
	}
	if stats.ResultFetches != uint64(results) || stats.ResultIDPreparedBytesViews != 1 || stats.ResultIDTypedBytesState != uint64(results) || stats.RowRefStatePreparedViews != 1 || stats.RowRefStateResultRefs != uint64(results) {
		tb.Fatalf("stats=%+v want prepared result IDs/row refs for %d results", stats, results)
	}
}

func assertColumnVectorGraphPreparedPublicStats2045(tb testing.TB, stats VectorIndexSearchStats, results int) {
	tb.Helper()
	if stats.PreparedGraphSearchViews != 1 || stats.GraphRowFallbacks != 0 || stats.GraphRows != 0 || stats.RowFetches != 0 || stats.RowsFetched != 0 {
		tb.Fatalf("stats=%+v want combined prepared public search without graph rows", stats)
	}
	if stats.TypedColumnFallbacks != 0 || stats.NormSourceFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencySourceFallbacks != 0 || stats.ResultIDGraphFallbacks != 0 || stats.RowRefVectorSourceLegacyGraphIDs != 0 {
		tb.Fatalf("stats=%+v want zero public compatibility/source fallbacks", stats)
	}
	if stats.ResultIDPreparedBytesViews != 1 || stats.ResultIDTypedBytesState != uint64(results) || stats.RowRefStatePreparedViews != 1 || stats.RowRefStateResultRefs != uint64(results) {
		tb.Fatalf("stats=%+v want prepared result IDs and row refs for %d results", stats, results)
	}
}

func assertColumnVectorGraphFullDiagnosticsStats2126(tb testing.TB, stats columnVectorGraphNativeSearchStats) {
	tb.Helper()
	if stats.Candidates == 0 || stats.Edges == 0 || stats.VisitedEdges != stats.Edges {
		tb.Fatalf("full diagnostics stats=%+v want detailed candidate/edge traversal counters", stats)
	}
	assertColumnVectorGraphDebugCountersZero2126(tb, stats)
}

func assertColumnVectorGraphBenchmarkDebugStats2126(tb testing.TB, stats columnVectorGraphNativeSearchStats) {
	tb.Helper()
	if stats.Candidates == 0 || stats.Edges == 0 || stats.VisitedEdges != stats.Edges {
		tb.Fatalf("benchmark debug stats=%+v want full traversal counters", stats)
	}
	if stats.BenchmarkDebugSearches != 1 || stats.NeighborTiles == 0 || stats.Layer0EdgeVisits == 0 || stats.FrontierPushes == 0 || stats.TopKInsertAttempts == 0 || stats.VisitedMarkChecks == 0 || stats.ExactModeSearches != 1 || stats.ExactCandidateOrderObservations == 0 {
		tb.Fatalf("benchmark debug stats=%+v want tile/frontier/top-k/visited/exact-order counters", stats)
	}
}

func assertColumnVectorGraphDebugCountersZero2126(tb testing.TB, stats columnVectorGraphNativeSearchStats) {
	tb.Helper()
	if stats.BenchmarkDebugSearches != 0 || stats.NeighborTiles != 0 || stats.Layer0EdgeVisits != 0 || stats.FrontierPushes != 0 || stats.TopKInsertAttempts != 0 || stats.VisitedMarkChecks != 0 || stats.ExactCandidateOrderObservations != 0 {
		tb.Fatalf("stats=%+v want benchmark/debug counters suppressed", stats)
	}
}

func assertVectorIndexSearchDebugCountersZero2126(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.BenchmarkDebugSearches != 0 || stats.NeighborTiles != 0 || stats.Layer0EdgeVisits != 0 || stats.FrontierPushes != 0 || stats.TopKInsertAttempts != 0 || stats.VisitedMarkChecks != 0 || stats.ExactCandidateOrderObservations != 0 {
		tb.Fatalf("stats=%+v want benchmark/debug counters suppressed", stats)
	}
}

func assertColumnVectorGraphPreparedMinimalStats2042(tb testing.TB, stats, full columnVectorGraphNativeSearchStats, results int) {
	tb.Helper()
	if stats.PreparedGraphSearchViews != 1 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.ResultIDGraphFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 {
		tb.Fatalf("minimal stats=%+v want prepared path with zero graph-row/source fallback", stats)
	}
	if stats.CandidateRows == 0 || stats.CandidateRows != full.CandidateRows {
		tb.Fatalf("minimal stats=%+v full=%+v want admission candidate rows without changing search domain", stats, full)
	}
	if stats.Candidates != 0 || stats.Edges != 0 || stats.VisitedEdges != 0 {
		tb.Fatalf("minimal stats=%+v want no detailed per-candidate/per-edge traversal diagnostics", stats)
	}
	assertColumnVectorGraphDebugCountersZero2126(tb, stats)
	if stats.PreparedScoreCalls == 0 || stats.PreparedScoreCalls != stats.CandidateFetches || stats.VectorPreparedDirectViews != stats.CandidateFetches || stats.NormPreparedDirectViews != stats.CandidateFetches {
		tb.Fatalf("minimal stats=%+v want prepared score/direct health counters", stats)
	}
	if stats.AdjacencyPreparedCSRMmapDirectViews == 0 || stats.AdjacencyTypedListMmapDirectViews != 0 || stats.AdjacencyTypedListScratchDecodes != 0 {
		tb.Fatalf("minimal stats=%+v want prepared CSR adjacency health counters only", stats)
	}
	if stats.VectorScratchDecodes != 0 || stats.NormScratchDecodes != 0 || stats.AdjacencySourceFallbacks != 0 || stats.NormSourceFallbacks != 0 || stats.RowRefStateSourceFallbacks != 0 {
		tb.Fatalf("minimal stats=%+v want healthy source fallback counters to remain zero", stats)
	}
	if stats.ResultFetches != uint64(results) || stats.ResultIDPreparedBytesViews != 1 || stats.ResultIDTypedBytesState != uint64(results) || stats.RowRefStatePreparedViews != 1 || stats.RowRefStateResultRefs != uint64(results) {
		tb.Fatalf("minimal stats=%+v want prepared result IDs/row refs for %d results", stats, results)
	}
}

func assertColumnVectorGraphPreparedPublicMinimalStats2042(tb testing.TB, stats VectorIndexSearchStats, results int) {
	tb.Helper()
	if stats.PreparedGraphSearchViews != 1 || stats.GraphRowFallbacks != 0 || stats.GraphRows != 0 || stats.RowFetches != 0 || stats.RowsFetched != 0 {
		tb.Fatalf("public minimal stats=%+v want combined prepared public search without graph rows", stats)
	}
	if stats.TypedColumnFallbacks != 0 || stats.NormSourceFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencySourceFallbacks != 0 || stats.ResultIDGraphFallbacks != 0 || stats.RowRefVectorSourceLegacyGraphIDs != 0 {
		tb.Fatalf("public minimal stats=%+v want zero public compatibility/source fallbacks", stats)
	}
	if stats.CandidateRows == 0 || stats.Candidates != 0 || stats.Edges != 0 || stats.VisitedEdges != 0 {
		tb.Fatalf("public minimal stats=%+v want admission rows but no detailed traversal diagnostics", stats)
	}
	assertVectorIndexSearchDebugCountersZero2126(tb, stats)
	if stats.CandidateFetches == 0 || stats.PreparedScoreCalls != stats.CandidateFetches || stats.VectorPreparedDirectViews != stats.CandidateFetches || stats.NormPreparedDirectViews != stats.CandidateFetches {
		tb.Fatalf("public minimal stats=%+v want production prepared score/source health counters", stats)
	}
	if stats.AdjacencyPreparedCSRMmapDirectViews == 0 || stats.VectorScratchDecodes != 0 || stats.NormScratchDecodes != 0 || stats.AdjacencyTypedListScratchDecodes != 0 {
		tb.Fatalf("public minimal stats=%+v want healthy prepared vector/norm/adjacency source counters", stats)
	}
	if stats.ResultIDPreparedBytesViews != 1 || stats.ResultIDTypedBytesState != uint64(results) || stats.RowRefStatePreparedViews != 1 || stats.RowRefStateResultRefs != uint64(results) {
		tb.Fatalf("public minimal stats=%+v want prepared result IDs and row refs for %d results", stats, results)
	}
}

func forceColumnVectorGraphPreparedSearchNonMmapCompatibility2045(reader *columnVectorGraphPhysicalRowReader) {
	if reader == nil {
		return
	}
	reader.preparedSearch = nil
	if reader.typedVectorSource != nil {
		reader.typedVectorSource.prepared = columnVectorGraphPreparedVectorView{}
		for _, part := range reader.typedVectorSource.parts {
			if part != nil {
				part.outcome = columnVectorGraphTypedColumnVectorOutcomeHeapCopyTypedView
				part.fallbackReason = ""
			}
		}
	}
	if reader.invNormSource != nil {
		reader.invNormSource.prepared = columnVectorGraphPreparedNormView{}
		reader.invNormSource.outcome = columnVectorGraphInvNormStateOutcomeHeapCopyTypedView
		reader.invNormSource.fallbackReason = ""
	}
	if reader.adjacencyLayerSources != nil {
		for _, source := range reader.adjacencyLayerSources.sources {
			if source != nil {
				source.outcome = columnVectorGraphLayer0AdjacencySourceOutcomeTypedListHeapCopyTypedView
			}
		}
	}
}

func cloneColumnVectorGraphPreparedResults2045(in []columnVectorGraphNativeSearchResult) []columnVectorGraphNativeSearchResult {
	out := make([]columnVectorGraphNativeSearchResult, len(in))
	for i, result := range in {
		out[i] = result
		out[i].ID = append([]byte(nil), result.ID...)
	}
	return out
}
