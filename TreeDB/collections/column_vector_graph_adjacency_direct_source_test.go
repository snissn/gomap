package collections

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

func TestColumnVectorGraphLayer0AdjacencySourceSearchParity1919(t *testing.T) {
	const (
		rows = 96
		dims = 32
		m    = 12
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[17].vector...)
	directReader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open direct reader: %v", err)
	}
	defer func() { _ = directReader.Close() }()
	if directReader.layer0AdjacencySource == nil {
		t.Fatalf("direct reader missing layer-0 adjacency source fallback=%s unavailable=%v", directReader.layer0AdjacencySourceFallbackReason, directReader.layer0AdjacencySourceUnavailable)
	}
	var directScratch columnVectorGraphNativeSearchScratch
	directResults, directStats, err := directReader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64}, &directScratch)
	if err != nil {
		t.Fatalf("direct SearchCosine: %v", err)
	}
	if len(directResults) == 0 {
		t.Fatal("direct SearchCosine returned no results")
	}
	if directStats.AdjacencyMmapDirectViews+directStats.AdjacencyHeapCopyTypedViews == 0 {
		t.Fatalf("direct stats=%+v want layer-0 direct adjacency source", directStats)
	}

	fallbackReader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open fallback reader: %v", err)
	}
	defer func() { _ = fallbackReader.Close() }()
	disableColumnVectorGraphAdjacencyDirectSourcesForTest(t, fallbackReader)
	var fallbackScratch columnVectorGraphNativeSearchScratch
	_, fallbackStats, err := fallbackReader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64}, &fallbackScratch)
	if err == nil || !strings.Contains(err.Error(), "adjacency graph-row fallback unavailable") {
		t.Fatalf("fallback SearchCosine err=%v want fail-closed missing TVIS adjacency", err)
	}
	if fallbackStats.AdjacencyLegacyFallbacks != 0 || fallbackStats.AdjacencyScratchDecodes != 0 {
		t.Fatalf("fallback stats=%+v want no graph row adjacency reads after TVIS source disabled", fallbackStats)
	}
}

func TestColumnVectorGraphSearchUsesVectorIndexStateAdjacency1988(t *testing.T) {
	const (
		rows = 128
		dims = 32
		m    = 12
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[23].vector...)
	directReader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open direct reader: %v", err)
	}
	defer func() { _ = directReader.Close() }()
	if directReader.adjacencyLayerSources == nil || !directReader.adjacencyLayerSources.allLayers || directReader.layer0AdjacencySource == nil {
		t.Fatalf("adjacency state sources=%+v layer0=%v want typed uint32_list state sources", directReader.adjacencyLayerSources, directReader.layer0AdjacencySource != nil)
	}
	var directScratch columnVectorGraphNativeSearchScratch
	directResults, directStats, err := directReader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64}, &directScratch)
	if err != nil {
		t.Fatalf("direct SearchCosine: %v", err)
	}
	if len(directResults) == 0 {
		t.Fatal("direct SearchCosine returned no results")
	}
	stateDirect := directStats.AdjacencyPreparedCSRMmapDirectViews + directStats.AdjacencyTypedListMmapDirectViews + directStats.AdjacencyTypedListHeapCopyTypedViews + directStats.AdjacencyTypedListScratchDecodes
	if stateDirect == 0 || directStats.AdjacencyLegacyFallbacks != 0 || directStats.AdjacencySourceFallbacks != 0 {
		t.Fatalf("direct stats=%+v want prepared/state adjacency and no legacy fallback", directStats)
	}

	fallbackReader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open fallback reader: %v", err)
	}
	defer func() { _ = fallbackReader.Close() }()
	disableColumnVectorGraphAdjacencyDirectSourcesForTest(t, fallbackReader)
	var fallbackScratch columnVectorGraphNativeSearchScratch
	_, fallbackStats, err := fallbackReader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64}, &fallbackScratch)
	if err == nil || !strings.Contains(err.Error(), "adjacency graph-row fallback unavailable") {
		t.Fatalf("fallback SearchCosine err=%v want fail-closed missing TVIS adjacency", err)
	}
	if fallbackStats.AdjacencyPreparedCSRMmapDirectViews+fallbackStats.AdjacencyTypedListMmapDirectViews+fallbackStats.AdjacencyTypedListHeapCopyTypedViews+fallbackStats.AdjacencyTypedListScratchDecodes != 0 || fallbackStats.AdjacencyLegacyFallbacks != 0 || fallbackStats.AdjacencyScratchDecodes != 0 {
		t.Fatalf("fallback stats=%+v want no legacy graph row adjacency reads after typed-state source disabled", fallbackStats)
	}
}

func TestColumnVectorGraphLayer0AdjacencySourceCertifiedMmapDirect1919(t *testing.T) {
	if !columnVectorGraphLayer0AdjacencyMmapExpectedOnPlatform1919() {
		t.Skipf("mmap direct source is unsupported on %s", runtime.GOOS)
	}
	input := columnGraphRebuildSyntheticRowsV2A(64, 16)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 16, 8, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.layer0AdjacencySource == nil {
		t.Fatalf("layer-0 adjacency source missing fallback=%s", reader.layer0AdjacencySourceFallbackReason)
	}
	if got := reader.layer0AdjacencySource.outcome; got != columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect {
		t.Fatalf("source outcome=%s want prepared_csr_mmap_direct", got)
	}
	var scratch columnVectorGraphNativeSearchScratch
	_, stats, err := reader.SearchCosine(input[11].vector, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: 32}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if stats.AdjacencyPreparedCSRMmapDirectViews == 0 || stats.AdjacencyTypedListMmapDirectViews != 0 || stats.AdjacencyTypedListHeapCopyTypedViews != 0 || stats.AdjacencyTypedListScratchDecodes != 0 || stats.AdjacencySourceFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 {
		t.Fatalf("stats=%+v want prepared CSR mmap direct adjacency and no typed-list/legacy fallback", stats)
	}
}

func TestColumnVectorGraphLayer0AdjacencySourceAbsentFallback1919(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1, 2}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0, 2}},
		{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, InvNorm: 1, Adjacency: []uint32{0, 1}},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.layer0AdjacencySource == nil || reader.adjacencyLayerSources == nil {
		t.Fatalf("source=%v all_layers=%v want typed adjacency state source before explicit row fallback", reader.layer0AdjacencySource != nil, reader.adjacencyLayerSources != nil)
	}
	disableColumnVectorGraphAdjacencyDirectSourcesForTest(t, reader)
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: 3}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(got) == 0 || stats.AdjacencySourceUnavailable != 1 || stats.AdjacencySourceFallbacks != 1 || stats.AdjacencyPreparedCSRMmapDirectViews != 0 || stats.AdjacencyTypedListMmapDirectViews != 0 || stats.AdjacencyScratchDecodes == 0 || stats.AdjacencyLegacyFallbacks == 0 {
		t.Fatalf("results=%d stats=%+v want explicit source-unavailable row fallback", len(got), stats)
	}
}

func TestColumnVectorGraphLegacyAdjacencySourceChecksumIgnoredWithState1989(t *testing.T) {
	d, col, def, graph, rows := openManualColumnGraphAllLayerSourceSearchFixture1921(t, nil)
	defer func() { _ = d.Close() }()
	if !graph.Layer0AdjacencySource.Present {
		t.Fatalf("manual compatibility fixture missing legacy layer-0 source: %+v", graph)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), graph.Layer0AdjacencySource.Ref)
	if err != nil {
		t.Fatalf("read legacy source: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	offsetsSection, _, ok := image.ColumnOffsetsListSections(columnVectorGraphLayer0AdjacencySourceColumnName)
	if !ok || offsetsSection.Length < 16 {
		t.Fatalf("legacy offsets section=%+v ok=%v", offsetsSection, ok)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), graph.Layer0AdjacencySource.Ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile legacy source: %v", err)
	}
	corruptByte := []byte{raw[offsetsSection.Offset+8] ^ 0xff}
	if _, err := file.WriteAt(corruptByte, graph.Layer0AdjacencySource.Ref.Offset+int64(offsetsSection.Offset+8)); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt corrupt legacy source: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt legacy source: %v", err)
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus after corrupt compatibility source: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader after corrupt compatibility source: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.layer0AdjacencySource == nil || reader.adjacencyLayerSources == nil {
		t.Fatalf("source=%v all_layers=%v fallback=%s want typed adjacency state despite corrupt legacy source", reader.layer0AdjacencySource != nil, reader.adjacencyLayerSources != nil, reader.layer0AdjacencySourceFallbackReason)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(rows[0].Vector, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine after corrupt compatibility source: %v", err)
	}
	if len(got) == 0 || stats.AdjacencyPreparedCSRMmapDirectViews+stats.AdjacencyTypedListMmapDirectViews+stats.AdjacencyTypedListHeapCopyTypedViews == 0 || stats.AdjacencySourceUnavailable != 0 || stats.AdjacencySourceFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 {
		t.Fatalf("results=%d stats=%+v want prepared/state adjacency and quarantined legacy source corruption", len(got), stats)
	}
}

func TestColumnVectorGraphLayer0AdjacencySourceStaleHandlesFallback1919(t *testing.T) {
	input := columnGraphRebuildSyntheticRowsV2A(64, 16)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 16, 8, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.layer0AdjacencySource == nil {
		t.Fatalf("layer-0 adjacency source missing fallback=%s", reader.layer0AdjacencySourceFallbackReason)
	}
	releaseColumnVectorGraphAdjacencySourceHandlesForTest(t, reader)
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(input[9].vector, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: 32}, &scratch)
	if err == nil || !strings.Contains(err.Error(), "adjacency graph-row fallback unavailable") {
		t.Fatalf("SearchCosine stale handles err=%v want fail-closed missing graph fallback", err)
	}
	if len(got) != 0 || stats.AdjacencyStaleHandles == 0 || stats.AdjacencySourceFallbacks == 0 || stats.AdjacencyPreparedCSRMmapDirectViews != 0 || stats.AdjacencyTypedListMmapDirectViews != 0 || stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencyScratchDecodes != 0 {
		t.Fatalf("results=%d stats=%+v want stale typed-state handles to fail closed without graph row reads", len(got), stats)
	}
}

func TestVectorIndexSearcherCloseReleasesLayer0AdjacencySource1919(t *testing.T) {
	input := columnGraphRebuildSyntheticRowsV2A(64, 16)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 16, 8, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	source := searcher.reader.layer0AdjacencySource
	if source == nil || source.manager == nil {
		t.Fatalf("source=%v manager=%v want active layer-0 source", source != nil, source != nil && source.manager != nil)
	}
	if stats := source.manager.Stats(); stats.ActiveHandles != 2 {
		t.Fatalf("resource stats before close=%+v want two active handles", stats)
	}
	pins := source.manager.PinSummary()
	if len(pins) != 2 {
		t.Fatalf("pins before close=%+v want two section pins", pins)
	}
	expectedPinnedRefs := make([]ColumnAssetRef, 0, len(pins))
	for _, pin := range pins {
		if pin.Root == "" || pin.Path == "" || pin.Key.Section.Column != columnVectorIndexStateAdjacencyColumnName {
			t.Fatalf("pin=%+v missing root/path/typed-state column identity", pin)
		}
		ref, ok := columnAssetRefForMappedResourceKey(pin.Key)
		if !ok {
			t.Fatalf("pin=%+v could not convert to column asset ref", pin)
		}
		expectedPinnedRefs = append(expectedPinnedRefs, ref)
	}
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability with active layer-0 source pins: %v", err)
	}
	if plan.MappedResources.PinnedRefs < len(expectedPinnedRefs) {
		t.Fatalf("reachability mappedresource stats=%+v want at least %d pinned source refs", plan.MappedResources, len(expectedPinnedRefs))
	}
	for _, ref := range expectedPinnedRefs {
		if !columnAssetReachabilityPlanHasSource1919(plan, ref, ColumnAssetReachabilitySourceMappedResourcePin) {
			t.Fatalf("reachability plan missing mappedresource pin for ref=%+v entries=%+v", ref, plan.Entries)
		}
	}
	if err := searcher.Close(); err != nil {
		t.Fatalf("Close searcher: %v", err)
	}
	if !source.closed {
		t.Fatal("source.closed=false after searcher close")
	}
	if stats := source.manager.Stats(); stats.ActiveHandles != 0 {
		t.Fatalf("resource stats after close=%+v want no active handles", stats)
	}
}

func TestColumnVectorGraphLayer0AdjacencySourceBadNeighborOrdinalFails1919(t *testing.T) {
	input := columnGraphRebuildSyntheticRowsV2A(16, 8)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 8, 4, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	disableColumnVectorGraphAdjacencyDirectSourcesForTest(t, reader)
	fake := fakeColumnVectorGraphLayer0AdjacencySource1919(t, reader.RowCount(), uint32(reader.RowCount()))
	reader.layer0AdjacencySource = fake
	reader.adjacencyLayerSources = &columnVectorGraphAdjacencyDirectSources{sources: []*columnVectorGraphLayer0AdjacencyDirectSource{fake}, allLayers: true}
	var scratch columnVectorGraphNativeSearchScratch
	_, _, err = reader.SearchCosine(input[0].vector, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 2}, &scratch)
	if !errors.Is(err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds) {
		t.Fatalf("SearchCosine err=%v want adjacency ordinal sentinel", err)
	}
}

func TestColumnVectorGraphLayer0AdjacencySourceParallelReadersSharePreparedHandles1735(t *testing.T) {
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		t.Skip("shared prepared adjacency-source handle test requires mmap_direct support")
	}
	input := columnGraphRebuildSyntheticRowsV2A(64, 16)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 16, 8, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	readers := make([]*columnVectorGraphPhysicalRowReader, 3)
	for i := range readers {
		reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("open reader %d: %v", i, err)
		}
		defer func() { _ = reader.Close() }()
		if reader.layer0AdjacencySource == nil || reader.layer0AdjacencySource.manager == nil || reader.sharedPreparedSearch == nil {
			t.Fatalf("reader %d source=%v shared=%v fallback=%s", i, reader.layer0AdjacencySource != nil, reader.sharedPreparedSearch != nil, reader.layer0AdjacencySourceFallbackReason)
		}
		if i > 0 {
			if reader.sharedPreparedSearch.holder != readers[0].sharedPreparedSearch.holder {
				t.Fatalf("reader %d did not share prepared holder with reader 0", i)
			}
			if reader.layer0AdjacencySource.manager != readers[0].layer0AdjacencySource.manager {
				t.Fatalf("reader %d layer-0 source manager differs from reader 0; want shared immutable manager", i)
			}
		}
		readers[i] = reader
	}
	snap := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	if snap.Entries != 1 || snap.Refs != len(readers) || snap.ActiveHandles == 0 || snap.ActiveMappedBytes == 0 {
		t.Fatalf("shared prepared adjacency snapshot=%+v want one active holder with %d refs", snap, len(readers))
	}
	for i, reader := range readers {
		var scratch columnVectorGraphNativeSearchScratch
		_, stats, err := reader.SearchCosine(input[3].vector, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: 32}, &scratch)
		if err != nil {
			t.Fatalf("reader %d SearchCosine: %v", i, err)
		}
		if stats.AdjacencyMmapDirectViews+stats.AdjacencyHeapCopyTypedViews == 0 {
			t.Fatalf("reader %d stats=%+v want direct layer-0 source", i, stats)
		}
	}
}

func disableColumnVectorGraphAdjacencyDirectSourcesForTest(tb testing.TB, reader *columnVectorGraphPhysicalRowReader) {
	tb.Helper()
	if reader == nil {
		return
	}
	if reader.adjacencyLayerSources != nil {
		if err := reader.adjacencyLayerSources.Close(); err != nil {
			tb.Fatalf("close adjacency direct sources: %v", err)
		}
		reader.adjacencyLayerSources = nil
	}
	if reader.layer0AdjacencySource != nil {
		if err := reader.layer0AdjacencySource.Close(); err != nil {
			tb.Fatalf("close layer-0 adjacency source: %v", err)
		}
		reader.layer0AdjacencySource = nil
	}
	reader.preparedSearch = nil
	reader.layer0AdjacencySourceUnavailable = true
	reader.layer0AdjacencySourceFallbackReason = ""
}

func releaseColumnVectorGraphAdjacencySourceHandlesForTest(tb testing.TB, reader *columnVectorGraphPhysicalRowReader) {
	tb.Helper()
	if reader == nil {
		return
	}
	reader.preparedSearch = nil
	if reader.adjacencyLayerSources != nil {
		for layer, source := range reader.adjacencyLayerSources.sources {
			if source == nil {
				continue
			}
			if source.offsetsHandle != nil {
				if err := source.offsetsHandle.Release(); err != nil {
					tb.Fatalf("release layer %d offsets handle: %v", layer, err)
				}
			}
			if source.valuesHandle != nil {
				if err := source.valuesHandle.Release(); err != nil {
					tb.Fatalf("release layer %d values handle: %v", layer, err)
				}
			}
		}
		return
	}
	if reader.layer0AdjacencySource == nil {
		return
	}
	if err := reader.layer0AdjacencySource.offsetsHandle.Release(); err != nil {
		tb.Fatalf("release offsets handle: %v", err)
	}
	if err := reader.layer0AdjacencySource.valuesHandle.Release(); err != nil {
		tb.Fatalf("release values handle: %v", err)
	}
}

func TestColumnVectorGraphAllLayerAdjacencyDirectSourcesSearchParity1921(t *testing.T) {
	d, col, def, graph, rows := openManualColumnGraphAllLayerSourceSearchFixture1921(t, nil)
	defer func() { _ = d.Close() }()
	if graph.AdjacencyLayerCount < 3 {
		t.Fatalf("manual graph layer count=%d want multi-layer fixture", graph.AdjacencyLayerCount)
	}
	query := append([]float32(nil), rows[0].Vector...)
	directReader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open direct reader: %v", err)
	}
	defer func() { _ = directReader.Close() }()
	if directReader.adjacencyLayerSources == nil || !directReader.adjacencyLayerSources.allLayers || len(directReader.adjacencyLayerSources.sources) != graph.AdjacencyLayerCount {
		t.Fatalf("direct all-layer sources=%+v want %d layers", directReader.adjacencyLayerSources, graph.AdjacencyLayerCount)
	}
	var directScratch columnVectorGraphNativeSearchScratch
	directResults, directStats, err := directReader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 3, EfSearch: len(rows)}, &directScratch)
	if err != nil {
		t.Fatalf("direct SearchCosine: %v", err)
	}
	if len(directResults) == 0 || directStats.AdjacencyScratchDecodes != 0 || directStats.AdjacencySourceFallbacks != 0 || directStats.AdjacencyMmapDirectViews+directStats.AdjacencyHeapCopyTypedViews == 0 {
		t.Fatalf("direct results=%d stats=%+v want clean all-layer direct adjacency and zero scratch", len(directResults), directStats)
	}

	fallbackReader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open fallback reader: %v", err)
	}
	defer func() { _ = fallbackReader.Close() }()
	disableColumnVectorGraphAdjacencyDirectSourcesForTest(t, fallbackReader)
	var fallbackScratch columnVectorGraphNativeSearchScratch
	fallbackResults, fallbackStats, err := fallbackReader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 3, EfSearch: len(rows)}, &fallbackScratch)
	if err != nil {
		t.Fatalf("fallback SearchCosine: %v", err)
	}
	if mismatch := columnGraphNativeSearchResultsMismatchV3(directResults, fallbackResults); mismatch != "" {
		t.Fatalf("direct/fallback mismatch: %s", mismatch)
	}
	if fallbackStats.AdjacencyScratchDecodes == 0 || fallbackStats.AdjacencyMmapDirectViews != 0 || fallbackStats.AdjacencySourceUnavailable != 1 {
		t.Fatalf("fallback stats=%+v want row-asset scratch fallback", fallbackStats)
	}
}

func TestColumnVectorGraphPreparedCSRAdjacencyNeighborsFastPath2274(t *testing.T) {
	source := &columnVectorGraphLayer0AdjacencyDirectSource{
		layer:   0,
		rows:    3,
		offsets: []uint64{0, 2, 2, 3},
		values:  []uint32{7, 11, 13},
		outcome: columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect,
		owned:   true,
	}
	group := &columnVectorGraphAdjacencyDirectSources{sources: []*columnVectorGraphLayer0AdjacencyDirectSource{source}, allLayers: true}
	got, reason, ok := group.preparedCSRNeighbors(0, 0)
	if !ok || reason != "" || len(got) != 2 || got[0] != 7 || got[1] != 11 {
		t.Fatalf("row 0 prepared neighbors=%v reason=%s ok=%v, want [7 11]", got, reason, ok)
	}
	got, reason, ok = group.preparedCSRNeighbors(0, 1)
	if !ok || reason != "" || len(got) != 0 {
		t.Fatalf("row 1 prepared neighbors=%v reason=%s ok=%v, want empty direct slice", got, reason, ok)
	}
	_, reason, ok = group.preparedCSRNeighbors(0, 3)
	if ok || reason != typeddecode.ReasonRowCountMismatch {
		t.Fatalf("out-of-range prepared neighbors reason=%s ok=%v, want row-count mismatch", reason, ok)
	}
	badOffsets := &columnVectorGraphLayer0AdjacencyDirectSource{
		rows:    1,
		offsets: []uint64{0, 2},
		values:  []uint32{1},
		outcome: columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect,
		owned:   true,
	}
	_, reason, ok = badOffsets.preparedCSRNeighbors(0)
	if ok || reason != typeddecode.ReasonValuesLengthMismatch {
		t.Fatalf("malformed prepared neighbors reason=%s ok=%v, want values-length mismatch", reason, ok)
	}
	source.outcome = columnVectorGraphLayer0AdjacencySourceOutcomeTypedListMmapDirect
	_, reason, ok = group.preparedCSRNeighbors(0, 0)
	if ok || reason != typeddecode.ReasonHandleSourceUnsupported {
		t.Fatalf("non-prepared outcome reason=%s ok=%v, want handle-source unsupported", reason, ok)
	}
	source.outcome = columnVectorGraphLayer0AdjacencySourceOutcomePreparedCSRMmapDirect
	source.owned = false
	_, reason, ok = group.preparedCSRNeighbors(0, 0)
	if ok || reason != typeddecode.ReasonStaleHandle {
		t.Fatalf("nil handle prepared neighbors reason=%s ok=%v, want stale handle", reason, ok)
	}
	source.owned = true
	source.closed = true
	_, reason, ok = group.preparedCSRNeighbors(0, 0)
	if ok || reason != typeddecode.ReasonStaleHandle {
		t.Fatalf("closed prepared neighbors reason=%s ok=%v, want stale handle", reason, ok)
	}
}

func TestColumnVectorGraphAllLayerAdjacencyDirectSourcesEmptyUpperLayers1921(t *testing.T) {
	d, col, def, _, rows := openManualColumnGraphAllLayerSourceSearchFixture1921(t, nil)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if layer1, _, reason, ok := reader.directAdjacencyLayerForOrdinal(1, 1); !ok || reason != "" || len(layer1) != 0 {
		t.Fatalf("row 1 layer 1 adjacency=%v reason=%s ok=%v, want empty direct upper layer", layer1, reason, ok)
	}
	maxLayer, _, counters, reason, ok := reader.adjacencyLayerSources.MaxLayerForOrdinal(1)
	if !ok || reason != "" || maxLayer != 0 {
		t.Fatalf("row 1 max layer=%d reason=%s ok=%v, want layer 0 after scanning empty upper layers", maxLayer, reason, ok)
	}
	if got, want := counters.AdjacencyMmapDirectViews+counters.AdjacencyHeapCopyTypedViews, uint64(len(reader.adjacencyLayerSources.sources)); got != want {
		t.Fatalf("max-layer counters=%+v want one classified direct lookup per layer (%d)", counters, want)
	}
	if layer0, _, reason, ok := reader.directAdjacencyLayerForOrdinal(3, 0); !ok || reason != "" || len(layer0) != 0 {
		t.Fatalf("row 3 layer 0 adjacency=%v reason=%s ok=%v, want empty direct layer 0", layer0, reason, ok)
	}
	if layer1, _, reason, ok := reader.directAdjacencyLayerForOrdinal(3, 1); !ok || reason != "" || len(layer1) != 1 || layer1[0] != 0 {
		t.Fatalf("row 3 layer 1 adjacency=%v reason=%s ok=%v, want direct upper edge to 0", layer1, reason, ok)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(rows[3].Vector, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(got) == 0 || stats.AdjacencyScratchDecodes != 0 {
		t.Fatalf("results=%d stats=%+v want zero adjacency scratch with empty direct layers", len(got), stats)
	}
	if source0 := reader.adjacencyLayerSources.sources[0]; source0 != nil {
		if err := source0.offsetsHandle.Release(); err != nil {
			t.Fatalf("release layer 0 offsets handle: %v", err)
		}
		if err := source0.valuesHandle.Release(); err != nil {
			t.Fatalf("release layer 0 values handle: %v", err)
		}
	}
	_, _, partialCounters, reason, ok := reader.adjacencyLayerSources.MaxLayerForOrdinal(1)
	if ok || reason != typeddecode.ReasonStaleHandle {
		t.Fatalf("partial max-layer ok=%v reason=%s want stale-handle fallback", ok, reason)
	}
	if got, want := partialCounters.AdjacencyMmapDirectViews+partialCounters.AdjacencyHeapCopyTypedViews, uint64(len(reader.adjacencyLayerSources.sources)-1); got != want {
		t.Fatalf("partial max-layer counters=%+v want upper-layer direct lookups preserved=%d", partialCounters, want)
	}
}

func TestColumnVectorGraphAllLayerAdjacencySourceMissingLayerFallback1921(t *testing.T) {
	d, col, def, _, rows := openManualColumnGraphAllLayerSourceSearchFixture1921(t, func(graph *columnVectorGraphManifestSnapshot) {
		if len(graph.AdjacencyLayerSources) < 2 {
			t.Fatalf("fixture adjacency layers=%d want at least 2", len(graph.AdjacencyLayerSources))
		}
		graph.AdjacencyLayerSources[1].Ref.FileID += 1000
	})
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader with missing optional source: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.adjacencyLayerSources == nil || reader.layer0AdjacencySource == nil {
		t.Fatalf("sources=%v layer0=%v fallback=%s want typed adjacency state despite missing legacy source", reader.adjacencyLayerSources != nil, reader.layer0AdjacencySource != nil, reader.layer0AdjacencySourceFallbackReason)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(rows[0].Vector, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine with missing optional source: %v", err)
	}
	if len(got) == 0 || stats.AdjacencyPreparedCSRMmapDirectViews+stats.AdjacencyTypedListMmapDirectViews+stats.AdjacencyTypedListHeapCopyTypedViews == 0 || stats.AdjacencySourceUnavailable != 0 || stats.AdjacencySourceFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 {
		t.Fatalf("results=%d stats=%+v want prepared/state adjacency after quarantined missing legacy source", len(got), stats)
	}
}

func TestColumnVectorGraphAllLayerAdjacencySourceCorruptLayerFallback1921(t *testing.T) {
	d, col, def, graph, rows := openManualColumnGraphAllLayerSourceSearchFixture1921(t, nil)
	defer func() { _ = d.Close() }()
	if len(graph.AdjacencyLayerSources) < 2 {
		t.Fatalf("adjacency layer sources=%d want layer 1", len(graph.AdjacencyLayerSources))
	}
	source := graph.AdjacencyLayerSources[1]
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), source.Ref)
	if err != nil {
		t.Fatalf("read layer source: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	offsetsSection, _, ok := image.ColumnOffsetsListSections(columnVectorGraphAdjacencySourceColumnName(1))
	if !ok || offsetsSection.Length < 16 {
		t.Fatalf("layer 1 offsets section=%+v ok=%v", offsetsSection, ok)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), source.Ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile source: %v", err)
	}
	corruptByte := []byte{raw[offsetsSection.Offset+8] ^ 0xff}
	if _, err := file.WriteAt(corruptByte, source.Ref.Offset+int64(offsetsSection.Offset+8)); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt corrupt source: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt source: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader after corrupt optional source: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.adjacencyLayerSources == nil || reader.layer0AdjacencySource == nil {
		t.Fatalf("sources=%v layer0=%v fallback=%s want typed adjacency state despite corrupt legacy source", reader.adjacencyLayerSources != nil, reader.layer0AdjacencySource != nil, reader.layer0AdjacencySourceFallbackReason)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(rows[2].Vector, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine after corrupt optional source: %v", err)
	}
	if len(got) == 0 || stats.AdjacencyPreparedCSRMmapDirectViews+stats.AdjacencyTypedListMmapDirectViews+stats.AdjacencyTypedListHeapCopyTypedViews == 0 || stats.AdjacencySourceUnavailable != 0 || stats.AdjacencySourceFallbacks != 0 || stats.AdjacencyLegacyFallbacks != 0 {
		t.Fatalf("results=%d stats=%+v want prepared/state adjacency after quarantined corrupt legacy source", len(got), stats)
	}
}

func TestVectorIndexSearcherCloseReleasesAllLayerAdjacencySources1921(t *testing.T) {
	d, col, def, graph, _ := openManualColumnGraphAllLayerSourceSearchFixture1921(t, nil)
	defer func() { _ = d.Close() }()
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	sources := searcher.reader.adjacencyLayerSources
	if sources == nil || !sources.allLayers || len(sources.sources) != graph.AdjacencyLayerCount {
		t.Fatalf("sources=%+v want %d all-layer sources", sources, graph.AdjacencyLayerCount)
	}
	var activeHandles int64
	expectedPinnedRefs := make([]ColumnAssetRef, 0, graph.AdjacencyLayerCount*2)
	for layer, source := range sources.sources {
		if source == nil || source.manager == nil {
			t.Fatalf("source[%d]=%v manager missing", layer, source != nil)
		}
		if stats := source.manager.Stats(); stats.ActiveHandles != 2 {
			t.Fatalf("source[%d] stats=%+v want two active handles", layer, stats)
		}
		activeHandles += source.manager.Stats().ActiveHandles
		for _, pin := range source.manager.PinSummary() {
			if pin.Root == "" || pin.Path == "" || pin.Key.Section.Column != columnVectorIndexStateAdjacencyColumnName {
				t.Fatalf("pin=%+v missing root/path/typed-state column identity for layer %d", pin, layer)
			}
			ref, ok := columnAssetRefForMappedResourceKey(pin.Key)
			if !ok {
				if pin.Key.Length != 0 {
					t.Fatalf("pin=%+v could not convert to column asset ref", pin)
				}
				continue
			}
			expectedPinnedRefs = append(expectedPinnedRefs, ref)
		}
	}
	if activeHandles != int64(graph.AdjacencyLayerCount*2) || sources.ActiveHandles() != activeHandles || len(expectedPinnedRefs) == 0 {
		t.Fatalf("active handles=%d grouped=%d pinnedRefs=%d want active handles and at least one convertible pinned ref", activeHandles, sources.ActiveHandles(), len(expectedPinnedRefs))
	}
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability with active all-layer source pins: %v", err)
	}
	for _, ref := range expectedPinnedRefs {
		if !columnAssetReachabilityPlanHasSource1919(plan, ref, ColumnAssetReachabilitySourceMappedResourcePin) {
			t.Fatalf("reachability plan missing mappedresource pin for ref=%+v", ref)
		}
	}
	if err := searcher.Close(); err != nil {
		t.Fatalf("Close searcher: %v", err)
	}
	for layer, source := range sources.sources {
		if source != nil {
			t.Fatalf("source[%d] retained after group close", layer)
		}
	}
	if sources.ActiveHandles() != 0 {
		t.Fatalf("active handles after close=%d want 0", sources.ActiveHandles())
	}
}

func TestColumnVectorGraphAllLayerAdjacencySourceReopenSearch1921(t *testing.T) {
	input := columnGraphRebuildSyntheticRowsV2A(96, 16)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 16, 8, input)
	dir := d.Dir()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reader, err := reopenedCol.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader reopened: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.adjacencyLayerSources == nil || !reader.adjacencyLayerSources.allLayers {
		t.Fatalf("reopened adjacency sources=%+v want all-layer direct sources", reader.adjacencyLayerSources)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(input[11].vector, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: 32}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine reopened: %v", err)
	}
	if len(got) == 0 || stats.AdjacencyScratchDecodes != 0 || stats.AdjacencyMmapDirectViews+stats.AdjacencyHeapCopyTypedViews == 0 {
		t.Fatalf("reopened results=%d stats=%+v want direct all-layer adjacency", len(got), stats)
	}
}

func openManualColumnGraphAllLayerSourceSearchFixture1921(tb testing.TB, mutateGraph func(*columnVectorGraphManifestSnapshot)) (*backenddb.DB, *Collection, VectorIndexDefinition, columnVectorGraphManifestSnapshot, []columnVectorGraphAssetRow) {
	tb.Helper()
	d, cfg, def, graph, rows := prepareManualColumnGraphAllLayerSources1920(tb)
	if mutateGraph != nil {
		mutateGraph(&graph)
	}
	identity := ColumnManifestIdentity{Generation: graph.BaseManifestGeneration, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, manifestIdentity := testColumnGraphManifestRecordsFromSnapshot1920(tb, *cfg, graph, identity)
	stateRows := columnVectorGraphStateRowsForTest1987(rows, graph.RowCount, def.Dimensions, graph.AdjacencyLayerCount)
	records, manifestIdentity = appendCompleteVectorIndexStateForGraphTest1987(tb, d, "docs", *cfg, def, graph, records, manifestIdentity, columnVectorIndexStateChecksumInput1986(*cfg), stateRows)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	publishColumnGraphCatalogForTestV2A(tb, d, meta, manifestIdentity, records)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	return d, col, def, graph, rows
}

func columnAssetReachabilityPlanHasSource1919(plan ColumnAssetReachabilityPlan, ref ColumnAssetRef, source ColumnAssetReachabilitySource) bool {
	for _, entry := range plan.Entries {
		if entry.Ref != ref {
			continue
		}
		for _, got := range entry.Sources {
			if got == source {
				return true
			}
		}
	}
	return false
}

func columnVectorGraphLayer0AdjacencyMmapExpectedOnPlatform1919() bool {
	if !mappedresource.NativeLittleEndian() {
		return false
	}
	switch runtime.GOOS {
	case "darwin", "linux", "freebsd", "netbsd", "openbsd":
		return true
	default:
		return false
	}
}

func fakeColumnVectorGraphLayer0AdjacencySource1919(tb testing.TB, rows int, badNeighbor uint32) *columnVectorGraphLayer0AdjacencyDirectSource {
	tb.Helper()
	offsets := make([]uint64, rows+1)
	for i := 1; i < len(offsets); i++ {
		offsets[i] = 1
	}
	values := []uint32{badNeighbor}
	offsetsRaw := make([]byte, len(offsets)*8)
	for i, offset := range offsets {
		binary.LittleEndian.PutUint64(offsetsRaw[i*8:], offset)
	}
	valuesRaw := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(valuesRaw[i*4:], value)
	}
	manager := mappedresource.NewManager()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "fake-layer0-adjacency-1919", Namespace: "test", Reason: "fake layer-0 adjacency source"}
	offsetsHandle, err := manager.AcquireBytes(mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "test", Kind: string(typedcolumn.ColumnPartImageSectionColumnOffsets), FileID: 1, Length: int64(len(offsetsRaw)), Encoding: typedcolumn.EncodingRawUint32OffsetsList.String()}, scope, mappedresource.SourceHeapCopy, offsetsRaw, mappedresource.AcquireOptions{Reason: "fake offsets"})
	if err != nil {
		tb.Fatalf("AcquireBytes offsets: %v", err)
	}
	valuesHandle, err := manager.AcquireBytes(mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "test", Kind: string(typedcolumn.ColumnPartImageSectionColumnValues), FileID: 1, Length: int64(len(valuesRaw)), Encoding: typedcolumn.EncodingRawUint32OffsetsList.String()}, scope, mappedresource.SourceHeapCopy, valuesRaw, mappedresource.AcquireOptions{Reason: "fake values"})
	if err != nil {
		_ = offsetsHandle.Release()
		tb.Fatalf("AcquireBytes values: %v", err)
	}
	return &columnVectorGraphLayer0AdjacencyDirectSource{
		rows:          rows,
		offsets:       offsets,
		values:        values,
		outcome:       columnVectorGraphLayer0AdjacencySourceOutcomeHeapCopyTypedView,
		manager:       manager,
		offsetsHandle: offsetsHandle,
		valuesHandle:  valuesHandle,
	}
}
