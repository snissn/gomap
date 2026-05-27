package collections

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
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
	if fallbackReader.layer0AdjacencySource != nil {
		if err := fallbackReader.layer0AdjacencySource.Close(); err != nil {
			t.Fatalf("close fallback source: %v", err)
		}
		fallbackReader.layer0AdjacencySource = nil
		fallbackReader.layer0AdjacencySourceUnavailable = true
	}
	var fallbackScratch columnVectorGraphNativeSearchScratch
	fallbackResults, fallbackStats, err := fallbackReader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64}, &fallbackScratch)
	if err != nil {
		t.Fatalf("fallback SearchCosine: %v", err)
	}
	if mismatch := columnGraphNativeSearchResultsMismatchV3(directResults, fallbackResults); mismatch != "" {
		t.Fatalf("direct/fallback mismatch: %s", mismatch)
	}
	if fallbackStats.AdjacencyMmapDirectViews != 0 || fallbackStats.AdjacencyHeapCopyTypedViews != 0 || fallbackStats.AdjacencySourceUnavailable != 1 {
		t.Fatalf("fallback stats=%+v want row-asset fallback without adjacency direct views", fallbackStats)
	}
	if fallbackStats.AdjacencyScratchDecodes <= directStats.AdjacencyScratchDecodes {
		t.Fatalf("direct stats=%+v fallback stats=%+v want direct source to reduce scratch decodes", directStats, fallbackStats)
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
	if got := reader.layer0AdjacencySource.outcome; got != columnVectorGraphLayer0AdjacencySourceOutcomeMmapDirect {
		t.Fatalf("source outcome=%s want mmap_direct", got)
	}
	var scratch columnVectorGraphNativeSearchScratch
	_, stats, err := reader.SearchCosine(input[11].vector, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: 32}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if stats.AdjacencyMmapDirectViews == 0 || stats.AdjacencyHeapCopyTypedViews != 0 {
		t.Fatalf("stats=%+v want mmap direct adjacency and no heap-copy typed view", stats)
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
	if reader.layer0AdjacencySource != nil || !reader.layer0AdjacencySourceUnavailable {
		t.Fatalf("source=%v unavailable=%v want absent source row fallback", reader.layer0AdjacencySource != nil, reader.layer0AdjacencySourceUnavailable)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: 3}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(got) == 0 || stats.AdjacencySourceUnavailable != 1 || stats.AdjacencySourceFallbacks != 1 || stats.AdjacencyMmapDirectViews != 0 || stats.AdjacencyScratchDecodes == 0 {
		t.Fatalf("results=%d stats=%+v want source-unavailable row fallback", len(got), stats)
	}
}

func TestColumnVectorGraphLayer0AdjacencySourceChecksumFallback1919(t *testing.T) {
	input := columnGraphRebuildSyntheticRowsV2A(48, 16)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 16, 8, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	graph := graphManifestFromRecords1918(t, records, def)
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), graph.Layer0AdjacencySource.Ref)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	offsetsSection, _, ok := image.ColumnOffsetsListSections(columnVectorGraphLayer0AdjacencySourceColumnName)
	if !ok || offsetsSection.Length < 16 {
		t.Fatalf("offsets section=%+v ok=%v", offsetsSection, ok)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), graph.Layer0AdjacencySource.Ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile source: %v", err)
	}
	corruptByte := []byte{raw[offsetsSection.Offset+8] ^ 0xff}
	if _, err := file.WriteAt(corruptByte, graph.Layer0AdjacencySource.Ref.Offset+int64(offsetsSection.Offset+8)); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt corrupt source: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupt source: %v", err)
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus after corrupt optional source: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open reader after corrupt source: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.layer0AdjacencySource != nil || reader.layer0AdjacencySourceFallbackReason == "" {
		t.Fatalf("source=%v fallback=%s want source fallback after checksum corruption", reader.layer0AdjacencySource != nil, reader.layer0AdjacencySourceFallbackReason)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(input[5].vector, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: 32}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine after corrupt source: %v", err)
	}
	if len(got) == 0 || stats.AdjacencySourceUnavailable != 1 || stats.AdjacencySourceFallbacks != 1 || stats.AdjacencyCertificationFailures == 0 || stats.AdjacencyMmapDirectViews != 0 {
		t.Fatalf("results=%d stats=%+v want row-asset fallback with source certification failure", len(got), stats)
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
	if err := reader.layer0AdjacencySource.offsetsHandle.Release(); err != nil {
		t.Fatalf("release offsets handle: %v", err)
	}
	if err := reader.layer0AdjacencySource.valuesHandle.Release(); err != nil {
		t.Fatalf("release values handle: %v", err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(input[9].vector, columnVectorGraphNativeSearchOptions{TopK: 5, EfSearch: 32}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine with stale source handles: %v", err)
	}
	if len(got) == 0 || stats.AdjacencyStaleHandles == 0 || stats.AdjacencySourceFallbacks == 0 || stats.AdjacencyMmapDirectViews != 0 {
		t.Fatalf("results=%d stats=%+v want stale-handle row fallback", len(got), stats)
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
		if pin.Root == "" || pin.Path == "" || pin.Key.Section.Column != columnVectorGraphLayer0AdjacencySourceColumnName {
			t.Fatalf("pin=%+v missing root/path/column identity", pin)
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
	if reader.layer0AdjacencySource != nil {
		if err := reader.layer0AdjacencySource.Close(); err != nil {
			t.Fatalf("close real source: %v", err)
		}
	}
	reader.layer0AdjacencySource = fakeColumnVectorGraphLayer0AdjacencySource1919(t, reader.RowCount(), uint32(reader.RowCount()))
	var scratch columnVectorGraphNativeSearchScratch
	_, _, err = reader.SearchCosine(input[0].vector, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 2}, &scratch)
	if !errors.Is(err, errColumnVectorGraphAdjacencyOrdinalOutOfBounds) {
		t.Fatalf("SearchCosine err=%v want adjacency ordinal sentinel", err)
	}
}

func TestColumnVectorGraphLayer0AdjacencySourceParallelReadersIndependent1919(t *testing.T) {
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
		if reader.layer0AdjacencySource == nil || reader.layer0AdjacencySource.manager == nil {
			t.Fatalf("reader %d source=%v fallback=%s", i, reader.layer0AdjacencySource != nil, reader.layer0AdjacencySourceFallbackReason)
		}
		if i > 0 && reader.layer0AdjacencySource.manager == readers[0].layer0AdjacencySource.manager {
			t.Fatalf("reader %d shares layer-0 source manager with reader 0", i)
		}
		readers[i] = reader
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
