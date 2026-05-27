package collections

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/page"
)

func columnGraphTypedColumnMmapDirectViewSupportedForTest() bool {
	// Windows currently falls back from AcquireFileRange with mmap_failed, and
	// non-little-endian hosts cannot expose native float32 slices safely. Those
	// paths still use the typed-column source, but they must decode to scratch and
	// must not report mmap_direct_view evidence for #1893.
	return runtime.GOOS != "windows" && columnPhysicalNativeLittleEndian
}

func TestColumnVectorGraphTypedColumnVectorReaderParity1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.5, 0.5, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 3, rows)
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
	defer func() { _ = reader.Close() }()
	if reader.typedVectorSource == nil || reader.typedVectorFallbackReason != "" {
		t.Fatalf("typed vector source=%v fallback=%q want active typed_column_part source", reader.typedVectorSource != nil, reader.typedVectorFallbackReason)
	}
	if reader.graph.AssetRef.Kind != ColumnAssetKindTCS1PartImage {
		t.Fatalf("graph asset kind=%q want derived physical graph asset kind %q", reader.graph.AssetRef.Kind, ColumnAssetKindTCS1PartImage)
	}
	assertColumnGraphTypedColumnAdjacencyDeferred1782(t, col, def.Name)

	var scratch columnPhysicalRowReaderScratch
	for ordinal := 0; ordinal < reader.RowCount(); ordinal++ {
		row, err := reader.FetchRow(ordinal, &scratch)
		if err != nil {
			t.Fatalf("FetchRow(%d): %v", ordinal, err)
		}
		input := columnGraphRebuildInputByIDV2B(t, rows, string(row.ID))
		if !float32SlicesEqual1782(row.Vector, input.vector) {
			t.Fatalf("row %d id=%q vector=%v want typed-column vector %v", ordinal, row.ID, row.Vector, input.vector)
		}
	}

	query := []float32{0, 0.2, 1}
	var searchScratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &searchScratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	assertColumnGraphNativeSearchResultsV3(t, got, exactColumnGraphTopKForTest(t, rows, query, 2))
	if stats.VectorDirectViews+stats.VectorScratchDecodes != stats.CandidateFetches {
		t.Fatalf("stats=%+v want vector direct+scratch accounting to match candidate fetches", stats)
	}
	if columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		if stats.VectorDirectViews == 0 {
			t.Fatalf("stats=%+v want typed-column vector mmap direct views on valid dense section", stats)
		}
		if stats.TypedColumnMappedBytes == 0 || stats.TypedColumnActiveHandles == 0 || stats.TypedColumnFallbacks != 0 {
			t.Fatalf("stats=%+v want live mappedresource-backed typed-column source without fallback", stats)
		}
	} else {
		if stats.VectorDirectViews != 0 || stats.VectorScratchDecodes == 0 || stats.TypedColumnDecodedBytes == 0 || stats.TypedColumnFallbacks != 0 {
			t.Fatalf("stats=%+v want platform scratch fallback from typed-column source", stats)
		}
	}
}

func TestColumnVectorGraphTypedColumnVectorCloseReleasesHandles1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	source := reader.typedVectorSource
	if source == nil || source.manager == nil {
		_ = reader.Close()
		t.Fatalf("typed vector source=%v manager=%v want active source", source != nil, source != nil && source.manager != nil)
	}
	if stats := source.manager.Stats(); columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		if stats.ActiveHandles == 0 || stats.ActiveMappedBytes == 0 {
			_ = reader.Close()
			t.Fatalf("stats before close=%+v want active typed-column mmap handles/bytes", stats)
		}
	} else if stats.ActiveHandles != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 || source.decodedDerivedBytes == 0 {
		_ = reader.Close()
		t.Fatalf("stats before close=%+v decoded=%d want released platform scratch fallback", stats, source.decodedDerivedBytes)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("reader.Close: %v", err)
	}
	if stats := source.manager.Stats(); stats.ActiveHandles != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 {
		t.Fatalf("stats after close=%+v want released typed-column handles", stats)
	}
	if vector, direct, ok := source.vectorForOrdinal(0); ok || direct || vector != nil {
		t.Fatalf("vectorForOrdinal after close vector=%v direct=%t ok=%t want no newly exposed stale slice", vector, direct, ok)
	}
}

func TestColumnVectorGraphTypedColumnVectorPinKeyUsesTypedColumnAssetRef1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.typedVectorSource == nil || reader.typedVectorSource.manager == nil {
		t.Fatalf("missing typed vector source")
	}
	pins := reader.typedVectorSource.manager.PinSummary()
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		if len(pins) != 0 {
			t.Fatalf("pins=%d want none when platform cannot provide mmap direct views", len(pins))
		}
		return
	}
	if len(pins) == 0 {
		t.Fatal("typed vector source has no mappedresource pins")
	}
	for _, pin := range pins {
		ref, ok := columnAssetRefForMappedResourceKey(pin.Key)
		if !ok {
			t.Fatalf("pin key not convertible to typed-column asset ref: %+v", pin.Key)
		}
		if ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Namespace == "" || ref.FileID == 0 || ref.Length <= 0 {
			t.Fatalf("unexpected typed-column pin ref: %+v from key %+v", ref, pin.Key)
		}
		if _, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("pin ref is not a checksum-valid column asset range: ref=%+v key=%+v err=%v", ref, pin.Key, err)
		}
		if pin.Key.Section.Kind != string(typedcolumn.ColumnPartImageSectionColumnData) || pin.Key.Section.Column == "" || pin.Key.Encoding == "" {
			t.Fatalf("pin key missing logical dense-section identity: %+v", pin.Key)
		}
		if pin.Scope.Kind != mappedresource.ScopeColumnPartReader || pin.Scope.ID != columnVectorGraphTypedColumnVectorScopeID || pin.Reason == "" {
			t.Fatalf("unexpected pin scope/reason: %+v", pin)
		}
	}
}

func TestSearchVectorIndexTypedColumnVectorReopenGeneration1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	dir, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
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

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	query := []float32{0, 0, 1}
	got, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("SearchVectorIndex reopen: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	if columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		if got.Stats.VectorDirectViews == 0 || got.Stats.TypedColumnMappedBytes == 0 || got.Stats.TypedColumnFallbacks != 0 {
			t.Fatalf("stats=%+v want durable typed-column vector mmap direct reads after reopen", got.Stats)
		}
	} else if got.Stats.VectorDirectViews != 0 || got.Stats.VectorScratchDecodes == 0 || got.Stats.TypedColumnDecodedBytes == 0 || got.Stats.TypedColumnFallbacks != 0 {
		t.Fatalf("stats=%+v want durable typed-column vector scratch fallback after reopen", got.Stats)
	}
}

func TestColumnVectorGraphTypedColumnVectorCorruptPartFallsBack1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	corruptFirstTypedColumnPartForVectorTest1782(t, col)

	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader with corrupt typed_column_part should fall back to graph asset: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.typedVectorSource != nil || reader.typedVectorFallbackReason == "" {
		t.Fatalf("typed source active=%v fallback=%q want fallback reason after corrupt typed_column_part", reader.typedVectorSource != nil, reader.typedVectorFallbackReason)
	}

	query := []float32{0, 0, 1}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine fallback: %v", err)
	}
	assertColumnGraphNativeSearchResultsV3(t, got, exactColumnGraphTopKForTest(t, rows, query, 2))
	if stats.TypedColumnFallbacks != 1 || stats.VectorDirectViews != 0 || stats.VectorScratchDecodes == 0 {
		t.Fatalf("stats=%+v want typed-column fallback to graph row vector scratch decodes", stats)
	}
}

func TestColumnVectorGraphTypedColumnVectorNonColumnPartOwnerUsesGraphVectors1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	mutateCurrentSnapshotColumnStoreForTest1782(t, d, col, func(cfg *ColumnStoreConfig) {
		for i := range cfg.Columns {
			if cfg.Columns[i].Path == def.Field {
				cfg.Columns[i].Owner = TypedStorageOwnerRowAsset
			}
		}
	})
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader non-column-part owner: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.typedVectorSource != nil || reader.typedVectorFallbackReason != "" {
		t.Fatalf("typed source active=%v fallback=%q want ordinary graph-vector path for non-column-part owner", reader.typedVectorSource != nil, reader.typedVectorFallbackReason)
	}
	query := []float32{0, 0, 1}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine non-column-part owner: %v", err)
	}
	assertColumnGraphNativeSearchResultsV3(t, got, exactColumnGraphTopKForTest(t, rows, query, 2))
	if stats.TypedColumnFallbacks != 0 || stats.VectorDirectViews != 0 || stats.VectorScratchDecodes == 0 {
		t.Fatalf("stats=%+v want ordinary graph row vector scratch decodes", stats)
	}
}

func TestColumnVectorGraphTypedColumnVectorTruncatedRefFailsClosed1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	cfg := *col.Meta().Options.ColumnStore
	field, adapterColumn, ok, err := columnVectorGraphTypedColumnVectorField(cfg, def.Field, def.Dimensions)
	if err != nil || !ok {
		t.Fatalf("columnVectorGraphTypedColumnVectorField ok=%v err=%v", ok, err)
	}
	typedRef := firstTypedColumnPartRefForVectorTest1782(t, col)
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), typedRef.Ref)
	if err != nil {
		t.Fatalf("read typed_column_part: %v", err)
	}
	if len(raw) < 2 {
		t.Fatalf("typed_column_part raw too small: %d", len(raw))
	}
	truncated := typedRef
	truncated.Ref.Length--
	truncated.Ref.Checksum = page.Checksum(raw[:len(raw)-1])
	part, _, err := col.loadColumnVectorGraphTypedColumnVectorPart("docs", cfg, truncated, typedRef.Rows, field, adapterColumn, mappedresource.NewManager())
	if err == nil || part != nil {
		t.Fatalf("load truncated typed_column_part part=%v err=%v want fail-closed error", part, err)
	}
}

func TestColumnVectorGraphTypedColumnVectorRowCountMismatchFailsClosed1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	cfg := *col.Meta().Options.ColumnStore
	field, adapterColumn, ok, err := columnVectorGraphTypedColumnVectorField(cfg, def.Field, def.Dimensions)
	if err != nil || !ok {
		t.Fatalf("columnVectorGraphTypedColumnVectorField ok=%v err=%v", ok, err)
	}
	typedRef := firstTypedColumnPartRefForVectorTest1782(t, col)
	part, _, err := col.loadColumnVectorGraphTypedColumnVectorPart("docs", cfg, typedRef, typedRef.Rows+1, field, adapterColumn, mappedresource.NewManager())
	if err == nil || part != nil {
		t.Fatalf("load row-count mismatched typed_column_part part=%v err=%v want fail-closed error", part, err)
	}
}

func TestColumnVectorGraphTypedColumnVectorMultiplePhysicalPartsFailsClosed1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	cfg := *catalog.meta.Options.ColumnStore
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		t.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	physicalRefs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan(records, manifest.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnManifestAssetRefsFromRecordsForScan: %v", err)
	}
	if mutationParts != 0 || len(physicalRefs) == 0 {
		t.Fatalf("physical refs=%d mutationParts=%d want insert physical refs", len(physicalRefs), mutationParts)
	}
	physicalRefs = append(append([]columnManifestAssetRefForScan(nil), physicalRefs[0]), physicalRefs[0])
	locations, rowsByGeneration, err := col.columnVectorGraphTypedColumnPhysicalLocations(catalog.meta.Name, cfg, physicalRefs)
	if err == nil || !strings.Contains(err.Error(), "multiple physical row parts") {
		t.Fatalf("locations=%v rowsByGeneration=%v err=%v want multipart fail-closed error", locations, rowsByGeneration, err)
	}
}

func TestColumnVectorGraphTypedColumnVectorStaleGraphFailsClosed1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		t.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		t.Fatalf("missing graph manifest record %q", def.Name)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		t.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReaderAtSnapshot(def.Name, snap, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReaderAtSnapshot: %v", err)
	}
	defer func() { _ = reader.Close() }()
	staleGraph := graph
	staleGraph.BaseManifestGeneration++
	source, err := col.newColumnVectorGraphTypedColumnVectorSource(catalog, *catalog.meta.Options.ColumnStore, manifest, records, staleGraph, reader)
	if err == nil || source != nil {
		t.Fatalf("new source with stale graph source=%v err=%v want fail-closed stale error", source, err)
	}
}

func TestColumnVectorGraphTypedColumnVectorMisalignedMappedSectionUsesScratchFallback1782(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	cfg, err := normalizeColumnStoreConfig("docs", columnGraphTypedColumnVectorStoreConfig1782(3))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	rows := typedColumnVectorDeclaredRows1782(t, *cfg, [][]float32{{1, 0, 0}, {0, 1, 0}})
	imageBytes, imageRows, err := buildTypedColumnPartImageForDeclaredRows(*cfg, 1, typedColumnPartAssetPartID, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnPartImageForDeclaredRows: %v", err)
	}
	if imageRows != len(rows) {
		t.Fatalf("image rows=%d want %d", imageRows, len(rows))
	}
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatalf("ensureColumnAssetManagerNamespace: %v", err)
	}
	// Bypass the writer-side appender padding to simulate a legacy/synthetic
	// misaligned ref; readers must fail closed or scratch-decode this shape.
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: cfg.AssetManager.Namespace, Generation: 1, PartID: typedColumnPartAssetPartID, FileID: columnAssetM12ASegmentFileID, Offset: 1, Length: int64(len(imageBytes)), Checksum: page.Checksum(imageBytes)}
	assetPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	legacyMisaligned := append([]byte{0}, imageBytes...)
	if err := os.WriteFile(assetPath, legacyMisaligned, 0o600); err != nil {
		t.Fatalf("write legacy misaligned typed_column_part: %v", err)
	}
	if ref.Offset%4 == 0 {
		t.Fatalf("test setup ref offset=%d is aligned; want misaligned dense section", ref.Offset)
	}
	image, err := typedcolumn.ParseColumnPartImage(imageBytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	_, adapterColumn, ok, err := columnVectorGraphTypedColumnVectorField(*cfg, "embedding", 3)
	if err != nil || !ok {
		t.Fatalf("columnVectorGraphTypedColumnVectorField ok=%v err=%v", ok, err)
	}
	section, err := columnVectorGraphTypedColumnVectorSection(image, adapterColumn.Definition.Name)
	if err != nil {
		t.Fatalf("columnVectorGraphTypedColumnVectorSection: %v", err)
	}
	manager := mappedresource.NewManager()
	sectionBytes, err := image.SectionBytes(section)
	if err != nil {
		t.Fatalf("SectionBytes: %v", err)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		t.Fatalf("missing layout certification for %q", adapterColumn.Definition.Name)
	}
	values, handle, direct, scratchDecode, err := (&Collection{db: d}).acquireColumnVectorGraphTypedColumnDenseVectorValues("docs", ref, image.Version, section, certColumn, page.Checksum(sectionBytes), imageRows, 3, manager)
	if err != nil {
		t.Fatalf("acquire dense vector values: %v", err)
	}
	defer func() {
		if handle != nil {
			_ = handle.Release()
		}
	}()
	handleSource := mappedresource.Source("<nil>")
	if handle != nil {
		handleSource = handle.Source()
	}
	if direct || !scratchDecode || handle != nil || handleSource != mappedresource.Source("<nil>") {
		t.Fatalf("direct=%v scratchDecode=%v handle=%v source=%s want scratch fallback, not mmap direct view", direct, scratchDecode, handle != nil, handleSource)
	}
	if !float32SlicesEqual1782(values[:3], []float32{1, 0, 0}) || !float32SlicesEqual1782(values[3:], []float32{0, 1, 0}) {
		t.Fatalf("values=%v want row-major typed vectors", values)
	}
	stats := manager.Stats()
	if stats.DirectViewSuccesses != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 || stats.ActiveHandles != 0 {
		t.Fatalf("stats=%+v want released scratch fallback, not direct-view evidence", stats)
	}
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() && stats.TotalHeapCopyBytes != uint64(section.Length) {
		t.Fatalf("stats=%+v want one platform heap-copy fallback decode", stats)
	}
}

func TestSearchVectorIndexColumnGraphL2RemainsFailClosed1782(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	mutateCurrentSnapshotVectorIndexForTestV4(t, d, col, def.Name, func(def *VectorIndexDefinition) {
		def.Metric = VectorMetricL2
	})
	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("SearchVectorIndex L2 err=%v want unavailable fail-closed", err)
	}
	if got.Path != "" || got.Status.Reason != VectorIndexReasonColumnGraphUnsupportedMetric {
		t.Fatalf("response=%+v want no path and unsupported-metric status", got)
	}
}

func openColumnGraphTypedColumnVectorTestCollection1782(tb testing.TB, dims, m int, rows []columnGraphRebuildInputRowV2A) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphRebuildVectorIndexDefinitionV2A(dims, m)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphTypedColumnVectorStoreConfig1782(dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	if len(rows) != 0 {
		insertColumnGraphRebuildRowsV2A(tb, col, rows)
	}
	return dir, d, col, def
}

func columnGraphTypedColumnVectorStoreConfig1782(dims int) *ColumnStoreConfig {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = append(append([]ColumnStoreColumn(nil), cfg.Columns...), ColumnStoreColumn{
		Name:       "embedding",
		Path:       "embedding",
		Owner:      TypedStorageOwnerColumnPart,
		ValueType:  ColumnStoreValueFloat32Vector,
		VectorDims: dims,
	})
	return cfg
}

func assertTypedColumnVectorFallbackSearch1782(tb testing.TB, col *Collection, def VectorIndexDefinition, rows []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		tb.Fatalf("openColumnVectorGraphPhysicalRowReader fallback: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if reader.typedVectorSource != nil || reader.typedVectorFallbackReason == "" {
		tb.Fatalf("typed source active=%v fallback=%q want fallback to graph vectors", reader.typedVectorSource != nil, reader.typedVectorFallbackReason)
	}
	query := []float32{0, 0, 1}
	var scratch columnVectorGraphNativeSearchScratch
	got, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: len(rows)}, &scratch)
	if err != nil {
		tb.Fatalf("SearchCosine fallback: %v", err)
	}
	assertColumnGraphNativeSearchResultsV3(tb, got, exactColumnGraphTopKForTest(tb, rows, query, 2))
	if stats.TypedColumnFallbacks != 1 || stats.VectorDirectViews != 0 || stats.VectorScratchDecodes == 0 {
		tb.Fatalf("stats=%+v want typed-column fallback to graph row vector scratch decodes", stats)
	}
}

func mutateCurrentSnapshotColumnStoreForTest1782(tb testing.TB, d *backenddb.DB, col *Collection, mutate func(*ColumnStoreConfig)) {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		tb.Fatalf("catalogForSnapshot: %v", err)
	}
	if catalog == nil || catalog.meta.Options.ColumnStore == nil {
		tb.Fatal("missing column store config")
	}
	mutate(catalog.meta.Options.ColumnStore)
}

func firstTypedColumnPartRefForVectorTest1782(tb testing.TB, col *Collection) columnManifestAssetRefForScan {
	tb.Helper()
	view, closeView, err := col.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		tb.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	if len(view.TypedColumnPartRefs) == 0 {
		tb.Fatal("missing typed_column_part refs")
	}
	return view.TypedColumnPartRefs[0]
}

func typedColumnVectorDeclaredRows1782(tb testing.TB, cfg ColumnStoreConfig, vectors [][]float32) []columnDeclaredRow {
	tb.Helper()
	rows := make([]columnDeclaredRow, len(vectors))
	embeddingIdx := -1
	for i, col := range cfg.Columns {
		if col.Path == "embedding" {
			embeddingIdx = i
			break
		}
	}
	if embeddingIdx < 0 {
		tb.Fatal("missing embedding column in test config")
	}
	for rowIdx, vector := range vectors {
		values := make([]columnDeclaredValue, len(cfg.Columns))
		values[embeddingIdx] = columnDeclaredValue{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: append([]float32(nil), vector...)}
		rows[rowIdx] = columnDeclaredRow{ID: []byte{byte(rowIdx + 1)}, Values: values}
	}
	return rows
}

func corruptFirstTypedColumnPartForVectorTest1782(tb testing.TB, col *Collection) {
	tb.Helper()
	typedRef := firstTypedColumnPartRefForVectorTest1782(tb, col)
	ref := typedRef.Ref
	path, err := columnAssetSegmentPath(col.db.ColumnAssetRootDir(), ref)
	if err != nil {
		tb.Fatalf("columnAssetSegmentPath: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		tb.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.WriteAt([]byte{0xff}, ref.Offset); err != nil {
		tb.Fatalf("corrupt typed_column_part: %v", err)
	}
}

func assertColumnGraphTypedColumnAdjacencyDeferred1782(tb testing.TB, col *Collection, indexName string) {
	tb.Helper()
	snap := col.db.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		tb.Fatalf("catalogForSnapshot: %v", err)
	}
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		tb.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, indexName)
	if !ok {
		tb.Fatalf("missing graph manifest record %q", indexName)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		tb.Fatalf("decode graph manifest: %v", err)
	}
	if graph.AssetRef.Kind != ColumnAssetKindTCS1PartImage {
		tb.Fatalf("graph asset kind=%q want derived tcs1_part_image asset, not authoritative adjacency typed-column storage", graph.AssetRef.Kind)
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil {
		tb.Fatal("missing column store config")
	}
	for _, col := range cfg.Columns {
		if col.ValueType == ColumnStoreValueAdjacencyList || bytes.Contains([]byte(col.Path), []byte("neighbors")) {
			tb.Fatalf("collection config unexpectedly declares authoritative adjacency column: %+v", col)
		}
	}
}

func float32SlicesEqual1782(left, right []float32) bool {
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
