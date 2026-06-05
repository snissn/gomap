package collections

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnVectorGraphRowRefStatePublishesReopenAndSources1993(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.5, 0.5, 0}},
	}
	dir, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	assets, found, err := columnVectorGraphRowRefStateAssetsByField(state)
	if err != nil || !found || len(assets) != len(columnVectorGraphRowRefStateFields) {
		_ = d.Close()
		t.Fatalf("row-ref state assets found=%t len=%d err=%v state=%+v", found, len(assets), err, state)
	}
	graph := graphManifestFromRecords1918(t, records, def)
	source, err := newColumnVectorGraphRowRefStateSourceFromRoot(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, state, records)
	if err != nil {
		_ = d.Close()
		t.Fatalf("newColumnVectorGraphRowRefStateSourceFromRoot: %v", err)
	}
	refs, ok := source.rowRefs()
	if !ok || len(refs) != len(rows) {
		_ = d.Close()
		t.Fatalf("row refs ok=%t len=%d want %d", ok, len(refs), len(rows))
	}
	for ordinal, ref := range refs {
		if ref.Generation != graph.BaseManifestGeneration || ref.PartID == 0 || ref.RowIndex < 0 || ref.AppliedCommandLSN == 0 {
			_ = d.Close()
			t.Fatalf("row ref ordinal=%d ref=%+v graph_generation=%d", ordinal, ref, graph.BaseManifestGeneration)
		}
	}
	if err := source.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("row ref source close: %v", err)
	}

	query := []float32{0.1, 0.2, 1}
	searchOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: 2, EfSearch: len(rows)}
	fetchPreset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
	if err != nil {
		_ = d.Close()
		t.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	fetchPreset.ApplyToSearchOptions(&searchOpts)
	got, err := col.SearchVectorIndex(searchOpts)
	if err != nil {
		_ = d.Close()
		t.Fatalf("SearchVectorIndex include docs: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, rows, query, 2), true)
	wantMmapDirectFields := uint64(len(columnVectorGraphRowRefStateFields))
	if !columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		wantMmapDirectFields = 0
	}
	if got.Stats.RowRefVectorSourceState != 1 || got.Stats.RowRefVectorSourceLegacyGraphIDs != 0 || got.Stats.RowRefStatePreparedViews != 1 || got.Stats.RowRefStateMmapDirectFields != wantMmapDirectFields || got.Stats.RowRefStateResultRefs != uint64(len(got.Results)) {
		_ = d.Close()
		t.Fatalf("stats=%+v want prepared row-ref vector/result source with mmap_direct_fields=%d", got.Stats, wantMmapDirectFields)
	}
	if got.Stats.ResultIDPreparedBytesViews != 1 || got.Stats.ResultIDTypedBytesState != uint64(len(got.Results)) || got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.ResultIDStateValidationFailures != 0 {
		_ = d.Close()
		t.Fatalf("stats=%+v want typed bytes result IDs without graph fallback", got.Stats)
	}
	if got.Stats.DocumentRowRefStateFetches != uint64(len(got.Results)) || got.Stats.DocumentRowRefLookupFallbacks != 0 || got.Stats.DocumentRowLocatorBuilds != 0 {
		_ = d.Close()
		t.Fatalf("stats=%+v want document fetch from vector-index row refs without ID lookup locator", got.Stats)
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
	reopenedGot, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: 2, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeBenchmarkDebug})
	if err != nil {
		t.Fatalf("SearchVectorIndex reopen: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, reopenedGot, def.Name, 2)
	if reopenedGot.Stats.RowRefVectorSourceState != 1 || reopenedGot.Stats.RowRefStatePreparedViews != 1 || reopenedGot.Stats.RowRefStateMmapDirectFields != wantMmapDirectFields || reopenedGot.Stats.RowRefStateResultRefs != uint64(len(reopenedGot.Results)) || reopenedGot.Stats.ResultIDPreparedBytesViews != 1 || reopenedGot.Stats.ResultIDTypedBytesState != uint64(len(reopenedGot.Results)) || reopenedGot.Stats.ResultIDGraphFallbacks != 0 {
		t.Fatalf("reopen stats=%+v want durable prepared row-ref source plus typed bytes result IDs with mmap_direct_fields=%d", reopenedGot.Stats, wantMmapDirectFields)
	}
}

func TestColumnVectorGraphRowRefStateCorruptFieldRejected2041(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, _, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	graph := graphManifestFromRecords1918(t, records, def)
	assets, found, err := columnVectorGraphRowRefStateAssetsByField(state)
	if err != nil || !found {
		t.Fatalf("row-ref assets found=%t err=%v", found, err)
	}
	asset := assets[columnVectorGraphRowRefStateFieldRowIndex]
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		t.Fatalf("read row-ref asset: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	section, err := columnVectorGraphRowRefStateSection(image, columnVectorGraphRowRefStateColumnName(columnVectorGraphRowRefStateFieldRowIndex))
	if err != nil {
		t.Fatalf("row-ref section: %v", err)
	}
	baseRows, err := columnVectorGraphRowRefBasePartRows(records, graph.BaseManifestGeneration, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("base part rows: %v", err)
	}
	outOfBoundsRow := 0
	for _, rows := range baseRows {
		if rows > outOfBoundsRow {
			outOfBoundsRow = rows
		}
	}
	corrupt := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint64(corrupt[section.Offset:], uint64(outOfBoundsRow))
	badAsset := asset
	badAsset.Ref.Checksum = page.Checksum(corrupt)
	badState := replaceColumnVectorIndexStateAssetForTest2041(t, state, badAsset)
	writeColumnVectorGraphAssetRawForTest2041(t, d.ColumnAssetRootDir(), asset.Ref, corrupt)
	if _, err := newColumnVectorGraphRowRefStateSourceFromRoot(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, badState, records); err == nil || (!strings.Contains(err.Error(), "outside base rows") && !strings.Contains(err.Error(), "layout certification") && !strings.Contains(err.Error(), "checksum")) {
		t.Fatalf("row-ref corrupt field err=%v, want field validation rejection", err)
	}
}

func TestColumnVectorGraphRowRefStateHeapCopyFallback2041(t *testing.T) {
	const rows = 2
	raw := make([]byte, rows*8)
	binary.LittleEndian.PutUint64(raw[0:], 7)
	binary.LittleEndian.PutUint64(raw[8:], 11)
	manager := mappedresource.NewManager()
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "test", Kind: string(ColumnAssetKindTCS1TypedColumnPart), Generation: 1, PartID: 2, FileID: 3, Length: int64(len(raw)), Encoding: typedcolumn.EncodingRawInt64.String()}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "row-ref-heap-fallback", Namespace: "test", Generation: 1}
	handle, err := manager.AcquireBytes(key, scope, mappedresource.SourceHeapCopy, raw, mappedresource.AcquireOptions{Reason: "row-ref heap fallback test"})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	expectation := columnVectorGraphDirectViewExpectation(columnVectorIndexStateAssetRoleRowRefs, columnVectorIndexStateAssetRoleRowRefs, columnVectorGraphRowRefStateColumnName(columnVectorGraphRowRefStateFieldRowIndex), rows, ColumnAssetRef{Namespace: "test", Kind: ColumnAssetKindTCS1TypedColumnPart, Generation: 1, PartID: 2, FileID: 3})
	view, err := columnVectorGraphRowRefStatePreparedViewFromFallbackHandle(expectation, manager, handle, rows, typeddecode.StreamingStatus(typeddecode.ReasonHandleSourceUnsupported, "heap fallback test"))
	if err != nil {
		_ = handle.Release()
		t.Fatalf("fallback view: %v", err)
	}
	if !view.Alive() || view.Handle.Source() != mappedresource.SourceHeapCopy {
		_ = view.Close()
		t.Fatalf("view alive=%t source=%s want heap-copy prepared view", view.Alive(), view.Handle.Source())
	}
	if got, ok := view.Value(1); !ok || got != 11 {
		_ = view.Close()
		t.Fatalf("view.Value(1)=%d ok=%t want 11", got, ok)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("view close: %v", err)
	}
}

func TestColumnVectorGraphRowRefStateMmapDirectFieldCountTracksCertification2041(t *testing.T) {
	raw := make([]byte, 8)
	manager := mappedresource.NewManager()
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "test", Kind: string(ColumnAssetKindTCS1TypedColumnPart), Generation: 1, PartID: 2, FileID: 3, Length: int64(len(raw)), Encoding: typedcolumn.EncodingRawInt64.String()}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "row-ref-mmap-count", Namespace: "test", Generation: 1}
	handle, err := manager.AcquireBytes(key, scope, mappedresource.SourceMapped, raw, mappedresource.AcquireOptions{Reason: "row-ref mmap count test"})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	view := typeddecode.PreparedInt64DirectView{Rows: 1, Values: []int64{42}, Handle: handle}
	source := &columnVectorGraphRowRefStateSource{rows: 1, generations: view, partIDs: view, rowIndexes: view, appliedCommandLSNs: view}
	if got := source.mmapDirectFieldCount(); got != 0 {
		_ = source.Close()
		t.Fatalf("mmapDirectFieldCount=%d want 0 for fallback values retained behind a mapped handle", got)
	}
	source.mmapDirectFields = 3
	if got := source.mmapDirectFieldCount(); got != 3 {
		_ = source.Close()
		t.Fatalf("mmapDirectFieldCount=%d want explicit certification count 3", got)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("source close: %v", err)
	}
}

func TestColumnVectorGraphRowRefStatePreservesOpaqueResultIDs1993(t *testing.T) {
	dir, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, nil)
	_ = dir
	id := []byte{0x00, 'd', 'o', 'c', 0xff}
	if _, err := col.Insert(id, []byte(`{"time_us":1,"kind":"vector","did":"opaque","embedding":[1,0,0],"title":"opaque"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert opaque id: %v", err)
	}
	if _, err := col.Insert([]byte("doc-b"), []byte(`{"time_us":2,"kind":"vector","did":"doc-b","embedding":[0,1,0],"title":"text"}`)); err != nil {
		_ = d.Close()
		t.Fatalf("Insert doc-b: %v", err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: 2, StatsMode: VectorIndexSearchStatsModeBenchmarkDebug})
	if err != nil {
		_ = d.Close()
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if len(got.Results) != 1 || !bytes.Equal(got.Results[0].ID, id) {
		_ = d.Close()
		t.Fatalf("result id=%v want exact opaque bytes %v", got.Results, id)
	}
	if got.Stats.ResultIDPreparedBytesViews != 1 || got.Stats.ResultIDTypedBytesState != 1 || got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.RowRefVectorSourceState != 1 || got.Stats.RowRefStatePreparedViews != 1 {
		_ = d.Close()
		t.Fatalf("stats=%+v want prepared row-ref state for locators and typed bytes state for opaque returned ID", got.Stats)
	}
	_ = d.Close()
}

func TestColumnVectorGraphRowRefStateValidationFailures1993(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	graph := graphManifestFromRecords1918(t, records, def)

	t.Run("missing_field", func(t *testing.T) {
		candidate := state
		candidate.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
		for i := range candidate.Assets {
			if candidate.Assets[i].Role == columnVectorIndexStateAssetRoleRowRefs && candidate.Assets[i].AssetID == columnVectorGraphRowRefStateAssetID(columnVectorGraphRowRefStateFieldRowIndex) {
				candidate.Assets = append(candidate.Assets[:i], candidate.Assets[i+1:]...)
				break
			}
		}
		if _, err := newColumnVectorGraphRowRefStateSourceFromRoot(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, candidate, records); err == nil || !strings.Contains(err.Error(), "missing fields") {
			t.Fatalf("row-ref source err=%v want missing fields", err)
		}
	})

	t.Run("wrong_type_encoding", func(t *testing.T) {
		candidate := state
		candidate.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
		for i := range candidate.Assets {
			if candidate.Assets[i].Role == columnVectorIndexStateAssetRoleRowRefs {
				candidate.Assets[i].PhysicalEncoding = columnVectorIndexStateEncodingRawFloat32
				break
			}
		}
		if _, err := encodeColumnVectorIndexStateRecord(candidate); err == nil || !strings.Contains(err.Error(), "type/encoding") {
			t.Fatalf("encode err=%v want type/encoding", err)
		}
	})

	t.Run("out_of_bounds", func(t *testing.T) {
		baseRows, err := columnVectorGraphRowRefBasePartRows(records, graph.BaseManifestGeneration, cfg.AssetManager.Namespace)
		if err != nil {
			t.Fatalf("base part rows: %v", err)
		}
		var key documentRowPartKey
		var baseRowCount int
		for candidate, rows := range baseRows {
			if rows > baseRowCount {
				key, baseRowCount = candidate, rows
			}
		}
		ref := DocumentRowRef{Generation: key.Generation, PartID: key.PartID, RowIndex: baseRowCount, AppliedCommandLSN: 1}
		if err := validateColumnVectorGraphRowRefStateBounds(def.Name, 0, ref, baseRows); err == nil || !strings.Contains(err.Error(), "outside base rows") {
			t.Fatalf("bounds err=%v want outside base rows", err)
		}
	})
}
