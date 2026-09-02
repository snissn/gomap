package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnVectorGraphDocumentIDStatePublishesSearchAndReopen2013(t *testing.T) {
	dir, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, nil)
	idA := []byte{0x00, 'd', 'o', 'c', 0xff, 0x00}
	idB := []byte{'d', 'o', 'c', '-', 'b'}
	idC := []byte{'d', 0x00, 'c'}
	insertVectorDocumentWithID2013(t, col, idA, []float32{1, 0, 0})
	insertVectorDocumentWithID2013(t, col, idB, []float32{0, 1, 0})
	insertVectorDocumentWithID2013(t, col, idC, []float32{0, 0, 1})
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	asset, found, err := findColumnVectorGraphDocumentIDStateAsset(state)
	if err != nil || !found {
		_ = d.Close()
		t.Fatalf("document-id state asset found=%t err=%v state=%+v", found, err, state)
	}
	if asset.LogicalType != columnVectorIndexStateLogicalTypeBytes || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawBytesOffsets || asset.RowCount != 3 || asset.AssetBytes <= 0 {
		_ = d.Close()
		t.Fatalf("document-id state asset=%+v", asset)
	}
	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	source, _, err := newColumnVectorGraphDocumentIDStateSourceFromRoot(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, state)
	if err != nil {
		_ = d.Close()
		t.Fatalf("newColumnVectorGraphDocumentIDStateSourceFromRoot: %v", err)
	}
	for ordinal, row := range scanned {
		got, ok := source.documentIDForOrdinal(ordinal)
		if !ok || !bytes.Equal(got, []byte(row.id)) {
			_ = d.Close()
			t.Fatalf("document-id state ordinal=%d got=%v ok=%t want scanned id %v", ordinal, got, ok, []byte(row.id))
		}
	}
	if err := source.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("source close: %v", err)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: 3})
	if err != nil {
		_ = d.Close()
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if len(got.Results) != 1 || !bytes.Equal(got.Results[0].ID, idA) {
		_ = d.Close()
		t.Fatalf("result id=%v want exact opaque bytes %v", got.Results, idA)
	}
	if got.Stats.ResultIDPreparedBytesViews != 1 || got.Stats.ResultIDTypedBytesState != 1 || got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.ResultIDStateValidationFailures != 0 {
		_ = d.Close()
		t.Fatalf("stats=%+v want prepared typed bytes result id without graph fallback", got.Stats)
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
	reopenedGot, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: 3})
	if err != nil {
		t.Fatalf("SearchVectorIndex reopen: %v", err)
	}
	if len(reopenedGot.Results) != 1 || !bytes.Equal(reopenedGot.Results[0].ID, idA) {
		t.Fatalf("reopen result id=%v want exact opaque bytes %v", reopenedGot.Results, idA)
	}
	if reopenedGot.Stats.ResultIDPreparedBytesViews != 1 || reopenedGot.Stats.ResultIDTypedBytesState != 1 || reopenedGot.Stats.ResultIDGraphFallbacks != 0 || reopenedGot.Stats.ResultIDStateValidationFailures != 0 {
		t.Fatalf("reopen stats=%+v want prepared typed bytes result id without graph fallback", reopenedGot.Stats)
	}
}

func TestColumnVectorGraphDocumentIDStateMissingFallsBack2013(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1, 2}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0, 2}},
		{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, InvNorm: 1, Adjacency: []uint32{0, 1}},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, rows)
	defer func() { _ = d.Close() }()

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 2, EfSearch: 3})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if len(got.Results) != 2 {
		t.Fatalf("results=%d want 2", len(got.Results))
	}
	assertSearchResultIDsMatchGraphRows2013(t, got.Results, rows)
	if got.Stats.ResultIDPreparedBytesViews != 0 || got.Stats.ResultIDTypedBytesState != 0 || got.Stats.ResultIDGraphFallbacks != uint64(len(got.Results)) || got.Stats.ResultIDStateValidationFailures != 0 {
		t.Fatalf("stats=%+v want missing document-id state graph fallback", got.Stats)
	}
}

func assertSearchResultIDsMatchGraphRows2013(t *testing.T, results []VectorIndexSearchResult, rows []columnVectorGraphAssetRow) {
	t.Helper()
	for resultIdx, result := range results {
		matched := false
		for _, row := range rows {
			if bytes.Equal(result.ID, row.ID) {
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("result[%d] id=%v does not match exact graph row IDs %v", resultIdx, result.ID, rows)
		}
	}
}

func TestColumnVectorGraphDocumentIDStateMissingAssetFailsClosed2013(t *testing.T) {
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
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	asset, found, err := findColumnVectorGraphDocumentIDStateAsset(state)
	if err != nil || !found {
		t.Fatalf("document-id state asset found=%t err=%v", found, err)
	}
	removeTypedColumnAssetPayload1755(t, d, col, asset.Ref)

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 2, EfSearch: 3})
	if err == nil || !strings.Contains(err.Error(), "missing required vector-index document-id state source") {
		t.Fatalf("SearchVectorIndex err=%v response=%+v want fail-closed missing document-id state", err, got)
	}
	if got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.ResultIDTypedBytesState != 0 {
		t.Fatalf("stats=%+v want no graph row result-id fallback", got.Stats)
	}
}

func TestColumnVectorGraphDocumentIDStateCorruptOffsetsRejected2041(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, _, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
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
	asset, found, err := findColumnVectorGraphDocumentIDStateAsset(state)
	if err != nil || !found {
		t.Fatalf("document-id state asset found=%t err=%v", found, err)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		t.Fatalf("read document-id asset: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	offsetsSection, _, ok := image.ColumnOffsetsListSections(columnVectorGraphDocumentIDStateColumnName)
	if !ok {
		t.Fatalf("missing document-id offsets section")
	}
	corrupt := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint64(corrupt[offsetsSection.Offset+8:], uint64(len(corrupt))*2)
	badAsset := asset
	badAsset.Ref.Checksum = page.Checksum(corrupt)
	badState := replaceColumnVectorIndexStateAssetForTest2041(t, state, badAsset)
	writeColumnVectorGraphAssetRawForTest2041(t, d.ColumnAssetRootDir(), asset.Ref, corrupt)
	if _, _, err := newColumnVectorGraphDocumentIDStateSourceFromRoot(d.ColumnAssetRootDir(), "docs", *cfg, def, graph, badState); err == nil || (!strings.Contains(err.Error(), "offset") && !strings.Contains(err.Error(), "values")) {
		t.Fatalf("document-id corrupt offsets err=%v, want offset/value rejection", err)
	}
}

func TestColumnVectorGraphDocumentIDStateHeapCopyFallback2041(t *testing.T) {
	const rows = 2
	offsetsRaw, err := typedcolumn.EncodeRawBytesOffsetsOffsets(nil, []uint64{0, 5, 9})
	if err != nil {
		t.Fatalf("EncodeRawBytesOffsetsOffsets: %v", err)
	}
	valuesRaw := []byte("alphabeta")
	offsetsSection, valuesSection, err := typedcolumn.NewRawBytesOffsetsImageSections(columnVectorGraphDocumentIDStateColumnName, rows, len(offsetsRaw), len(valuesRaw))
	if err != nil {
		t.Fatalf("NewRawBytesOffsetsImageSections: %v", err)
	}
	certColumn := typedcolumn.ColumnPartLayoutContractColumn{
		Name:                columnVectorGraphDocumentIDStateColumnName,
		LogicalType:         columnVectorIndexStateLogicalTypeBytes,
		Type:                typedcolumn.ColumnTypeBytes,
		Encoding:            typedcolumn.EncodingRawBytesOffsets,
		Compression:         typedcolumn.CompressionNone,
		Rows:                rows,
		OffsetsSection:      typedcolumn.ColumnPartLayoutContractSection{Length: len(offsetsRaw)},
		ValuesSection:       typedcolumn.ColumnPartLayoutContractSection{Length: len(valuesRaw)},
		OffsetsBytes:        len(offsetsRaw),
		ValuesBytes:         len(valuesRaw),
		ElementSize:         1,
		Alignment:           1,
		Endian:              typedcolumn.ColumnPartLayoutEndianLittle,
		LengthMultiple:      1,
		DirectViewCertified: true,
	}
	manager := mappedresource.NewManager()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "document-id-heap-fallback", Namespace: "test", Generation: 1}
	offsetsKey := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "test", Kind: string(ColumnAssetKindTCS1TypedColumnPart), Generation: 1, PartID: 2, FileID: 3, Length: int64(len(offsetsRaw)), Encoding: typedcolumn.EncodingRawBytesOffsets.String()}
	valuesKey := offsetsKey
	valuesKey.Length = int64(len(valuesRaw))
	offsetsHandle, err := manager.AcquireBytes(offsetsKey, scope, mappedresource.SourceHeapCopy, offsetsRaw, mappedresource.AcquireOptions{Reason: "document-id offsets heap fallback test"})
	if err != nil {
		t.Fatalf("AcquireBytes offsets: %v", err)
	}
	valuesHandle, err := manager.AcquireBytes(valuesKey, scope, mappedresource.SourceHeapCopy, valuesRaw, mappedresource.AcquireOptions{Reason: "document-id values heap fallback test"})
	if err != nil {
		_ = offsetsHandle.Release()
		t.Fatalf("AcquireBytes values: %v", err)
	}
	expectation := columnVectorGraphDirectViewExpectation(columnVectorIndexStateAssetRoleDocumentIDs, columnVectorIndexStateAssetRoleDocumentIDs, columnVectorGraphDocumentIDStateColumnName, rows, ColumnAssetRef{Namespace: "test", Kind: ColumnAssetKindTCS1TypedColumnPart, Generation: 1, PartID: 2, FileID: 3})
	view, err := columnVectorGraphDocumentIDStatePreparedViewFromFallbackHandles(expectation, certColumn, offsetsSection, valuesSection, offsetsHandle, valuesHandle, manager, rows, typeddecode.StreamingStatus(typeddecode.ReasonHandleSourceUnsupported, "heap fallback test"))
	if err != nil {
		_ = offsetsHandle.Release()
		_ = valuesHandle.Release()
		t.Fatalf("fallback view: %v", err)
	}
	if !view.Alive() || view.OffsetsHandle.Source() != mappedresource.SourceHeapCopy || view.ValuesHandle.Source() != mappedresource.SourceHeapCopy {
		_ = view.Close()
		t.Fatalf("view alive=%t offsets_source=%s values_source=%s want heap-copy prepared view", view.Alive(), view.OffsetsHandle.Source(), view.ValuesHandle.Source())
	}
	if got := string(view.Row(1)); got != "beta" {
		_ = view.Close()
		t.Fatalf("view.Row(1)=%q want beta", got)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("view close: %v", err)
	}
}

func TestColumnVectorGraphDocumentIDStateCorruptFailsClosed2013(t *testing.T) {
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
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	asset, found, err := findColumnVectorGraphDocumentIDStateAsset(state)
	if err != nil || !found {
		t.Fatalf("document-id state asset found=%t err=%v", found, err)
	}
	corruptTypedColumnAssetPayload1755(t, d, asset.Ref)

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 2, EfSearch: 3})
	if err == nil || !strings.Contains(err.Error(), "missing required vector-index document-id state source") {
		t.Fatalf("SearchVectorIndex err=%v response=%+v want fail-closed corrupt document-id state", err, got)
	}
	if got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.ResultIDTypedBytesState != 0 {
		t.Fatalf("stats=%+v want no graph row result-id fallback", got.Stats)
	}
}

func insertVectorDocumentWithID2013(tb testing.TB, col *Collection, id []byte, vector []float32) {
	tb.Helper()
	raw, err := jsonMarshalVectorDocument2013(vector)
	if err != nil {
		tb.Fatalf("jsonMarshalVectorDocument2013: %v", err)
	}
	if _, err := col.Insert(id, raw); err != nil {
		tb.Fatalf("Insert id %v: %v", id, err)
	}
}

func jsonMarshalVectorDocument2013(vector []float32) ([]byte, error) {
	return json.Marshal(map[string]any{
		"time_us":   int64(1),
		"kind":      "vector",
		"did":       "opaque",
		"embedding": vector,
	})
}
