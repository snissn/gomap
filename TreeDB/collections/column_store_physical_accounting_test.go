package collections

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestColumnStorePhysicalAccountingReportsTypedColumnSections2118(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, typedRefs := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()
	if len(typedRefs) != 1 {
		t.Fatalf("fixture typed refs=%d want 1", len(typedRefs))
	}

	accounting, err := collection.ColumnStorePhysicalAccounting(context.Background(), ColumnStorePhysicalAccountingOptions{DetailedSections: true})
	if err != nil {
		t.Fatalf("ColumnStorePhysicalAccounting: %v", err)
	}
	if !accounting.Complete {
		t.Fatal("Complete=false")
	}
	if accounting.Collection != "events" || accounting.Namespace != "events/column-assets" {
		t.Fatalf("identity=%s/%s want events/events/column-assets", accounting.Collection, accounting.Namespace)
	}
	if accounting.ManifestGeneration == 0 || accounting.ManifestChecksum == 0 || accounting.ManifestRecords == 0 {
		t.Fatalf("manifest identity missing: %+v", accounting)
	}
	if got, want := accounting.TypedColumnPartRefs, 1; got != want {
		t.Fatalf("typed part refs=%d want %d", got, want)
	}
	if got, want := accounting.RowAssetRefs, 1; got != want {
		t.Fatalf("row asset refs=%d want %d", got, want)
	}
	if got, want := len(accounting.RowAssets), 1; got != want {
		t.Fatalf("row assets=%d want %d", got, want)
	}
	if got, want := len(accounting.TypedColumnParts), 1; got != want {
		t.Fatalf("typed parts=%d want %d", got, want)
	}

	rowAsset := accounting.RowAssets[0]
	if rowAsset.Asset.Ref.Kind != ColumnAssetKindTCS1PartImage {
		t.Fatalf("row asset kind=%q want %q", rowAsset.Asset.Ref.Kind, ColumnAssetKindTCS1PartImage)
	}
	if got, want := rowAsset.Asset.Rows, len(events); got != want {
		t.Fatalf("row asset rows=%d want %d", got, want)
	}
	if rowAsset.Payload.TotalStoredBytes != rowAsset.Asset.Bytes || rowAsset.Payload.SerializedAssetBytes != rowAsset.Asset.Bytes {
		t.Fatalf("row payload bytes=%d serialized=%d asset=%d", rowAsset.Payload.TotalStoredBytes, rowAsset.Payload.SerializedAssetBytes, rowAsset.Asset.Bytes)
	}
	if got, want := rowAsset.Payload.Columns, 0; got != want {
		t.Fatalf("row asset columns=%d want %d for all-typed-column fixture", got, want)
	}
	if got, want := rowAsset.Payload.Rows, len(events); got != want {
		t.Fatalf("row payload rows=%d want %d", got, want)
	}
	if rowAsset.Payload.FormatHeaderBytes == 0 || rowAsset.Payload.RowEncodingHeaderBytes == 0 || rowAsset.Payload.RowIDStoredBytes == 0 {
		t.Fatalf("row payload accounting missing header/encoding/id bytes: %+v", rowAsset.Payload)
	}
	var wantIDValueBytes int64
	for _, event := range events {
		wantIDValueBytes += int64(len(event.ID))
	}
	if got := rowAsset.Payload.RowIDValueBytes; got != wantIDValueBytes {
		t.Fatalf("row id value bytes=%d want %d", got, wantIDValueBytes)
	}
	if got, want := rowAsset.Payload.RowIDStoredBytes, wantIDValueBytes; got != want {
		t.Fatalf("row id stored bytes=%d want %d", got, want)
	}
	if got, want := rowAsset.Payload.RowDeletedFlagBytes, int64(0); got != want {
		t.Fatalf("row deleted flag bytes=%d want %d", got, want)
	}
	if rowAsset.Payload.RowValueHeaderBytes != 0 || rowAsset.Payload.RowValuePayloadBytes != 0 || len(rowAsset.Payload.ColumnsDetail) != 0 {
		t.Fatalf("row value bytes/detail should be empty for zero row-owned columns: %+v", rowAsset.Payload)
	}
	rowCategoryBytes := rowAsset.Payload.FormatHeaderBytes +
		rowAsset.Payload.ColumnMetadataBytes +
		rowAsset.Payload.RowEncodingHeaderBytes +
		rowAsset.Payload.RowIDStoredBytes +
		rowAsset.Payload.RowDeletedFlagBytes +
		rowAsset.Payload.RowValueHeaderBytes +
		rowAsset.Payload.RowValuePayloadBytes
	if rowCategoryBytes != rowAsset.Payload.TotalStoredBytes {
		t.Fatalf("row category bytes=%d want total stored bytes %d", rowCategoryBytes, rowAsset.Payload.TotalStoredBytes)
	}

	part := accounting.TypedColumnParts[0]
	if part.Asset.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
		t.Fatalf("typed part kind=%q want %q", part.Asset.Ref.Kind, ColumnAssetKindTCS1TypedColumnPart)
	}
	if got, want := part.Asset.Rows, len(events); got != want {
		t.Fatalf("typed part rows=%d want %d", got, want)
	}
	if got, want := part.Asset.Bytes, typedRefs[0].Length; got != want {
		t.Fatalf("typed part bytes=%d want ref length %d", got, want)
	}
	if part.Image.SerializedImageBytes != part.Asset.Bytes || part.Image.TotalStoredBytes != part.Asset.Bytes {
		t.Fatalf("image bytes=%d total=%d asset=%d", part.Image.SerializedImageBytes, part.Image.TotalStoredBytes, part.Asset.Bytes)
	}
	if part.Image.SerializedManifestBytes == 0 || part.Image.DescriptorBytes == 0 || part.Image.DeclaredColumnBytes == 0 || part.Image.DictionaryBytes == 0 || part.Image.LocatorBytes == 0 {
		t.Fatalf("section categories missing from image accounting: %+v", part.Image)
	}
	if got, want := part.Image.Rows, len(events); got != want {
		t.Fatalf("image rows=%d want %d", got, want)
	}
	if part.Image.BytesPerRow <= 0 {
		t.Fatalf("bytes_per_row=%f want >0", part.Image.BytesPerRow)
	}
	if len(part.Image.SerializedSections) == 0 {
		t.Fatal("serialized sections empty with DetailedSections=true")
	}
	sectionBytes := int64(0)
	for _, section := range part.Image.SerializedSections {
		if section.Category == "" || section.Kind == "" || section.Bytes < 0 {
			t.Fatalf("bad section accounting: %+v", section)
		}
		sectionBytes += section.Bytes
	}
	if sectionBytes != part.Image.TotalStoredBytes {
		t.Fatalf("section bytes=%d want total stored bytes %d", sectionBytes, part.Image.TotalStoredBytes)
	}
	categoryBytes := part.Image.SerializedManifestBytes +
		part.Image.SerializedPaddingBytes +
		part.Image.DeclaredColumnBytes +
		part.Image.DeclaredColumnOffsetsBytes +
		part.Image.DeclaredColumnValuesBytes +
		part.Image.DictionaryBytes +
		part.Image.MarkBytes +
		part.Image.SortKeyMetadataBytes +
		part.Image.AggregateMetadataBytes +
		part.Image.ColumnStatsBytes +
		part.Image.PruningMetadataBytes +
		part.Image.DescriptorBytes +
		part.Image.LayoutContractBytes +
		part.Image.LocatorBytes
	if categoryBytes != part.Image.TotalStoredBytes {
		t.Fatalf("category bytes=%d want total stored bytes %d", categoryBytes, part.Image.TotalStoredBytes)
	}

	if got, want := accounting.Totals.TypedColumnPartBytes, typedRefs[0].Length; got != want {
		t.Fatalf("typed column part total=%d want %d", got, want)
	}
	if got, want := accounting.Totals.RowAssetSections.TotalStoredBytes, accounting.Totals.RowAssetBytes; got != want {
		t.Fatalf("row section total=%d want row asset bytes %d", got, want)
	}
	if got, want := accounting.Totals.RowAssetSections.RowIDValueBytes, wantIDValueBytes; got != want {
		t.Fatalf("total row id value bytes=%d want %d", got, want)
	}
	if got, want := accounting.Totals.TypedColumnSections.TotalStoredBytes, accounting.Totals.TypedColumnPartBytes; got != want {
		t.Fatalf("typed section total=%d want typed part bytes %d", got, want)
	}
	if accounting.Totals.ReferencedAssetBytes < accounting.Totals.TypedColumnPartBytes {
		t.Fatalf("referenced bytes=%d below typed part bytes=%d", accounting.Totals.ReferencedAssetBytes, accounting.Totals.TypedColumnPartBytes)
	}
	if !columnStorePhysicalAccountingHasKind(accounting.AssetKinds, ColumnAssetKindTCS1TypedColumnPart, typedRefs[0].Length) {
		t.Fatalf("asset kind totals missing typed-column part: %+v", accounting.AssetKinds)
	}
	totalCategoryBytes := accounting.Totals.TypedColumnSections.SerializedManifestBytes +
		accounting.Totals.TypedColumnSections.SerializedPaddingBytes +
		accounting.Totals.TypedColumnSections.DeclaredColumnBytes +
		accounting.Totals.TypedColumnSections.DeclaredColumnOffsetsBytes +
		accounting.Totals.TypedColumnSections.DeclaredColumnValuesBytes +
		accounting.Totals.TypedColumnSections.DictionaryBytes +
		accounting.Totals.TypedColumnSections.MarkBytes +
		accounting.Totals.TypedColumnSections.SortKeyMetadataBytes +
		accounting.Totals.TypedColumnSections.AggregateMetadataBytes +
		accounting.Totals.TypedColumnSections.ColumnStatsBytes +
		accounting.Totals.TypedColumnSections.PruningMetadataBytes +
		accounting.Totals.TypedColumnSections.DescriptorBytes +
		accounting.Totals.TypedColumnSections.LayoutContractBytes +
		accounting.Totals.TypedColumnSections.LocatorBytes
	if totalCategoryBytes != accounting.Totals.TypedColumnSections.TotalStoredBytes {
		t.Fatalf("total category bytes=%d want total stored bytes %d", totalCategoryBytes, accounting.Totals.TypedColumnSections.TotalStoredBytes)
	}
}

func TestColumnStorePhysicalAccountingColumnDataSectionCompressionLabel1952(t *testing.T) {
	tests := []struct {
		name   string
		detail typedcolumn.ColumnPartColumnByteAccounting
		want   string
	}{
		{name: "all none", detail: typedcolumn.ColumnPartColumnByteAccounting{RequestedCompression: typedcolumn.CompressionSnappy, ActualCompressionMix: map[string]int{"none": 2}}, want: "none"},
		{name: "all snappy", detail: typedcolumn.ColumnPartColumnByteAccounting{RequestedCompression: typedcolumn.CompressionSnappy, ActualCompressionMix: map[string]int{"snappy": 2}}, want: "snappy"},
		{name: "mixed", detail: typedcolumn.ColumnPartColumnByteAccounting{RequestedCompression: typedcolumn.CompressionSnappy, ActualCompressionMix: map[string]int{"none": 1, "snappy": 1}}, want: "mixed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := columnStoreTypedColumnPartColumnSectionCompression(tc.detail); got != tc.want {
				t.Fatalf("column section compression=%q want %q", got, tc.want)
			}
		})
	}
}

func TestColumnStorePhysicalAccountingOmitDetailedSections2118(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	accounting, err := collection.ColumnStorePhysicalAccounting(context.Background(), ColumnStorePhysicalAccountingOptions{})
	if err != nil {
		t.Fatalf("ColumnStorePhysicalAccounting: %v", err)
	}
	if len(accounting.TypedColumnParts) != 1 {
		t.Fatalf("typed parts=%d want 1", len(accounting.TypedColumnParts))
	}
	if len(accounting.TypedColumnParts[0].Image.SerializedSections) != 0 {
		t.Fatalf("serialized sections emitted without DetailedSections: %+v", accounting.TypedColumnParts[0].Image.SerializedSections)
	}
	if accounting.TypedColumnParts[0].Image.TotalStoredBytes == 0 || accounting.Totals.TypedColumnSections.TotalStoredBytes == 0 {
		t.Fatalf("compact section totals missing: part=%+v totals=%+v", accounting.TypedColumnParts[0].Image, accounting.Totals.TypedColumnSections)
	}
	encoded, err := json.Marshal(accounting.Totals)
	if err != nil {
		t.Fatalf("Marshal totals: %v", err)
	}
	if !strings.Contains(string(encoded), `"typed_column_sections":`) {
		t.Fatalf("json=%s want typed_column_sections field", encoded)
	}
}

func TestPhysicalAccountingIncompleteOnTypedPartError2118(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecars(context.Background(), columnManifestScanAllSidecars())
	if err != nil {
		t.Fatalf("prepare snapshot view: %v", err)
	}
	if len(view.TypedColumnPartRefs) != 1 {
		closeView()
		t.Fatalf("typed part refs=%d want 1", len(view.TypedColumnPartRefs))
	}
	root := view.ColumnAssetRootDir
	ref := view.TypedColumnPartRefs[0].Ref
	closeView()

	corruptColumnAssetPayloadByte(t, root, ref)
	accounting, err := collection.ColumnStorePhysicalAccounting(context.Background(), ColumnStorePhysicalAccountingOptions{})
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("accounting error=%v, want checksum failure", err)
	}
	if accounting.Complete {
		t.Fatalf("Complete=true on error: %+v", accounting)
	}
	if accounting.TypedColumnPartRefs != 1 {
		t.Fatalf("typed part refs=%d want partial identity before error", accounting.TypedColumnPartRefs)
	}
}

func TestPhysicalAccountingRejectsTypedPartManifestMismatch2118(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()

	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecars(context.Background(), columnManifestScanAllSidecars())
	if err != nil {
		t.Fatalf("prepare snapshot view: %v", err)
	}
	defer func() {
		if closeView != nil {
			closeView()
		}
	}()
	if len(view.TypedColumnPartRefs) != 1 {
		t.Fatalf("typed part refs=%d want 1", len(view.TypedColumnPartRefs))
	}
	base := view.TypedColumnPartRefs[0]

	rowsMismatch := base
	rowsMismatch.Rows++
	_, err = columnStoreTypedColumnPartAccountingFromRef(view.ColumnAssetRootDir, rowsMismatch, ColumnStorePhysicalAccountingOptions{})
	if err == nil || !strings.Contains(err.Error(), "image rows") {
		t.Fatalf("rows mismatch error=%v, want image rows mismatch", err)
	}

	raw, err := readColumnPhysicalAssetFromManager(view.ColumnAssetRootDir, base.Ref)
	if err != nil {
		t.Fatalf("read typed part asset: %v", err)
	}
	cfg := view.FullConfig
	closeView()
	closeView = nil

	copyRoot := t.TempDir()
	copiedRef, err := writeColumnAssetToManagerSegment(copyRoot, cfg, raw, base.Ref.Kind, base.Ref.Generation, base.Ref.PartID+1000, base.Ref.FileID)
	if err != nil {
		t.Fatalf("write copied typed part asset: %v", err)
	}
	partIDMismatch := base
	partIDMismatch.Ref = copiedRef
	_, err = columnStoreTypedColumnPartAccountingFromRef(copyRoot, partIDMismatch, ColumnStorePhysicalAccountingOptions{})
	if err == nil || !strings.Contains(err.Error(), "image part_id") {
		t.Fatalf("part_id mismatch error=%v, want image part_id mismatch", err)
	}
}

func TestPhysicalAccountingCountsGraphDuplicateTypedPartBytes2118(t *testing.T) {
	events := columnPhysicalJSONBenchParityEventsP0()
	_, collection, closeFn, typedRefs := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(t, events)
	defer closeFn()
	if len(typedRefs) != 1 {
		t.Fatalf("typed refs=%d want 1", len(typedRefs))
	}

	view, closeView, err := collection.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecars(context.Background(), columnManifestScanAllSidecars())
	if err != nil {
		t.Fatalf("prepare snapshot view: %v", err)
	}
	defer closeView()
	if len(view.GraphAssetRefs) != 0 {
		t.Fatalf("fixture graph refs=%d want 0 before duplicate injection", len(view.GraphAssetRefs))
	}
	view.GraphAssetRefs = append(view.GraphAssetRefs, typedRefs[0])

	accounting, err := physicalAccountingFromScanView(context.Background(), view, ColumnStorePhysicalAccountingOptions{})
	if err != nil {
		t.Fatalf("physicalAccountingFromScanView: %v", err)
	}
	if !accounting.Complete {
		t.Fatal("Complete=false")
	}
	if got, want := accounting.GraphAssetRefs, 1; got != want {
		t.Fatalf("graph refs=%d want %d", got, want)
	}
	if got, want := len(accounting.GraphAssets), 1; got != want {
		t.Fatalf("graph assets=%d want %d", got, want)
	}
	if got, want := accounting.Totals.GraphAssetBytes, typedRefs[0].Length; got != want {
		t.Fatalf("graph bytes=%d want duplicate typed ref length %d", got, want)
	}
	if got, want := accounting.Totals.TypedColumnPartBytes, typedRefs[0].Length; got != want {
		t.Fatalf("typed part bytes=%d want %d", got, want)
	}
	var wantReferenced int64
	for _, ref := range columnPhysicalScanSnapshotViewAssetRefs(view) {
		wantReferenced = addColumnStorePhysicalAccountingBytes(wantReferenced, positiveColumnStorePhysicalAccountingBytes(ref.Length))
	}
	if got := accounting.Totals.ReferencedAssetBytes; got != wantReferenced {
		t.Fatalf("referenced bytes=%d want globally deduped %d", got, wantReferenced)
	}
	if accounting.Totals.ReferencedAssetBytes == accounting.Totals.TypedColumnPartBytes+accounting.Totals.GraphAssetBytes {
		t.Fatalf("referenced bytes double-counted duplicate typed graph ref: %+v", accounting.Totals)
	}
}

func TestColumnStorePhysicalAssetRefAccountingJSONIncludesZeroIdentityFields2118(t *testing.T) {
	encoded, err := json.Marshal(ColumnStorePhysicalAssetRefAccounting{
		Kind: ColumnAssetKindTCS1TypedColumnPart,
	})
	if err != nil {
		t.Fatalf("Marshal asset ref accounting: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal asset ref accounting: %v", err)
	}
	for _, field := range []string{"generation", "part_id", "file_id", "offset", "length", "checksum"} {
		raw, ok := decoded[field]
		if !ok {
			t.Fatalf("json=%s missing explicit zero identity field %q", encoded, field)
		}
		if string(raw) != "0" {
			t.Fatalf("json=%s field %q=%s want 0", encoded, field, raw)
		}
	}
}

func TestColumnStoreTypedColumnPartByteAccountingAggregatesDistinctColumns2118(t *testing.T) {
	var total ColumnStoreTypedColumnPartByteAccounting
	addColumnStoreTypedColumnPartByteAccounting(&total, ColumnStoreTypedColumnPartByteAccounting{
		Rows:             2,
		Columns:          2,
		TotalStoredBytes: 20,
		columnNames:      []string{"id", "value"},
	})
	addColumnStoreTypedColumnPartByteAccounting(&total, ColumnStoreTypedColumnPartByteAccounting{
		Rows:             3,
		Columns:          2,
		TotalStoredBytes: 30,
		columnNames:      []string{"id", "kind"},
	})

	if got, want := total.Columns, 3; got != want {
		t.Fatalf("columns=%d want distinct union %d: %+v", got, want, total)
	}
	if got, want := strings.Join(total.columnNames, ","), "id,kind,value"; got != want {
		t.Fatalf("column names=%q want %q", got, want)
	}
	if got, want := total.Rows, 5; got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	if got, want := total.TotalStoredBytes, int64(50); got != want {
		t.Fatalf("total stored bytes=%d want %d", got, want)
	}
	if got, want := total.BytesPerRow, 10.0; got != want {
		t.Fatalf("bytes_per_row=%f want %f", got, want)
	}
}

func BenchmarkColumnStorePhysicalAccounting2118(b *testing.B) {
	events := columnPhysicalQ3DenseBenchmarkEvents1950(4096)
	_, collection, closeFn, _ := openColumnPhysicalJSONBenchTypedColumnPartFixture1947(b, events)
	defer closeFn()
	ctx := context.Background()
	opts := ColumnStorePhysicalAccountingOptions{DetailedSections: true, ReadIntegrity: ColumnAssetReadIntegrityCachedVerify}
	b.ReportAllocs()
	b.ResetTimer()
	var bytes int64
	for i := 0; i < b.N; i++ {
		accounting, err := collection.ColumnStorePhysicalAccounting(ctx, opts)
		if err != nil {
			b.Fatalf("ColumnStorePhysicalAccounting: %v", err)
		}
		bytes = accounting.Totals.ReferencedAssetBytes
		if len(accounting.TypedColumnParts) == 0 {
			b.Fatal("typed column parts empty")
		}
	}
	b.ReportMetric(float64(bytes), "referenced_bytes/op")
}

func columnStorePhysicalAccountingHasKind(kinds []ColumnStorePhysicalAssetKindAccounting, kind ColumnAssetKind, bytes int64) bool {
	for _, total := range kinds {
		if total.Kind == kind && total.Bytes == bytes && total.Count > 0 {
			return true
		}
	}
	return false
}
