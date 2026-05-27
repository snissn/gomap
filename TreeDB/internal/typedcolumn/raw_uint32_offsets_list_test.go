package typedcolumn

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestRawUint32OffsetsListLittleEndianFixtures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		rows        int
		offsets     []uint64
		values      []uint32
		wantOffsets []byte
		wantValues  []byte
	}{
		{
			name:        "empty_rows",
			rows:        0,
			offsets:     []uint64{0},
			values:      nil,
			wantOffsets: []byte{0, 0, 0, 0, 0, 0, 0, 0},
			wantValues:  []byte{},
		},
		{
			name:        "all_empty_rows",
			rows:        3,
			offsets:     []uint64{0, 0, 0, 0},
			values:      nil,
			wantOffsets: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			wantValues:  []byte{},
		},
		{
			name:    "mixed_empty_and_non_empty_rows",
			rows:    4,
			offsets: []uint64{0, 0, 2, 2, 3},
			values:  []uint32{7, 8, 9},
			wantOffsets: []byte{
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0,
				2, 0, 0, 0, 0, 0, 0, 0,
				2, 0, 0, 0, 0, 0, 0, 0,
				3, 0, 0, 0, 0, 0, 0, 0,
			},
			wantValues: []byte{7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0},
		},
		{
			name:    "multi_value_rows",
			rows:    2,
			offsets: []uint64{0, 3, 5},
			values:  []uint32{1, 2, 3, 0x01020304, math.MaxUint32},
			wantOffsets: []byte{
				0, 0, 0, 0, 0, 0, 0, 0,
				3, 0, 0, 0, 0, 0, 0, 0,
				5, 0, 0, 0, 0, 0, 0, 0,
			},
			wantValues: []byte{
				1, 0, 0, 0,
				2, 0, 0, 0,
				3, 0, 0, 0,
				4, 3, 2, 1,
				255, 255, 255, 255,
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			offsetsRaw, err := EncodeRawUint32OffsetsListOffsets(nil, tc.offsets)
			if err != nil {
				t.Fatalf("encode offsets: %v", err)
			}
			valuesRaw, err := EncodeRawUint32OffsetsListValues(nil, tc.values)
			if err != nil {
				t.Fatalf("encode values: %v", err)
			}
			if !bytes.Equal(offsetsRaw, tc.wantOffsets) {
				t.Fatalf("offset bytes\n got %v\nwant %v", offsetsRaw, tc.wantOffsets)
			}
			if !bytes.Equal(valuesRaw, tc.wantValues) {
				t.Fatalf("value bytes\n got %v\nwant %v", valuesRaw, tc.wantValues)
			}

			decoded, err := DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, tc.rows)
			if err != nil {
				t.Fatalf("decode fallback: %v", err)
			}
			if decoded.Rows != tc.rows || !equalUint64s(decoded.Offsets, tc.offsets) || !equalUint32s(decoded.Values, tc.values) {
				t.Fatalf("decoded=%+v want rows=%d offsets=%v values=%v", decoded, tc.rows, tc.offsets, tc.values)
			}
			if len(offsetsRaw) != 0 && len(decoded.Offsets) != 0 {
				offsetsRaw[0] = 99
				if decoded.Offsets[0] != 0 {
					t.Fatalf("decoded offsets alias raw bytes")
				}
			}
			if len(valuesRaw) != 0 && len(decoded.Values) != 0 {
				valuesRaw[0] = 99
				if decoded.Values[0] != tc.values[0] {
					t.Fatalf("decoded values alias raw bytes")
				}
			}
		})
	}
}

func TestRawUint32OffsetsListCorruptionValidation(t *testing.T) {
	t.Parallel()
	offsetsRaw := mustOffsetsListOffsets(t, []uint64{0, 1, 3})
	valuesRaw := mustOffsetsListValues(t, []uint32{10, 20, 30})

	tests := []struct {
		name       string
		offsetsRaw []byte
		valuesRaw  []byte
		rows       int
		want       string
	}{
		{name: "offsets_first_not_zero", offsetsRaw: mustOffsetsListOffsets(t, []uint64{1, 1}), valuesRaw: nil, rows: 1, want: "offsets[0]"},
		{name: "non_monotonic_offsets", offsetsRaw: mustOffsetsListOffsets(t, []uint64{0, 2, 1}), valuesRaw: mustOffsetsListValues(t, []uint32{1, 2}), rows: 2, want: "before previous"},
		{name: "final_offset_less_than_values", offsetsRaw: mustOffsetsListOffsets(t, []uint64{0, 1}), valuesRaw: mustOffsetsListValues(t, []uint32{1, 2}), rows: 1, want: "final offset=1 values=2"},
		{name: "final_offset_greater_than_values", offsetsRaw: mustOffsetsListOffsets(t, []uint64{0, 2}), valuesRaw: mustOffsetsListValues(t, []uint32{1}), rows: 1, want: "final offset=2 values=1"},
		{name: "go_int_overflow", offsetsRaw: mustOffsetsListOffsets(t, []uint64{0, math.MaxUint64}), valuesRaw: nil, rows: 1, want: "exceeds host int"},
		{name: "truncated_offsets", offsetsRaw: offsetsRaw[:len(offsetsRaw)-1], valuesRaw: valuesRaw, rows: 2, want: "offsets bytes"},
		{name: "truncated_values", offsetsRaw: mustOffsetsListOffsets(t, []uint64{0, 1}), valuesRaw: []byte{1, 0, 0}, rows: 1, want: "not multiple of 4"},
		{name: "row_count_mismatch", offsetsRaw: offsetsRaw, valuesRaw: valuesRaw, rows: 1, want: "offsets bytes"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeRawUint32OffsetsListFallback(nil, nil, tc.offsetsRaw, tc.valuesRaw, tc.rows)
			if err == nil {
				t.Fatalf("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestRawUint32OffsetsListGranuleEncodingMismatchFailsClosed(t *testing.T) {
	raw, err := EncodeRawUint32OffsetsListPayload(nil, 1, []uint64{0, 1}, []uint32{42})
	if err != nil {
		t.Fatalf("EncodeRawUint32OffsetsListPayload: %v", err)
	}
	g := EncodedGranule{Rows: 1, Encoding: EncodingRawUint32Dense, Compression: CompressionNone, RawBytes: len(raw), StoredBytes: len(raw), PayloadRef: PayloadRef{Kind: PayloadRefInline, Length: len(raw)}, Payload: raw}
	var reader GranuleReader
	if _, err := reader.DecodeUint32OffsetsListInto(nil, nil, g); err == nil || !strings.Contains(err.Error(), "encoding") {
		t.Fatalf("DecodeUint32OffsetsListInto err=%v want encoding mismatch", err)
	}
}

func TestRawUint32OffsetsListSectionMetadataValidation(t *testing.T) {
	t.Parallel()
	offsetsRaw := mustOffsetsListOffsets(t, []uint64{0, 2})
	valuesRaw := mustOffsetsListValues(t, []uint32{1, 2})
	offsets, values, err := NewRawUint32OffsetsListImageSections("neighbors", 1, len(offsetsRaw), len(valuesRaw))
	if err != nil {
		t.Fatalf("sections: %v", err)
	}
	if err := ValidateRawUint32OffsetsListSections(offsets, values, offsetsRaw, valuesRaw, 1); err != nil {
		t.Fatalf("validate sections: %v", err)
	}

	t.Run("unsupported_encoding", func(t *testing.T) {
		bad := offsets
		bad.Encoding = EncodingRawUint32Dense
		if err := ValidateRawUint32OffsetsListSections(bad, values, offsetsRaw, valuesRaw, 1); err == nil || !strings.Contains(err.Error(), "encoding") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("mismatched_encoding", func(t *testing.T) {
		bad := values
		bad.Encoding = EncodingRawUint32Dense
		if err := ValidateRawUint32OffsetsListSections(offsets, bad, offsetsRaw, valuesRaw, 1); err == nil || !strings.Contains(err.Error(), "encoding") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("row_count_mismatch", func(t *testing.T) {
		bad := values
		bad.Rows = 2
		if err := ValidateRawUint32OffsetsListSections(offsets, bad, offsetsRaw, valuesRaw, 1); err == nil || !strings.Contains(err.Error(), "rows") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("section_length_mismatch", func(t *testing.T) {
		bad := values
		bad.Length++
		if err := ValidateRawUint32OffsetsListSections(offsets, bad, offsetsRaw, valuesRaw, 1); err == nil || !strings.Contains(err.Error(), "lengths") {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestRawUint32OffsetsListColumnPartImageRoundTrip1915(t *testing.T) {
	opts := Options{
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Encoding: EncodingRawUint32OffsetsList, Compression: CompressionNone, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
	}
	batch := Batch{
		Rows:    4,
		Columns: map[string][]int64{"id": []int64{0, 1, 2, 3}},
		Uint32OffsetsLists: map[string]RawUint32OffsetsList{
			"neighbors": {Rows: 4, Offsets: []uint64{0, 0, 2, 2, 5}, Values: []uint32{7, 8, 9, 10, 11}},
		},
	}
	part, err := BuildColumnPart(1915, opts, batch)
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	neighbors := part.Columns["neighbors"]
	if got := len(part.Descriptor.Granules); got != 2 {
		t.Fatalf("granules=%d want 2", got)
	}
	if got := len(neighbors.Blocks); got != 2 {
		t.Fatalf("offsets-list blocks=%d want 2", got)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"neighbors": "adjacency_list"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	offsetsSection, valuesSection, ok := image.columnOffsetsListSections("neighbors")
	if !ok {
		t.Fatalf("missing offsets-list sections: %+v", image.Sections)
	}
	if offsetsSection.Kind != ColumnPartImageSectionColumnOffsets || offsetsSection.Name != "offsets" || offsetsSection.Length != 5*8 {
		t.Fatalf("offsets section=%+v", offsetsSection)
	}
	if valuesSection.Kind != ColumnPartImageSectionColumnValues || valuesSection.Name != "values" || valuesSection.Length != 5*4 {
		t.Fatalf("values section=%+v", valuesSection)
	}
	if offsetsSection.Offset >= valuesSection.Offset {
		t.Fatalf("section order offsets=%d values=%d", offsetsSection.Offset, valuesSection.Offset)
	}
	if !bytes.Equal(image.sectionBytes(offsetsSection), mustOffsetsListOffsets(t, []uint64{0, 0, 2, 2, 5})) {
		t.Fatalf("global offsets section bytes=%v", image.sectionBytes(offsetsSection))
	}
	if !bytes.Equal(image.sectionBytes(valuesSection), mustOffsetsListValues(t, []uint32{7, 8, 9, 10, 11})) {
		t.Fatalf("values section bytes=%v", image.sectionBytes(valuesSection))
	}
	if err := ValidateRawUint32OffsetsListSections(offsetsSection, valuesSection, image.sectionBytes(offsetsSection), image.sectionBytes(valuesSection), 4); err != nil {
		t.Fatalf("ValidateRawUint32OffsetsListSections: %v", err)
	}
	cert, err := CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	certColumn, ok := cert.Column("neighbors")
	if !ok {
		t.Fatalf("missing certified neighbors column")
	}
	if certColumn.OffsetsBytes != offsetsSection.Length || certColumn.ValuesBytes != valuesSection.Length || certColumn.DirectViewCertified {
		t.Fatalf("certified offsets-list column=%+v", certColumn)
	}
	for i, block := range certColumn.Blocks {
		if block.PayloadOffset != 0 || block.PayloadLength != 0 {
			t.Fatalf("certified offsets-list block %d payload offset/length=(%d,%d) want (0,0)", i, block.PayloadOffset, block.PayloadLength)
		}
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	decodedPart, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	decoded, err := decodedPart.Uint32OffsetsListColumn("neighbors", nil, nil)
	if err != nil {
		t.Fatalf("Uint32OffsetsListColumn: %v", err)
	}
	if !equalUint64s(decoded.Offsets, []uint64{0, 0, 2, 2, 5}) || !equalUint32s(decoded.Values, []uint32{7, 8, 9, 10, 11}) {
		t.Fatalf("decoded=%+v", decoded)
	}
	accounting := part.ByteAccountingFromImage(image)
	if accounting.DeclaredColumnOffsetsBytes != offsetsSection.Length || accounting.DeclaredColumnValuesBytes != valuesSection.Length {
		t.Fatalf("accounting offsets=%d values=%d sections=%+v", accounting.DeclaredColumnOffsetsBytes, accounting.DeclaredColumnValuesBytes, accounting.SerializedSections)
	}
	if accounting.SerializedPaddingBytes != image.PaddingBytes() {
		t.Fatalf("accounting padding=%d image padding=%d", accounting.SerializedPaddingBytes, image.PaddingBytes())
	}
}

func TestRawUint32OffsetsListImageStorageAccounting(t *testing.T) {
	t.Parallel()
	part := &ColumnPart{Descriptor: ColumnPartDescriptor{RowCount: 2}}
	image := ColumnPartImage{
		ManifestBytes: 10,
		Bytes:         make([]byte, 52),
		Sections: []ColumnPartImageSection{
			{Kind: ColumnPartImageSectionColumnOffsets, Category: ColumnPartImageCategoryDeclaredColumnOffsets, Column: "neighbors", Offset: 16, Length: 16},
			{Kind: ColumnPartImageSectionColumnValues, Category: ColumnPartImageCategoryDeclaredColumnValues, Column: "neighbors", Offset: 40, Length: 12},
		},
	}
	accounting := part.ByteAccountingFromImage(image)
	if accounting.DeclaredColumnOffsetsBytes != 16 {
		t.Fatalf("offsets bytes=%d", accounting.DeclaredColumnOffsetsBytes)
	}
	if accounting.DeclaredColumnValuesBytes != 12 {
		t.Fatalf("values bytes=%d", accounting.DeclaredColumnValuesBytes)
	}
	if accounting.DeclaredColumnBytes != 28 {
		t.Fatalf("declared column bytes=%d", accounting.DeclaredColumnBytes)
	}
	if accounting.SerializedPaddingBytes != 14 {
		t.Fatalf("padding bytes=%d", accounting.SerializedPaddingBytes)
	}
	if accounting.TotalStoredBytes != 52 {
		t.Fatalf("total stored bytes=%d", accounting.TotalStoredBytes)
	}
}

func BenchmarkRawUint32OffsetsListFallbackDecode(b *testing.B) {
	const rows = 8192
	offsets := make([]uint64, rows+1)
	values := make([]uint32, 0, rows*3)
	for row := 0; row < rows; row++ {
		count := row%7 + 1
		for j := 0; j < count; j++ {
			values = append(values, uint32(row+j))
		}
		offsets[row+1] = uint64(len(values))
	}
	offsetsRaw := mustOffsetsListOffsets(b, offsets)
	valuesRaw := mustOffsetsListValues(b, values)
	decodedBytes := len(offsetsRaw) + len(valuesRaw)
	b.SetBytes(int64(decodedBytes))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, rows)
		if err != nil {
			b.Fatal(err)
		}
		rawUint32OffsetsListSink = decoded
	}
	b.ReportMetric(float64(rows), "rows/op")
	b.ReportMetric(float64(len(values)), "total_values/op")
	b.ReportMetric(float64(decodedBytes), "decoded_bytes/op")
}

var rawUint32OffsetsListSink RawUint32OffsetsList

func mustOffsetsListOffsets(tb testing.TB, offsets []uint64) []byte {
	tb.Helper()
	raw, err := EncodeRawUint32OffsetsListOffsets(nil, offsets)
	if err != nil {
		tb.Fatalf("encode offsets: %v", err)
	}
	return raw
}

func mustOffsetsListValues(tb testing.TB, values []uint32) []byte {
	tb.Helper()
	raw, err := EncodeRawUint32OffsetsListValues(nil, values)
	if err != nil {
		tb.Fatalf("encode values: %v", err)
	}
	return raw
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalUint32s(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
