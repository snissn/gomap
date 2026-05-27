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
