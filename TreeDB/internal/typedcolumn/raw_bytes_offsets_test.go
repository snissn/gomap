package typedcolumn

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestRawBytesOffsetsRoundTripRowsAndAliasing(t *testing.T) {
	t.Parallel()
	offsets := []uint64{0, 0, 3, 5, 9}
	values := []byte{0x00, 'A', 0xff, 0x00, 0x80, 0xfe, 'x', 0x00, 'z'}

	offsetsRaw, err := EncodeRawBytesOffsetsOffsets(nil, offsets)
	if err != nil {
		t.Fatalf("EncodeRawBytesOffsetsOffsets: %v", err)
	}
	valuesRaw, err := EncodeRawBytesOffsetsValues(nil, values)
	if err != nil {
		t.Fatalf("EncodeRawBytesOffsetsValues: %v", err)
	}
	if !bytes.Equal(valuesRaw, values) {
		t.Fatalf("values raw=%v want exact opaque payload %v", valuesRaw, values)
	}

	decoded, err := DecodeRawBytesOffsetsFallback(nil, nil, offsetsRaw, valuesRaw, 4)
	if err != nil {
		t.Fatalf("DecodeRawBytesOffsetsFallback: %v", err)
	}
	if decoded.Rows != 4 || !equalUint64s(decoded.Offsets, offsets) || !bytes.Equal(decoded.Values, values) {
		t.Fatalf("decoded=%+v want rows=4 offsets=%v values=%v", decoded, offsets, values)
	}

	payload, err := EncodeRawBytesOffsetsPayload(nil, 4, offsets, values)
	if err != nil {
		t.Fatalf("EncodeRawBytesOffsetsPayload: %v", err)
	}
	fromPayload, err := DecodeRawBytesOffsetsPayload(nil, nil, payload, 4)
	if err != nil {
		t.Fatalf("DecodeRawBytesOffsetsPayload: %v", err)
	}
	if !equalUint64s(fromPayload.Offsets, offsets) || !bytes.Equal(fromPayload.Values, values) {
		t.Fatalf("payload decoded=%+v want offsets=%v values=%v", fromPayload, offsets, values)
	}

	wantRows := [][]byte{{}, {0x00, 'A', 0xff}, {0x00, 0x80}, {0xfe, 'x', 0x00, 'z'}}
	for row, want := range wantRows {
		got, err := decoded.Row(row)
		if err != nil {
			t.Fatalf("Row(%d): %v", row, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Row(%d)=%v want %v", row, got, want)
		}
	}
	row1, err := decoded.Row(1)
	if err != nil {
		t.Fatalf("Row(1): %v", err)
	}
	row1[0] = 0x7f
	if decoded.Values[offsets[1]] != 0x7f {
		t.Fatalf("Row did not alias Values backing slice")
	}
	if _, err := decoded.Row(4); err == nil || !strings.Contains(err.Error(), "outside rows") {
		t.Fatalf("Row out-of-range err=%v want bounds failure", err)
	}
}

func TestRawBytesOffsetsCorruptionValidation(t *testing.T) {
	t.Parallel()
	validOffsets := mustRawBytesOffsets(t, []uint64{0, 1, 3})
	validValues := []byte{'a', 0x00, 0xff}

	cases := []struct {
		name       string
		offsetsRaw []byte
		valuesRaw  []byte
		rows       int
		want       string
	}{
		{name: "offsets_first_not_zero", offsetsRaw: mustRawBytesOffsets(t, []uint64{1, 1}), valuesRaw: []byte{'x'}, rows: 1, want: "offsets[0]"},
		{name: "non_monotonic_offsets", offsetsRaw: mustRawBytesOffsets(t, []uint64{0, 2, 1}), valuesRaw: []byte{'a', 'b'}, rows: 2, want: "before previous"},
		{name: "final_offset_less_than_values", offsetsRaw: mustRawBytesOffsets(t, []uint64{0, 1}), valuesRaw: []byte{'a', 'b'}, rows: 1, want: "final offset=1 values=2"},
		{name: "final_offset_greater_than_values", offsetsRaw: mustRawBytesOffsets(t, []uint64{0, 2}), valuesRaw: []byte{'a'}, rows: 1, want: "final offset=2 values=1"},
		{name: "truncated_offsets", offsetsRaw: validOffsets[:len(validOffsets)-1], valuesRaw: validValues, rows: 2, want: "offsets bytes"},
		{name: "row_count_mismatch", offsetsRaw: validOffsets, valuesRaw: validValues, rows: 1, want: "offsets bytes"},
		{name: "host_int_overflow", offsetsRaw: mustRawBytesOffsets(t, []uint64{0, maxHostIntUint64() + 1}), valuesRaw: nil, rows: 1, want: "exceeds host int"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeRawBytesOffsetsFallback(nil, nil, tc.offsetsRaw, tc.valuesRaw, tc.rows)
			if err == nil {
				t.Fatalf("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestRawBytesOffsetsGranuleAndSectionValidation(t *testing.T) {
	t.Parallel()
	offsets := []uint64{0, 2, 2, 5}
	values := []byte{'o', 'k', 0x00, 0x80, 0xff}
	builder := NewGranuleBuilder(Config{Encoding: EncodingRawBytesOffsets, Compression: CompressionNone})
	granule, err := builder.BuildBytes(3, offsets, values)
	if err != nil {
		t.Fatalf("BuildBytes: %v", err)
	}
	var reader GranuleReader
	decoded, err := reader.DecodeBytesInto(nil, nil, granule)
	if err != nil {
		t.Fatalf("DecodeBytesInto: %v", err)
	}
	if !equalUint64s(decoded.Offsets, offsets) || !bytes.Equal(decoded.Values, values) {
		t.Fatalf("decoded=%+v want offsets=%v values=%v", decoded, offsets, values)
	}
	badEncoding := granule
	badEncoding.Encoding = EncodingRawUint32OffsetsList
	if _, err := reader.DecodeBytesInto(nil, nil, badEncoding); err == nil || !strings.Contains(err.Error(), "encoding") {
		t.Fatalf("DecodeBytesInto bad encoding err=%v want encoding failure", err)
	}

	offsetsRaw, err := EncodeRawBytesOffsetsOffsets(nil, offsets)
	if err != nil {
		t.Fatalf("EncodeRawBytesOffsetsOffsets: %v", err)
	}
	valuesRaw, err := EncodeRawBytesOffsetsValues(nil, values)
	if err != nil {
		t.Fatalf("EncodeRawBytesOffsetsValues: %v", err)
	}
	offsetsSection, valuesSection, err := NewRawBytesOffsetsImageSections("opaque", 3, len(offsetsRaw), len(valuesRaw))
	if err != nil {
		t.Fatalf("NewRawBytesOffsetsImageSections: %v", err)
	}
	if err := ValidateRawBytesOffsetsSections(offsetsSection, valuesSection, offsetsRaw, valuesRaw, 3); err != nil {
		t.Fatalf("ValidateRawBytesOffsetsSections: %v", err)
	}
	badSection := offsetsSection
	badSection.Kind = ColumnPartImageSectionColumnValues
	if err := ValidateRawBytesOffsetsSections(badSection, valuesSection, offsetsRaw, valuesRaw, 3); err == nil || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("bad section kind err=%v want kind failure", err)
	}
	badValues := valuesSection
	badValues.Length++
	if err := ValidateRawBytesOffsetsSections(offsetsSection, badValues, offsetsRaw, valuesRaw, 3); err == nil || !strings.Contains(err.Error(), "section lengths") {
		t.Fatalf("bad section length err=%v want length failure", err)
	}
}

func TestRawBytesOffsetsColumnPartFromImageRejectsNullCount(t *testing.T) {
	t.Parallel()
	part, err := BuildColumnPart(92010, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "opaque", Type: ColumnTypeBytes, Encoding: EncodingRawBytesOffsets, Compression: CompressionNone},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:    2,
		Columns: map[string][]int64{"id": {1, 2}},
		BytesColumns: map[string]RawBytesOffsets{
			"opaque": {Rows: 2, Offsets: []uint64{0, 3, 3}, Values: []byte{'a', 0x00, 0xff}},
		},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := cloneColumnPartImageBytes(image)
	nullCountOffset := mustRawBytesOffsetsDescriptorBlockNullCountOffset(t, corrupt, "opaque")
	binary.LittleEndian.PutUint64(corrupt.Bytes[nullCountOffset:nullCountOffset+8], 1)
	parsed, err := ParseColumnPartImage(corrupt.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	_, err = ColumnPartFromImage(parsed)
	if err == nil || !strings.Contains(err.Error(), "bytes null/default count=1/0 want 0/0") {
		t.Fatalf("ColumnPartFromImage bytes null count err=%v", err)
	}
}

func mustRawBytesOffsetsDescriptorBlockNullCountOffset(t *testing.T, image ColumnPartImage, column string) int {
	t.Helper()
	section := mustValidationSection(t, image, ColumnPartImageSectionDescriptor)
	dec := columnPartImageDecoder{data: image.sectionBytes(section)}
	mustValidationSkipU16(t, &dec)
	mustValidationSkipU64(t, &dec)
	mustValidationReadU32(t, &dec)
	mustValidationSkipI64(t, &dec)
	mustValidationSkipI64(t, &dec)
	if _, err := dec.stringSlice(); err != nil {
		t.Fatalf("decode logical primary key: %v", err)
	}
	granuleCount := mustValidationReadU32(t, &dec)
	granuleBytes := int(granuleCount) * 64
	if err := dec.require(granuleBytes); err != nil {
		t.Fatalf("skip descriptor granules: %v", err)
	}
	dec.offset += granuleBytes
	columnCount := mustValidationReadU32(t, &dec)
	for i := 0; i < int(columnCount); i++ {
		name, err := dec.str()
		if err != nil {
			t.Fatalf("decode descriptor column name: %v", err)
		}
		mustValidationSkipU16(t, &dec)
		mustValidationReadU32(t, &dec)
		mustValidationReadU32(t, &dec)
		mustValidationReadU32(t, &dec) // bits_per_element
		blockCount := mustValidationReadU32(t, &dec)
		if name == column {
			if blockCount == 0 {
				t.Fatalf("descriptor column %q has no blocks", column)
			}
			return section.Offset + dec.offset + 60
		}
		blockBytes := int(blockCount) * 94
		if err := dec.require(blockBytes); err != nil {
			t.Fatalf("skip descriptor column %q blocks: %v", name, err)
		}
		dec.offset += blockBytes
	}
	t.Fatalf("descriptor column %q not found", column)
	return 0
}

func mustRawBytesOffsets(t *testing.T, offsets []uint64) []byte {
	t.Helper()
	raw, err := EncodeRawBytesOffsetsOffsets(nil, offsets)
	if err != nil {
		t.Fatalf("EncodeRawBytesOffsetsOffsets(%v): %v", offsets, err)
	}
	return raw
}
