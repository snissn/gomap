package typedcolumn

import (
	"slices"
	"strings"
	"testing"
)

func TestPrimitiveScalarRawLittleEndianFixtures1929(t *testing.T) {
	builder := NewGranuleBuilder(Config{Compression: CompressionNone})
	var reader GranuleReader

	builder.Reset(Config{Encoding: EncodingRawInt8, Compression: CompressionNone})
	gInt8, err := builder.BuildInt8([]int8{-128, -1, 0, 127})
	if err != nil {
		t.Fatalf("BuildInt8: %v", err)
	}
	if want := []byte{0x80, 0xff, 0x00, 0x7f}; !slices.Equal(gInt8.Payload, want) {
		t.Fatalf("int8 raw=% x want % x", gInt8.Payload, want)
	}
	gotInt8, err := reader.DecodeInt8(gInt8)
	if err != nil || !slices.Equal(gotInt8, []int8{-128, -1, 0, 127}) {
		t.Fatalf("DecodeInt8=%v err=%v", gotInt8, err)
	}

	builder.Reset(Config{Encoding: EncodingRawUint8, Compression: CompressionNone})
	gUint8, err := builder.BuildUint8([]uint8{0, 1, 255})
	if err != nil {
		t.Fatalf("BuildUint8: %v", err)
	}
	if want := []byte{0x00, 0x01, 0xff}; !slices.Equal(gUint8.Payload, want) {
		t.Fatalf("uint8 raw=% x want % x", gUint8.Payload, want)
	}
	gotUint8, err := reader.DecodeUint8(gUint8)
	if err != nil || !slices.Equal(gotUint8, []uint8{0, 1, 255}) {
		t.Fatalf("DecodeUint8=%v err=%v", gotUint8, err)
	}

	builder.Reset(Config{Encoding: EncodingRawInt16, Compression: CompressionNone})
	gInt16, err := builder.BuildInt16([]int16{-32768, -2, 0, 0x1234, 32767})
	if err != nil {
		t.Fatalf("BuildInt16: %v", err)
	}
	if want := []byte{0x00, 0x80, 0xfe, 0xff, 0x00, 0x00, 0x34, 0x12, 0xff, 0x7f}; !slices.Equal(gInt16.Payload, want) {
		t.Fatalf("int16 raw=% x want % x", gInt16.Payload, want)
	}
	gotInt16, err := reader.DecodeInt16(gInt16)
	if err != nil || !slices.Equal(gotInt16, []int16{-32768, -2, 0, 0x1234, 32767}) {
		t.Fatalf("DecodeInt16=%v err=%v", gotInt16, err)
	}

	builder.Reset(Config{Encoding: EncodingRawUint16, Compression: CompressionNone})
	gUint16, err := builder.BuildUint16([]uint16{0, 0x8000, 0x7e00, 0x7fff, 0xffff})
	if err != nil {
		t.Fatalf("BuildUint16: %v", err)
	}
	if want := []byte{0x00, 0x00, 0x00, 0x80, 0x00, 0x7e, 0xff, 0x7f, 0xff, 0xff}; !slices.Equal(gUint16.Payload, want) {
		t.Fatalf("uint16 raw=% x want % x", gUint16.Payload, want)
	}
	gotUint16, err := reader.DecodeUint16(gUint16)
	if err != nil || !slices.Equal(gotUint16, []uint16{0, 0x8000, 0x7e00, 0x7fff, 0xffff}) {
		t.Fatalf("DecodeUint16=%v err=%v", gotUint16, err)
	}

	builder.Reset(Config{Encoding: EncodingRawInt32, Compression: CompressionNone})
	gInt32, err := builder.BuildInt32([]int32{-2147483648, -2, 0, 0x01020304, 2147483647})
	if err != nil {
		t.Fatalf("BuildInt32: %v", err)
	}
	if want := []byte{0x00, 0x00, 0x00, 0x80, 0xfe, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x04, 0x03, 0x02, 0x01, 0xff, 0xff, 0xff, 0x7f}; !slices.Equal(gInt32.Payload, want) {
		t.Fatalf("int32 raw=% x want % x", gInt32.Payload, want)
	}
	gotInt32, err := reader.DecodeInt32(gInt32)
	if err != nil || !slices.Equal(gotInt32, []int32{-2147483648, -2, 0, 0x01020304, 2147483647}) {
		t.Fatalf("DecodeInt32=%v err=%v", gotInt32, err)
	}

	builder.Reset(Config{Encoding: EncodingRawUint32, Compression: CompressionNone})
	gUint32, err := builder.BuildUint32([]uint32{0, 0x01020304, 0xffffffff})
	if err != nil {
		t.Fatalf("BuildUint32: %v", err)
	}
	if want := []byte{0x00, 0x00, 0x00, 0x00, 0x04, 0x03, 0x02, 0x01, 0xff, 0xff, 0xff, 0xff}; !slices.Equal(gUint32.Payload, want) {
		t.Fatalf("uint32 raw=% x want % x", gUint32.Payload, want)
	}
	gotUint32, err := reader.DecodeUint32(gUint32)
	if err != nil || !slices.Equal(gotUint32, []uint32{0, 0x01020304, 0xffffffff}) {
		t.Fatalf("DecodeUint32=%v err=%v", gotUint32, err)
	}

	builder.Reset(Config{Encoding: EncodingRawUint64, Compression: CompressionNone})
	gUint64, err := builder.BuildUint64([]uint64{0, 0x0102030405060708, 0xffffffffffffffff})
	if err != nil {
		t.Fatalf("BuildUint64: %v", err)
	}
	if want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}; !slices.Equal(gUint64.Payload, want) {
		t.Fatalf("uint64 raw=% x want % x", gUint64.Payload, want)
	}
	gotUint64, err := reader.DecodeUint64(gUint64)
	if err != nil || !slices.Equal(gotUint64, []uint64{0, 0x0102030405060708, 0xffffffffffffffff}) {
		t.Fatalf("DecodeUint64=%v err=%v", gotUint64, err)
	}
}

func TestPrimitiveScalarFloat16BFloat16RawBits1929(t *testing.T) {
	builder := NewGranuleBuilder(Config{Compression: CompressionNone})
	var reader GranuleReader

	float16Bits := []uint16{0x0000, 0x8000, 0x7c00, 0xfc00, 0x7bff, 0xfbff, 0x7e01, 0x7dff}
	builder.Reset(Config{Encoding: EncodingRawFloat16, Compression: CompressionNone})
	gFloat16, err := builder.BuildFloat16Bits(float16Bits)
	if err != nil {
		t.Fatalf("BuildFloat16Bits: %v", err)
	}
	if want := []byte{0x00, 0x00, 0x00, 0x80, 0x00, 0x7c, 0x00, 0xfc, 0xff, 0x7b, 0xff, 0xfb, 0x01, 0x7e, 0xff, 0x7d}; !slices.Equal(gFloat16.Payload, want) {
		t.Fatalf("float16 raw=% x want % x", gFloat16.Payload, want)
	}
	gotFloat16, err := reader.DecodeFloat16Bits(gFloat16)
	if err != nil || !slices.Equal(gotFloat16, float16Bits) {
		t.Fatalf("DecodeFloat16Bits=%04x err=%v", gotFloat16, err)
	}
	if gFloat16.HasMinMax {
		t.Fatalf("float16 raw bits must not advertise numeric min/max: %+v", gFloat16)
	}

	bfloat16Bits := []uint16{0x0000, 0x8000, 0x7f80, 0xff80, 0x7f7f, 0xff7f, 0x7fc1, 0x7fa1}
	builder.Reset(Config{Encoding: EncodingRawBFloat16, Compression: CompressionNone})
	gBFloat16, err := builder.BuildBFloat16Bits(bfloat16Bits)
	if err != nil {
		t.Fatalf("BuildBFloat16Bits: %v", err)
	}
	if want := []byte{0x00, 0x00, 0x00, 0x80, 0x80, 0x7f, 0x80, 0xff, 0x7f, 0x7f, 0x7f, 0xff, 0xc1, 0x7f, 0xa1, 0x7f}; !slices.Equal(gBFloat16.Payload, want) {
		t.Fatalf("bfloat16 raw=% x want % x", gBFloat16.Payload, want)
	}
	gotBFloat16, err := reader.DecodeBFloat16Bits(gBFloat16)
	if err != nil || !slices.Equal(gotBFloat16, bfloat16Bits) {
		t.Fatalf("DecodeBFloat16Bits=%04x err=%v", gotBFloat16, err)
	}
	if gBFloat16.HasMinMax {
		t.Fatalf("bfloat16 raw bits must not advertise numeric min/max: %+v", gBFloat16)
	}
}

func TestPrimitiveScalarColumnPartImageRoundTripStatsAndPruning1929(t *testing.T) {
	part := buildPrimitiveScalarPart1929(t)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{
		"id": "int64", "i8": "int8", "u8": "uint8", "i16": "int16", "u16": "uint16", "i32": "int32", "u32": "uint32", "u64": "uint64", "f16": "float16", "bf16": "bfloat16",
	}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	cert, err := CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	for _, name := range []string{"i8", "u8", "i16", "u16", "i32", "u32", "u64", "f16", "bf16"} {
		column, ok := cert.Column(name)
		if !ok || !column.DirectViewCertified || column.ElementSize == 0 || column.Endian != ColumnPartLayoutEndianLittle {
			t.Fatalf("layout contract column %s=%+v ok=%v want direct-view little-endian scalar certification", name, column, ok)
		}
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	reconstructed, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	for _, tc := range []struct {
		name     string
		wantType ColumnType
		wantEnc  Encoding
	}{
		{"i8", ColumnTypeInt8, EncodingRawInt8},
		{"u8", ColumnTypeUint8, EncodingRawUint8},
		{"i16", ColumnTypeInt16, EncodingRawInt16},
		{"u16", ColumnTypeUint16, EncodingRawUint16},
		{"i32", ColumnTypeInt32, EncodingRawInt32},
		{"u32", ColumnTypeUint32, EncodingRawUint32},
		{"u64", ColumnTypeUint64, EncodingRawUint64},
		{"f16", ColumnTypeFloat16, EncodingRawFloat16},
		{"bf16", ColumnTypeBFloat16, EncodingRawBFloat16},
	} {
		column := reconstructed.Columns[tc.name]
		if column.Definition.Type != tc.wantType || column.Definition.Encoding != tc.wantEnc || column.Definition.FixedWidthElements != 0 {
			t.Fatalf("%s definition=%+v want type=%s encoding=%s fixed_width_elements=0", tc.name, column.Definition, tc.wantType, tc.wantEnc)
		}
	}

	assertPrimitiveScalarColumnDataBytes1929(t, image, "i16", []byte{0xfe, 0xff, 0x34, 0x12, 0x00, 0x80})
	assertPrimitiveScalarColumnDataBytes1929(t, image, "u64", []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	assertPrimitiveScalarColumnDataBytes1929(t, image, "f16", []byte{0x00, 0x80, 0x00, 0x7c, 0x01, 0x7e})

	var reader GranuleReader
	if got := decodeAllInt8Blocks1929(t, &reader, reconstructed.Columns["i8"].Blocks); !slices.Equal(got, []int8{-2, 127, -128}) {
		t.Fatalf("i8=%v", got)
	}
	if got := decodeAllUint32Blocks1929(t, &reader, reconstructed.Columns["u32"].Blocks); !slices.Equal(got, []uint32{0, 0xffffffff, 0x80000000}) {
		t.Fatalf("u32=%v", got)
	}
	if got := decodeAllUint64Blocks1929(t, &reader, reconstructed.Columns["u64"].Blocks); !slices.Equal(got, []uint64{0x0102030405060708, 0xffffffffffffffff, 0}) {
		t.Fatalf("u64=%v", got)
	}
	if got := decodeAllFloat16Blocks1929(t, &reader, reconstructed.Columns["f16"].Blocks); !slices.Equal(got, []uint16{0x8000, 0x7c00, 0x7e01}) {
		t.Fatalf("f16=%04x", got)
	}

	for _, tc := range []struct {
		name string
		min  int64
		max  int64
	}{
		{"i8", -128, 127},
		{"u8", 0, 255},
		{"i16", -32768, 0x1234},
		{"u16", 0, 0xffff},
		{"i32", -2147483648, 2147483647},
		{"u32", 0, 0xffffffff},
	} {
		stats, ok := reconstructed.ColumnStats.Int64Column(tc.name)
		if !ok || !stats.HasMinMax || stats.Min != tc.min || stats.Max != tc.max {
			t.Fatalf("stats[%s]=%+v ok=%v want min=%d max=%d", tc.name, stats, ok, tc.min, tc.max)
		}
		pruning, ok := reconstructed.PruningMetadata.Int64Column(tc.name)
		if !ok || len(pruning.Entries) != reconstructed.Descriptor.RowCount || len(pruning.Blocks) == 0 {
			t.Fatalf("pruning[%s]=%+v ok=%v", tc.name, pruning, ok)
		}
	}
	u8Desc := primitiveScalarColumnDesc1929(t, reconstructed.Descriptor, "u8")
	u8Column := reconstructed.Columns["u8"]
	u8Stats, ok := reconstructed.ColumnStats.Int64Column("u8")
	if !ok {
		t.Fatalf("missing u8 stats")
	}
	wrongStatsType := cloneInt64ColumnStats(u8Stats)
	wrongStatsType.Envelope.ColumnType = ColumnTypeInt8
	if err := ValidateInt64ColumnStats(wrongStatsType, reconstructed.Descriptor, u8Desc, u8Column); err == nil || !strings.Contains(err.Error(), ColumnStatsReasonIdentityMismatch) {
		t.Fatalf("mismatched primitive stats type err=%v want %s", err, ColumnStatsReasonIdentityMismatch)
	}
	u8Pruning, ok := reconstructed.PruningMetadata.Int64Column("u8")
	if !ok {
		t.Fatalf("missing u8 pruning")
	}
	wrongPruningType := cloneInt64ValueRowIndex(u8Pruning)
	wrongPruningType.Envelope.ColumnType = ColumnTypeInt8
	if err := ValidateInt64ValueRowIndex(wrongPruningType, reconstructed.Descriptor, u8Desc, u8Column); err == nil || !strings.Contains(err.Error(), ColumnPruningReasonIdentityMismatch) {
		t.Fatalf("mismatched primitive pruning type err=%v want %s", err, ColumnPruningReasonIdentityMismatch)
	}

	for _, name := range []string{"u64", "f16", "bf16"} {
		if stats, ok := reconstructed.ColumnStats.Int64Column(name); ok {
			t.Fatalf("stats[%s]=%+v want absent", name, stats)
		}
		if pruning, ok := reconstructed.PruningMetadata.Int64Column(name); ok {
			t.Fatalf("pruning[%s]=%+v want absent", name, pruning)
		}
	}
}

func TestPrimitiveScalarTypeCodesAreStable1929(t *testing.T) {
	cases := []struct {
		t    ColumnType
		code uint16
	}{
		{ColumnTypeInt8, 10},
		{ColumnTypeUint8, 11},
		{ColumnTypeInt16, 12},
		{ColumnTypeUint16, 13},
		{ColumnTypeInt32, 14},
		{ColumnTypeUint32, 15},
		{ColumnTypeUint64, 16},
		{ColumnTypeFloat16, 17},
		{ColumnTypeBFloat16, 18},
	}
	for _, tc := range cases {
		got, err := columnTypeCode(tc.t)
		if err != nil || got != tc.code {
			t.Fatalf("columnTypeCode(%s)=%d err=%v want %d", tc.t, got, err, tc.code)
		}
		roundTrip, err := columnTypeFromCode(tc.code)
		if err != nil || roundTrip != tc.t {
			t.Fatalf("columnTypeFromCode(%d)=%s err=%v want %s", tc.code, roundTrip, err, tc.t)
		}
	}
}

func TestPrimitiveScalarValidationFailsClosed1929(t *testing.T) {
	if _, err := NewColumnPartBuilder(Options{Columns: []ColumnDefinition{{Name: "id", Type: ColumnTypeInt64}, {Name: "bad", Type: ColumnTypeInt8, Encoding: EncodingRawUint8}}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}}); err == nil || !strings.Contains(err.Error(), "unsupported int8 encoding") {
		t.Fatalf("wrong int8 encoding err=%v", err)
	}
	if _, err := NewColumnPartBuilder(Options{Columns: []ColumnDefinition{{Name: "id", Type: ColumnTypeInt64}, {Name: "bad", Type: ColumnTypeUint16, Encoding: EncodingRawUint16, FixedWidthElements: 1}}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}}); err == nil || !strings.Contains(err.Error(), "fixed_width_elements=0") {
		t.Fatalf("uint16 fixed_width_elements err=%v", err)
	}
	if _, err := NewColumnPartBuilder(Options{Columns: []ColumnDefinition{{Name: "id", Type: ColumnTypeInt64}, {Name: "bad", Type: ColumnTypeFloat16, Encoding: EncodingRawFloat16, Compression: CompressionZSTDDict, CompressionSet: true}}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}}); err == nil || !strings.Contains(err.Error(), "unsupported compression") {
		t.Fatalf("float16 unsupported compression err=%v", err)
	}
	var builder GranuleBuilder
	builder.Reset(Config{Encoding: EncodingRawUint8, Compression: CompressionNone})
	if _, err := builder.BuildUint8(make([]uint8, maxGranuleDecodeRows+1)); err == nil || !strings.Contains(err.Error(), "exceed cap") {
		t.Fatalf("oversized primitive build err=%v want row cap rejection", err)
	}

	var reader GranuleReader
	if _, err := reader.DecodeUint16(EncodedGranule{Rows: 1, Encoding: EncodingRawInt16, Compression: CompressionNone, RawBytes: 2, StoredBytes: 2, PayloadRef: PayloadRef{Kind: PayloadRefInline, Length: 2}, Payload: []byte{1, 0}}); err == nil || !strings.Contains(err.Error(), "uint16 decode got encoding") {
		t.Fatalf("DecodeUint16 wrong encoding err=%v", err)
	}
	if _, err := DecodeRawUint64Payload(nil, []byte{1, 2, 3, 4}, 1); err == nil || !strings.Contains(err.Error(), "raw bytes=4 want=8") {
		t.Fatalf("DecodeRawUint64Payload truncated err=%v", err)
	}

	opts := Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "u8", Type: ColumnTypeUint8, Encoding: EncodingRawUint8, Compression: CompressionNone, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}
	if _, err := BuildColumnPart(1929, opts, Batch{Columns: map[string][]int64{"id": {1, 2}, "u8": {7, 8}}, Uint8Columns: map[string][]uint8{"u8": {7, 8}}}); err == nil || !strings.Contains(err.Error(), "u8 supplied in int64 carrier but declared type uint8") {
		t.Fatalf("generic-carrier primitive mismatch err=%v", err)
	}
	if _, err := BuildColumnPart(1929, opts, Batch{Columns: map[string][]int64{"id": {1, 2}}, Uint8Columns: map[string][]uint8{"u8": {7, 8}}, Int8Columns: map[string][]int8{"u8": {7}}}); err == nil || !strings.Contains(err.Error(), "u8 supplied in int8 carrier but declared type uint8") {
		t.Fatalf("typed-carrier primitive mismatch err=%v", err)
	}

	block := ColumnBlockDescriptor{FirstRow: 0, RowCount: 2, FirstGranule: 0, LastGranule: 0, Encoding: EncodingRawUint32, Compression: CompressionNone, RawBytes: 7, StoredBytes: 7}
	desc := ColumnPartDescriptor{RowCount: 2, Granules: []GranuleDescriptor{{Ordinal: 0, FirstRow: 0, RowCount: 2, VisibleRows: 2}}}
	if err := validateDecodedColumnBlockDescriptor(desc, "u32", ColumnTypeUint32, 0, 0, 0, block); err == nil || !strings.Contains(err.Error(), "fixed-width raw bytes=7 want 8") {
		t.Fatalf("validate decoded raw uint32 length err=%v", err)
	}
}

func primitiveScalarColumnDesc1929(t *testing.T, desc ColumnPartDescriptor, name string) ColumnPartColumnDescriptor {
	t.Helper()
	for _, columnDesc := range desc.Columns {
		if columnDesc.Name == name {
			return columnDesc
		}
	}
	t.Fatalf("missing descriptor column %q", name)
	return ColumnPartColumnDescriptor{}
}

func buildPrimitiveScalarPart1929(t *testing.T) *ColumnPart {
	t.Helper()
	part, err := BuildColumnPart(1929, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: "i8", Type: ColumnTypeInt8, Encoding: EncodingRawInt8, Compression: CompressionNone, CompressionSet: true},
			{Name: "u8", Type: ColumnTypeUint8, Encoding: EncodingRawUint8, Compression: CompressionNone, CompressionSet: true},
			{Name: "i16", Type: ColumnTypeInt16, Encoding: EncodingRawInt16, Compression: CompressionNone, CompressionSet: true},
			{Name: "u16", Type: ColumnTypeUint16, Encoding: EncodingRawUint16, Compression: CompressionNone, CompressionSet: true},
			{Name: "i32", Type: ColumnTypeInt32, Encoding: EncodingRawInt32, Compression: CompressionNone, CompressionSet: true},
			{Name: "u32", Type: ColumnTypeUint32, Encoding: EncodingRawUint32, Compression: CompressionNone, CompressionSet: true},
			{Name: "u64", Type: ColumnTypeUint64, Encoding: EncodingRawUint64, Compression: CompressionNone, CompressionSet: true},
			{Name: "f16", Type: ColumnTypeFloat16, Encoding: EncodingRawFloat16, Compression: CompressionNone, CompressionSet: true},
			{Name: "bf16", Type: ColumnTypeBFloat16, Encoding: EncodingRawBFloat16, Compression: CompressionNone, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows: 3,
		Columns: map[string][]int64{
			"id": {1, 2, 3},
		},
		Int8Columns:     map[string][]int8{"i8": {-2, 127, -128}},
		Uint8Columns:    map[string][]uint8{"u8": {0, 255, 7}},
		Int16Columns:    map[string][]int16{"i16": {-2, 0x1234, -32768}},
		Uint16Columns:   map[string][]uint16{"u16": {0xffff, 0, 0x8000}},
		Int32Columns:    map[string][]int32{"i32": {-2147483648, 2147483647, -7}},
		Uint32Columns:   map[string][]uint32{"u32": {0, 0xffffffff, 0x80000000}},
		Uint64Columns:   map[string][]uint64{"u64": {0x0102030405060708, 0xffffffffffffffff, 0}},
		Float16Columns:  map[string][]uint16{"f16": {0x8000, 0x7c00, 0x7e01}},
		BFloat16Columns: map[string][]uint16{"bf16": {0x8000, 0x7f80, 0x7fc1}},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	return part
}

func assertPrimitiveScalarColumnDataBytes1929(t *testing.T, image ColumnPartImage, column string, want []byte) {
	t.Helper()
	section, ok := image.columnDataSection(column)
	if !ok {
		t.Fatalf("missing column data section %q", column)
	}
	if got := image.sectionBytes(section); !slices.Equal(got, want) {
		t.Fatalf("column %s raw=% x want % x", column, got, want)
	}
}

func decodeAllInt8Blocks1929(t *testing.T, reader *GranuleReader, blocks []ColumnBlock) []int8 {
	t.Helper()
	var out []int8
	for _, block := range blocks {
		values, err := reader.DecodeInt8(block.Granule)
		if err != nil {
			t.Fatalf("DecodeInt8: %v", err)
		}
		out = append(out, values...)
	}
	return out
}

func decodeAllUint32Blocks1929(t *testing.T, reader *GranuleReader, blocks []ColumnBlock) []uint32 {
	t.Helper()
	var out []uint32
	for _, block := range blocks {
		values, err := reader.DecodeUint32(block.Granule)
		if err != nil {
			t.Fatalf("DecodeUint32: %v", err)
		}
		out = append(out, values...)
	}
	return out
}

func decodeAllUint64Blocks1929(t *testing.T, reader *GranuleReader, blocks []ColumnBlock) []uint64 {
	t.Helper()
	var out []uint64
	for _, block := range blocks {
		values, err := reader.DecodeUint64(block.Granule)
		if err != nil {
			t.Fatalf("DecodeUint64: %v", err)
		}
		out = append(out, values...)
	}
	return out
}

func decodeAllFloat16Blocks1929(t *testing.T, reader *GranuleReader, blocks []ColumnBlock) []uint16 {
	t.Helper()
	var out []uint16
	for _, block := range blocks {
		values, err := reader.DecodeFloat16Bits(block.Granule)
		if err != nil {
			t.Fatalf("DecodeFloat16Bits: %v", err)
		}
		out = append(out, values...)
	}
	return out
}

func BenchmarkPrimitiveScalarSequentialDecode1929(b *testing.B) {
	const rows = 8192
	builder := NewGranuleBuilder(Config{Compression: CompressionNone})

	uint8Values := make([]uint8, rows)
	int16Values := make([]int16, rows)
	uint32Values := make([]uint32, rows)
	uint64Values := make([]uint64, rows)
	float16Values := make([]uint16, rows)
	for i := 0; i < rows; i++ {
		uint8Values[i] = uint8(i)
		int16Values[i] = int16(i%65536 - 32768)
		uint32Values[i] = uint32(i) * 2654435761
		uint64Values[i] = uint64(i)*0x9e3779b97f4a7c15 + 0x0102030405060708
		float16Values[i] = uint16(i) ^ 0x7e01
	}

	benchmarks := []struct {
		name  string
		build func() (EncodedGranule, error)
		read  func(*GranuleReader, EncodedGranule) error
	}{
		{
			name: "uint8",
			build: func() (EncodedGranule, error) {
				builder.Reset(Config{Encoding: EncodingRawUint8, Compression: CompressionNone})
				return builder.BuildUint8(uint8Values)
			},
			read: func(reader *GranuleReader, g EncodedGranule) error {
				_, err := reader.DecodeUint8(g)
				return err
			},
		},
		{
			name: "int16",
			build: func() (EncodedGranule, error) {
				builder.Reset(Config{Encoding: EncodingRawInt16, Compression: CompressionNone})
				return builder.BuildInt16(int16Values)
			},
			read: func(reader *GranuleReader, g EncodedGranule) error {
				_, err := reader.DecodeInt16(g)
				return err
			},
		},
		{
			name: "uint32",
			build: func() (EncodedGranule, error) {
				builder.Reset(Config{Encoding: EncodingRawUint32, Compression: CompressionNone})
				return builder.BuildUint32(uint32Values)
			},
			read: func(reader *GranuleReader, g EncodedGranule) error {
				_, err := reader.DecodeUint32(g)
				return err
			},
		},
		{
			name: "uint64",
			build: func() (EncodedGranule, error) {
				builder.Reset(Config{Encoding: EncodingRawUint64, Compression: CompressionNone})
				return builder.BuildUint64(uint64Values)
			},
			read: func(reader *GranuleReader, g EncodedGranule) error {
				_, err := reader.DecodeUint64(g)
				return err
			},
		},
		{
			name: "float16_bits",
			build: func() (EncodedGranule, error) {
				builder.Reset(Config{Encoding: EncodingRawFloat16, Compression: CompressionNone})
				return builder.BuildFloat16Bits(float16Values)
			},
			read: func(reader *GranuleReader, g EncodedGranule) error {
				_, err := reader.DecodeFloat16Bits(g)
				return err
			},
		},
	}

	for _, bm := range benchmarks {
		g, err := bm.build()
		if err != nil {
			b.Fatalf("build %s: %v", bm.name, err)
		}
		var reader GranuleReader
		if err := bm.read(&reader, g); err != nil {
			b.Fatalf("warm read %s: %v", bm.name, err)
		}
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(g.RawBytes))
			for i := 0; i < b.N; i++ {
				if err := bm.read(&reader, g); err != nil {
					b.Fatalf("read %s: %v", bm.name, err)
				}
			}
		})
	}
}
