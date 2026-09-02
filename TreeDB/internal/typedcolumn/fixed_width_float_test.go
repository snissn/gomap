package typedcolumn

import (
	"math"
	"slices"
	"strings"
	"testing"
)

func TestRawFixedWidthLittleEndianFixtures1737(t *testing.T) {
	intRaw, err := encodeInt64Payload(nil, []int64{1, -2, math.MinInt64}, EncodingRawInt64)
	if err != nil {
		t.Fatalf("encodeInt64Payload: %v", err)
	}
	wantInt := []byte{
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
	}
	if !slices.Equal(intRaw, wantInt) {
		t.Fatalf("raw int64 bytes=% x want % x", intRaw, wantInt)
	}

	f32Values := []float32{math.Float32frombits(0x3f800000), math.Float32frombits(0x80000000), math.Float32frombits(0x7fc12345)}
	f32Raw, err := encodeFloat32Payload(nil, f32Values)
	if err != nil {
		t.Fatalf("encodeFloat32Payload: %v", err)
	}
	wantF32 := []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x80, 0x45, 0x23, 0xc1, 0x7f}
	if !slices.Equal(f32Raw, wantF32) {
		t.Fatalf("raw float32 bytes=% x want % x", f32Raw, wantF32)
	}

	f64Values := []float64{math.Float64frombits(0x3ff0000000000000), math.Float64frombits(0x8000000000000000), math.Float64frombits(0x7ff8000000000042)}
	f64Raw, err := encodeFloat64Payload(nil, f64Values)
	if err != nil {
		t.Fatalf("encodeFloat64Payload: %v", err)
	}
	wantF64 := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
		0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f,
	}
	if !slices.Equal(f64Raw, wantF64) {
		t.Fatalf("raw float64 bytes=% x want % x", f64Raw, wantF64)
	}

	vecRaw, err := encodeFloat32DensePayload(nil, []float32{1, 0.5, -0.25, 2})
	if err != nil {
		t.Fatalf("encodeFloat32DensePayload: %v", err)
	}
	wantVec := []byte{
		0x00, 0x00, 0x80, 0x3f,
		0x00, 0x00, 0x00, 0x3f,
		0x00, 0x00, 0x80, 0xbe,
		0x00, 0x00, 0x00, 0x40,
	}
	if !slices.Equal(vecRaw, wantVec) {
		t.Fatalf("dense float32_vector bytes=% x want % x", vecRaw, wantVec)
	}
}

func TestScalarFloatRawRoundTripPreservesBits1737(t *testing.T) {
	f32Bits := []uint32{0x00000000, 0x80000000, 0x7f800000, 0xff800000, 0x7f7fffff, 0xff7fffff, 0x00800000, 0x7fc12345, 0x7fa12345, 0xffc0d00d}
	f32 := make([]float32, len(f32Bits))
	for i, bits := range f32Bits {
		f32[i] = math.Float32frombits(bits)
	}
	builder := NewGranuleBuilder(Config{Encoding: EncodingRawFloat32, Compression: CompressionNone})
	g32, err := builder.BuildFloat32(f32)
	if err != nil {
		t.Fatalf("BuildFloat32: %v", err)
	}
	var reader GranuleReader
	got32, err := reader.DecodeFloat32(g32)
	if err != nil {
		t.Fatalf("DecodeFloat32: %v", err)
	}
	if len(got32) != len(f32Bits) {
		t.Fatalf("DecodeFloat32 rows=%d want %d", len(got32), len(f32Bits))
	}
	for i, want := range f32Bits {
		if got := math.Float32bits(got32[i]); got != want {
			t.Fatalf("float32 row %d bits=%08x want %08x", i, got, want)
		}
	}

	f64Bits := []uint64{0x0000000000000000, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7fefffffffffffff, 0xffefffffffffffff, 0x0010000000000000, 0x7ff8000000000042, 0x7ff0000000000042, 0xfff800000000d00d}
	f64 := make([]float64, len(f64Bits))
	for i, bits := range f64Bits {
		f64[i] = math.Float64frombits(bits)
	}
	builder.Reset(Config{Encoding: EncodingRawFloat64, Compression: CompressionNone})
	g64, err := builder.BuildFloat64(f64)
	if err != nil {
		t.Fatalf("BuildFloat64: %v", err)
	}
	got64, err := reader.DecodeFloat64(g64)
	if err != nil {
		t.Fatalf("DecodeFloat64: %v", err)
	}
	if len(got64) != len(f64Bits) {
		t.Fatalf("DecodeFloat64 rows=%d want %d", len(got64), len(f64Bits))
	}
	for i, want := range f64Bits {
		if got := math.Float64bits(got64[i]); got != want {
			t.Fatalf("float64 row %d bits=%016x want %016x", i, got, want)
		}
	}
}

func TestScalarFloatColumnPartImageRoundTrip1737(t *testing.T) {
	part, err := BuildColumnPart(1737, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "f32", Type: ColumnTypeFloat32, Encoding: EncodingRawFloat32, Compression: CompressionNone, CompressionSet: true},
			{Name: "f64", Type: ColumnTypeFloat64, Encoding: EncodingRawFloat64, Compression: CompressionNone, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:           2,
		Columns:        map[string][]int64{"id": {1, 2}},
		Float32Columns: map[string][]float32{"f32": {math.Float32frombits(0x7fc12345), math.Float32frombits(0x80000000)}},
		Float64Columns: map[string][]float64{"f64": {math.Float64frombits(0x7ff8000000000042), math.Float64frombits(0x8000000000000000)}},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "f32": "float32", "f64": "double"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	reconstructed, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	var reader GranuleReader
	f32, err := reader.DecodeFloat32(reconstructed.Columns["f32"].Blocks[0].Granule)
	if err != nil {
		t.Fatalf("DecodeFloat32 reconstructed: %v", err)
	}
	if got := []uint32{math.Float32bits(f32[0]), math.Float32bits(f32[1])}; !slices.Equal(got, []uint32{0x7fc12345, 0x80000000}) {
		t.Fatalf("reconstructed f32 bits=%08x", got)
	}
	f64, err := reader.DecodeFloat64(reconstructed.Columns["f64"].Blocks[0].Granule)
	if err != nil {
		t.Fatalf("DecodeFloat64 reconstructed: %v", err)
	}
	if got := []uint64{math.Float64bits(f64[0]), math.Float64bits(f64[1])}; !slices.Equal(got, []uint64{0x7ff8000000000042, 0x8000000000000000}) {
		t.Fatalf("reconstructed f64 bits=%016x", got)
	}
}

func TestScalarFloatRawDecodeFailsClosed1737(t *testing.T) {
	var reader GranuleReader
	if _, err := reader.DecodeFloat32(EncodedGranule{Rows: 1, Encoding: EncodingRawInt64, Compression: CompressionNone, RawBytes: 8, StoredBytes: 8, PayloadRef: PayloadRef{Kind: PayloadRefInline, Length: 8}, Payload: make([]byte, 8)}); err == nil || !strings.Contains(err.Error(), "float32 decode got encoding") {
		t.Fatalf("DecodeFloat32 wrong encoding err=%v", err)
	}
	if _, err := DecodeRawFloat32Payload(nil, []byte{1, 2, 3}, 1); err == nil || !strings.Contains(err.Error(), "raw bytes") {
		t.Fatalf("DecodeRawFloat32Payload short err=%v", err)
	}
	if _, err := DecodeRawFloat64Payload(nil, []byte{1, 2, 3, 4}, 1); err == nil || !strings.Contains(err.Error(), "raw bytes") {
		t.Fatalf("DecodeRawFloat64Payload short err=%v", err)
	}
	block := ColumnBlockDescriptor{FirstRow: 0, RowCount: 1, FirstGranule: 0, LastGranule: 0, Encoding: EncodingRawFloat32, Compression: CompressionNone, RawBytes: 3, StoredBytes: 3}
	desc := ColumnPartDescriptor{RowCount: 1, Granules: []GranuleDescriptor{{Ordinal: 0, FirstRow: 0, RowCount: 1, VisibleRows: 1}}}
	if err := validateDecodedColumnBlockDescriptor(desc, "f32", ColumnTypeFloat32, 0, 0, 0, block); err == nil || !strings.Contains(err.Error(), "fixed-width raw bytes") {
		t.Fatalf("validate scalar float truncated err=%v", err)
	}
}

func BenchmarkRawFixedWidthDecodeHelpers1737(b *testing.B) {
	values := make([]int64, 8192)
	for i := range values {
		values[i] = int64(i) - 4096
	}
	raw, err := encodeInt64Payload(nil, values, EncodingRawInt64)
	if err != nil {
		b.Fatalf("encodeInt64Payload: %v", err)
	}
	dst := make([]int64, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeLittleEndian8Payload(dst, raw, len(values), "raw int64"); err != nil {
			b.Fatalf("decodeLittleEndian8Payload: %v", err)
		}
	}
}

func BenchmarkScalarFloatRawDecode1737(b *testing.B) {
	values := make([]float32, 8192)
	for i := range values {
		values[i] = math.Float32frombits(uint32(i) | 0x3f800000)
	}
	raw, err := encodeFloat32Payload(nil, values)
	if err != nil {
		b.Fatalf("encodeFloat32Payload: %v", err)
	}
	dst := make([]float32, len(values))
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeRawFloat32Payload(dst, raw, len(values)); err != nil {
			b.Fatalf("DecodeRawFloat32Payload: %v", err)
		}
	}
}
