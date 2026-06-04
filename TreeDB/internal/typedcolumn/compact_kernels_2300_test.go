package typedcolumn

import (
	"math"
	"slices"
	"testing"
)

func TestNumericScalarCompressionKernels2300(t *testing.T) {
	for _, compression := range []Compression{CompressionSnappy, CompressionLZ4} {
		t.Run(compression.String()+"/uint32", func(t *testing.T) {
			values := make([]uint32, 4096)
			for i := range values {
				values[i] = uint32(i % 4)
			}
			builder := NewGranuleBuilder(Config{Encoding: EncodingRawUint32, Compression: compression})
			granule, err := builder.BuildUint32(values)
			if err != nil {
				t.Fatalf("BuildUint32: %v", err)
			}
			assertKeptCompression2300(t, granule, EncodingRawUint32, compression)
			var reader GranuleReader
			got, err := reader.DecodeUint32(granule)
			if err != nil {
				t.Fatalf("DecodeUint32: %v", err)
			}
			if !slices.Equal(got, values) {
				t.Fatalf("DecodeUint32 mismatch")
			}
		})
		t.Run(compression.String()+"/float32", func(t *testing.T) {
			values := make([]float32, 4096)
			for i := range values {
				values[i] = math.Float32frombits(0x3f800000 + uint32(i%2))
			}
			builder := NewGranuleBuilder(Config{Encoding: EncodingRawFloat32, Compression: compression})
			granule, err := builder.BuildFloat32(values)
			if err != nil {
				t.Fatalf("BuildFloat32: %v", err)
			}
			assertKeptCompression2300(t, granule, EncodingRawFloat32, compression)
			var reader GranuleReader
			got, err := reader.DecodeFloat32(granule)
			if err != nil {
				t.Fatalf("DecodeFloat32: %v", err)
			}
			for i, want := range values {
				if math.Float32bits(got[i]) != math.Float32bits(want) {
					t.Fatalf("DecodeFloat32 row %d bits=%08x want %08x", i, math.Float32bits(got[i]), math.Float32bits(want))
				}
			}
		})
		t.Run(compression.String()+"/float64", func(t *testing.T) {
			values := make([]float64, 4096)
			for i := range values {
				values[i] = math.Float64frombits(0x3ff0000000000000 + uint64(i%2))
			}
			builder := NewGranuleBuilder(Config{Encoding: EncodingRawFloat64, Compression: compression})
			granule, err := builder.BuildFloat64(values)
			if err != nil {
				t.Fatalf("BuildFloat64: %v", err)
			}
			assertKeptCompression2300(t, granule, EncodingRawFloat64, compression)
			var reader GranuleReader
			got, err := reader.DecodeFloat64(granule)
			if err != nil {
				t.Fatalf("DecodeFloat64: %v", err)
			}
			for i, want := range values {
				if math.Float64bits(got[i]) != math.Float64bits(want) {
					t.Fatalf("DecodeFloat64 row %d bits=%016x want %016x", i, math.Float64bits(got[i]), math.Float64bits(want))
				}
			}
		})
	}
}

func TestColumnPartImageCompressedNumericScalars2300(t *testing.T) {
	const rows = 1024
	ids := make([]int64, rows)
	u32 := make([]uint32, rows)
	f32 := make([]float32, rows)
	f64 := make([]float64, rows)
	for i := range ids {
		ids[i] = int64(i)
		u32[i] = uint32(i % 3)
		f32[i] = math.Float32frombits(0x3f800000 + uint32(i%2))
		f64[i] = math.Float64frombits(0x3ff0000000000000 + uint64(i%2))
	}
	part, err := BuildColumnPart(2300, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: "u32", Type: ColumnTypeUint32, Encoding: EncodingRawUint32, Compression: CompressionLZ4, CompressionSet: true},
			{Name: "f32", Type: ColumnTypeFloat32, Encoding: EncodingRawFloat32, Compression: CompressionLZ4, CompressionSet: true},
			{Name: "f64", Type: ColumnTypeFloat64, Encoding: EncodingRawFloat64, Compression: CompressionLZ4, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: rows},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:           rows,
		Columns:        map[string][]int64{"id": ids},
		Uint32Columns:  map[string][]uint32{"u32": u32},
		Float32Columns: map[string][]float32{"f32": f32},
		Float64Columns: map[string][]float64{"f64": f64},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	for name, encoding := range map[string]Encoding{"u32": EncodingRawUint32, "f32": EncodingRawFloat32, "f64": EncodingRawFloat64} {
		blocks := part.Columns[name].Blocks
		if len(blocks) != 1 {
			t.Fatalf("%s blocks=%d want 1", name, len(blocks))
		}
		assertKeptCompression2300(t, blocks[0].Granule, encoding, CompressionLZ4)
		if blocks[0].Descriptor.Compression != CompressionLZ4 || blocks[0].Descriptor.StoredBytes != blocks[0].Granule.StoredBytes {
			t.Fatalf("%s descriptor=%+v granule=%+v", name, blocks[0].Descriptor, blocks[0].Granule)
		}
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "u32": "uint32", "f32": "float32", "f64": "double"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	reconstructed, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	var reader GranuleReader
	gotU32, err := reader.DecodeUint32(reconstructed.Columns["u32"].Blocks[0].Granule)
	if err != nil {
		t.Fatalf("DecodeUint32 reconstructed: %v", err)
	}
	if !slices.Equal(gotU32, u32) {
		t.Fatalf("reconstructed u32 mismatch")
	}
	gotF32, err := reader.DecodeFloat32(reconstructed.Columns["f32"].Blocks[0].Granule)
	if err != nil {
		t.Fatalf("DecodeFloat32 reconstructed: %v", err)
	}
	for i, want := range f32 {
		if math.Float32bits(gotF32[i]) != math.Float32bits(want) {
			t.Fatalf("reconstructed f32 row %d bits=%08x want %08x", i, math.Float32bits(gotF32[i]), math.Float32bits(want))
		}
	}
	gotF64, err := reader.DecodeFloat64(reconstructed.Columns["f64"].Blocks[0].Granule)
	if err != nil {
		t.Fatalf("DecodeFloat64 reconstructed: %v", err)
	}
	for i, want := range f64 {
		if math.Float64bits(gotF64[i]) != math.Float64bits(want) {
			t.Fatalf("reconstructed f64 row %d bits=%016x want %016x", i, math.Float64bits(gotF64[i]), math.Float64bits(want))
		}
	}
}

func assertKeptCompression2300(t *testing.T, granule EncodedGranule, encoding Encoding, compression Compression) {
	t.Helper()
	if granule.Encoding != encoding || granule.Compression != compression {
		t.Fatalf("encoding/compression=%s/%s want %s/%s report=%+v", granule.Encoding, granule.Compression, encoding, compression, granule.CodecReport)
	}
	if granule.StoredBytes >= granule.RawBytes {
		t.Fatalf("stored/raw=%d/%d want compressed smaller report=%+v", granule.StoredBytes, granule.RawBytes, granule.CodecReport)
	}
	if !granule.CodecReport.CompressionAttempted || !granule.CodecReport.CompressionKept || granule.CodecReport.ActualCompression != compression {
		t.Fatalf("codec report=%+v want kept %s", granule.CodecReport, compression)
	}
}
