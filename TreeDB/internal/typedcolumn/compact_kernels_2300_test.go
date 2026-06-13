package typedcolumn

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"
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

func TestColumnPartImageMixedActualCompressionKeepsRequestedCompression2300(t *testing.T) {
	const rows = 4096
	const blockRows = 2048
	ids := make([]int64, rows)
	u32 := make([]uint32, rows)
	x := uint32(0x9e3779b9)
	for i := 0; i < blockRows; i++ {
		ids[i] = int64(i)
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		u32[i] = x
	}
	for i := blockRows; i < rows; i++ {
		ids[i] = int64(i)
		u32[i] = 7
	}
	part, err := BuildColumnPart(2302, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: "u32", Type: ColumnTypeUint32, Encoding: EncodingRawUint32, Compression: CompressionLZ4, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: blockRows, DefaultCodecBlockRows: blockRows},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:          rows,
		Columns:       map[string][]int64{"id": ids},
		Uint32Columns: map[string][]uint32{"u32": u32},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	blocks := part.Columns["u32"].Blocks
	if len(blocks) != 2 {
		t.Fatalf("u32 blocks=%d want 2", len(blocks))
	}
	if blocks[0].Descriptor.Compression != CompressionNone || blocks[1].Descriptor.Compression != CompressionLZ4 {
		t.Fatalf("u32 actual block compression=(%s,%s) want (%s,%s)", blocks[0].Descriptor.Compression, blocks[1].Descriptor.Compression, CompressionNone, CompressionLZ4)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "u32": "uint32"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	section, ok := image.columnDataSection("u32")
	if !ok {
		t.Fatalf("missing u32 column_data section")
	}
	if section.Compression != CompressionLZ4 {
		t.Fatalf("u32 section compression=%s want requested %s", section.Compression, CompressionLZ4)
	}
	if section.RawBytes != 0 {
		t.Fatalf("u32 section raw_bytes=%d want 0 for per-block compressed column_data", section.RawBytes)
	}
	reconstructed, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	reconstructedColumn := reconstructed.Columns["u32"]
	if reconstructedColumn.Definition.Compression != CompressionLZ4 {
		t.Fatalf("reconstructed u32 definition compression=%s want requested %s", reconstructedColumn.Definition.Compression, CompressionLZ4)
	}
	if reconstructedColumn.Blocks[0].Descriptor.Compression != CompressionNone || reconstructedColumn.Blocks[1].Descriptor.Compression != CompressionLZ4 {
		t.Fatalf("reconstructed u32 actual block compression=(%s,%s) want (%s,%s)", reconstructedColumn.Blocks[0].Descriptor.Compression, reconstructedColumn.Blocks[1].Descriptor.Compression, CompressionNone, CompressionLZ4)
	}
	if _, err := BuildColumnPartImage(reconstructed, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "u32": "uint32"}}); err != nil {
		t.Fatalf("BuildColumnPartImage reconstructed mixed-compression part: %v", err)
	}
}

func TestColumnPartImageRejectsUnsupportedScalarBlockCompression2300(t *testing.T) {
	const rows = 1024
	ids := make([]int64, rows)
	u32 := make([]uint32, rows)
	for i := range ids {
		ids[i] = int64(i)
		u32[i] = uint32(i % 3)
	}
	part, err := BuildColumnPart(2303, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: "u32", Type: ColumnTypeUint32, Encoding: EncodingRawUint32, Compression: CompressionLZ4, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: rows},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:          rows,
		Columns:       map[string][]int64{"id": ids},
		Uint32Columns: map[string][]uint32{"u32": u32},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	if got := part.Columns["u32"].Blocks[0].Descriptor.Compression; got != CompressionLZ4 {
		t.Fatalf("u32 block compression=%s want %s", got, CompressionLZ4)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "u32": "uint32"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := cloneColumnPartImageBytes(image)
	compressionOffset := mustColumnPartDescriptorBlockCompressionOffset2300(t, corrupt, "u32", 0)
	binary.LittleEndian.PutUint16(corrupt.Bytes[compressionOffset:compressionOffset+2], uint16(CompressionZSTDDict))

	if _, err := ColumnPartFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "compression=zstd_dict is unsupported") {
		t.Fatalf("ColumnPartFromImage unsupported scalar block compression err=%v want fail-closed unsupported compression", err)
	}
}

func TestColumnPartImageRejectsCompressedScalarStoredBytesNotSmaller2300(t *testing.T) {
	const rows = 1024
	ids := make([]int64, rows)
	u32 := make([]uint32, rows)
	for i := range ids {
		ids[i] = int64(i)
		u32[i] = uint32(i % 3)
	}
	part, err := BuildColumnPart(2304, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: "u32", Type: ColumnTypeUint32, Encoding: EncodingRawUint32, Compression: CompressionLZ4, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: rows},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:          rows,
		Columns:       map[string][]int64{"id": ids},
		Uint32Columns: map[string][]uint32{"u32": u32},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	block := part.Columns["u32"].Blocks[0].Descriptor
	if block.Compression != CompressionLZ4 || block.StoredBytes >= block.RawBytes {
		t.Fatalf("u32 block descriptor=%+v want compressed and smaller", block)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"id": "int64", "u32": "uint32"}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := cloneColumnPartImageBytes(image)
	storedBytesOffset := mustColumnPartDescriptorBlockStoredBytesOffset2300(t, corrupt, "u32", 0)
	binary.LittleEndian.PutUint64(corrupt.Bytes[storedBytesOffset:storedBytesOffset+8], uint64(block.RawBytes))

	if _, err := ColumnPartFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "want 0 < stored < raw") {
		t.Fatalf("ColumnPartFromImage compressed scalar stored/raw equality err=%v want fail-closed raw/stored rejection", err)
	}
}

func TestColumnPartImageCompressedDictionaries2300(t *testing.T) {
	const rows = 1024
	const cardinality = 128
	ids := make([]int64, rows)
	codes := make([]int64, rows)
	dictionary := make(map[string]int64, cardinality)
	for i := 0; i < cardinality; i++ {
		dictionary[fmt.Sprintf("%s%03d", strings.Repeat("atlas://did:plc:jsonbench-storage-parity/", 4), i)] = int64(i)
	}
	for i := range ids {
		ids[i] = int64(i)
		codes[i] = int64(i % cardinality)
	}
	part, err := BuildColumnPart(2301, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "tag_code", Type: ColumnTypeLowCardinalityCode, Compression: CompressionNone, Cardinality: cardinality},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: rows},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:    rows,
		Columns: map[string][]int64{"id": ids, "tag_code": codes},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{
		Dictionaries:       map[string]map[string]int64{"tag_code": dictionary},
		SectionCompression: CompressionLZ4,
	})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	dictSection, err := image.singleSection(ColumnPartImageSectionDictionaries)
	if err != nil {
		t.Fatalf("dictionary section: %v", err)
	}
	if dictSection.Compression != CompressionLZ4 {
		t.Fatalf("dictionary compression=%s want %s raw/stored=%d/%d", dictSection.Compression, CompressionLZ4, dictSection.RawBytes, dictSection.Length)
	}
	if dictSection.RawBytes <= dictSection.Length {
		t.Fatalf("dictionary raw/stored=%d/%d want compressed section smaller", dictSection.RawBytes, dictSection.Length)
	}
	accounting := image.SectionByteAccounting()
	foundAccounting := false
	for _, row := range accounting {
		if row.Kind == ColumnPartImageSectionDictionaries {
			foundAccounting = true
			if row.RawBytes != dictSection.RawBytes || row.StoredBytes != dictSection.Length || row.Compression != CompressionLZ4 {
				t.Fatalf("dictionary accounting=%+v section=%+v", row, dictSection)
			}
		}
	}
	if !foundAccounting {
		t.Fatalf("dictionary section missing from accounting: %+v", accounting)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	dictionaries, err := parsed.Dictionaries()
	if err != nil {
		t.Fatalf("Dictionaries: %v", err)
	}
	if len(dictionaries["tag_code"]) != cardinality {
		t.Fatalf("dictionary entries=%d want %d", len(dictionaries["tag_code"]), cardinality)
	}
	for value, code := range dictionary {
		if got := dictionaries["tag_code"][value]; got != code {
			t.Fatalf("dictionary value %q code=%d want %d", value, got, code)
		}
	}
	if _, err := ColumnPartFromImage(parsed); err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	tcs1, _, err := EncodeTCS1ColumnPartImage(parsed)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	decoded, _, err := DecodeTCS1ColumnPartImage(tcs1)
	if err != nil {
		t.Fatalf("DecodeTCS1ColumnPartImage: %v", err)
	}
	if _, err := decoded.Dictionaries(); err != nil {
		t.Fatalf("decoded TCS1 dictionaries: %v", err)
	}

	corrupt := parsed
	corrupt.Sections = append([]ColumnPartImageSection(nil), parsed.Sections...)
	for i := range corrupt.Sections {
		if corrupt.Sections[i].Kind == ColumnPartImageSectionDictionaries {
			corrupt.Sections[i].RawBytes++
			break
		}
	}
	if _, err := corrupt.Dictionaries(); err == nil || !strings.Contains(err.Error(), "decoded length") {
		t.Fatalf("corrupt dictionary raw bytes err=%v want decoded length mismatch", err)
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

func mustColumnPartDescriptorBlockCompressionOffset2300(t *testing.T, image ColumnPartImage, column string, blockIndex int) int {
	t.Helper()
	return mustColumnPartDescriptorBlockFieldOffset2300(t, image, column, blockIndex, 34)
}

func mustColumnPartDescriptorBlockStoredBytesOffset2300(t *testing.T, image ColumnPartImage, column string, blockIndex int) int {
	t.Helper()
	return mustColumnPartDescriptorBlockFieldOffset2300(t, image, column, blockIndex, 44)
}

func mustColumnPartDescriptorBlockFieldOffset2300(t *testing.T, image ColumnPartImage, column string, blockIndex int, fieldOffset int) int {
	t.Helper()
	const encodedBlockDescriptorBytes = 94
	if fieldOffset < 0 || fieldOffset >= encodedBlockDescriptorBytes {
		t.Fatalf("descriptor block field offset=%d outside block bytes=%d", fieldOffset, encodedBlockDescriptorBytes)
	}
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
		mustValidationReadU32(t, &dec) // cardinality
		mustValidationReadU32(t, &dec) // fixed_width_elements
		mustValidationReadU32(t, &dec) // bits_per_element
		blockCount := mustValidationReadU32(t, &dec)
		if name == column {
			if blockIndex < 0 || blockIndex >= int(blockCount) {
				t.Fatalf("descriptor column %q block index=%d outside blocks=%d", column, blockIndex, blockCount)
			}
			return section.Offset + dec.offset + blockIndex*encodedBlockDescriptorBytes + fieldOffset
		}
		blockBytes := int(blockCount) * encodedBlockDescriptorBytes
		if err := dec.require(blockBytes); err != nil {
			t.Fatalf("skip descriptor column %q blocks: %v", name, err)
		}
		dec.offset += blockBytes
	}
	t.Fatalf("descriptor column %q not found", column)
	return 0
}
