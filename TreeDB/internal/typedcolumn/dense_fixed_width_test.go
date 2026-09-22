package typedcolumn

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestDenseFixedWidthVectorRoundTripAllTypes1930(t *testing.T) {
	cases := []struct {
		name     string
		typeName ColumnType
		encoding Encoding
		width    int
		elements int
	}{
		{"uint8", ColumnTypeUint8Vector, EncodingRawUint8Vector, 1, 5},
		{"int8", ColumnTypeInt8Vector, EncodingRawInt8Vector, 1, 5},
		{"uint16", ColumnTypeUint16Vector, EncodingRawUint16Vector, 2, 3},
		{"int16", ColumnTypeInt16Vector, EncodingRawInt16Vector, 2, 3},
		{"uint32", ColumnTypeUint32Vector, EncodingRawUint32Vector, 4, 3},
		{"int32", ColumnTypeInt32Vector, EncodingRawInt32Vector, 4, 3},
		{"uint64", ColumnTypeUint64Vector, EncodingRawUint64Vector, 8, 2},
		{"int64", ColumnTypeInt64Vector, EncodingRawInt64Vector, 8, 2},
		{"float16", ColumnTypeFloat16Vector, EncodingRawFloat16Vector, 2, 3},
		{"bfloat16", ColumnTypeBFloat16Vector, EncodingRawBFloat16Vector, 2, 3},
		{"float64", ColumnTypeFloat64Vector, EncodingRawFloat64Vector, 8, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := 3
			raw := denseFixedWidthFixtureBytes1930(rows, tc.elements, tc.width)
			part := buildDenseFixedWidthPart1930(t, tc.typeName, tc.encoding, rows, tc.elements, tc.width, raw)
			got, err := part.DenseFixedWidthColumn("v", nil)
			if err != nil {
				t.Fatalf("DenseFixedWidthColumn: %v", err)
			}
			if got.Rows != rows || got.ElementsPerRow != tc.elements || got.ElementWidthBytes != tc.width || !bytes.Equal(got.Values, raw) {
				t.Fatalf("decoded=%+v bytes=%x want rows=%d elements=%d width=%d bytes=%x", got, got.Values, rows, tc.elements, tc.width, raw)
			}
			row1, err := got.RowBytes(1)
			if err != nil {
				t.Fatalf("RowBytes: %v", err)
			}
			rowBytes := tc.elements * tc.width
			if !bytes.Equal(row1, raw[rowBytes:2*rowBytes]) {
				t.Fatalf("row-major row1=%x want %x", row1, raw[rowBytes:2*rowBytes])
			}
			rangeBytes, err := got.RowRangeBytes(1, 3)
			if err != nil {
				t.Fatalf("RowRangeBytes: %v", err)
			}
			if !bytes.Equal(rangeBytes, raw[rowBytes:]) {
				t.Fatalf("row range=%x want %x", rangeBytes, raw[rowBytes:])
			}
			image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"v": string(tc.typeName)}})
			if err != nil {
				t.Fatalf("BuildColumnPartImage: %v", err)
			}
			parsed, err := ParseColumnPartImage(image.Bytes)
			if err != nil {
				t.Fatalf("ParseColumnPartImage: %v", err)
			}
			reopened, err := ColumnPartFromImage(parsed)
			if err != nil {
				t.Fatalf("ColumnPartFromImage: %v", err)
			}
			reopenedDense, err := reopened.DenseFixedWidthColumn("v", nil)
			if err != nil {
				t.Fatalf("reopened DenseFixedWidthColumn: %v", err)
			}
			if !bytes.Equal(reopenedDense.Values, raw) {
				t.Fatalf("reopened bytes=%x want %x", reopenedDense.Values, raw)
			}
			cert, err := CertifyColumnPartLayoutContractFromImage(parsed)
			if err != nil {
				t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
			}
			column, ok := cert.Column("v")
			if !ok {
				t.Fatalf("missing contract column")
			}
			if !column.DirectViewCertified || column.Type != tc.typeName || column.Encoding != tc.encoding || column.ElementSize != tc.width || column.FixedWidthElements != tc.elements {
				t.Fatalf("contract=%+v want direct type=%s encoding=%s width=%d elements=%d", column, tc.typeName, tc.encoding, tc.width, tc.elements)
			}
		})
	}
}

func TestDenseFixedWidthVectorValidationFailsClosed1930(t *testing.T) {
	_, err := BuildColumnPart(193002, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "v", Type: ColumnTypeUint16Vector, Encoding: EncodingRawUint16Vector, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 3},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:    2,
		Columns: map[string][]int64{"id": {0, 1}},
		DenseFixedWidthVectors: map[string]RawDenseFixedWidth{
			"v": {Rows: 2, ElementsPerRow: 3, ElementWidthBytes: 2, Values: make([]byte, 10)},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "raw bytes=10 want=12") {
		t.Fatalf("non-divisible dense vector err=%v want raw byte fail-closed", err)
	}

	part := buildDenseFixedWidthPart1930(t, ColumnTypeUint16Vector, EncodingRawUint16Vector, 2, 3, 2, denseFixedWidthFixtureBytes1930(2, 3, 2))
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"v": string(ColumnTypeUint16Vector)}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := cloneColumnPartImageBytes(image)
	fixedWidthOffset := mustValidationDescriptorFixedWidthOffset(t, corrupt, "v")
	binary.LittleEndian.PutUint32(corrupt.Bytes[fixedWidthOffset:fixedWidthOffset+4], 4)
	if _, err := ColumnPartFromImage(corrupt); err == nil || !(strings.Contains(err.Error(), "raw bytes") || strings.Contains(err.Error(), "fixed-width elements")) {
		t.Fatalf("ColumnPartFromImage corrupt fixed_width_elements err=%v want fail-closed", err)
	}
}

func TestEmptyFloat32VectorImageDefaultsEncoding1930(t *testing.T) {
	sortKey := []SortKeyColumn{{Column: "id"}}
	part := &ColumnPart{
		Descriptor: ColumnPartDescriptor{
			Version:           columnPartDescriptorVersion,
			PartID:            193004,
			SchemaVersion:     1,
			LogicalPrimaryKey: []string{"id"},
			SortKey:           sortKey,
			Columns: []ColumnPartColumnDescriptor{
				{Name: "id", Type: ColumnTypeInt64},
				{Name: "embedding", Type: ColumnTypeFloat32Vector, FixedWidthElements: 3},
			},
		},
		Columns: map[string]ColumnPartColumn{
			"id":        {Definition: ColumnDefinition{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone}},
			"embedding": {Definition: ColumnDefinition{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, Compression: CompressionNone, FixedWidthElements: 3}},
		},
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"embedding": string(ColumnTypeFloat32Vector)}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	reopened, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	definition := reopened.Columns["embedding"].Definition
	if definition.Encoding != EncodingRawFloat32Vector || definition.Compression != CompressionNone {
		t.Fatalf("empty float32_vector definition=%+v want raw_float32_vector compression=none", definition)
	}
}

func TestDenseFixedWidthVectorFloat32CompatibilityAndAdjacencyIsolation1930(t *testing.T) {
	floatPart, err := BuildColumnPart(193003, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 3},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{Rows: 2, Columns: map[string][]int64{"id": {0, 1}}, Float32Vectors: map[string][]float32{"embedding": {1, 2, 3, 4, 5, 6}}})
	if err != nil {
		t.Fatalf("BuildColumnPart float32_vector: %v", err)
	}
	legacy, err := floatPart.DenseFloat32VectorColumn("embedding", nil)
	if err != nil || len(legacy.Values) != 6 || legacy.Values[5] != 6 {
		t.Fatalf("legacy DenseFloat32VectorColumn=%+v err=%v", legacy, err)
	}
	if _, err := floatPart.DenseFixedWidthColumn("embedding", nil); err == nil || !strings.Contains(err.Error(), "not generic dense fixed-width vector") {
		t.Fatalf("generic DenseFixedWidthColumn on float32_vector err=%v want compatibility isolation", err)
	}

	rawUint32 := denseFixedWidthFixtureBytes1930(2, 3, 4)
	generic := buildDenseFixedWidthPart1930(t, ColumnTypeUint32Vector, EncodingRawUint32Vector, 2, 3, 4, rawUint32)
	if generic.Columns["v"].Definition.Type != ColumnTypeUint32Vector || generic.Columns["v"].Definition.Encoding != EncodingRawUint32Vector {
		t.Fatalf("uint32_vector definition=%+v want generic vector, not adjacency_list/raw_uint32_dense", generic.Columns["v"].Definition)
	}
	if _, err := generic.DenseUint32Column("v", nil); err == nil || !strings.Contains(err.Error(), "not adjacency_list") {
		t.Fatalf("DenseUint32Column on uint32_vector err=%v want adjacency isolation", err)
	}
}

func buildDenseFixedWidthPart1930(t *testing.T, columnType ColumnType, encoding Encoding, rows, elementsPerRow, width int, raw []byte) *ColumnPart {
	t.Helper()
	part, err := BuildColumnPart(193001, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "v", Type: columnType, Encoding: encoding, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: elementsPerRow},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:    rows,
		Columns: map[string][]int64{"id": denseFixedWidthIDs1930(rows)},
		DenseFixedWidthVectors: map[string]RawDenseFixedWidth{
			"v": {Rows: rows, ElementsPerRow: elementsPerRow, ElementWidthBytes: width, Values: raw},
		},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart(%s): %v", columnType, err)
	}
	return part
}

func denseFixedWidthIDs1930(rows int) []int64 {
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	return ids
}

func denseFixedWidthFixtureBytes1930(rows, elementsPerRow, width int) []byte {
	out := make([]byte, rows*elementsPerRow*width)
	for row := 0; row < rows; row++ {
		for elem := 0; elem < elementsPerRow; elem++ {
			base := (row*elementsPerRow + elem) * width
			for b := 0; b < width; b++ {
				out[base+b] = byte(1 + row*17 + elem*3 + b)
			}
		}
	}
	return out
}
