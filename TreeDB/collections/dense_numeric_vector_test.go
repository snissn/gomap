package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

func TestTypedColumnDenseNumericVectorAdapterRoundTripAllTypes1930(t *testing.T) {
	cases := []struct {
		valueType ColumnStoreValueType
		wantType  typedcolumn.ColumnType
		wantEnc   typedcolumn.Encoding
		width     int
		elements  int
	}{
		{ColumnStoreValueUint8Vector, typedcolumn.ColumnTypeUint8Vector, typedcolumn.EncodingRawUint8Vector, 1, 5},
		{ColumnStoreValueInt8Vector, typedcolumn.ColumnTypeInt8Vector, typedcolumn.EncodingRawInt8Vector, 1, 5},
		{ColumnStoreValueUint16Vector, typedcolumn.ColumnTypeUint16Vector, typedcolumn.EncodingRawUint16Vector, 2, 3},
		{ColumnStoreValueInt16Vector, typedcolumn.ColumnTypeInt16Vector, typedcolumn.EncodingRawInt16Vector, 2, 3},
		{ColumnStoreValueUint32Vector, typedcolumn.ColumnTypeUint32Vector, typedcolumn.EncodingRawUint32Vector, 4, 3},
		{ColumnStoreValueInt32Vector, typedcolumn.ColumnTypeInt32Vector, typedcolumn.EncodingRawInt32Vector, 4, 3},
		{ColumnStoreValueUint64Vector, typedcolumn.ColumnTypeUint64Vector, typedcolumn.EncodingRawUint64Vector, 8, 2},
		{ColumnStoreValueInt64Vector, typedcolumn.ColumnTypeInt64Vector, typedcolumn.EncodingRawInt64Vector, 8, 2},
		{ColumnStoreValueFloat16Vector, typedcolumn.ColumnTypeFloat16Vector, typedcolumn.EncodingRawFloat16Vector, 2, 3},
		{ColumnStoreValueBFloat16Vector, typedcolumn.ColumnTypeBFloat16Vector, typedcolumn.EncodingRawBFloat16Vector, 2, 3},
		{ColumnStoreValueFloat64Vector, typedcolumn.ColumnTypeFloat64Vector, typedcolumn.EncodingRawFloat64Vector, 8, 2},
	}
	for _, tc := range cases {
		t.Run(string(tc.valueType), func(t *testing.T) {
			field := typedColumnAdapterField("codes", tc.valueType)
			field.ElementsPerRow = tc.elements
			row0 := denseNumericVectorRowBytes1930(tc.elements, tc.width, 1)
			row1 := denseNumericVectorRowBytes1930(tc.elements, tc.width, 99)
			values := []columnDeclaredValue{
				{Type: tc.valueType, Present: true, DenseNumericVector: row0},
				{Type: tc.valueType, Present: true, DenseNumericVector: row1},
			}
			got := typedColumnAdapterRoundTrip(t, field, values)
			if len(got) != len(values) {
				t.Fatalf("roundtrip rows=%d want %d", len(got), len(values))
			}
			for i := range got {
				if got[i].Type != tc.valueType || !got[i].Present || !bytes.Equal(got[i].DenseNumericVector, values[i].DenseNumericVector) {
					t.Fatalf("row %d got=%+v bytes=%x want bytes=%x", i, got[i], got[i].DenseNumericVector, values[i].DenseNumericVector)
				}
			}
			column, err := typedColumnAdapterMapField(field)
			if err != nil {
				t.Fatalf("typedColumnAdapterMapField: %v", err)
			}
			if column.Definition.Type != tc.wantType || column.Definition.Encoding != tc.wantEnc || column.Definition.FixedWidthElements != tc.elements {
				t.Fatalf("definition=%+v want type=%s encoding=%s elements=%d", column.Definition, tc.wantType, tc.wantEnc, tc.elements)
			}
			part := typedColumnAdapterBuildPart(t, field, values)
			image, err := part.buildImage()
			if err != nil {
				t.Fatalf("buildImage: %v", err)
			}
			cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
			if err != nil {
				t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
			}
			certColumn, ok := cert.Column("codes")
			if !ok {
				t.Fatalf("missing contract column")
			}
			if !certColumn.DirectViewCertified || certColumn.ElementSize != tc.width || certColumn.FixedWidthElements != tc.elements {
				t.Fatalf("contract=%+v want dense direct view width=%d elements=%d", certColumn, tc.width, tc.elements)
			}
		})
	}
}

func TestTypedColumnDenseNumericVectorDirectViewContractBFloat16Vector1930(t *testing.T) {
	field := typedColumnAdapterField("codes", ColumnStoreValueBFloat16Vector)
	field.ElementsPerRow = 3
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
		{Type: ColumnStoreValueBFloat16Vector, Present: true, DenseNumericVector: denseNumericVectorRowBytes1930(3, 2, 1)},
		{Type: ColumnStoreValueBFloat16Vector, Present: true, DenseNumericVector: denseNumericVectorRowBytes1930(3, 2, 9)},
	})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	certColumn, ok := cert.Column("codes")
	if !ok {
		t.Fatalf("missing contract column")
	}
	classification := typedColumnDirectViewClassificationFor(ColumnStoreValueBFloat16Vector, typedColumnDirectViewStorageTypedColumnPart, typedColumnDirectViewConsumerTypedColumnPartGeneric)
	if classification.Support != typedColumnDirectViewActiveLittleEndianCandidate || classification.ElementSize != 2 || !classification.RequiresElementsPerRow {
		t.Fatalf("classification=%+v want active bfloat16_vector direct view contract", classification)
	}
	plan := typeddecode.DenseFixedWidthVectorBytesPlan(certColumn, columnsemantics.LogicalBFloat16Vector, typedcolumn.ColumnTypeBFloat16Vector, typedcolumn.EncodingRawBFloat16Vector, 2, 3)
	if !plan.DirectCandidate() {
		t.Fatalf("plan=%+v want direct candidate", plan)
	}
	status := typeddecode.ValidateDirectViewColumn(typeddecode.DirectViewColumnRequest{Plan: plan, Certification: certColumn, Rows: 2, PayloadBytes: certColumn.Section.Length, AssetOffset: 0, HasAssetOffset: true})
	if !status.Direct() {
		t.Fatalf("direct-view validation status=%+v", status)
	}
	bad := typeddecode.ValidateDirectViewColumn(typeddecode.DirectViewColumnRequest{Plan: typeddecode.DenseFixedWidthVectorBytesPlan(certColumn, columnsemantics.LogicalBFloat16Vector, typedcolumn.ColumnTypeBFloat16Vector, typedcolumn.EncodingRawBFloat16Vector, 2, 2), Certification: certColumn, Rows: 2, PayloadBytes: certColumn.Section.Length, AssetOffset: 0, HasAssetOffset: true})
	if bad.Reason != typeddecode.ReasonDimensionMismatch {
		t.Fatalf("wrong elements_per_row status=%+v want %s", bad, typeddecode.ReasonDimensionMismatch)
	}
}

func TestTypedColumnDenseNumericVectorJSONAndAdjacencyIsolation1930(t *testing.T) {
	byteCol := ColumnStoreColumn{Name: "byte_codes", Path: "byte_codes", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueUint8Vector, ElementsPerRow: 3}
	byteValue, err := convertColumnDeclaredValue(byteCol, []any{jsonNumber1930("1"), jsonNumber1930("2"), jsonNumber1930("255")}, true)
	if err != nil {
		t.Fatalf("convertColumnDeclaredValue uint8_vector: %v", err)
	}
	byteJSON, err := columnDeclaredValueToJSON(byteValue)
	if err != nil {
		t.Fatalf("columnDeclaredValueToJSON uint8_vector: %v", err)
	}
	gotBytes, ok := byteJSON.([]int)
	if !ok || len(gotBytes) != 3 || gotBytes[2] != 255 {
		t.Fatalf("uint8_vector json=%T %[1]v want numeric array, not base64 bytes", byteJSON)
	}

	col := ColumnStoreColumn{Name: "codes", Path: "codes", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueUint16Vector, ElementsPerRow: 3}
	value, err := convertColumnDeclaredValue(col, []any{jsonNumber1930("1"), jsonNumber1930("513"), jsonNumber1930("65535")}, true)
	if err != nil {
		t.Fatalf("convertColumnDeclaredValue uint16_vector: %v", err)
	}
	wantRaw := []byte{1, 0, 1, 2, 0xff, 0xff}
	if !bytes.Equal(value.DenseNumericVector, wantRaw) {
		t.Fatalf("uint16_vector raw=%x want little-endian %x", value.DenseNumericVector, wantRaw)
	}
	jsonValue, err := columnDeclaredValueToJSON(value)
	if err != nil {
		t.Fatalf("columnDeclaredValueToJSON: %v", err)
	}
	gotUint16, ok := jsonValue.([]uint16)
	if !ok || len(gotUint16) != 3 || gotUint16[1] != 513 || gotUint16[2] != 65535 {
		t.Fatalf("json value=%T %[1]v want []uint16", jsonValue)
	}

	floatCol := ColumnStoreColumn{Name: "scores", Path: "scores", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat64Vector, ElementsPerRow: 2}
	floatValue, err := convertColumnDeclaredValue(floatCol, []any{jsonNumber1930("1.5"), jsonNumber1930("-2.25")}, true)
	if err != nil {
		t.Fatalf("convertColumnDeclaredValue float64_vector: %v", err)
	}
	if binary.LittleEndian.Uint64(floatValue.DenseNumericVector[:8]) != math.Float64bits(1.5) || binary.LittleEndian.Uint64(floatValue.DenseNumericVector[8:]) != math.Float64bits(-2.25) {
		t.Fatalf("float64_vector raw=%x not little-endian float64 bits", floatValue.DenseNumericVector)
	}

	uint32Field := typedColumnAdapterField("ids", ColumnStoreValueUint32Vector)
	uint32Field.ElementsPerRow = 3
	uint32Column, err := typedColumnAdapterMapField(uint32Field)
	if err != nil {
		t.Fatalf("typedColumnAdapterMapField uint32_vector: %v", err)
	}
	if uint32Column.Definition.Type != typedcolumn.ColumnTypeUint32Vector || uint32Column.Definition.Encoding != typedcolumn.EncodingRawUint32Vector {
		t.Fatalf("uint32_vector definition=%+v want generic dense vector", uint32Column.Definition)
	}
	if cap, err := typedColumnAdapterCapability(uint32Column, columnsemantics.OpAdjacencyTraversal); err != nil || cap.Status == columnsemantics.StatusSupported {
		t.Fatalf("uint32_vector adjacency capability=%+v err=%v want unsupported adjacency semantics", cap, err)
	}
	if columnStoreValueTypeIsDenseNumericVector(ColumnStoreValueAdjacencyList) {
		t.Fatal("adjacency_list must not be classified as a generic dense numeric vector")
	}
}

func denseNumericVectorRowBytes1930(elements, width, seed int) []byte {
	out := make([]byte, elements*width)
	for i := range out {
		out[i] = byte(seed + i)
	}
	return out
}

func jsonNumber1930(s string) any { return json.Number(s) }
