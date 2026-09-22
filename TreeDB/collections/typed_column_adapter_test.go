package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

var typedColumnNullableBenchSink1784 int64

func TestTimeOrderTopKAllowedCodesNullableEmptyOnlyNoDictionary3175(t *testing.T) {
	column := typedColumnAdapterColumn{
		Field:      typedColumnAdapterNullableField("maybe_kind", "string"),
		Definition: typedcolumn.ColumnDefinition{Name: "maybe_kind", Type: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingNullableInt64, Cardinality: 2},
	}
	allowed, missingMatchesEmpty, rejectsAll, err := timeOrderTopKAllowedCodes(column, 2, columnPhysicalQueryPredicateSpec{
		column: "maybe_kind",
		values: []string{""},
	})
	if err != nil {
		t.Fatalf("nullable empty-only allowed codes: %v", err)
	}
	if !missingMatchesEmpty || rejectsAll {
		t.Fatalf("missingMatchesEmpty=%v rejectsAll=%v want missing match without reject-all", missingMatchesEmpty, rejectsAll)
	}
	if len(allowed) != 1 || allowed[0] != 0 {
		t.Fatalf("allowed=%v want no concrete dictionary codes", allowed)
	}

	if _, _, _, err := timeOrderTopKAllowedCodes(column, 2, columnPhysicalQueryPredicateSpec{
		column: "maybe_kind",
		values: []string{"app.bsky.feed.post"},
	}); err == nil || !strings.Contains(err.Error(), "missing forward dictionary") {
		t.Fatalf("non-empty predicate without dictionary err=%v want missing forward dictionary", err)
	}
}

func TestTypedColumnAdapterMapsTreeDBDeclaredTypes(t *testing.T) {
	want := map[ColumnStoreValueType]typedColumnAdapterTypeStatus{
		ColumnStoreValueBool:          typedColumnAdapterRepresented,
		ColumnStoreValueInt64:         typedColumnAdapterRepresented,
		ColumnStoreValueFloat32:       typedColumnAdapterRepresented,
		ColumnStoreValueDouble:        typedColumnAdapterRepresented,
		ColumnStoreValueString:        typedColumnAdapterRepresented,
		ColumnStoreValueFloat32Vector: typedColumnAdapterRepresented,
		ColumnStoreValueUint32List:    typedColumnAdapterRepresented,
		ColumnStoreValueBytes:         typedColumnAdapterRepresented,
		ColumnStoreValueAdjacencyList: typedColumnAdapterRepresented,
	}
	got := make(map[ColumnStoreValueType]typedColumnAdapterTypeStatus)
	for _, mapping := range typedColumnAdapterTypeMatrix() {
		got[mapping.ValueType] = mapping.Status
	}
	for valueType, status := range want {
		if got[valueType] != status {
			t.Fatalf("value type %s status=%s want %s matrix=%+v", valueType, got[valueType], status, got)
		}
	}
	field := typedColumnAdapterField("score", ColumnStoreValueFloat32)
	column, err := typedColumnAdapterMapField(field)
	if err != nil {
		t.Fatalf("typedColumnAdapterMapField(float32): %v", err)
	}
	if column.Definition.Type != typedcolumn.ColumnTypeInt64 || column.Definition.Encoding != typedcolumn.EncodingRawInt64 {
		t.Fatalf("float32 mapping definition=%+v", column.Definition)
	}
}

func TestTypedColumnAdapterMapFieldSelectsNativeScalarFixedWidthEncoding(t *testing.T) {
	for _, tc := range []struct {
		name      string
		valueType ColumnStoreValueType
		wantType  typedcolumn.ColumnType
		wantEnc   typedcolumn.Encoding
	}{
		{name: "int64", valueType: ColumnStoreValueInt64, wantType: typedcolumn.ColumnTypeInt64, wantEnc: typedcolumn.EncodingRawInt64},
		{name: "float32", valueType: ColumnStoreValueFloat32, wantType: typedcolumn.ColumnTypeFloat32, wantEnc: typedcolumn.EncodingRawFloat32},
		{name: "double", valueType: ColumnStoreValueDouble, wantType: typedcolumn.ColumnTypeFloat64, wantEnc: typedcolumn.EncodingRawFloat64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := typedColumnAdapterField(tc.name, tc.valueType)
			field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
			col, err := typedColumnAdapterMapField(field)
			if err != nil {
				t.Fatalf("typedColumnAdapterMapField: %v", err)
			}
			if col.Definition.Type != tc.wantType || col.Definition.Encoding != tc.wantEnc || col.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				t.Fatalf("column=%+v want type=%s encoding=%s fixed_width=%s", col, tc.wantType, tc.wantEnc, ColumnFixedWidthEncodingLittleEndian)
			}
		})
	}
}

func TestTypedColumnAdapterSortKeyRejectsEngineCap1948(t *testing.T) {
	columns := make([]typedColumnAdapterColumn, typedColumnPartSortKeyMaxColumns+1)
	sortKeys := make([]ColumnSortKey, len(columns))
	for i := range columns {
		name := fmt.Sprintf("c%02d", i)
		column, err := typedColumnAdapterMapField(typedColumnAdapterField(name, ColumnStoreValueInt64))
		if err != nil {
			t.Fatalf("typedColumnAdapterMapField(%s): %v", name, err)
		}
		columns[i] = column
		sortKeys[i] = ColumnSortKey{Column: name, Direction: ColumnSortAscending}
	}
	_, err := typedColumnAdapterSortKey(typedColumnAdapterOptions{SortKey: sortKeys}, columns)
	if err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("typedColumnAdapterSortKey oversized err=%v want exceeds cap", err)
	}
}

func TestTypedColumnAdapterMapFieldRejectsInvalidScalarFixedWidthEncoding(t *testing.T) {
	field := typedColumnAdapterField("score", ColumnStoreValueFloat32)
	field.FixedWidthEncoding = ColumnFixedWidthEncoding("future")
	if _, err := typedColumnAdapterMapField(field); err == nil || !strings.Contains(err.Error(), "unsupported float32 fixed_width_encoding") {
		t.Fatalf("typedColumnAdapterMapField unsupported err=%v want fixed_width_encoding", err)
	}
	field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	field.Nullable = true
	if _, err := typedColumnAdapterMapField(field); err == nil || !strings.Contains(err.Error(), "nullable float32 raw fixed-width encoding is unsupported") {
		t.Fatalf("typedColumnAdapterMapField nullable err=%v want nullable rejection", err)
	}
}

func TestTypedColumnAdapterMapFieldPreservesVectorAndAdjacencyFixedWidthEncoding(t *testing.T) {
	for _, tc := range []struct {
		name      string
		valueType ColumnStoreValueType
		dims      int
		degree    int
		wantType  typedcolumn.ColumnType
		wantEnc   typedcolumn.Encoding
	}{
		{name: "vector", valueType: ColumnStoreValueFloat32Vector, dims: 3, wantType: typedcolumn.ColumnTypeFloat32Vector, wantEnc: typedcolumn.EncodingRawFloat32Vector},
		{name: "adjacency", valueType: ColumnStoreValueAdjacencyList, degree: 4, wantType: typedcolumn.ColumnTypeAdjacencyList, wantEnc: typedcolumn.EncodingRawUint32Dense},
	} {
		t.Run(tc.name, func(t *testing.T) {
			col, err := typedColumnAdapterMapField(TypedStorageField{Name: tc.name, Path: tc.name, Owner: TypedStorageOwnerColumnPart, ValueType: tc.valueType, VectorDims: tc.dims, AdjacencyDegree: tc.degree, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian})
			if err != nil {
				t.Fatalf("typedColumnAdapterMapField: %v", err)
			}
			if col.Definition.Type != tc.wantType || col.Definition.Encoding != tc.wantEnc || col.FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
				t.Fatalf("column=%+v want type=%s encoding=%s fixed_width=%s", col, tc.wantType, tc.wantEnc, ColumnFixedWidthEncodingLittleEndian)
			}
		})
	}
}

func TestTypedColumnAdapterRoundTripBool(t *testing.T) {
	got := typedColumnAdapterRoundTrip(t, typedColumnAdapterField("flag", ColumnStoreValueBool), []columnDeclaredValue{
		{Type: ColumnStoreValueBool, Present: true, Bool: true},
		{Type: ColumnStoreValueBool, Present: true, Bool: false},
		{Type: ColumnStoreValueBool, Present: true, Bool: true},
	})
	if !got[0].Bool || got[1].Bool || !got[2].Bool {
		t.Fatalf("bool round trip=%+v", got)
	}
}

func TestTypedColumnAdapterRoundTripInt64(t *testing.T) {
	want := []int64{-7, 0, 99}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Int64: v}
	}
	got := typedColumnAdapterRoundTrip(t, typedColumnAdapterField("count", ColumnStoreValueInt64), values)
	for i := range want {
		if got[i].Int64 != want[i] {
			t.Fatalf("int64[%d]=%d want %d all=%+v", i, got[i].Int64, want[i], got)
		}
	}
}

func TestTypedColumnAdapterRoundTripFloat32(t *testing.T) {
	want := []float32{-1.25, 0, 3.5}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueFloat32, Present: true, Float32: v}
	}
	got := typedColumnAdapterRoundTrip(t, typedColumnAdapterField("score", ColumnStoreValueFloat32), values)
	for i := range want {
		if math.Float32bits(got[i].Float32) != math.Float32bits(want[i]) {
			t.Fatalf("float32[%d]=%v want %v all=%+v", i, got[i].Float32, want[i], got)
		}
	}
}

func TestTypedColumnAdapterRoundTripFloat64(t *testing.T) {
	want := []float64{-1.25, 0, 3.5}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueDouble, Present: true, Double: v}
	}
	got := typedColumnAdapterRoundTrip(t, typedColumnAdapterField("ratio", ColumnStoreValueDouble), values)
	for i := range want {
		if math.Float64bits(got[i].Double) != math.Float64bits(want[i]) {
			t.Fatalf("float64[%d]=%v want %v all=%+v", i, got[i].Double, want[i], got)
		}
	}
}

func TestTypedColumnAdapterRoundTripNativeFixedWidthScalars(t *testing.T) {
	t.Run("float32 raw bits", func(t *testing.T) {
		wantBits := []uint32{0x00000000, 0x80000000, 0x7f800000, 0xff800000, 0x7f7fffff, 0xff7fffff, 0x7fc12345, 0x7fa12345}
		values := make([]columnDeclaredValue, len(wantBits))
		for i, bits := range wantBits {
			values[i] = columnDeclaredValue{Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(bits)}
		}
		field := typedColumnAdapterField("score", ColumnStoreValueFloat32)
		field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
		got := typedColumnAdapterRoundTrip(t, field, values)
		for i, want := range wantBits {
			if gotBits := math.Float32bits(got[i].Float32); gotBits != want {
				t.Fatalf("float32[%d] bits=0x%08x want 0x%08x all=%+v", i, gotBits, want, got)
			}
		}
	})
	t.Run("double raw bits", func(t *testing.T) {
		wantBits := []uint64{0x0000000000000000, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7fefffffffffffff, 0xffefffffffffffff, 0x7ff8000000000042, 0x7ff0000000000042}
		values := make([]columnDeclaredValue, len(wantBits))
		for i, bits := range wantBits {
			values[i] = columnDeclaredValue{Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(bits)}
		}
		field := typedColumnAdapterField("ratio", ColumnStoreValueDouble)
		field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
		got := typedColumnAdapterRoundTrip(t, field, values)
		for i, want := range wantBits {
			if gotBits := math.Float64bits(got[i].Double); gotBits != want {
				t.Fatalf("double[%d] bits=0x%016x want 0x%016x all=%+v", i, gotBits, want, got)
			}
		}
	})
}

func TestTypedColumnAdapterNativeFixedWidthScalarByteFixtures(t *testing.T) {
	f32Field := typedColumnAdapterField("score", ColumnStoreValueFloat32)
	f32Field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	f32Part := typedColumnAdapterBuildPart(t, f32Field, []columnDeclaredValue{
		{Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x7fc12345)},
		{Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x80000000)},
	})
	f32Image, err := f32Part.buildImage()
	if err != nil {
		t.Fatalf("buildImage float32: %v", err)
	}
	f32Section := typedColumnAdapterFindColumnSection(t, f32Image, "score")
	f32Raw := f32Image.Bytes[f32Section.Offset : f32Section.Offset+f32Section.Length]
	wantF32 := []byte{0x45, 0x23, 0xc1, 0x7f, 0x00, 0x00, 0x00, 0x80}
	if !bytes.Equal(f32Raw, wantF32) {
		t.Fatalf("native float32 bytes=% x want % x", f32Raw, wantF32)
	}
	f32Cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(f32Image)
	if err != nil {
		t.Fatalf("certify float32 image: %v", err)
	}
	assertTypedColumnAdapterNativeScalarDirectViewContract(t, f32Cert, f32Section, "score", "float32", typedcolumn.ColumnTypeFloat32, typedcolumn.EncodingRawFloat32, 4, 4, len(wantF32))
	f32Path := filepath.Join(t.TempDir(), "f32.tcs1")
	if err := os.WriteFile(f32Path, f32Image.Bytes, 0o600); err != nil {
		t.Fatalf("write float32 image: %v", err)
	}
	f32Mgr := mappedresource.NewManager()
	f32Reader := typedColumnAdapterResourceReader{Manager: f32Mgr, Image: f32Image, Path: f32Path, Namespace: "typed-column-adapter-f32", PartID: f32Image.PartID, PreferMapped: true, AllowHeapCopy: true}
	f32Column, ok := f32Part.columnByName("score")
	if !ok {
		t.Fatalf("missing float32 adapter column")
	}
	f32View, err := typedColumnAdapterAcquireFloat32ScalarColumnView(f32Reader, f32Column, f32Image.Rows)
	if err != nil {
		t.Fatalf("AcquireFloat32ScalarColumnView: %v", err)
	}
	if len(f32View.Values) != 2 {
		t.Fatalf("float32 view len=%d want 2", len(f32View.Values))
	}
	for i, want := range []uint32{0x7fc12345, 0x80000000} {
		if got := math.Float32bits(f32View.Values[i]); got != want {
			t.Fatalf("float32 direct view bits[%d]=0x%08x want 0x%08x", i, got, want)
		}
	}
	if typedColumnInt64DirectViewSupportedForTest() {
		if !f32View.Direct || f32View.Handle == nil {
			t.Fatalf("float32 view=%+v want mapped direct view", f32View)
		}
		if err := f32View.Handle.Release(); err != nil {
			t.Fatalf("release float32 view: %v", err)
		}
	} else if f32View.Direct {
		t.Fatalf("float32 view direct on unsupported platform: %+v", f32View)
	}
	assertTypedColumnAdapterNoActive(t, f32Mgr)

	f64Field := typedColumnAdapterField("ratio", ColumnStoreValueDouble)
	f64Field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	f64Part := typedColumnAdapterBuildPart(t, f64Field, []columnDeclaredValue{
		{Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x7ff8000000000042)},
		{Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x8000000000000000)},
	})
	f64Image, err := f64Part.buildImage()
	if err != nil {
		t.Fatalf("buildImage double: %v", err)
	}
	f64Section := typedColumnAdapterFindColumnSection(t, f64Image, "ratio")
	f64Raw := f64Image.Bytes[f64Section.Offset : f64Section.Offset+f64Section.Length]
	wantF64 := []byte{0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}
	if !bytes.Equal(f64Raw, wantF64) {
		t.Fatalf("native double bytes=% x want % x", f64Raw, wantF64)
	}
	f64Cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(f64Image)
	if err != nil {
		t.Fatalf("certify double image: %v", err)
	}
	assertTypedColumnAdapterNativeScalarDirectViewContract(t, f64Cert, f64Section, "ratio", "double", typedcolumn.ColumnTypeFloat64, typedcolumn.EncodingRawFloat64, 8, 8, len(wantF64))
	f64Path := filepath.Join(t.TempDir(), "f64.tcs1")
	if err := os.WriteFile(f64Path, f64Image.Bytes, 0o600); err != nil {
		t.Fatalf("write float64 image: %v", err)
	}
	f64Mgr := mappedresource.NewManager()
	f64Reader := typedColumnAdapterResourceReader{Manager: f64Mgr, Image: f64Image, Path: f64Path, Namespace: "typed-column-adapter-f64", PartID: f64Image.PartID, PreferMapped: true, AllowHeapCopy: true}
	f64Column, ok := f64Part.columnByName("ratio")
	if !ok {
		t.Fatalf("missing float64 adapter column")
	}
	f64View, err := typedColumnAdapterAcquireFloat64ScalarColumnView(f64Reader, f64Column, f64Image.Rows)
	if err != nil {
		t.Fatalf("AcquireFloat64ScalarColumnView: %v", err)
	}
	if len(f64View.Values) != 2 {
		t.Fatalf("float64 view len=%d want 2", len(f64View.Values))
	}
	for i, want := range []uint64{0x7ff8000000000042, 0x8000000000000000} {
		if got := math.Float64bits(f64View.Values[i]); got != want {
			t.Fatalf("float64 direct view bits[%d]=0x%016x want 0x%016x", i, got, want)
		}
	}
	if typedColumnInt64DirectViewSupportedForTest() {
		if !f64View.Direct || f64View.Handle == nil {
			t.Fatalf("float64 view=%+v want mapped direct view", f64View)
		}
		if err := f64View.Handle.Release(); err != nil {
			t.Fatalf("release float64 view: %v", err)
		}
	} else if f64View.Direct {
		t.Fatalf("float64 view direct on unsupported platform: %+v", f64View)
	}
	assertTypedColumnAdapterNoActive(t, f64Mgr)
}

func assertTypedColumnAdapterNativeScalarDirectViewContract(t *testing.T, cert typedcolumn.ColumnPartLayoutCertification, section typedcolumn.ColumnPartImageSection, name string, logicalType string, columnType typedcolumn.ColumnType, encoding typedcolumn.Encoding, elementSize int, alignment int, payloadLength int) {
	t.Helper()
	column, ok := cert.Column(name)
	if !ok {
		t.Fatalf("missing certified column %q", name)
	}
	if !column.DirectViewCertified || column.LogicalType != logicalType || column.Type != columnType || column.Encoding != encoding || column.Compression != typedcolumn.CompressionNone {
		t.Fatalf("column %q contract=%+v want native scalar direct-view identity (%q,%s,%s)", name, column, logicalType, columnType, encoding)
	}
	if column.Section.Offset != section.Offset || column.Section.Length != section.Length || column.Section.Length != payloadLength || column.ElementSize != elementSize || column.Alignment != alignment || column.Endian != typedcolumn.ColumnPartLayoutEndianLittle || column.LengthMultiple != elementSize || column.FixedWidthElements != 0 || column.Rows != section.Rows {
		t.Fatalf("column %q section/layout=%+v image_section=%+v want element=%d align=%d payload=%d", name, column, section, elementSize, alignment, payloadLength)
	}
	if section.Offset%alignment != 0 || section.Length%elementSize != 0 || column.NullMaskPresent || column.DefaultMaskPresent || column.NullCount != 0 || column.DefaultCount != 0 {
		t.Fatalf("column %q direct-view invariants contract=%+v image_section=%+v", name, column, section)
	}
	if len(column.Blocks) != 1 {
		t.Fatalf("column %q blocks=%d want 1", name, len(column.Blocks))
	}
	block := column.Blocks[0]
	if block.FirstRow != 0 || block.RowCount != section.Rows || block.Encoding != encoding || block.Compression != typedcolumn.CompressionNone || block.RawBytes != payloadLength || block.StoredBytes != payloadLength || block.PayloadOffset != section.Offset || block.PayloadLength != payloadLength || block.NullCount != 0 || block.DefaultCount != 0 {
		t.Fatalf("column %q block=%+v section=%+v want exact native scalar payload length=%d", name, block, section, payloadLength)
	}
}

func TestTypedColumnAdapterRoundTripFloat32Vector(t *testing.T) {
	want := [][]float32{{1, 0.5, -0.25}, {2, 3, 4}}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: v}
	}
	field := typedColumnAdapterField("embedding", ColumnStoreValueFloat32Vector)
	field.VectorDims = 3
	got := typedColumnAdapterRoundTrip(t, field, values)
	for i := range want {
		if !slices.Equal(got[i].Float32Vector, want[i]) {
			t.Fatalf("vector[%d]=%v want %v all=%+v", i, got[i].Float32Vector, want[i], got)
		}
	}
}

func TestTypedColumnAdapterRoundTripUint32List1985(t *testing.T) {
	want := [][]uint32{{}, {7, 8}, {}, {9, 10, 11}}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueUint32List, Present: true, Uint32List: v}
	}
	field := typedColumnAdapterField("tags", ColumnStoreValueUint32List)
	column, err := typedColumnAdapterMapField(field)
	if err != nil {
		t.Fatalf("typedColumnAdapterMapField(uint32_list): %v", err)
	}
	if column.Definition.Type != typedcolumn.ColumnTypeUint32List || column.Definition.Encoding != typedcolumn.EncodingRawUint32OffsetsList || column.Definition.FixedWidthElements != 0 {
		t.Fatalf("uint32_list mapping=%+v", column.Definition)
	}
	got := typedColumnAdapterRoundTrip(t, field, values)
	for i := range want {
		if !slices.Equal(got[i].Uint32List, want[i]) {
			t.Fatalf("uint32_list[%d]=%v want %v all=%+v", i, got[i].Uint32List, want[i], got)
		}
		if len(got[i].AdjacencyList) != 0 {
			t.Fatalf("uint32_list[%d] populated legacy adjacency field: %+v", i, got[i])
		}
	}

	bad := field
	bad.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	if _, err := typedColumnAdapterMapField(bad); err == nil || !strings.Contains(err.Error(), "uint32_list must not set adjacency_layout") {
		t.Fatalf("uint32_list adjacency_layout err=%v want generic admission rejection", err)
	}
}

func TestTypedColumnAdapterRoundTripBytes2010(t *testing.T) {
	want := [][]byte{{}, {0x00, 'A', 0xff}, {0xfe, 0x80}}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueBytes, Present: true, Bytes: v}
	}
	field := typedColumnAdapterField("opaque", ColumnStoreValueBytes)
	part := typedColumnAdapterBuildPart(t, field, values)
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	certColumn, ok := cert.Column("opaque")
	if !ok {
		t.Fatal("missing bytes certification")
	}
	if certColumn.LogicalType != string(columnsemantics.LogicalBytes) || certColumn.Type != typedcolumn.ColumnTypeBytes || certColumn.Encoding != typedcolumn.EncodingRawBytesOffsets || !certColumn.DirectViewCertified {
		t.Fatalf("bytes certification=%+v want logical bytes/raw_bytes_offsets direct-view", certColumn)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	got, err := parsed.scanColumnValues(field.Name)
	if err != nil {
		t.Fatalf("scanColumnValues(%s): %v", field.Name, err)
	}
	for i := range want {
		if !bytes.Equal(got[i].Bytes, want[i]) {
			t.Fatalf("bytes[%d]=%v want %v all=%+v", i, got[i].Bytes, want[i], got)
		}
	}
}

func TestTypedColumnAdapterRoundTripAdjacencyList(t *testing.T) {
	want := [][]uint32{{1, 2, 3}, {4, 5, 6}}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: v}
	}
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyDegree = 3
	got := typedColumnAdapterRoundTrip(t, field, values)
	for i := range want {
		if !slices.Equal(got[i].AdjacencyList, want[i]) {
			t.Fatalf("adjacency[%d]=%v want %v all=%+v", i, got[i].AdjacencyList, want[i], got)
		}
	}
}

func TestTypedColumnAdapterRoundTripAdjacencyOffsetsList1915(t *testing.T) {
	want := [][]uint32{{}, {7, 8}, {}, {9, 10, 11}}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: v}
	}
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	field.AdjacencyDegree = 0
	got := typedColumnAdapterRoundTrip(t, field, values)
	for i := range want {
		if !slices.Equal(got[i].AdjacencyList, want[i]) {
			t.Fatalf("offsets-list adjacency[%d]=%v want %v all=%+v", i, got[i].AdjacencyList, want[i], got)
		}
	}
}

func TestTypedColumnAdapterMixedDenseAndOffsetsListAdjacency1917(t *testing.T) {
	dense := typedColumnAdapterField("neighbors_dense", ColumnStoreValueAdjacencyList)
	dense.AdjacencyDegree = 2
	offsets := typedColumnAdapterField("neighbors_variable", ColumnStoreValueAdjacencyList)
	offsets.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	rows := []typedColumnAdapterRow{
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{
			"neighbors_dense":    {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{1, 2}},
			"neighbors_variable": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: nil},
		}},
		{PrimaryID: 2, Values: map[string]columnDeclaredValue{
			"neighbors_dense":    {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{3, 4}},
			"neighbors_variable": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{7}},
		}},
		{PrimaryID: 3, Values: map[string]columnDeclaredValue{
			"neighbors_dense":    {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{5, 6}},
			"neighbors_variable": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{8, 9, 10}},
		}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1917, RowsPerGranule: 2, Fields: []TypedStorageField{dense, offsets}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	gotRows, err := parsed.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	for i := range rows {
		if !slices.Equal(gotRows[i].Values["neighbors_dense"].AdjacencyList, rows[i].Values["neighbors_dense"].AdjacencyList) || !slices.Equal(gotRows[i].Values["neighbors_variable"].AdjacencyList, rows[i].Values["neighbors_variable"].AdjacencyList) {
			t.Fatalf("row[%d]=%+v want %+v", i, gotRows[i].Values, rows[i].Values)
		}
	}
}

func TestTypedColumnAdapterUint32OffsetsListDirectViewReader1916(t *testing.T) {
	part, column, image, offsetsSection, valuesSection, offsetsRaw, valuesRaw := typedColumnAdapterOffsetsListDirectFixture(t)
	_ = part
	path := filepath.Join(t.TempDir(), "offsets-list.tcs1")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	mgr := mappedresource.NewManager()
	reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-adapter-offsets-list", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: false}
	view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(reader, column, image.Rows)
	if err != nil {
		if strings.Contains(err.Error(), "mmap unsupported") {
			t.Skipf("mmap direct view unsupported on this platform: %v", err)
		}
		t.Fatalf("AcquireUint32OffsetsListColumnView: %v", err)
	}
	if !slices.Equal(view.Offsets, []uint64{0, 0, 2, 2, 5}) || !slices.Equal(view.Values, []uint32{7, 8, 9, 10, 11}) {
		_ = view.Close()
		t.Fatalf("offsets=%v values=%v", view.Offsets, view.Values)
	}
	if !view.Direct || view.HeapCopy || view.Scratch || view.OffsetsHandle == nil || view.ValuesHandle == nil || view.Class.Class != typedColumnAdapterUint32OffsetsListViewMmapDirect {
		_ = view.Close()
		if view.HeapCopy || view.Scratch {
			t.Skipf("mmap direct view unavailable; fallback class=%+v", view.Class)
		}
		t.Fatalf("view=%+v want mmap direct with pinned handles", view)
	}
	if len(offsetsRaw) != offsetsSection.Length || len(valuesRaw) != valuesSection.Length {
		t.Fatalf("fixture raw lengths offsets=%d/%d values=%d/%d", len(offsetsRaw), offsetsSection.Length, len(valuesRaw), valuesSection.Length)
	}
	if stats := mgr.Stats(); stats.ActiveHandles != 2 || stats.DirectViewSuccesses != 2 || stats.DirectViewFailures != 0 {
		_ = view.Close()
		t.Fatalf("stats=%+v want two active mmap direct handles", stats)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if stats := mgr.Stats(); stats.TotalReleases != 2 {
		t.Fatalf("stats=%+v want exactly two releases after idempotent Close", stats)
	}
	assertTypedColumnAdapterNoActive(t, mgr)
}

func TestTypedColumnAdapterUint32ListDirectViewReader1985(t *testing.T) {
	_, column, image, offsetsSection, valuesSection, offsetsRaw, valuesRaw := typedColumnAdapterUint32ListDirectFixture(t)
	path := filepath.Join(t.TempDir(), "uint32-list.tcs1")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	certColumn, ok := certification.Column("tags")
	if !ok {
		t.Fatal("missing tags certification")
	}
	if certColumn.LogicalType != string(columnsemantics.LogicalUint32List) || certColumn.Type != typedcolumn.ColumnTypeUint32List || certColumn.Encoding != typedcolumn.EncodingRawUint32OffsetsList {
		t.Fatalf("certified uint32_list column=%+v", certColumn)
	}
	mgr := mappedresource.NewManager()
	reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-adapter-uint32-list", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: false}
	view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(reader, column, image.Rows)
	if err != nil {
		if strings.Contains(err.Error(), "mmap unsupported") {
			t.Skipf("mmap direct view unsupported on this platform: %v", err)
		}
		t.Fatalf("AcquireUint32OffsetsListColumnView: %v", err)
	}
	if !slices.Equal(view.Offsets, []uint64{0, 0, 2, 2, 5}) || !slices.Equal(view.Values, []uint32{7, 8, 9, 10, 11}) {
		_ = view.Close()
		t.Fatalf("offsets=%v values=%v", view.Offsets, view.Values)
	}
	if !view.Direct || view.HeapCopy || view.Scratch || view.OffsetsHandle == nil || view.ValuesHandle == nil || view.Class.Class != typedColumnAdapterUint32OffsetsListViewMmapDirect {
		_ = view.Close()
		if view.HeapCopy || view.Scratch {
			t.Skipf("mmap direct view unavailable; fallback class=%+v", view.Class)
		}
		t.Fatalf("view=%+v want mmap direct with pinned handles", view)
	}
	if len(offsetsRaw) != offsetsSection.Length || len(valuesRaw) != valuesSection.Length {
		t.Fatalf("fixture raw lengths offsets=%d/%d values=%d/%d", len(offsetsRaw), offsetsSection.Length, len(valuesRaw), valuesSection.Length)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertTypedColumnAdapterNoActive(t, mgr)
}

func TestTypedColumnAdapterUint32ListScratchFallbackAndCorruption1985(t *testing.T) {
	_, _, image, offsetsSection, valuesSection, offsetsRaw, valuesRaw := typedColumnAdapterUint32ListDirectFixture(t)
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	base, ok := certification.Column("tags")
	if !ok {
		t.Fatal("missing tags certification")
	}
	alignedOffsets := typedColumnAdapterAlignedCopy(offsetsRaw, int(unsafe.Alignof(uint64(0))))
	alignedValues := typedColumnAdapterAlignedCopy(valuesRaw, int(unsafe.Alignof(uint32(0))))

	t.Run("mixed handles scratch fallback", func(t *testing.T) {
		mgr := mappedresource.NewManager()
		offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedOffsets, mappedresource.SourceMapped, "offsets")
		valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceHeapCopy, "values")
		view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
		if err != nil || !view.Scratch || view.Direct || view.HeapCopy || view.Class.Class != typedColumnAdapterUint32OffsetsListViewSourceUnsupported {
			t.Fatalf("view=%+v err=%v want source-unsupported scratch fallback", view, err)
		}
		if !slices.Equal(view.Offsets, []uint64{0, 0, 2, 2, 5}) || !slices.Equal(view.Values, []uint32{7, 8, 9, 10, 11}) {
			t.Fatalf("offsets=%v values=%v", view.Offsets, view.Values)
		}
		_ = offsetsHandle.Release()
		_ = valuesHandle.Release()
		assertTypedColumnAdapterNoActive(t, mgr)
	})

	t.Run("corrupt final offset fails closed", func(t *testing.T) {
		mgr := mappedresource.NewManager()
		badOffsets := typedColumnAdapterAlignedCopy(typedColumnAdapterMustOffsetsListOffsets(t, []uint64{0, 0, 2, 2, 6}), int(unsafe.Alignof(uint64(0))))
		offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, badOffsets, mappedresource.SourceMapped, "offsets")
		valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceMapped, "values")
		view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
		if err == nil || view.Class.Class != typedColumnAdapterUint32OffsetsListViewValidationFailure {
			t.Fatalf("view=%+v err=%v want validation failure", view, err)
		}
		_ = offsetsHandle.Release()
		_ = valuesHandle.Release()
		assertTypedColumnAdapterNoActive(t, mgr)
	})
}

func TestTypedColumnAdapterUint32OffsetsListDuplicateSectionsFailClosed1916(t *testing.T) {
	_, _, image, offsetsSection, valuesSection, _, _ := typedColumnAdapterOffsetsListDirectFixture(t)
	for _, tc := range []struct {
		name    string
		section typedcolumn.ColumnPartImageSection
	}{
		{name: "offsets", section: offsetsSection},
		{name: "values", section: valuesSection},
	} {
		t.Run(tc.name, func(t *testing.T) {
			duplicate := image
			duplicate.Sections = append(append([]typedcolumn.ColumnPartImageSection(nil), image.Sections...), tc.section)
			if _, _, ok := typedColumnAdapterColumnOffsetsListSections(duplicate, "neighbors"); ok {
				t.Fatalf("duplicate %s offsets-list sections unexpectedly selected", tc.name)
			}
		})
	}
}

func TestTypedColumnAdapterUint32OffsetsListDirectViewClassifications1916(t *testing.T) {
	_, _, image, offsetsSection, valuesSection, offsetsRaw, valuesRaw := typedColumnAdapterOffsetsListDirectFixture(t)
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	base, ok := certification.Column("neighbors")
	if !ok {
		t.Fatal("missing neighbors certification")
	}
	alignedOffsets := typedColumnAdapterAlignedCopy(offsetsRaw, int(unsafe.Alignof(uint64(0))))
	alignedValues := typedColumnAdapterAlignedCopy(valuesRaw, int(unsafe.Alignof(uint32(0))))

	t.Run("heap copy typed view is not mmap direct", func(t *testing.T) {
		mgr := mappedresource.NewManager()
		offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedOffsets, mappedresource.SourceHeapCopy, "offsets")
		valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceHeapCopy, "values")
		view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
		if err != nil {
			t.Fatalf("open heap view: %v", err)
		}
		if view.Direct || !view.HeapCopy || view.Class.Class != typedColumnAdapterUint32OffsetsListViewHeapCopyTyped {
			_ = view.Close()
			t.Fatalf("view=%+v want heap-copy typed view classification", view)
		}
		if stats := mgr.Stats(); stats.DirectViewSuccesses != 0 || stats.DirectViewFailures != 0 {
			_ = view.Close()
			t.Fatalf("heap-copy typed view counted as mmap direct manager stats=%+v", stats)
		}
		if err := view.Close(); err != nil {
			t.Fatalf("Close heap view: %v", err)
		}
		assertTypedColumnAdapterNoActive(t, mgr)
	})

	t.Run("mixed mmap heap-copy handles fall back to scratch", func(t *testing.T) {
		mgr := mappedresource.NewManager()
		offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedOffsets, mappedresource.SourceMapped, "offsets")
		valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceHeapCopy, "values")
		view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
		if err != nil || !view.Scratch || view.Direct || view.HeapCopy || view.Class.Class != typedColumnAdapterUint32OffsetsListViewSourceUnsupported {
			t.Fatalf("view=%+v err=%v want source-unsupported scratch fallback", view, err)
		}
		if view.Class.Counter != typeddecode.CounterSourceUnsupported || !slices.Contains(view.Class.Counters, typeddecode.CounterScratchDecode) {
			t.Fatalf("class=%+v want source counter plus scratch counter", view.Class)
		}
		if !slices.Equal(view.Offsets, []uint64{0, 0, 2, 2, 5}) || !slices.Equal(view.Values, []uint32{7, 8, 9, 10, 11}) {
			t.Fatalf("offsets=%v values=%v", view.Offsets, view.Values)
		}
		_ = offsetsHandle.Release()
		_ = valuesHandle.Release()
		assertTypedColumnAdapterNoActive(t, mgr)
	})

	for _, tc := range []struct {
		name          string
		offsetsSource mappedresource.Source
		valuesSource  mappedresource.Source
		class         typedColumnAdapterUint32OffsetsListViewClass
	}{
		{name: "source unsupported offsets", offsetsSource: mappedresource.SourceDerivedMetadata, valuesSource: mappedresource.SourceMapped, class: typedColumnAdapterUint32OffsetsListViewSourceUnsupported},
		{name: "source unsupported values", offsetsSource: mappedresource.SourceMapped, valuesSource: mappedresource.SourceDerivedMetadata, class: typedColumnAdapterUint32OffsetsListViewSourceUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := mappedresource.NewManager()
			offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedOffsets, tc.offsetsSource, "offsets")
			valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, tc.valuesSource, "values")
			view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
			if err == nil || view.Class.Class != tc.class {
				t.Fatalf("view=%+v err=%v want class=%s", view, err, tc.class)
			}
			_ = offsetsHandle.Release()
			_ = valuesHandle.Release()
			assertTypedColumnAdapterNoActive(t, mgr)
		})
	}

	for _, tc := range []struct {
		name            string
		misalignOffsets bool
		misalignValues  bool
	}{
		{name: "actual pointer unaligned offsets", misalignOffsets: true},
		{name: "actual pointer unaligned values", misalignValues: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := mappedresource.NewManager()
			offRaw := alignedOffsets
			valRaw := alignedValues
			if tc.misalignOffsets {
				offRaw = typedColumnAdapterMisalignedCopy(offsetsRaw, int(unsafe.Alignof(uint64(0))))
			}
			if tc.misalignValues {
				valRaw = typedColumnAdapterMisalignedCopy(valuesRaw, int(unsafe.Alignof(uint32(0))))
			}
			offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, offRaw, mappedresource.SourceMapped, "offsets")
			valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, valRaw, mappedresource.SourceMapped, "values")
			view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
			if err != nil || !view.Scratch || view.Class.Class != typedColumnAdapterUint32OffsetsListViewActualPointerUnaligned {
				t.Fatalf("view=%+v err=%v want scratch actual-pointer classification", view, err)
			}
			if view.Class.Counter != typeddecode.CounterActualPointerUnaligned || !slices.Contains(view.Class.Counters, typeddecode.CounterScratchDecode) {
				t.Fatalf("class=%+v want actual-pointer counter plus scratch counter", view.Class)
			}
			_ = offsetsHandle.Release()
			_ = valuesHandle.Release()
			assertTypedColumnAdapterNoActive(t, mgr)
		})
	}

	t.Run("nil handle classifies with lifetime failures", func(t *testing.T) {
		mgr := mappedresource.NewManager()
		valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceMapped, "values")
		view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, nil, valuesHandle)
		if err == nil || view.Class.Class != typedColumnAdapterUint32OffsetsListViewStaleHandle || view.Class.Counter != typeddecode.CounterStaleHandle {
			t.Fatalf("view=%+v err=%v want nil handle lifetime classification", view, err)
		}
		_ = valuesHandle.Release()
		assertTypedColumnAdapterNoActive(t, mgr)
	})

	for _, tc := range []struct {
		name           string
		releaseOffsets bool
		releaseValues  bool
	}{
		{name: "stale offsets", releaseOffsets: true},
		{name: "stale values", releaseValues: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := mappedresource.NewManager()
			offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedOffsets, mappedresource.SourceMapped, "offsets")
			valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceMapped, "values")
			if tc.releaseOffsets {
				_ = offsetsHandle.Release()
			}
			if tc.releaseValues {
				_ = valuesHandle.Release()
			}
			view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
			if err == nil || view.Class.Class != typedColumnAdapterUint32OffsetsListViewStaleHandle {
				t.Fatalf("view=%+v err=%v want stale classification", view, err)
			}
			_ = offsetsHandle.Release()
			_ = valuesHandle.Release()
			assertTypedColumnAdapterNoActive(t, mgr)
		})
	}
}

func TestTypedColumnAdapterUint32OffsetsListPartialOpenFailureCleansUp1916(t *testing.T) {
	_, column, image, _, valuesSection, _, _ := typedColumnAdapterOffsetsListDirectFixture(t)
	path := filepath.Join(t.TempDir(), "truncated-offsets-list.tcs1")
	if err := os.WriteFile(path, image.Bytes[:valuesSection.Offset], 0o600); err != nil {
		t.Fatalf("write truncated image: %v", err)
	}
	mgr := mappedresource.NewManager()
	reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-adapter-offsets-list-partial", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: true}
	view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(reader, column, image.Rows)
	if err == nil {
		_ = view.Close()
		t.Fatalf("AcquireUint32OffsetsListColumnView truncated values err=nil, want partial open failure")
	}
	if stats := mgr.Stats(); stats.ActiveHandles != 0 || stats.TotalAcquires != 1 || stats.TotalReleases != 1 {
		t.Fatalf("stats=%+v want offsets handle released exactly once after values acquire failure", stats)
	}
	assertTypedColumnAdapterNoActive(t, mgr)
}

func TestTypedColumnAdapterUint32OffsetsListCertificationAndShapeFailures1916(t *testing.T) {
	_, _, image, offsetsSection, valuesSection, offsetsRaw, valuesRaw := typedColumnAdapterOffsetsListDirectFixture(t)
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	base, ok := certification.Column("neighbors")
	if !ok {
		t.Fatal("missing neighbors certification")
	}
	alignedOffsets := typedColumnAdapterAlignedCopy(offsetsRaw, int(unsafe.Alignof(uint64(0))))
	alignedValues := typedColumnAdapterAlignedCopy(valuesRaw, int(unsafe.Alignof(uint32(0))))

	certCases := []struct {
		name  string
		edit  func(*typedcolumn.ColumnPartLayoutContractColumn)
		class typedColumnAdapterUint32OffsetsListViewClass
	}{
		{name: "missing certification", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.DirectViewCertified = false }, class: typedColumnAdapterUint32OffsetsListViewCertificationFailure},
		{name: "wrong logical", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.LogicalType = "int64" }, class: typedColumnAdapterUint32OffsetsListViewValidationFailure},
		{name: "wrong type", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.Type = typedcolumn.ColumnTypeInt64 }, class: typedColumnAdapterUint32OffsetsListViewValidationFailure},
		{name: "wrong encoding", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.Encoding = typedcolumn.EncodingRawUint32Dense }, class: typedColumnAdapterUint32OffsetsListViewValidationFailure},
		{name: "wrong endian", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) {
			c.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
		}, class: typedColumnAdapterUint32OffsetsListViewCertificationFailure},
	}
	for _, tc := range certCases {
		t.Run(tc.name, func(t *testing.T) {
			cert := base
			tc.edit(&cert)
			mgr := mappedresource.NewManager()
			offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedOffsets, mappedresource.SourceMapped, "offsets")
			valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceMapped, "values")
			view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, cert, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
			if tc.name == "missing certification" || tc.name == "wrong endian" {
				if err != nil || !view.Scratch || view.Class.Class != tc.class {
					t.Fatalf("view=%+v err=%v want scratch class=%s", view, err, tc.class)
				}
			} else if err == nil || view.Class.Class != tc.class {
				t.Fatalf("view=%+v err=%v want failure class=%s", view, err, tc.class)
			}
			_ = offsetsHandle.Release()
			_ = valuesHandle.Release()
			assertTypedColumnAdapterNoActive(t, mgr)
		})
	}

	t.Run("length count mismatch", func(t *testing.T) {
		mgr := mappedresource.NewManager()
		offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedOffsets, mappedresource.SourceMapped, "offsets")
		valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceMapped, "values")
		view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length-8, valuesSection.Length, offsetsHandle, valuesHandle)
		if err == nil || view.Class.Class != typedColumnAdapterUint32OffsetsListViewValidationFailure {
			t.Fatalf("view=%+v err=%v want length/count validation failure", view, err)
		}
		_ = offsetsHandle.Release()
		_ = valuesHandle.Release()
		assertTypedColumnAdapterNoActive(t, mgr)
	})

	for _, tc := range []struct {
		name string
		edit func(*typedcolumn.ColumnPartLayoutContractColumn)
	}{
		{name: "absolute offset unaligned offsets", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.OffsetsSection.Offset = 4 }},
		{name: "absolute offset unaligned values", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.ValuesSection.Offset = 26 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert := base
			tc.edit(&cert)
			mgr := mappedresource.NewManager()
			offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedOffsets, mappedresource.SourceMapped, "offsets")
			valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, alignedValues, mappedresource.SourceMapped, "values")
			view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, cert, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
			if err != nil || !view.Scratch || view.Class.Class != typedColumnAdapterUint32OffsetsListViewAbsoluteOffsetUnaligned {
				t.Fatalf("view=%+v err=%v want absolute-offset scratch classification", view, err)
			}
			_ = offsetsHandle.Release()
			_ = valuesHandle.Release()
			assertTypedColumnAdapterNoActive(t, mgr)
		})
	}

	shapeCases := []struct {
		name    string
		offsets []uint64
		values  []uint32
	}{
		{name: "offsets start", offsets: []uint64{1, 1, 2, 2, 5}, values: []uint32{7, 8, 9, 10, 11}},
		{name: "offsets non monotonic", offsets: []uint64{0, 3, 2, 2, 5}, values: []uint32{7, 8, 9, 10, 11}},
		{name: "final offset", offsets: []uint64{0, 0, 2, 2, 6}, values: []uint32{7, 8, 9, 10, 11}},
	}
	for _, tc := range shapeCases {
		t.Run(tc.name, func(t *testing.T) {
			mgr := mappedresource.NewManager()
			offsetsHandle := typedColumnAdapterAcquireBytesSource(t, mgr, typedColumnAdapterAlignedCopy(typedColumnAdapterMustOffsetsListOffsets(t, tc.offsets), int(unsafe.Alignof(uint64(0)))), mappedresource.SourceMapped, "offsets")
			valuesHandle := typedColumnAdapterAcquireBytesSource(t, mgr, typedColumnAdapterAlignedCopy(typedColumnAdapterMustOffsetsListValues(t, tc.values), int(unsafe.Alignof(uint32(0)))), mappedresource.SourceMapped, "values")
			view, err := typedColumnAdapterOpenUint32OffsetsListColumnViewFromHandles(mgr, base, image.Rows, offsetsSection.Length, valuesSection.Length, offsetsHandle, valuesHandle)
			if err == nil || view.Class.Class != typedColumnAdapterUint32OffsetsListViewValidationFailure {
				t.Fatalf("view=%+v err=%v want validation failure", view, err)
			}
			_ = offsetsHandle.Release()
			_ = valuesHandle.Release()
			assertTypedColumnAdapterNoActive(t, mgr)
		})
	}
}

func TestTypedColumnAdapterAdjacencyRequiresDegreeAndLength(t *testing.T) {
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	if _, err := typedColumnAdapterMapField(field); err == nil || !strings.Contains(err.Error(), "adjacency_degree") {
		t.Fatalf("map adjacency without degree err=%v want adjacency_degree", err)
	}
	field.AdjacencyDegree = 3
	rows := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{
		"neighbors": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{1, 2}},
	}}}
	if _, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, rows); err == nil || !strings.Contains(err.Error(), "adjacency_list length=2 want adjacency_degree=3") {
		t.Fatalf("build mismatched adjacency err=%v want degree length failure", err)
	}
}

func TestTypedColumnAdapterRoundTripString(t *testing.T) {
	want := []string{"beta", "alpha", "beta"}
	values := make([]columnDeclaredValue, len(want))
	for i, v := range want {
		values[i] = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: v}
	}
	field := typedColumnAdapterField("kind", ColumnStoreValueString)
	part := typedColumnAdapterBuildPart(t, field, values)
	if got := part.Dictionary["kind"]; got["alpha"] != 0 || got["beta"] != 1 {
		t.Fatalf("dictionary=%+v want sorted stable codes", got)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	got, err := parsed.scanColumnValues("kind")
	if err != nil {
		t.Fatalf("scanColumnValues(kind): %v", err)
	}
	for i := range want {
		if got[i].String != want[i] {
			t.Fatalf("string[%d]=%q want %q all=%+v", i, got[i].String, want[i], got)
		}
	}
}

func TestTypedColumnAdapterDeclaredRowsFusedStringDictionarySortedNullable(t *testing.T) {
	kind := typedColumnAdapterField("kind", ColumnStoreValueString)
	stringType := kind.ValueType
	maybeKind := typedColumnAdapterNullableField("maybe_kind", stringType)
	fields := []TypedStorageField{kind, maybeKind}
	allColumns := []ColumnStoreColumn{
		{Name: kind.Name, Path: kind.Path, ValueType: kind.ValueType, Owner: kind.Owner},
		{Name: maybeKind.Name, Path: maybeKind.Path, ValueType: maybeKind.ValueType, Owner: maybeKind.Owner, Nullable: true},
	}
	rows := []columnDeclaredRow{
		{ID: []byte("r0"), Values: []columnDeclaredValue{
			{Type: stringType, Present: true, String: "zeta"},
			{Type: stringType, Present: true, Null: true},
		}},
		{ID: []byte("r1"), Values: []columnDeclaredValue{
			{Type: stringType, Present: true, String: "alpha"},
			{Type: stringType, Present: false, Null: true},
		}},
		{ID: []byte("r2"), Values: []columnDeclaredValue{
			{Type: stringType, Present: true, String: "mu"},
			{Type: stringType, Present: true, String: "beta"},
		}},
		{ID: []byte("r3"), Values: []columnDeclaredValue{
			{Type: stringType, Present: true, String: "alpha"},
			{Type: stringType, Present: true, String: "alpha"},
		}},
	}
	part, err := buildTypedColumnAdapterPartFromDeclaredRows(typedColumnAdapterOptions{PartID: 43, RowsPerGranule: 2, Fields: fields}, allColumns, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPartFromDeclaredRows: %v", err)
	}
	if dict := part.Dictionary["kind"]; dict["alpha"] != 0 || dict["mu"] != 1 || dict["zeta"] != 2 {
		t.Fatalf("kind dictionary=%+v want lexical codes", dict)
	}
	if dict := part.Dictionary["maybe_kind"]; dict["alpha"] != 0 || dict["beta"] != 1 || len(dict) != 2 {
		t.Fatalf("maybe_kind dictionary=%+v want lexical non-null codes", dict)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	gotRows, err := parsed.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	wantKind := []string{"zeta", "alpha", "mu", "alpha"}
	wantMaybe := []columnDeclaredValue{
		{Type: stringType, Present: true, Null: true},
		{Type: stringType, Present: false, Null: true},
		{Type: stringType, Present: true, String: "beta"},
		{Type: stringType, Present: true, String: "alpha"},
	}
	if len(gotRows) != len(wantKind) {
		t.Fatalf("scanRows returned %d rows, want %d: %+v", len(gotRows), len(wantKind), gotRows)
	}
	for i := range gotRows {
		if got := gotRows[i].Values["kind"].String; got != wantKind[i] {
			t.Fatalf("row %d kind=%q want %q rows=%+v", i, got, wantKind[i], gotRows)
		}
		got := gotRows[i].Values["maybe_kind"]
		want := wantMaybe[i]
		if got.Present != want.Present || got.Null != want.Null || got.String != want.String {
			t.Fatalf("row %d maybe_kind=%+v want %+v rows=%+v", i, got, want, gotRows)
		}
	}
}

func TestTypedColumnAdapterScalarExtremes(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		want := []int64{math.MinInt64, -1, 0, math.MaxInt64}
		values := make([]columnDeclaredValue, len(want))
		for i, v := range want {
			values[i] = columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Int64: v}
		}
		got := typedColumnAdapterRoundTrip(t, typedColumnAdapterField("count", ColumnStoreValueInt64), values)
		for i := range want {
			if got[i].Int64 != want[i] {
				t.Fatalf("int64[%d]=%d want %d all=%+v", i, got[i].Int64, want[i], got)
			}
		}
	})

	t.Run("float32", func(t *testing.T) {
		wantBits := []uint32{
			0,
			0x80000000, // negative zero
			0x00800000, // smallest positive normal
			0x80800000, // negative smallest normal
			math.Float32bits(math.MaxFloat32),
			math.Float32bits(-math.MaxFloat32),
			0x7fc01234, // quiet NaN payload
		}
		values := make([]columnDeclaredValue, len(wantBits))
		for i, bits := range wantBits {
			values[i] = columnDeclaredValue{Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(bits)}
		}
		got := typedColumnAdapterRoundTrip(t, typedColumnAdapterField("score", ColumnStoreValueFloat32), values)
		for i, bits := range wantBits {
			if gotBits := math.Float32bits(got[i].Float32); gotBits != bits {
				t.Fatalf("float32[%d] bits=0x%08x want 0x%08x all=%+v", i, gotBits, bits, got)
			}
		}
	})

	t.Run("float64", func(t *testing.T) {
		wantBits := []uint64{
			0,
			0x8000000000000000, // negative zero
			0x0010000000000000, // smallest positive normal
			0x8010000000000000, // negative smallest normal
			math.Float64bits(math.MaxFloat64),
			math.Float64bits(-math.MaxFloat64),
			0x7ff8000000001234, // quiet NaN payload
		}
		values := make([]columnDeclaredValue, len(wantBits))
		for i, bits := range wantBits {
			values[i] = columnDeclaredValue{Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(bits)}
		}
		got := typedColumnAdapterRoundTrip(t, typedColumnAdapterField("ratio", ColumnStoreValueDouble), values)
		for i, bits := range wantBits {
			if gotBits := math.Float64bits(got[i].Double); gotBits != bits {
				t.Fatalf("float64[%d] bits=0x%016x want 0x%016x all=%+v", i, gotBits, bits, got)
			}
		}
	})

	t.Run("string", func(t *testing.T) {
		want := []string{"", "こんにちは🌲", strings.Repeat("tree-db-", 256)}
		values := make([]columnDeclaredValue, len(want))
		for i, v := range want {
			values[i] = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: v}
		}
		got := typedColumnAdapterRoundTrip(t, typedColumnAdapterField("label", ColumnStoreValueString), values)
		for i := range want {
			if got[i].String != want[i] {
				t.Fatalf("string[%d]=%q want %q all=%+v", i, got[i].String, want[i], got)
			}
		}
	})
}

func TestTypedColumnAdapterMixedColumnsRoundTrip(t *testing.T) {
	fields := []TypedStorageField{
		typedColumnAdapterField("flag", ColumnStoreValueBool),
		typedColumnAdapterField("count", ColumnStoreValueInt64),
		typedColumnAdapterField("score", ColumnStoreValueFloat32),
		typedColumnAdapterField("ratio", ColumnStoreValueDouble),
		typedColumnAdapterField("kind", ColumnStoreValueString),
	}
	rows := []typedColumnAdapterRow{
		{PrimaryID: 101, Values: map[string]columnDeclaredValue{
			"flag":  {Type: ColumnStoreValueBool, Present: true, Bool: true},
			"count": {Type: ColumnStoreValueInt64, Present: true, Int64: math.MinInt64},
			"score": {Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x80000000)},
			"ratio": {Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x7ff8000000001234)},
			"kind":  {Type: ColumnStoreValueString, Present: true, String: "beta"},
		}},
		{PrimaryID: 102, Values: map[string]columnDeclaredValue{
			"flag":  {Type: ColumnStoreValueBool, Present: true, Bool: false},
			"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 0},
			"score": {Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x00800000)},
			"ratio": {Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x8000000000000000)},
			"kind":  {Type: ColumnStoreValueString, Present: true, String: "alpha"},
		}},
		{PrimaryID: 103, Values: map[string]columnDeclaredValue{
			"flag":  {Type: ColumnStoreValueBool, Present: true, Bool: true},
			"count": {Type: ColumnStoreValueInt64, Present: true, Int64: math.MaxInt64},
			"score": {Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x7fc01234)},
			"ratio": {Type: ColumnStoreValueDouble, Present: true, Double: math.MaxFloat64},
			"kind":  {Type: ColumnStoreValueString, Present: true, String: "beta"},
		}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 7, RowsPerGranule: 2, Fields: fields}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	gotRows, err := parsed.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	if len(gotRows) != len(rows) {
		t.Fatalf("rows=%d want %d", len(gotRows), len(rows))
	}
	for i, row := range gotRows {
		if row.PrimaryID != rows[i].PrimaryID {
			t.Fatalf("row[%d] primary_id=%d want %d", i, row.PrimaryID, rows[i].PrimaryID)
		}
		want := rows[i].Values
		got := row.Values
		if got["flag"].Bool != want["flag"].Bool || got["count"].Int64 != want["count"].Int64 || got["kind"].String != want["kind"].String {
			t.Fatalf("row[%d] scalar values=%+v want %+v", i, got, want)
		}
		if math.Float32bits(got["score"].Float32) != math.Float32bits(want["score"].Float32) {
			t.Fatalf("row[%d] score bits=0x%08x want 0x%08x", i, math.Float32bits(got["score"].Float32), math.Float32bits(want["score"].Float32))
		}
		if math.Float64bits(got["ratio"].Double) != math.Float64bits(want["ratio"].Double) {
			t.Fatalf("row[%d] ratio bits=0x%016x want 0x%016x", i, math.Float64bits(got["ratio"].Double), math.Float64bits(want["ratio"].Double))
		}
	}
}

func TestTypedColumnAdapterNestedPathNameRoundTrip(t *testing.T) {
	field := TypedStorageField{Name: "display_score", Path: "metrics.score", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64}
	rows := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{
		"metrics.score": {Type: ColumnStoreValueInt64, Present: true, Int64: 42},
	}}}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart path-keyed: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	gotRows, err := parsed.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	if gotRows[0].Values["metrics.score"].Int64 != 42 {
		t.Fatalf("nested path value=%+v", gotRows[0].Values)
	}
	if _, ok := gotRows[0].Values["display_score"]; ok {
		t.Fatalf("scanRows keyed value by display name: %+v", gotRows[0].Values)
	}

	nameOnly := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{
		"display_score": {Type: ColumnStoreValueInt64, Present: true, Int64: 42},
	}}}
	if _, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, nameOnly); err == nil || !strings.Contains(err.Error(), "missing field \"metrics.score\"") {
		t.Fatalf("build display-name-keyed row err=%v want missing path field", err)
	}
}

func TestTypedColumnAdapterNullMissingTypeMismatchFailClosed(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	tests := []struct {
		name  string
		value columnDeclaredValue
		want  string
	}{
		{name: "missing", value: columnDeclaredValue{Type: ColumnStoreValueInt64, Present: false, Int64: 1}, want: "null or missing values"},
		{name: "null", value: columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Null: true, Int64: 1}, want: "null or missing values"},
		{name: "type_mismatch", value: columnDeclaredValue{Type: ColumnStoreValueDouble, Present: true, Double: 1}, want: "value type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": tt.value}}}
			_, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, rows)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("build err=%v want %q", err, tt.want)
			}
		})
	}
}

func TestTypedColumnAdapterNullableScalarRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		field     TypedStorageField
		nonNullA  columnDeclaredValue
		nonNullB  columnDeclaredValue
		checkRows func(t *testing.T, got []typedColumnAdapterRow)
	}{
		{
			name:     "bool",
			field:    typedColumnAdapterNullableField("flag", ColumnStoreValueBool),
			nonNullA: columnDeclaredValue{Type: ColumnStoreValueBool, Present: true, Bool: true},
			nonNullB: columnDeclaredValue{Type: ColumnStoreValueBool, Present: true, Bool: false},
			checkRows: func(t *testing.T, got []typedColumnAdapterRow) {
				if !got[0].Values["flag"].Bool || got[3].Values["flag"].Bool {
					t.Fatalf("bool nullable rows=%+v", got)
				}
			},
		},
		{
			name:     "int64",
			field:    typedColumnAdapterNullableField("count", ColumnStoreValueInt64),
			nonNullA: columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Int64: math.MinInt64},
			nonNullB: columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Int64: math.MaxInt64},
			checkRows: func(t *testing.T, got []typedColumnAdapterRow) {
				if got[0].Values["count"].Int64 != math.MinInt64 || got[3].Values["count"].Int64 != math.MaxInt64 {
					t.Fatalf("int64 nullable rows=%+v", got)
				}
			},
		},
		{
			name:     "float32",
			field:    typedColumnAdapterNullableField("score32", ColumnStoreValueFloat32),
			nonNullA: columnDeclaredValue{Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x80000000)},
			nonNullB: columnDeclaredValue{Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x7fc01234)},
			checkRows: func(t *testing.T, got []typedColumnAdapterRow) {
				if math.Float32bits(got[0].Values["score32"].Float32) != 0x80000000 || math.Float32bits(got[3].Values["score32"].Float32) != 0x7fc01234 {
					t.Fatalf("float32 nullable rows=%+v", got)
				}
			},
		},
		{
			name:     "double",
			field:    typedColumnAdapterNullableField("ratio", ColumnStoreValueDouble),
			nonNullA: columnDeclaredValue{Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x8000000000000000)},
			nonNullB: columnDeclaredValue{Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x7ff8000000001234)},
			checkRows: func(t *testing.T, got []typedColumnAdapterRow) {
				if math.Float64bits(got[0].Values["ratio"].Double) != 0x8000000000000000 || math.Float64bits(got[3].Values["ratio"].Double) != 0x7ff8000000001234 {
					t.Fatalf("float64 nullable rows=%+v", got)
				}
			},
		},
		{
			name:     "string",
			field:    typedColumnAdapterNullableField("kind", ColumnStoreValueString),
			nonNullA: columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: "alpha"},
			nonNullB: columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: "beta"},
			checkRows: func(t *testing.T, got []typedColumnAdapterRow) {
				if got[0].Values["kind"].String != "alpha" || got[3].Values["kind"].String != "beta" {
					t.Fatalf("string nullable rows=%+v", got)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nullValue := columnDeclaredValue{Type: tc.field.ValueType, Present: true, Null: true}
			rows := []typedColumnAdapterRow{
				{PrimaryID: 1, Values: map[string]columnDeclaredValue{tc.field.Path: tc.nonNullA}},
				{PrimaryID: 2, Values: map[string]columnDeclaredValue{tc.field.Path: nullValue}},
				{PrimaryID: 3, Values: map[string]columnDeclaredValue{}},
				{PrimaryID: 4, Values: map[string]columnDeclaredValue{tc.field.Path: tc.nonNullB}},
			}
			part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 11, RowsPerGranule: 4, Fields: []TypedStorageField{tc.field}}, rows)
			if err != nil {
				t.Fatalf("buildTypedColumnAdapterPart: %v", err)
			}
			if got := part.Part.Columns[tc.field.Name].Definition.Encoding; got != typedcolumn.EncodingNullableInt64 {
				t.Fatalf("encoding=%s want %s", got, typedcolumn.EncodingNullableInt64)
			}
			image, err := part.buildImage()
			if err != nil {
				t.Fatalf("buildImage: %v", err)
			}
			parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
			if err != nil {
				t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
			}
			gotRows, err := parsed.scanRows()
			if err != nil {
				t.Fatalf("scanRows: %v", err)
			}
			if gotRows[1].Values[tc.field.Path].Present != true || gotRows[1].Values[tc.field.Path].Null != true {
				t.Fatalf("explicit null row=%+v", gotRows[1].Values[tc.field.Path])
			}
			if gotRows[2].Values[tc.field.Path].Present != false || gotRows[2].Values[tc.field.Path].Null != true {
				t.Fatalf("missing row=%+v", gotRows[2].Values[tc.field.Path])
			}
			tc.checkRows(t, gotRows)
		})
	}
}

func TestTypedColumnAdapterNullableVectorAdjacencyFailClosed(t *testing.T) {
	vector := typedColumnAdapterNullableField("embedding", ColumnStoreValueFloat32Vector)
	vector.VectorDims = 3
	if _, err := typedColumnAdapterMapField(vector); !errors.Is(err, errTypedColumnAdapterUnsupportedType) || !strings.Contains(err.Error(), "nullable float32_vector") {
		t.Fatalf("nullable vector err=%v want unsupported nullable float32_vector", err)
	}
	adjacency := typedColumnAdapterNullableField("neighbors", ColumnStoreValueAdjacencyList)
	adjacency.AdjacencyDegree = 3
	if _, err := typedColumnAdapterMapField(adjacency); !errors.Is(err, errTypedColumnAdapterUnsupportedType) || !strings.Contains(err.Error(), "nullable adjacency_list") {
		t.Fatalf("nullable adjacency err=%v want unsupported nullable adjacency_list", err)
	}
}

func TestTypedColumnAdapterNullableAbsentWithoutNullMarkerFailsClosed(t *testing.T) {
	field := typedColumnAdapterNullableField("count", ColumnStoreValueInt64)
	rows := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{
		"count": {Type: ColumnStoreValueInt64, Present: false},
	}}}
	_, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, rows)
	if err == nil || !strings.Contains(err.Error(), "absent nullable value is not marked null") {
		t.Fatalf("build absent-without-null err=%v want absent nullable failure", err)
	}
}

func TestTypedColumnAdapterNullableAllNullStringRoundTrip(t *testing.T) {
	field := typedColumnAdapterNullableField("kind", ColumnStoreValueString)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
		{Type: ColumnStoreValueString, Present: true, Null: true},
		{Type: ColumnStoreValueString, Present: false, Null: true},
	})
	if got := part.Part.Columns["kind"].Definition.Cardinality; got != 0 {
		t.Fatalf("all-null nullable string cardinality=%d want 0", got)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	got, err := parsed.scanColumnValues("kind")
	if err != nil {
		t.Fatalf("scanColumnValues: %v", err)
	}
	if len(got) != 2 || !got[0].Present || !got[0].Null || got[1].Present || !got[1].Null {
		t.Fatalf("all-null nullable string values=%+v", got)
	}
}

func TestTypedColumnAdapterNullableCorruptPayloadFailsClosed(t *testing.T) {
	field := typedColumnAdapterNullableField("count", ColumnStoreValueInt64)
	rows := []typedColumnAdapterRow{
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 7}}},
		{PrimaryID: 2, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Null: true}}},
		{PrimaryID: 3, Values: map[string]columnDeclaredValue{}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 12, RowsPerGranule: 3, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	section := typedColumnAdapterFindColumnSection(t, image, "count")
	if section.Length < 23 {
		t.Fatalf("nullable section too short: %+v", section)
	}
	corrupt := image
	corrupt.Bytes = bytes.Clone(image.Bytes)
	payload := corrupt.Bytes[section.Offset : section.Offset+section.Length]
	nullMaskLen := int(binary.LittleEndian.Uint32(payload[13:17]))
	defaultMaskStart := nullableInt64HeaderBytesForTest() + nullMaskLen
	payload[defaultMaskStart] |= 1 << 1
	parsed, err := typedColumnAdapterPartFromImage(part.Options, corrupt)
	if err != nil {
		t.Fatalf("partFromImage corrupt nullable payload: %v", err)
	}
	if _, err := parsed.scanColumnValues("count"); err == nil || !strings.Contains(err.Error(), "both null and default") {
		t.Fatalf("scan corrupt nullable err=%v want overlap failure", err)
	}
}

func BenchmarkTypedColumnNullableDecodedValuesRowHotLoop1784(b *testing.B) {
	fields := []TypedStorageField{
		func() TypedStorageField {
			f := typedColumnAdapterField("count", ColumnStoreValueInt64)
			f.Nullable = true
			return f
		}(),
		func() TypedStorageField {
			f := typedColumnAdapterField("kind", ColumnStoreValueString)
			f.Nullable = true
			return f
		}(),
		func() TypedStorageField {
			f := typedColumnAdapterField("flag", ColumnStoreValueBool)
			f.Nullable = true
			return f
		}(),
	}
	const rowsN = 128
	rows := make([]typedColumnAdapterRow, rowsN)
	for i := range rows {
		kind := columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: "alpha"}
		count := columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Int64: int64(i)}
		flag := columnDeclaredValue{Type: ColumnStoreValueBool, Present: true, Bool: i%2 == 0}
		if i%5 == 0 {
			kind = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, Null: true}
		}
		if i%7 == 0 {
			count = columnDeclaredValue{Type: ColumnStoreValueInt64, Present: false, Null: true}
		}
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i), Values: map[string]columnDeclaredValue{
			"count": count,
			"kind":  kind,
			"flag":  flag,
		}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: fields}, rows)
	if err != nil {
		b.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	decoded, err := part.scanDecodedValues()
	if err != nil {
		b.Fatalf("scanDecodedValues: %v", err)
	}
	dst := make([]columnDeclaredValue, 0, len(fields))
	var sink int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, err := decoded.valuesForRowInto(i&(rowsN-1), dst)
		if err != nil {
			b.Fatalf("valuesForRowInto: %v", err)
		}
		sink += int64(len(values)) + values[0].Int64
	}
	typedColumnNullableBenchSink1784 += sink
}

func BenchmarkTypedColumnNullableMergeHotLoop1784(b *testing.B) {
	cfg := ColumnStoreConfig{Columns: []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Nullable: true},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart, Nullable: true},
		{Name: "flag", Path: "flag", ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart, Nullable: true},
	}}
	rowValues := []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 42}}
	typedValues := []columnDeclaredValue{
		{Type: ColumnStoreValueString, Present: true, Null: true},
		{Type: ColumnStoreValueDouble, Present: false, Null: true},
		{Type: ColumnStoreValueBool, Present: true, Bool: true},
	}
	dst := make([]columnDeclaredValue, 0, len(cfg.Columns))
	var sink int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, err := mergeColumnReconstructionValuesInto(cfg, rowValues, typedValues, dst)
		if err != nil {
			b.Fatalf("mergeColumnReconstructionValuesInto: %v", err)
		}
		sink += int64(len(values)) + values[0].Int64
	}
	typedColumnNullableBenchSink1784 += sink
}

func TestTypedColumnAdapterStringDictionaryHighCardinalityStable(t *testing.T) {
	const unique = 96
	values := make([]columnDeclaredValue, unique*2)
	for i := range values {
		idx := (i * 37) % unique
		values[i] = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: fmt.Sprintf("value-%03d", idx)}
	}
	field := typedColumnAdapterField("kind", ColumnStoreValueString)
	part := typedColumnAdapterBuildPart(t, field, values)
	dict := part.Dictionary["kind"]
	if len(dict) != unique {
		t.Fatalf("dictionary len=%d want %d", len(dict), unique)
	}
	for i := 0; i < unique; i++ {
		label := fmt.Sprintf("value-%03d", i)
		if got := dict[label]; got != int64(i) {
			t.Fatalf("dictionary[%q]=%d want %d dict=%+v", label, got, i, dict)
		}
	}
	again := typedColumnAdapterBuildPart(t, field, values)
	for label, code := range dict {
		if again.Dictionary["kind"][label] != code {
			t.Fatalf("dictionary unstable for %q: got %d want %d", label, again.Dictionary["kind"][label], code)
		}
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	got, err := parsed.scanColumnValues("kind")
	if err != nil {
		t.Fatalf("scanColumnValues(kind): %v", err)
	}
	for i := range values {
		if got[i].String != values[i].String {
			t.Fatalf("string[%d]=%q want %q", i, got[i].String, values[i].String)
		}
	}
}

func TestTypedColumnAdapterLegacyDictionaryOrderMetadataAcceptedWhenSorted1948(t *testing.T) {
	field := typedColumnAdapterField("kind", ColumnStoreValueString)
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueString, Present: true, String: "beta"},
		{Type: ColumnStoreValueString, Present: true, String: "alpha"},
	}
	part := typedColumnAdapterBuildPart(t, field, values)
	column, ok := part.columnByName("kind")
	if !ok {
		t.Fatal("missing kind column")
	}
	nextMetadataCode := func(m map[string]int64) int64 {
		var maxCode int64 = -1
		for _, code := range m {
			if code > maxCode {
				maxCode = code
			}
		}
		return maxCode + 1
	}
	metadata := part.Dictionary[typedColumnAdapterMetadataDictionary]
	delete(metadata, typedColumnAdapterMetadataEntryKey(column, typedColumnAdapterMetadataDictionaryOrderMark, typedColumnAdapterStringDictionaryOrder))
	delete(metadata, typedColumnAdapterMetadataEntryKey(column, typedColumnAdapterMetadataDictionaryCollationMark, typedColumnAdapterStringDictionaryCollation))
	legacyCode := nextMetadataCode(metadata)
	metadata[typedColumnAdapterMetadataEntryKey(column, typedColumnAdapterMetadataDictionaryOrderMark, typedColumnAdapterStringDictionaryLegacyOrder)] = legacyCode
	metadata[typedColumnAdapterMetadataEntryKey(column, typedColumnAdapterMetadataDictionaryCollationMark, typedColumnAdapterStringDictionaryLegacyCollation)] = legacyCode + 1
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage legacy metadata: %v", err)
	}
	if _, err := typedColumnAdapterPartFromImage(part.Options, image); err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage legacy sorted dictionary metadata: %v", err)
	}

	badPart := typedColumnAdapterBuildPart(t, field, values)
	badColumn, ok := badPart.columnByName("kind")
	if !ok {
		t.Fatal("missing kind column")
	}
	badMetadata := badPart.Dictionary[typedColumnAdapterMetadataDictionary]
	delete(badMetadata, typedColumnAdapterMetadataEntryKey(badColumn, typedColumnAdapterMetadataDictionaryOrderMark, typedColumnAdapterStringDictionaryOrder))
	delete(badMetadata, typedColumnAdapterMetadataEntryKey(badColumn, typedColumnAdapterMetadataDictionaryCollationMark, typedColumnAdapterStringDictionaryCollation))
	badLegacyCode := nextMetadataCode(badMetadata)
	badMetadata[typedColumnAdapterMetadataEntryKey(badColumn, typedColumnAdapterMetadataDictionaryOrderMark, typedColumnAdapterStringDictionaryLegacyOrder)] = badLegacyCode
	badMetadata[typedColumnAdapterMetadataEntryKey(badColumn, typedColumnAdapterMetadataDictionaryCollationMark, typedColumnAdapterStringDictionaryLegacyCollation)] = badLegacyCode + 1
	badPart.Dictionary["kind"]["alpha"] = 1
	badPart.Dictionary["kind"]["beta"] = 0
	badImage, err := badPart.buildImage()
	if err != nil {
		t.Fatalf("buildImage bad legacy metadata: %v", err)
	}
	if _, err := typedColumnAdapterPartFromImage(badPart.Options, badImage); err == nil || !strings.Contains(err.Error(), "not logical bytewise ascending") {
		t.Fatalf("typedColumnAdapterPartFromImage bad legacy dictionary err=%v want logical order rejection", err)
	}
}

func TestTypedColumnAdapterCorruptDictionaryCodeFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("kind", ColumnStoreValueString)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
		{Type: ColumnStoreValueString, Present: true, String: "alpha"},
		{Type: ColumnStoreValueString, Present: true, String: "beta"},
		{Type: ColumnStoreValueString, Present: true, String: "gamma"},
	})
	corrupt := make(map[string]map[string]int64, len(part.Dictionary))
	for name, dict := range part.Dictionary {
		clone := make(map[string]int64, len(dict))
		for value, code := range dict {
			clone[value] = code
		}
		corrupt[name] = clone
	}
	delete(corrupt["kind"], "gamma")
	part.Dictionary = corrupt
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage corrupt dictionary: %v", err)
	}
	_, err = typedColumnAdapterPartFromImage(part.Options, image)
	if err == nil || !strings.Contains(err.Error(), "missing dictionary code") {
		t.Fatalf("partFromImage corrupt dictionary err=%v want missing dictionary code", err)
	}
}

func TestTypedColumnAdapterUnexpectedExtraColumnPolicy(t *testing.T) {
	count := typedColumnAdapterField("count", ColumnStoreValueInt64)
	extra := typedColumnAdapterField("debug", ColumnStoreValueBool)
	rows := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{
		"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 7},
		"debug": {Type: ColumnStoreValueBool, Present: true, Bool: true},
	}}}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{count, extra}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart with extra: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	_, err = typedColumnAdapterPartFromImage(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{count}}, image)
	if err == nil || !strings.Contains(err.Error(), "unexpected column \"debug\"") {
		t.Fatalf("partFromImage extra column err=%v want unexpected column", err)
	}
}

func TestTypedColumnAdapterResourceReaderLifecycleErrors(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 7}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	section := typedColumnAdapterFindColumnSection(t, image, "count")
	want, err := image.SectionBytes(section)
	if err != nil {
		t.Fatalf("SectionBytes: %v", err)
	}
	mgr := mappedresource.NewManager()
	reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Namespace: "typed-column-adapter-lifecycle", PartID: image.PartID, FileID: 9, AllowHeapCopy: true}

	h, err := reader.AcquireSection(section)
	if err != nil {
		t.Fatalf("AcquireSection: %v", err)
	}
	if stats := mgr.Stats(); stats.ActiveHandles != 1 || stats.ActiveHeapCopyBytes != int64(len(want)) {
		t.Fatalf("active stats after acquire=%+v want one heap pin", stats)
	}
	if pins := mgr.PinSummary(); len(pins) != 1 || pins[0].Key.Namespace != "typed-column-adapter-lifecycle" {
		t.Fatalf("pins after acquire=%+v", pins)
	}
	if !slices.Equal(h.Bytes(), want) {
		t.Fatalf("handle bytes=%x want %x", h.Bytes(), want)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	assertTypedColumnAdapterNoActive(t, mgr)

	readBytes, err := reader.ReadSection(section)
	if err != nil {
		t.Fatalf("ReadSection: %v", err)
	}
	assertTypedColumnAdapterNoActive(t, mgr)
	if !slices.Equal(readBytes, want) {
		t.Fatalf("ReadSection bytes=%x want %x", readBytes, want)
	}
	if len(readBytes) != 0 {
		originalFirst := want[0]
		readBytes[0] ^= 0xff
		fresh, err := image.SectionBytes(section)
		if err != nil {
			t.Fatalf("fresh SectionBytes: %v", err)
		}
		if fresh[0] != originalFirst {
			t.Fatalf("ReadSection returned an alias into image bytes")
		}
	}

	oob := section
	oob.Offset = len(image.Bytes) + 1
	if _, err := reader.ReadSection(oob); err == nil {
		t.Fatalf("ReadSection OOB err=nil, want failure")
	}
	assertTypedColumnAdapterNoActive(t, mgr)

	mismatch := reader
	mismatch.Scope = mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "mismatch", Namespace: "other"}
	if _, err := mismatch.AcquireSection(section); err == nil || !strings.Contains(err.Error(), "does not match key namespace") {
		t.Fatalf("AcquireSection namespace mismatch err=%v want namespace mismatch", err)
	}
	assertTypedColumnAdapterNoActive(t, mgr)

	nilReader := reader
	nilReader.Manager = nil
	if _, err := nilReader.ReadSection(section); err == nil || !strings.Contains(err.Error(), "requires manager") {
		t.Fatalf("ReadSection nil manager err=%v want requires manager", err)
	}
	assertTypedColumnAdapterNoActive(t, mgr)
}

func TestTypedColumnAdapterVectorAdjacencyRepresented(t *testing.T) {
	mapping, err := typedColumnAdapterMappingForValueType(ColumnStoreValueFloat32Vector)
	if err != nil {
		t.Fatalf("float32_vector mapping err=%v", err)
	}
	if mapping.Status != typedColumnAdapterRepresented || mapping.ColumnType != typedcolumn.ColumnTypeFloat32Vector || mapping.Encoding != typedcolumn.EncodingRawFloat32Vector {
		t.Fatalf("float32_vector mapping=%+v want dense represented", mapping)
	}

	mapping, err = typedColumnAdapterMappingForValueType(ColumnStoreValueAdjacencyList)
	if err != nil {
		t.Fatalf("adjacency_list mapping err=%v", err)
	}
	if mapping.Status != typedColumnAdapterRepresented || mapping.ColumnType != typedcolumn.ColumnTypeAdjacencyList || mapping.Encoding != typedcolumn.EncodingRawUint32Dense {
		t.Fatalf("adjacency_list mapping=%+v want dense uint32 represented", mapping)
	}
}

func TestTypedColumnAdapterAdjacencyOffsetsListSelectorSupported1915(t *testing.T) {
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	field.AdjacencyDegree = 0
	column, err := typedColumnAdapterMapField(field)
	if err != nil {
		t.Fatalf("typedColumnAdapterMapField offsets-list: %v", err)
	}
	if column.Definition.Encoding != typedcolumn.EncodingRawUint32OffsetsList || column.Definition.FixedWidthElements != 0 {
		t.Fatalf("offsets-list column definition=%+v", column.Definition)
	}
}

func TestTypedColumnAdapterAdjacencyOffsetsListWrongSelectorsFailClosed1917(t *testing.T) {
	withDegree := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	withDegree.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	withDegree.AdjacencyDegree = 2
	if _, err := typedColumnAdapterMapField(withDegree); err == nil || !strings.Contains(err.Error(), "must be zero for adjacency_layout") {
		t.Fatalf("offsets-list with degree err=%v want selector/degree failure", err)
	}

	withFixedWidth := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	withFixedWidth.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	withFixedWidth.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	if _, err := typedColumnAdapterMapField(withFixedWidth); err == nil || !strings.Contains(err.Error(), "fixed_width_encoding is unsupported") {
		t.Fatalf("offsets-list with fixed_width_encoding err=%v want selector/encoding failure", err)
	}
}

func TestTypedColumnAdapterExistingConfigStaysTypedRow(t *testing.T) {
	layout, err := ResolveTypedStorageLayout(CollectionMeta{
		Name: "typed_column_adapter_existing_config",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{{Name: "count", Path: "count", ValueType: ColumnStoreValueInt64}},
		}},
	})
	if err != nil {
		t.Fatalf("ResolveTypedStorageLayout: %v", err)
	}
	owner, ok := layout.OwnerForPath("count")
	if !ok || owner != TypedStorageOwnerRowAsset {
		t.Fatalf("owner=%s ok=%v want typed_row_asset layout=%+v", owner, ok, layout)
	}
	if layout.HasTypedColumnPartOwners() {
		t.Fatalf("existing config unexpectedly has typed-column owner: %+v", layout)
	}
	if err := layout.EnsureReadSupported(); err != nil {
		t.Fatalf("EnsureReadSupported existing config: %v", err)
	}
}

func TestTypedColumnAdapterRetainedPayloadSplitRestore(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{
			{Name: "count", Path: "count", ValueType: ColumnStoreValueInt64},
			{Name: "flag", Path: "nested.flag", ValueType: ColumnStoreValueBool},
		},
	}
	doc := []byte(`{"count":7,"keep":"yes","nested":{"flag":true,"other":9}}`)
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Present: true, Int64: 7},
		{Type: ColumnStoreValueBool, Present: true, Bool: true},
	}
	retained, restored, err := typedColumnAdapterRetainedPayloadSplitRestore(cfg, doc, values)
	if err != nil {
		t.Fatalf("typedColumnAdapterRetainedPayloadSplitRestore: %v", err)
	}
	if strings.Contains(string(retained), "count") || strings.Contains(string(retained), "flag") {
		t.Fatalf("retained payload still contains declared fields: %s", retained)
	}
	var restoredObj map[string]any
	if err := json.Unmarshal(restored, &restoredObj); err != nil {
		t.Fatalf("unmarshal restored: %v", err)
	}
	if restoredObj["keep"] != "yes" || restoredObj["count"].(float64) != 7 {
		t.Fatalf("restored top-level=%s", restored)
	}
	nested := restoredObj["nested"].(map[string]any)
	if nested["flag"] != true || nested["other"].(float64) != 9 {
		t.Fatalf("restored nested=%s", restored)
	}
}

func TestTypedColumnAdapterMappedResourceMmapHeapParity(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
		{Type: ColumnStoreValueInt64, Present: true, Int64: 2},
	})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	section := typedColumnAdapterFindColumnSection(t, image, "count")
	path := filepath.Join(t.TempDir(), "part.tcs1")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	mgr := mappedresource.NewManager()
	mappedReader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-adapter-test", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: true}
	heapReader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-adapter-test", PartID: image.PartID, AllowHeapCopy: true}
	mappedBytes, err := mappedReader.ReadSection(section)
	if err != nil {
		t.Fatalf("mapped ReadSection: %v", err)
	}
	heapBytes, err := heapReader.ReadSection(section)
	if err != nil {
		t.Fatalf("heap ReadSection: %v", err)
	}
	want, err := image.SectionBytes(section)
	if err != nil {
		t.Fatalf("image.SectionBytes: %v", err)
	}
	if !slices.Equal(mappedBytes, want) || !slices.Equal(heapBytes, want) {
		t.Fatalf("section parity mapped=%x heap=%x want=%x", mappedBytes, heapBytes, want)
	}
}

func TestTypedColumnAdapterAdjacencyDenseFallbackOnly(t *testing.T) {
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyDegree = 3
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{1, 2, 3}},
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{4, 5, 6}},
	})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	column, ok := part.columnByName("neighbors")
	if !ok {
		t.Fatalf("missing adapter column")
	}
	path := filepath.Join(t.TempDir(), "part.tcs1")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	mgr := mappedresource.NewManager()
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	certColumn, ok := certification.Column("neighbors")
	if !ok {
		t.Fatal("missing adjacency layout certification")
	}
	if plan := typeddecode.AdjacencyListPlan(certColumn, field.AdjacencyDegree); plan.DirectCandidate() {
		t.Fatalf("adjacency direct-view plan=%+v want fallback-only", plan)
	}
	reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-adapter-adjacency", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: true}
	view, err := typedColumnAdapterAcquireDenseUint32ColumnView(reader, column, image.Rows)
	if err != nil {
		t.Fatalf("AcquireDenseUint32ColumnView: %v", err)
	}
	if view.Direct || view.Handle != nil || !slices.Equal(view.Values, []uint32{1, 2, 3, 4, 5, 6}) {
		if view.Handle != nil {
			_ = view.Handle.Release()
		}
		t.Fatalf("adjacency view=%+v want decoded fallback values without retained direct handle", view)
	}
	assertTypedColumnAdapterNoActive(t, mgr)
}

func TestTypedColumnAdapterDenseDecodeFallbackAllowsHostEndianMismatch(t *testing.T) {
	if !typedColumnDenseDecodeFallbackAllowed(typeddecode.StreamingStatus(typeddecode.ReasonWrongEndian, "host endian mismatch")) {
		t.Fatalf("wrong-endian direct-view status should allow safe little-endian decode fallback")
	}
	if !typedColumnDenseDecodeFallbackAllowed(typeddecode.UnsupportedStatus(typeddecode.ReasonDirectViewDeferred, "adjacency direct views deferred")) {
		t.Fatalf("deferred adjacency direct-view status should allow safe decode fallback")
	}
	if typedColumnDenseDecodeFallbackAllowed(typeddecode.UnsupportedStatus(typeddecode.ReasonPayloadLengthMismatch, "short payload")) {
		t.Fatalf("payload length mismatch must fail closed, not decode fallback")
	}
}

func TestTypedColumnAdapterTypedViewsValidateFixedWidth(t *testing.T) {
	mgr := mappedresource.NewManager()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "typed-column-adapter-views", Namespace: "typed-column-adapter-test"}

	i64 := typedColumnAdapterAlignedBytes(16, int(unsafe.Alignof(int64(0))))
	binary.LittleEndian.PutUint64(i64[0:8], 7)
	binary.LittleEndian.PutUint64(i64[8:16], 11)
	i64Handle := typedColumnAdapterAcquireBytes(t, mgr, scope, i64, "i64")
	defer i64Handle.Release()
	if got, err := typedColumnAdapterInt64View(mgr, i64Handle); err != nil || !slices.Equal(got, []int64{7, 11}) {
		t.Fatalf("Int64View=%v err=%v", got, err)
	}

	f32Bytes := typedColumnAdapterAlignedBytes(8, int(unsafe.Alignof(float32(0))))
	binary.LittleEndian.PutUint32(f32Bytes[0:4], math.Float32bits(1.5))
	binary.LittleEndian.PutUint32(f32Bytes[4:8], math.Float32bits(2.5))
	f32Handle := typedColumnAdapterAcquireBytes(t, mgr, scope, f32Bytes, "f32")
	defer f32Handle.Release()
	if got, err := typedColumnAdapterFloat32View(mgr, f32Handle); err != nil || len(got) != 2 || got[0] != 1.5 || got[1] != 2.5 {
		t.Fatalf("Float32View=%v err=%v", got, err)
	}

	f64Bytes := typedColumnAdapterAlignedBytes(16, int(unsafe.Alignof(float64(0))))
	binary.LittleEndian.PutUint64(f64Bytes[0:8], math.Float64bits(1.5))
	binary.LittleEndian.PutUint64(f64Bytes[8:16], math.Float64bits(2.5))
	f64Handle := typedColumnAdapterAcquireBytes(t, mgr, scope, f64Bytes, "f64")
	defer f64Handle.Release()
	if got, err := typedColumnAdapterFloat64View(mgr, f64Handle); err != nil || len(got) != 2 || got[0] != 1.5 || got[1] != 2.5 {
		t.Fatalf("Float64View=%v err=%v", got, err)
	}

	u32Bytes := typedColumnAdapterAlignedBytes(8, int(unsafe.Alignof(uint32(0))))
	binary.LittleEndian.PutUint32(u32Bytes[0:4], 3)
	binary.LittleEndian.PutUint32(u32Bytes[4:8], 5)
	u32Handle := typedColumnAdapterAcquireBytes(t, mgr, scope, u32Bytes, "u32")
	defer u32Handle.Release()
	if got, err := typedColumnAdapterUint32View(mgr, u32Handle); err != nil || !slices.Equal(got, []uint32{3, 5}) {
		t.Fatalf("Uint32View=%v err=%v", got, err)
	}

	truncated := typedColumnAdapterAlignedBytes(6, int(unsafe.Alignof(uint32(0))))
	truncatedHandle := typedColumnAdapterAcquireBytes(t, mgr, scope, truncated, "truncated")
	defer truncatedHandle.Release()
	if _, err := typedColumnAdapterUint32View(mgr, truncatedHandle); err == nil {
		t.Fatalf("Uint32View truncated err=nil, want failure")
	}
	if stats := mgr.Stats(); stats.DirectViewSuccesses != 4 || stats.DirectViewFailures != 1 {
		t.Fatalf("direct view stats=%+v", stats)
	}
}

func TestTypedColumnAdapterReservedPrimaryIDFailsClosed(t *testing.T) {
	for _, field := range []TypedStorageField{
		typedColumnAdapterField(typedColumnAdapterPrimaryIDColumn, ColumnStoreValueInt64),
		{Name: "user_id", Path: typedColumnAdapterPrimaryIDColumn, Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
	} {
		_, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, nil)
		if err == nil || !strings.Contains(err.Error(), "reserved primary-id column") {
			t.Fatalf("build reserved field %+v err=%v want reserved primary-id column", field, err)
		}
	}
	metadata := typedColumnAdapterField(typedColumnAdapterMetadataDictionary, ColumnStoreValueString)
	_, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{metadata}}, nil)
	if err == nil || !strings.Contains(err.Error(), "reserved metadata dictionary") {
		t.Fatalf("build metadata-reserved field err=%v want reserved metadata dictionary", err)
	}
}

func TestTypedColumnAdapterDuplicateOrAmbiguousFieldsFailClosed(t *testing.T) {
	duplicate := []TypedStorageField{
		{Name: "dup", Path: "left", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
		{Name: "dup", Path: "right", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
	}
	if _, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: duplicate}, nil); err == nil || !strings.Contains(err.Error(), "duplicate column") {
		t.Fatalf("build duplicate fields err=%v want duplicate column", err)
	}

	duplicatePath := []TypedStorageField{
		{Name: "left", Path: "same", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
		{Name: "right", Path: "same", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
	}
	if _, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: duplicatePath}, nil); err == nil || !strings.Contains(err.Error(), "duplicate field path") {
		t.Fatalf("build duplicate path fields err=%v want duplicate field path", err)
	}

	crossCollision := []TypedStorageField{
		{Name: "left", Path: "right", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
		{Name: "right", Path: "other", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64},
	}
	if _, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: crossCollision}, nil); err == nil || !strings.Contains(err.Error(), "ambiguous field name") {
		t.Fatalf("build cross-collision fields err=%v want ambiguous field name", err)
	}
}

func TestTypedColumnAdapterImageDescriptorVersionFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 10}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	corrupt := image
	corrupt.Bytes = bytes.Clone(image.Bytes)
	corrupt.Sections = slices.Clone(image.Sections)
	descriptor := typedColumnAdapterFindSection(t, corrupt, typedcolumn.ColumnPartImageSectionDescriptor)
	binary.LittleEndian.PutUint16(corrupt.Bytes[descriptor.Offset:descriptor.Offset+2], 99)

	if _, err := typedColumnAdapterPartFromImage(part.Options, corrupt); err == nil || !strings.Contains(err.Error(), "unsupported descriptor version") {
		t.Fatalf("partFromImage descriptor version err=%v want unsupported descriptor version", err)
	}
}

func TestTypedColumnAdapterImageSchemaVersionMismatchFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	rows := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 10}}}}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, SchemaVersion: 77, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	if _, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{PartID: 1, SchemaVersion: 78, Fields: []TypedStorageField{field}}, image); err == nil || !strings.Contains(err.Error(), "schema_version=77 want 78") {
		t.Fatalf("partFromImage schema version mismatch err=%v want schema_version mismatch", err)
	}
}

func TestTypedColumnAdapterImageSchemaMismatchFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 10}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	mismatch := typedColumnAdapterField("count", ColumnStoreValueBool)
	if _, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{mismatch}}, image); err == nil || !strings.Contains(err.Error(), "schema mismatch") {
		t.Fatalf("partFromImage schema mismatch err=%v want schema mismatch", err)
	}
}

func TestTypedColumnAdapterImageOwnerMismatchFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 10}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	mismatch := typedColumnAdapterField("count", ColumnStoreValueInt64)
	mismatch.Owner = TypedStorageOwnerRowAsset
	if _, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{mismatch}}, image); err == nil || !strings.Contains(err.Error(), "owner=\"typed_row_asset\" want \"typed_column_part\"") {
		t.Fatalf("partFromImage owner mismatch err=%v want owner mismatch", err)
	}
}

func TestTypedColumnAdapterImageFixedWidthEncodingMismatchFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("score", ColumnStoreValueFloat32)
	field.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x7fc12345)}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	mismatch := typedColumnAdapterField("score", ColumnStoreValueFloat32)
	if _, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{mismatch}}, image); err == nil || !strings.Contains(err.Error(), "schema mismatch") {
		t.Fatalf("partFromImage fixed_width mismatch err=%v want schema mismatch", err)
	}
}

func TestTypedColumnAdapterImageValueTypeMetadataMismatchFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("score", ColumnStoreValueDouble)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueDouble, Present: true, Double: 42.5}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	mismatch := typedColumnAdapterField("score", ColumnStoreValueFloat32)
	if _, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{mismatch}}, image); err == nil || !strings.Contains(err.Error(), "value type metadata mismatch") {
		t.Fatalf("partFromImage value-type mismatch err=%v want value type metadata mismatch", err)
	}
}

func TestTypedColumnAdapterImageAdjacencyDegreeMismatchFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyDegree = 3
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{1, 2, 3}},
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{4, 5, 6}},
	})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	mismatch := field
	mismatch.AdjacencyDegree = 2
	if _, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{mismatch}}, image); err == nil || !strings.Contains(err.Error(), "fixed_width_elements=3 want") {
		t.Fatalf("partFromImage adjacency_degree mismatch err=%v want fixed_width_elements schema mismatch", err)
	}
}

func TestTypedColumnAdapterImageAdjacencyTruncatedFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyDegree = 3
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{1, 2, 3}}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	section := typedColumnAdapterFindColumnSection(t, image, "neighbors")
	corrupt := image
	corrupt.Bytes = bytes.Clone(image.Bytes[:len(image.Bytes)-4])
	corrupt.Sections = append([]typedcolumn.ColumnPartImageSection(nil), corrupt.Sections...)
	for i := range corrupt.Sections {
		if corrupt.Sections[i].Kind == typedcolumn.ColumnPartImageSectionColumnData && corrupt.Sections[i].Column == "neighbors" {
			if corrupt.Sections[i].Offset+corrupt.Sections[i].Length != len(image.Bytes) {
				t.Fatalf("neighbors section is not final: %+v total=%d", section, len(image.Bytes))
			}
			corrupt.Sections[i].Length -= 4
			break
		}
	}
	if _, err := typedColumnAdapterPartFromImage(part.Options, corrupt); err == nil || !strings.Contains(err.Error(), "outside section") {
		t.Fatalf("partFromImage truncated adjacency err=%v want outside section failure", err)
	}
}

func TestTypedColumnAdapterImageVectorDimsMismatchFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("embedding", ColumnStoreValueFloat32Vector)
	field.VectorDims = 3
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{
		{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 2, 3}},
		{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{4, 5, 6}},
	})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	mismatch := field
	mismatch.VectorDims = 4
	if _, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{mismatch}}, image); err == nil || !strings.Contains(err.Error(), "fixed_width_elements=3 want") {
		t.Fatalf("partFromImage vector_dims mismatch err=%v want fixed_width_elements schema mismatch", err)
	}
}

func TestTypedColumnAdapterPrepareInt64SchemaHashMismatchFailsBeforeScan(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 10}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	_, _, _, err = typedColumnAdapterPrepareInt64PredicateScanPart([]TypedStorageField{field}, image.Bytes, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion+1), "count")
	if err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("prepare int64 predicate schema hash mismatch err=%v want schema_version failure before scan", err)
	}
}

func TestTypedColumnAdapterPrepareInt64AggregateSkipsDictionaryDecode(t *testing.T) {
	countField := typedColumnAdapterField("count", ColumnStoreValueInt64)
	kindField := typedColumnAdapterField("kind", ColumnStoreValueString)
	fields := []TypedStorageField{countField, kindField}
	rows := []typedColumnAdapterRow{
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 10}, "kind": {Type: ColumnStoreValueString, Present: true, String: "alpha"}}},
		{PrimaryID: 2, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 20}, "kind": {Type: ColumnStoreValueString, Present: true, String: "beta"}}},
		{PrimaryID: 3, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 30}, "kind": {Type: ColumnStoreValueString, Present: true, String: "beta"}}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 77, RowsPerGranule: 2, Fields: fields}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	corruptRaw := bytes.Clone(image.Bytes)
	dictSection := typedColumnAdapterFindSection(t, image, typedcolumn.ColumnPartImageSectionDictionaries)
	if dictSection.Length < 4 {
		t.Fatalf("dictionary section too short: %+v", dictSection)
	}
	binary.LittleEndian.PutUint32(corruptRaw[dictSection.Offset:dictSection.Offset+4], ^uint32(0))
	corruptImage, err := typedcolumn.ParseColumnPartImage(corruptRaw)
	if err != nil {
		t.Fatalf("ParseColumnPartImage(corrupt dictionaries): %v", err)
	}

	if _, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{Fields: fields}, corruptImage); err == nil || !strings.Contains(err.Error(), "dictionar") {
		t.Fatalf("generic typedColumnAdapterPartFromImage err=%v want dictionary validation failure", err)
	}
	if _, err := typedColumnAdapterPrepareStringPredicateScanPart(fields, corruptRaw, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "kind", "beta"); err == nil || !strings.Contains(err.Error(), "dictionar") {
		t.Fatalf("string predicate prepare err=%v want dictionary validation failure", err)
	}

	adapterPart, adapterColumn, manifestBytes, err := typedColumnAdapterPrepareInt64PredicateAggregatePart(fields, corruptRaw, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "count")
	if err != nil {
		t.Fatalf("typedColumnAdapterPrepareInt64PredicateAggregatePart: %v", err)
	}
	if manifestBytes == 0 {
		t.Fatalf("manifestBytes=0 want decoded manifest metadata")
	}
	var result TypedColumnInt64PredicateAggregateResult
	partPruned, err := scanTypedColumnInt64PredicateAggregatePart(adapterPart.Part, adapterColumn.Definition.Name, TypedColumnInt64PredicateScanRequest{Kind: TypedColumnInt64PredicateAll}, &result)
	if err != nil {
		t.Fatalf("scanTypedColumnInt64PredicateAggregatePart: %v", err)
	}
	if partPruned || result.Count != 3 || result.Sum != 60 || result.Diagnostics.RowsScanned != 3 || result.Diagnostics.RowsMatched != 3 {
		t.Fatalf("partPruned=%v result=%+v diagnostics=%+v want aggregate over corrupt-dictionary image", partPruned, result, result.Diagnostics)
	}
}

func TestTypedColumnAdapterInt64AggregateScratchReusedAcrossScans(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	rows := []typedColumnAdapterRow{
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 1}}},
		{PrimaryID: 2, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 2}}},
		{PrimaryID: 3, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 3}}},
		{PrimaryID: 4, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 4}}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 78, RowsPerGranule: 2, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	var scratch typedColumnInt64PredicateAggregateScanScratch
	req := TypedColumnInt64PredicateScanRequest{Kind: TypedColumnInt64PredicateAll}
	var first TypedColumnInt64PredicateAggregateResult
	partPruned, err := scanTypedColumnInt64PredicateAggregatePartWithVisibilityAndScratch(part.Part, "count", req, &first, nil, &scratch)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if partPruned || first.Count != 4 || first.Sum != 10 || first.Diagnostics.BlocksDecoded != 2 {
		t.Fatalf("first partPruned=%v result=%+v diagnostics=%+v", partPruned, first, first.Diagnostics)
	}
	if len(scratch.values) == 0 {
		t.Fatal("first scan left empty scratch values")
	}
	firstPtr := &scratch.values[0]
	firstCap := cap(scratch.values)

	var second TypedColumnInt64PredicateAggregateResult
	partPruned, err = scanTypedColumnInt64PredicateAggregatePartWithVisibilityAndScratch(part.Part, "count", req, &second, nil, &scratch)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if partPruned || second.Count != first.Count || second.Sum != first.Sum || second.Diagnostics.BlocksDecoded != first.Diagnostics.BlocksDecoded {
		t.Fatalf("second partPruned=%v result=%+v diagnostics=%+v want first=%+v", partPruned, second, second.Diagnostics, first)
	}
	if got := &scratch.values[0]; got != firstPtr || cap(scratch.values) != firstCap {
		t.Fatalf("scratch reallocated: ptr %p -> %p cap %d -> %d", firstPtr, got, firstCap, cap(scratch.values))
	}
}

func TestTypedColumnAdapterPrepareInt64AggregateValidationFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	part := typedColumnAdapterBuildPart(t, field, []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 10}})
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	stringField := typedColumnAdapterField("count", ColumnStoreValueString)
	stringPart := typedColumnAdapterBuildPart(t, stringField, []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "ten"}})
	stringImage, err := stringPart.buildImage()
	if err != nil {
		t.Fatalf("string buildImage: %v", err)
	}
	missingPrimary := typedColumnAdapterBuildCustomInt64AggregateImage(t, []typedcolumn.ColumnDefinition{
		{Name: "id", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true},
		{Name: "count", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone, CompressionSet: true},
	}, "id", 2)
	primaryEncodingMismatch := typedColumnAdapterBuildCustomInt64AggregateImage(t, []typedcolumn.ColumnDefinition{
		{Name: typedColumnAdapterPrimaryIDColumn, Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingNullableInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true},
		{Name: "count", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone, CompressionSet: true},
	}, typedColumnAdapterPrimaryIDColumn, 2)
	selectedEncodingMismatch := typedColumnAdapterBuildCustomInt64AggregateImage(t, []typedcolumn.ColumnDefinition{
		{Name: typedColumnAdapterPrimaryIDColumn, Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true},
		{Name: "count", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingNullableInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true},
	}, typedColumnAdapterPrimaryIDColumn, 2)
	fields := []TypedStorageField{field}
	for _, tc := range []struct {
		name         string
		raw          []byte
		refPartID    uint64
		typedRows    int
		physicalRows int
		schemaHash   uint64
		wantErr      string
	}{
		{name: "schema_hash_mismatch", raw: image.Bytes, refPartID: image.PartID, typedRows: image.Rows, physicalRows: image.Rows, schemaHash: uint64(part.Part.Descriptor.SchemaVersion + 1), wantErr: "schema_version"},
		{name: "part_id_mismatch", raw: image.Bytes, refPartID: image.PartID + 1, typedRows: image.Rows, physicalRows: image.Rows, schemaHash: uint64(part.Part.Descriptor.SchemaVersion), wantErr: "image/ref mismatch"},
		{name: "typed_rows_mismatch", raw: image.Bytes, refPartID: image.PartID, typedRows: image.Rows + 1, physicalRows: image.Rows, schemaHash: uint64(part.Part.Descriptor.SchemaVersion), wantErr: "image/ref mismatch"},
		{name: "physical_rows_mismatch", raw: image.Bytes, refPartID: image.PartID, typedRows: image.Rows, physicalRows: image.Rows + 1, schemaHash: uint64(part.Part.Descriptor.SchemaVersion), wantErr: "image/ref mismatch"},
		{name: "column_schema_mismatch", raw: stringImage.Bytes, refPartID: stringImage.PartID, typedRows: stringImage.Rows, physicalRows: stringImage.Rows, schemaHash: uint64(stringPart.Part.Descriptor.SchemaVersion), wantErr: "schema mismatch"},
		{name: "missing_primary_column", raw: missingPrimary.Bytes, refPartID: missingPrimary.PartID, typedRows: missingPrimary.Rows, physicalRows: missingPrimary.Rows, schemaHash: uint64(missingPrimary.PartID), wantErr: "missing primary-id column"},
		{name: "primary_column_encoding_mismatch", raw: primaryEncodingMismatch.Bytes, refPartID: primaryEncodingMismatch.PartID, typedRows: primaryEncodingMismatch.Rows, physicalRows: primaryEncodingMismatch.Rows, schemaHash: uint64(primaryEncodingMismatch.PartID), wantErr: "primary-id column"},
		{name: "selected_column_encoding_mismatch", raw: selectedEncodingMismatch.Bytes, refPartID: selectedEncodingMismatch.PartID, typedRows: selectedEncodingMismatch.Rows, physicalRows: selectedEncodingMismatch.Rows, schemaHash: uint64(selectedEncodingMismatch.PartID), wantErr: "encoding=nullable_int64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := typedColumnAdapterPrepareInt64PredicateAggregatePart(fields, tc.raw, tc.refPartID, tc.typedRows, tc.physicalRows, tc.schemaHash, "count")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("prepare aggregate err=%v want %q", err, tc.wantErr)
			}
		})
	}
}

func TestTypedColumnAdapterPrepareInt64AggregateTargetedMetadataSectionsFailClosed(t *testing.T) {
	countField := typedColumnAdapterField("count", ColumnStoreValueInt64)
	kindField := typedColumnAdapterField("kind", ColumnStoreValueString)
	fields := []TypedStorageField{countField, kindField}
	rows := []typedColumnAdapterRow{
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 10}, "kind": {Type: ColumnStoreValueString, Present: true, String: "alpha"}}},
		{PrimaryID: 2, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 20}, "kind": {Type: ColumnStoreValueString, Present: true, String: "beta"}}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 78, RowsPerGranule: 2, Fields: fields}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	descriptor := typedColumnAdapterFindSection(t, image, typedcolumn.ColumnPartImageSectionDescriptor)
	descriptorRaw := image.Bytes[descriptor.Offset : descriptor.Offset+descriptor.Length]
	missingKindSection := image
	missingKindSection.Sections = make([]typedcolumn.ColumnPartImageSection, 0, len(image.Sections))
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == "kind" {
			continue
		}
		missingKindSection.Sections = append(missingKindSection.Sections, section)
	}
	_, err = typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromSections(fields, missingKindSection, descriptorRaw, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "count", TypedColumnInt64PredicateScanRequest{Kind: TypedColumnInt64PredicateAll})
	if err == nil || !strings.Contains(err.Error(), "missing column data section") {
		t.Fatalf("targeted aggregate missing unrelated column-data section err=%v want fail-closed missing section", err)
	}

	unexpectedSection := image
	unexpectedSection.Sections = append([]typedcolumn.ColumnPartImageSection(nil), image.Sections...)
	ghost := typedColumnAdapterFindColumnSection(t, image, "count")
	ghost.Column = "ghost"
	unexpectedSection.Sections = append(unexpectedSection.Sections, ghost)
	_, err = typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromSections(fields, unexpectedSection, descriptorRaw, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "count", TypedColumnInt64PredicateScanRequest{Kind: TypedColumnInt64PredicateAll})
	if err == nil || !strings.Contains(err.Error(), "unexpected column data section") {
		t.Fatalf("targeted aggregate unexpected column-data section err=%v want fail-closed unexpected section", err)
	}

	badLength := image
	badLength.Sections = append([]typedcolumn.ColumnPartImageSection(nil), image.Sections...)
	for i := range badLength.Sections {
		if badLength.Sections[i].Kind == typedcolumn.ColumnPartImageSectionColumnData && badLength.Sections[i].Column == "kind" {
			badLength.Sections[i].Length++
			break
		}
	}
	_, err = typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromSections(fields, badLength, descriptorRaw, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "count", TypedColumnInt64PredicateScanRequest{Kind: TypedColumnInt64PredicateAll})
	if err == nil || !strings.Contains(err.Error(), "section length") {
		t.Fatalf("targeted aggregate bad non-selected column-data length err=%v want fail-closed length mismatch", err)
	}
}

func TestTypedColumnAdapterPrepareInt64AggregateTargetedRestoresSectionCompressionBeforeValidation(t *testing.T) {
	const (
		partID       = uint64(2297)
		rowsPerBlock = 512
		rows         = rowsPerBlock * 2
	)
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	primaryIDs := make([]int64, rows)
	counts := make([]int64, rows)
	x := uint64(0x9e3779b97f4a7c15)
	for row := 0; row < rows; row++ {
		primaryIDs[row] = int64(row + 1)
		if row < rowsPerBlock {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			counts[row] = int64(x)
			continue
		}
		counts[row] = 7
	}
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{
		SchemaVersion: uint32(partID),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			{Name: typedColumnAdapterPrimaryIDColumn, Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true},
			{Name: "count", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionLZ4, CompressionSet: true},
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: rowsPerBlock, DefaultCodecBlockRows: rowsPerBlock},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{
		Rows: rows,
		Columns: map[string][]int64{
			typedColumnAdapterPrimaryIDColumn: primaryIDs,
			"count":                           counts,
		},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	countColumn := part.Columns["count"]
	if len(countColumn.Blocks) != 2 {
		t.Fatalf("count blocks=%d want 2", len(countColumn.Blocks))
	}
	if countColumn.Blocks[0].Descriptor.Compression != typedcolumn.CompressionNone || countColumn.Blocks[1].Descriptor.Compression != typedcolumn.CompressionLZ4 {
		t.Fatalf("count block compression=%s/%s want none/lz4", countColumn.Blocks[0].Descriptor.Compression, countColumn.Blocks[1].Descriptor.Compression)
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	countSection := typedColumnAdapterFindColumnSection(t, image, "count")
	if countSection.Compression != typedcolumn.CompressionLZ4 {
		t.Fatalf("count section compression=%s want lz4", countSection.Compression)
	}
	descriptor := typedColumnAdapterFindSection(t, image, typedcolumn.ColumnPartImageSectionDescriptor)
	descriptorRaw := image.Bytes[descriptor.Offset : descriptor.Offset+descriptor.Length]
	targeted, err := typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromSections([]TypedStorageField{field}, image, descriptorRaw, image.PartID, image.Rows, image.Rows, uint64(part.Descriptor.SchemaVersion), "count", TypedColumnInt64PredicateScanRequest{Kind: TypedColumnInt64PredicateAll})
	if err != nil {
		t.Fatalf("prepare targeted aggregate: %v", err)
	}
	if got := targeted.adapterColumn.Definition.Compression; got != typedcolumn.CompressionLZ4 {
		t.Fatalf("targeted adapter compression=%s want lz4", got)
	}
	if got := targeted.adapterPart.Part.Columns["count"].Definition.Compression; got != typedcolumn.CompressionLZ4 {
		t.Fatalf("targeted part column compression=%s want lz4", got)
	}
}

func TestTypedColumnAdapterPrepareInt64AggregateTargetedSkipsCorruptPrunedPayload(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	fields := []TypedStorageField{field}
	rows := []typedColumnAdapterRow{
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 1}}},
		{PrimaryID: 2, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 2}}},
		{PrimaryID: 3, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 100}}},
		{PrimaryID: 4, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 101}}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 79, RowsPerGranule: 2, Fields: fields}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	req := TypedColumnInt64PredicateScanRequest{Kind: TypedColumnInt64PredicateRange, Low: 1, High: 2}
	raw := bytes.Clone(image.Bytes)
	readRange := func(data []byte, corruptOffset int) typedColumnInt64AggregateRangeReader {
		return func(offset int, length int, section bool) ([]byte, error) {
			if offset < 0 || length <= 0 || offset+length > len(data) {
				return nil, fmt.Errorf("range offset=%d length=%d outside bytes=%d", offset, length, len(data))
			}
			if !section && corruptOffset >= 0 && offset <= corruptOffset && corruptOffset < offset+length {
				return nil, fmt.Errorf("pruned payload range was read offset=%d length=%d corrupt_offset=%d", offset, length, corruptOffset)
			}
			return data[offset : offset+length], nil
		}
	}
	targeted, err := typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromRanges(fields, int64(len(raw)), image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "count", req, readRange(raw, -1))
	if err != nil {
		t.Fatalf("prepare targeted metadata: %v", err)
	}
	section := typedColumnAdapterFindColumnSection(t, image, "count")
	candidate := make([]bool, section.Length)
	for _, blockRange := range targeted.blockRanges {
		for off := blockRange.offset; off < blockRange.offset+blockRange.length; off++ {
			if off >= section.Offset && off < section.Offset+section.Length {
				candidate[off-section.Offset] = true
			}
		}
	}
	corruptOffset := -1
	for i, isCandidate := range candidate {
		if !isCandidate {
			corruptOffset = section.Offset + i
			break
		}
	}
	if corruptOffset < 0 {
		t.Fatalf("no pruned byte found section=%+v block_ranges=%+v", section, targeted.blockRanges)
	}
	corruptRaw := bytes.Clone(raw)
	corruptRaw[corruptOffset] ^= 0xff
	targeted, err = typedColumnAdapterPrepareInt64PredicateAggregateTargetedPartFromRanges(fields, int64(len(corruptRaw)), image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "count", req, readRange(corruptRaw, corruptOffset))
	if err != nil {
		t.Fatalf("prepare targeted metadata with corrupt pruned payload: %v", err)
	}
	adapterPart, adapterColumn, err := targeted.instantiate(func(offset int, length int) ([]byte, error) {
		return readRange(corruptRaw, corruptOffset)(offset, length, false)
	})
	if err != nil {
		t.Fatalf("instantiate targeted part: %v", err)
	}
	var result TypedColumnInt64PredicateAggregateResult
	partPruned, err := scanTypedColumnInt64PredicateAggregatePart(adapterPart.Part, adapterColumn.Definition.Name, req, &result)
	if err != nil {
		t.Fatalf("scan targeted part: %v", err)
	}
	if partPruned || result.Count != 2 || result.Sum != 3 || result.Diagnostics.BlocksPruned == 0 || result.Diagnostics.BlocksDecoded == 0 {
		t.Fatalf("partPruned=%v result=%+v diagnostics=%+v want corrupt pruned payload skipped", partPruned, result, result.Diagnostics)
	}
}

func TestTypedColumnAdapterAmbiguousRowKeysFailClosed(t *testing.T) {
	field := TypedStorageField{Name: "count", Path: "metrics.count", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueInt64}
	rows := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{
		"count":         {Type: ColumnStoreValueInt64, Present: true, Int64: 10},
		"metrics.count": {Type: ColumnStoreValueInt64, Present: true, Int64: 20},
	}}}
	if _, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, rows); err == nil || !strings.Contains(err.Error(), "ambiguous field keys") {
		t.Fatalf("build ambiguous row err=%v want ambiguous field keys", err)
	}
}

func TestTypedColumnAdapterMissingDeclaredValueTypeFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	rows := []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{
		"count": {Present: true, Int64: 10},
	}}}
	if _, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, rows); err == nil || !strings.Contains(err.Error(), "declared type required") {
		t.Fatalf("build missing declared type err=%v want declared type required", err)
	}
}

func TestTypedColumnAdapterUnsupportedTypeFailsClosed(t *testing.T) {
	field := typedColumnAdapterField("future", ColumnStoreValueType("decimal128"))
	if _, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{field}}, nil); !errors.Is(err, errTypedColumnAdapterUnsupportedType) {
		t.Fatalf("build unsupported err=%v want errTypedColumnAdapterUnsupportedType", err)
	}
	missing := typedColumnAdapterField("missing", ColumnStoreValueInt64)
	_, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1, Fields: []TypedStorageField{missing}}, []typedColumnAdapterRow{{PrimaryID: 1, Values: nil}})
	if err == nil || !strings.Contains(err.Error(), "missing field") {
		t.Fatalf("build missing field err=%v want missing field", err)
	}
}

var typedColumnAdapterBenchmarkSink columnDeclaredValue
var typedColumnAdapterAdjacencyBenchSink uint64

func BenchmarkTypedColumnAdjacencyDenseFallbackScan(b *testing.B) {
	const rowsN = 8192
	const degree = 16
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyDegree = degree
	rows := make([]typedColumnAdapterRow, rowsN)
	for i := range rows {
		neighbors := make([]uint32, degree)
		for j := range neighbors {
			neighbors[j] = uint32((i + j) & 0xffff)
		}
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{
			"neighbors": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: neighbors},
		}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 77, RowsPerGranule: rowsN, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		b.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		b.Fatalf("buildImage: %v", err)
	}
	path := filepath.Join(b.TempDir(), "part.tcs1")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		b.Fatalf("write image: %v", err)
	}
	column, ok := part.columnByName("neighbors")
	if !ok {
		b.Fatalf("missing adapter column")
	}
	mgr := mappedresource.NewManager()
	reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-adjacency-bench", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: true}
	view, err := typedColumnAdapterAcquireDenseUint32ColumnView(reader, column, image.Rows)
	if err != nil {
		b.Fatalf("AcquireDenseUint32ColumnView: %v", err)
	}
	if view.Handle != nil {
		defer view.Handle.Release()
	}
	if view.Direct || view.Handle != nil {
		b.Fatalf("expected fallback-only adjacency view direct=%v handle=%v", view.Direct, view.Handle != nil)
	}
	values := view.Values
	var sink uint64
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum uint64
		for _, value := range values {
			sum += uint64(value)
		}
		sink += sum
	}
	typedColumnAdapterAdjacencyBenchSink = sink
}

func BenchmarkTypedColumnAdapterUint32OffsetsListDirectReader1916(b *testing.B) {
	const rowsN = 8192
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	rows := make([]typedColumnAdapterRow, rowsN)
	for i := range rows {
		degree := i % 9
		neighbors := make([]uint32, degree)
		for j := range neighbors {
			neighbors[j] = uint32((i*17 + j) & 0xffff)
		}
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{
			"neighbors": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: neighbors},
		}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1916, RowsPerGranule: rowsN, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		b.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		b.Fatalf("buildImage: %v", err)
	}
	column, ok := part.columnByName("neighbors")
	if !ok {
		b.Fatalf("missing adapter column")
	}
	offsetsSection, valuesSection, ok := typedColumnAdapterColumnOffsetsListSections(image, "neighbors")
	if !ok {
		b.Fatalf("missing offsets-list sections")
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		b.Fatalf("offsets SectionBytes: %v", err)
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		b.Fatalf("values SectionBytes: %v", err)
	}
	path := filepath.Join(b.TempDir(), "offsets-list.tcs1")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		b.Fatalf("write image: %v", err)
	}
	reader := typedColumnAdapterResourceReader{Image: image, Path: path, Namespace: "typed-column-offsets-list-bench", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: false}

	b.Run("mmap_direct_open_prepare", func(b *testing.B) {
		mgr := mappedresource.NewManager()
		benchReader := reader
		benchReader.Manager = mgr
		b.ReportAllocs()
		b.ResetTimer()
		var mmapDirect uint64
		for i := 0; i < b.N; i++ {
			view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(benchReader, column, image.Rows)
			if err != nil {
				if strings.Contains(err.Error(), "mmap unsupported") {
					b.Skipf("mmap direct view unsupported on this platform: %v", err)
				}
				b.Fatalf("AcquireUint32OffsetsListColumnView: %v", err)
			}
			if !view.Direct {
				_ = view.Close()
				b.Skipf("mmap direct view unavailable; fallback class=%+v", view.Class)
			}
			mmapDirect++
			if err := view.Close(); err != nil {
				b.Fatalf("Close: %v", err)
			}
		}
		b.ReportMetric(float64(mmapDirect)/float64(b.N), "mmap_direct/op")
		b.ReportMetric(0, "heap_copy_typed_view/op")
		b.ReportMetric(0, "scratch_decode/op")
	})

	b.Run("scratch_fallback_decode_prepare", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var scratchDecode uint64
		for i := 0; i < b.N; i++ {
			decoded, err := typedcolumn.DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, image.Rows)
			if err != nil {
				b.Fatalf("DecodeRawUint32OffsetsListFallback: %v", err)
			}
			scratchDecode++
			typedColumnAdapterAdjacencyBenchSink += uint64(len(decoded.Offsets) + len(decoded.Values))
		}
		b.ReportMetric(0, "mmap_direct/op")
		b.ReportMetric(0, "heap_copy_typed_view/op")
		b.ReportMetric(float64(scratchDecode)/float64(b.N), "scratch_decode/op")
	})

	b.Run("mmap_direct_iterate_rows", func(b *testing.B) {
		mgr := mappedresource.NewManager()
		benchReader := reader
		benchReader.Manager = mgr
		view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(benchReader, column, image.Rows)
		if err != nil {
			if strings.Contains(err.Error(), "mmap unsupported") {
				b.Skipf("mmap direct view unsupported on this platform: %v", err)
			}
			b.Fatalf("AcquireUint32OffsetsListColumnView: %v", err)
		}
		defer view.Close()
		if !view.Direct {
			b.Skipf("mmap direct view unavailable; fallback class=%+v", view.Class)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(view.Values) * 4))
		b.ResetTimer()
		var sink uint64
		var mmapDirect uint64
		for i := 0; i < b.N; i++ {
			mmapDirect++
			sink += typedColumnAdapterSumUint32OffsetsListRows(view.Offsets, view.Values, view.Rows)
		}
		b.ReportMetric(float64(mmapDirect)/float64(b.N), "mmap_direct/op")
		b.ReportMetric(0, "heap_copy_typed_view/op")
		b.ReportMetric(0, "scratch_decode/op")
		typedColumnAdapterAdjacencyBenchSink = sink
	})

	b.Run("scratch_fallback_iterate_rows", func(b *testing.B) {
		decoded, err := typedcolumn.DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, image.Rows)
		if err != nil {
			b.Fatalf("DecodeRawUint32OffsetsListFallback: %v", err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(decoded.Values) * 4))
		b.ResetTimer()
		var sink uint64
		for i := 0; i < b.N; i++ {
			sink += typedColumnAdapterSumUint32OffsetsListRows(decoded.Offsets, decoded.Values, decoded.Rows)
		}
		b.ReportMetric(0, "mmap_direct/op")
		b.ReportMetric(0, "heap_copy_typed_view/op")
		b.ReportMetric(0, "scratch_decode/op")
		typedColumnAdapterAdjacencyBenchSink = sink
	})
}

func BenchmarkTypedColumnAdapterUint32ListPrimitive1985(b *testing.B) {
	const rowsN = 8192
	field := typedColumnAdapterField("tags", ColumnStoreValueUint32List)
	rows := make([]typedColumnAdapterRow, rowsN)
	totalValues := 0
	for i := range rows {
		count := i % 9
		values := make([]uint32, count)
		for j := range values {
			values[j] = uint32((i*17 + j) & 0xffff)
		}
		totalValues += len(values)
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{
			"tags": {Type: ColumnStoreValueUint32List, Present: true, Uint32List: values},
		}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1985, RowsPerGranule: rowsN, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		b.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		b.Fatalf("buildImage: %v", err)
	}
	column, ok := part.columnByName("tags")
	if !ok {
		b.Fatalf("missing adapter column")
	}
	offsetsSection, valuesSection, ok := typedColumnAdapterColumnOffsetsListSections(image, "tags")
	if !ok {
		b.Fatalf("missing offsets-list sections")
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		b.Fatalf("offsets SectionBytes: %v", err)
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		b.Fatalf("values SectionBytes: %v", err)
	}
	path := filepath.Join(b.TempDir(), "uint32-list-primitive.tcs1")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		b.Fatalf("write image: %v", err)
	}
	readBytes := offsetsSection.Length + valuesSection.Length
	reportShape := func(b *testing.B, elapsedSeconds float64) {
		b.Helper()
		b.ReportMetric(float64(rowsN), "rows/op")
		b.ReportMetric(float64(totalValues), "values/op")
		b.ReportMetric(float64(readBytes), "read_bytes/op")
		if elapsedSeconds > 0 {
			b.ReportMetric(float64(b.N)/elapsedSeconds, "ops/sec")
		}
	}

	b.Run("fallback_decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(readBytes))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoded, err := typedcolumn.DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, image.Rows)
			if err != nil {
				b.Fatalf("DecodeRawUint32OffsetsListFallback: %v", err)
			}
			typedColumnAdapterAdjacencyBenchSink += uint64(len(decoded.Offsets) + len(decoded.Values))
		}
		b.StopTimer()
		reportShape(b, b.Elapsed().Seconds())
	})

	b.Run("direct_open_validate", func(b *testing.B) {
		mgr := mappedresource.NewManager()
		reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-uint32-list-primitive-bench", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: false}
		b.ReportAllocs()
		b.SetBytes(int64(readBytes))
		b.ResetTimer()
		var direct uint64
		for i := 0; i < b.N; i++ {
			view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(reader, column, image.Rows)
			if err != nil {
				if strings.Contains(err.Error(), "mmap unsupported") {
					b.Skipf("mmap direct view unsupported on this platform: %v", err)
				}
				b.Fatalf("AcquireUint32OffsetsListColumnView: %v", err)
			}
			if !view.Direct {
				_ = view.Close()
				b.Skipf("mmap direct view unavailable; fallback class=%+v", view.Class)
			}
			direct++
			if err := view.Close(); err != nil {
				b.Fatalf("Close: %v", err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(direct)/float64(b.N), "mmap_direct/op")
		reportShape(b, b.Elapsed().Seconds())
	})

	b.Run("row_slice_access_preopened", func(b *testing.B) {
		mgr := mappedresource.NewManager()
		reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-uint32-list-primitive-bench-preopened", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: false}
		view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(reader, column, image.Rows)
		if err != nil {
			if strings.Contains(err.Error(), "mmap unsupported") {
				b.Skipf("mmap direct view unsupported on this platform: %v", err)
			}
			b.Fatalf("AcquireUint32OffsetsListColumnView: %v", err)
		}
		defer view.Close()
		if !view.Direct {
			b.Skipf("mmap direct view unavailable; fallback class=%+v", view.Class)
		}
		b.ReportAllocs()
		b.SetBytes(int64(readBytes))
		b.ResetTimer()
		var sink uint64
		for i := 0; i < b.N; i++ {
			sink += typedColumnAdapterSumUint32OffsetsListRows(view.Offsets, view.Values, view.Rows)
		}
		b.StopTimer()
		reportShape(b, b.Elapsed().Seconds())
		typedColumnAdapterAdjacencyBenchSink = sink
	})
}

func BenchmarkTypedColumnAdapterVariableAdjacencyScan1917(b *testing.B) {
	const rowsN = 8192
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	rows := make([]typedColumnAdapterRow, rowsN)
	totalValues := 0
	for i := range rows {
		degree := i % 9
		neighbors := make([]uint32, degree)
		for j := range neighbors {
			neighbors[j] = uint32((i*17 + j) & 0xffff)
		}
		totalValues += len(neighbors)
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{
			"neighbors": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: neighbors},
		}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1917, RowsPerGranule: rowsN, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		b.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		b.Fatalf("buildImage: %v", err)
	}
	column, ok := part.columnByName("neighbors")
	if !ok {
		b.Fatalf("missing adapter column")
	}
	offsetsSection, valuesSection, ok := typedColumnAdapterColumnOffsetsListSections(image, "neighbors")
	if !ok {
		b.Fatalf("missing offsets-list sections")
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		b.Fatalf("offsets SectionBytes: %v", err)
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		b.Fatalf("values SectionBytes: %v", err)
	}
	path := filepath.Join(b.TempDir(), "variable-adjacency.tcs1")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		b.Fatalf("write image: %v", err)
	}
	readBytes := offsetsSection.Length + valuesSection.Length
	reportShape := func(b *testing.B) {
		b.Helper()
		b.ReportMetric(float64(rowsN), "rows/op")
		b.ReportMetric(float64(totalValues), "values/op")
		b.ReportMetric(float64(readBytes), "read_bytes/op")
		b.ReportMetric(float64(image.TotalBytes()), "storage_B/op")
		b.ReportMetric(float64(image.PaddingBytes()), "padding_B/op")
		b.ReportMetric(float64(offsetsSection.Length), "offsets_storage_B/op")
		b.ReportMetric(float64(valuesSection.Length), "values_storage_B/op")
	}

	b.Run("mmap_direct_scan_preopened", func(b *testing.B) {
		mgr := mappedresource.NewManager()
		reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-variable-adjacency-bench", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: false}
		view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(reader, column, image.Rows)
		if err != nil {
			if strings.Contains(err.Error(), "mmap unsupported") {
				b.Skipf("mmap direct view unsupported on this platform: %v", err)
			}
			b.Fatalf("AcquireUint32OffsetsListColumnView: %v", err)
		}
		defer view.Close()
		if !view.Direct {
			b.Skipf("mmap direct view unavailable; fallback class=%+v", view.Class)
		}
		b.ReportAllocs()
		b.SetBytes(int64(readBytes))
		// Timer boundary: setup/write/open/certification are complete; timed work is row iteration over the prepared mmap direct view only.
		b.ResetTimer()
		var sink uint64
		for i := 0; i < b.N; i++ {
			sink += typedColumnAdapterSumUint32OffsetsListRows(view.Offsets, view.Values, view.Rows)
		}
		b.StopTimer()
		elapsed := b.Elapsed().Seconds()
		reportShape(b)
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
		b.ReportMetric(1, "adjacency_mmap_direct/op")
		b.ReportMetric(0, "adjacency_heap_copy_typed_view/op")
		b.ReportMetric(0, "adjacency_scratch_decode/op")
		typedColumnAdapterAdjacencyBenchSink = sink
	})

	b.Run("heap_copy_typed_view_scan_preopened", func(b *testing.B) {
		mgr := mappedresource.NewManager()
		reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-variable-adjacency-bench-heap", PartID: image.PartID, PreferMapped: false, AllowHeapCopy: true}
		view, err := typedColumnAdapterAcquireUint32OffsetsListColumnView(reader, column, image.Rows)
		if err != nil {
			b.Fatalf("AcquireUint32OffsetsListColumnView heap copy: %v", err)
		}
		defer view.Close()
		if !view.HeapCopy || view.Direct || view.Scratch {
			b.Fatalf("view=%+v want heap-copy typed view, not mmap direct or scratch", view)
		}
		b.ReportAllocs()
		b.SetBytes(int64(readBytes))
		// Timer boundary: setup/write/open/certification/heap copy are complete; timed work is row iteration over the prepared heap-copy typed view only.
		b.ResetTimer()
		var sink uint64
		for i := 0; i < b.N; i++ {
			sink += typedColumnAdapterSumUint32OffsetsListRows(view.Offsets, view.Values, view.Rows)
		}
		b.StopTimer()
		elapsed := b.Elapsed().Seconds()
		reportShape(b)
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
		b.ReportMetric(0, "adjacency_mmap_direct/op")
		b.ReportMetric(1, "adjacency_heap_copy_typed_view/op")
		b.ReportMetric(0, "adjacency_scratch_decode/op")
		typedColumnAdapterAdjacencyBenchSink = sink
	})

	b.Run("scratch_fallback_decode_and_scan", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(readBytes))
		// Timer boundary: setup/write/open are excluded; timed work is fallback decode plus row iteration.
		b.ResetTimer()
		var sink uint64
		for i := 0; i < b.N; i++ {
			decoded, err := typedcolumn.DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, image.Rows)
			if err != nil {
				b.Fatalf("DecodeRawUint32OffsetsListFallback: %v", err)
			}
			sink += typedColumnAdapterSumUint32OffsetsListRows(decoded.Offsets, decoded.Values, decoded.Rows)
		}
		b.StopTimer()
		elapsed := b.Elapsed().Seconds()
		reportShape(b)
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
		b.ReportMetric(0, "adjacency_mmap_direct/op")
		b.ReportMetric(0, "adjacency_heap_copy_typed_view/op")
		b.ReportMetric(1, "adjacency_scratch_decode/op")
		typedColumnAdapterAdjacencyBenchSink = sink
	})
}

func typedColumnAdapterSumUint32OffsetsListRows(offsets []uint64, values []uint32, rows int) uint64 {
	var sum uint64
	for row := 0; row < rows; row++ {
		begin := int(offsets[row])
		end := int(offsets[row+1])
		for _, value := range values[begin:end] {
			sum += uint64(value)
		}
	}
	return sum
}

func BenchmarkTypedColumnAdapterNullableScanValues(b *testing.B) {
	const rowCount = 8192
	field := typedColumnAdapterNullableField("count", ColumnStoreValueInt64)
	rows := make([]typedColumnAdapterRow, rowCount)
	for i := range rows {
		value := columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Int64: int64(i * 7)}
		values := map[string]columnDeclaredValue{"count": value}
		switch {
		case i%17 == 0:
			values["count"] = columnDeclaredValue{Type: ColumnStoreValueInt64, Present: true, Null: true}
		case i%19 == 0:
			values = nil
		}
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: values}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 77, RowsPerGranule: rowCount, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		b.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	column, ok := part.columnByName("count")
	if !ok {
		b.Fatalf("missing adapter column")
	}
	b.Run("materialize_baseline", func(b *testing.B) {
		got, err := part.scanNullableColumnValues(column)
		if err != nil {
			b.Fatalf("warm scanNullableColumnValues: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, err = part.scanNullableColumnValues(column)
			if err != nil {
				b.Fatalf("scanNullableColumnValues: %v", err)
			}
		}
		typedColumnAdapterBenchmarkSink = got[rowCount-1]
	})

	b.Run("scratch_final", func(b *testing.B) {
		var scratch typedColumnAdapterNullableScanScratch
		dst := make([]columnDeclaredValue, rowCount)
		dst, err = part.scanNullableColumnValuesInto(column, dst[:0], &scratch)
		if err != nil {
			b.Fatalf("warm scanNullableColumnValuesInto: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst, err = part.scanNullableColumnValuesInto(column, dst[:0], &scratch)
			if err != nil {
				b.Fatalf("scanNullableColumnValuesInto: %v", err)
			}
		}
		typedColumnAdapterBenchmarkSink = dst[rowCount-1]
	})
}

func typedColumnAdapterField(name string, valueType ColumnStoreValueType) TypedStorageField {
	return TypedStorageField{Name: name, Path: name, Owner: TypedStorageOwnerColumnPart, ValueType: valueType}
}

func typedColumnAdapterNullableField(name string, valueType ColumnStoreValueType) TypedStorageField {
	field := typedColumnAdapterField(name, valueType)
	field.Nullable = true
	return field
}

func nullableInt64HeaderBytesForTest() int { return 21 }

func typedColumnAdapterRoundTrip(t *testing.T, field TypedStorageField, values []columnDeclaredValue) []columnDeclaredValue {
	t.Helper()
	part := typedColumnAdapterBuildPart(t, field, values)
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	parsed, err := typedColumnAdapterPartFromImage(part.Options, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	got, err := parsed.scanColumnValues(field.Name)
	if err != nil {
		t.Fatalf("scanColumnValues(%s): %v", field.Name, err)
	}
	return got
}

func typedColumnAdapterBuildPart(t *testing.T, field TypedStorageField, values []columnDeclaredValue) *typedColumnAdapterPart {
	t.Helper()
	rows := make([]typedColumnAdapterRow, len(values))
	for i, value := range values {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{field.Path: value}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 42, RowsPerGranule: 2, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	return part
}

func typedColumnAdapterBuildCustomInt64AggregateImage(t *testing.T, defs []typedcolumn.ColumnDefinition, primaryKey string, rows int) typedcolumn.ColumnPartImage {
	t.Helper()
	const partID = uint64(99)
	batch := typedcolumn.Batch{Rows: rows, Columns: make(map[string][]int64, len(defs))}
	for i := range defs {
		if !defs[i].CompressionSet {
			defs[i].Compression = typedcolumn.CompressionNone
			defs[i].CompressionSet = true
		}
		values := make([]int64, rows)
		for row := range values {
			values[row] = int64(row + 1)
			if defs[i].Name == "count" {
				values[row] = int64((row + 1) * 10)
			}
		}
		batch.Columns[defs[i].Name] = values
	}
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{
		SchemaVersion: uint32(partID),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns:       defs,
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{
			Columns: []string{primaryKey},
		},
		SortKey:    typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: primaryKey}}},
		PartPolicy: typedcolumn.ColumnPartPolicy{RowsPerGranule: rows},
		Compression: typedcolumn.ColumnCompressionPolicy{
			Default: typedcolumn.CompressionNone,
		},
	}, batch)
	if err != nil {
		t.Fatalf("BuildColumnPart custom image: %v", err)
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage custom image: %v", err)
	}
	return image
}

func typedColumnAdapterFindColumnSection(t *testing.T, image typedcolumn.ColumnPartImage, column string) typedcolumn.ColumnPartImageSection {
	t.Helper()
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == column {
			return section
		}
	}
	t.Fatalf("missing column data section %q in %+v", column, image.Sections)
	return typedcolumn.ColumnPartImageSection{}
}

func typedColumnAdapterFindSection(t *testing.T, image typedcolumn.ColumnPartImage, kind typedcolumn.ColumnPartImageSectionKind) typedcolumn.ColumnPartImageSection {
	t.Helper()
	for _, section := range image.Sections {
		if section.Kind == kind {
			return section
		}
	}
	t.Fatalf("missing section %q in %+v", kind, image.Sections)
	return typedcolumn.ColumnPartImageSection{}
}

func typedColumnAdapterAcquireBytes(t *testing.T, mgr *mappedresource.Manager, scope mappedresource.Scope, data []byte, kind string) *mappedresource.Handle {
	t.Helper()
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: scope.Namespace, Kind: kind, FileID: 1, Length: int64(len(data))}
	h, err := mgr.AcquireBytes(key, scope, mappedresource.SourceMapped, data, mappedresource.AcquireOptions{Reason: kind, ValidationMode: mappedresource.ValidationVerify})
	if err != nil {
		t.Fatalf("AcquireBytes(%s): %v", kind, err)
	}
	return h
}

func typedColumnAdapterAcquireBytesSource(t *testing.T, mgr *mappedresource.Manager, data []byte, source mappedresource.Source, kind string) *mappedresource.Handle {
	t.Helper()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "typed-column-adapter-offsets-list", Namespace: "typed-column-adapter-test"}
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: scope.Namespace, Kind: kind, FileID: 1, Length: int64(len(data))}
	h, err := mgr.AcquireBytes(key, scope, source, data, mappedresource.AcquireOptions{Reason: kind, ValidationMode: mappedresource.ValidationVerify})
	if err != nil {
		t.Fatalf("AcquireBytes(%s/%s): %v", kind, source, err)
	}
	return h
}

func typedColumnAdapterOffsetsListDirectFixture(t *testing.T) (*typedColumnAdapterPart, typedColumnAdapterColumn, typedcolumn.ColumnPartImage, typedcolumn.ColumnPartImageSection, typedcolumn.ColumnPartImageSection, []byte, []byte) {
	t.Helper()
	field := typedColumnAdapterField("neighbors", ColumnStoreValueAdjacencyList)
	field.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: nil},
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{7, 8}},
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: nil},
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{9, 10, 11}},
	}
	return typedColumnAdapterOffsetsListDirectFixtureForValues(t, field, values)
}

func typedColumnAdapterUint32ListDirectFixture(t *testing.T) (*typedColumnAdapterPart, typedColumnAdapterColumn, typedcolumn.ColumnPartImage, typedcolumn.ColumnPartImageSection, typedcolumn.ColumnPartImageSection, []byte, []byte) {
	t.Helper()
	field := typedColumnAdapterField("tags", ColumnStoreValueUint32List)
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueUint32List, Present: true, Uint32List: nil},
		{Type: ColumnStoreValueUint32List, Present: true, Uint32List: []uint32{7, 8}},
		{Type: ColumnStoreValueUint32List, Present: true, Uint32List: nil},
		{Type: ColumnStoreValueUint32List, Present: true, Uint32List: []uint32{9, 10, 11}},
	}
	return typedColumnAdapterOffsetsListDirectFixtureForValues(t, field, values)
}

func typedColumnAdapterOffsetsListDirectFixtureForValues(t *testing.T, field TypedStorageField, values []columnDeclaredValue) (*typedColumnAdapterPart, typedColumnAdapterColumn, typedcolumn.ColumnPartImage, typedcolumn.ColumnPartImageSection, typedcolumn.ColumnPartImageSection, []byte, []byte) {
	t.Helper()
	part := typedColumnAdapterBuildPart(t, field, values)
	column, ok := part.columnByName(field.Name)
	if !ok {
		t.Fatalf("missing %s column", field.Name)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage offsets-list: %v", err)
	}
	offsetsSection, valuesSection, ok := typedColumnAdapterColumnOffsetsListSections(image, field.Name)
	if !ok {
		t.Fatalf("missing offsets-list sections in %+v", image.Sections)
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		t.Fatalf("offsets SectionBytes: %v", err)
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		t.Fatalf("values SectionBytes: %v", err)
	}
	return part, column, image, offsetsSection, valuesSection, offsetsRaw, valuesRaw
}

func typedColumnAdapterAlignedCopy(src []byte, align int) []byte {
	out := typedColumnAdapterAlignedBytes(len(src), align)
	copy(out, src)
	return out
}

func typedColumnAdapterMisalignedCopy(src []byte, align int) []byte {
	buf := make([]byte, len(src)+align+1)
	for off := 1; off <= align; off++ {
		candidate := buf[off : off+len(src)]
		if uintptr(unsafe.Pointer(unsafe.SliceData(candidate)))%uintptr(align) != 0 {
			copy(candidate, src)
			return candidate
		}
	}
	panic("no misaligned offset found")
}

func typedColumnAdapterMustOffsetsListOffsets(t *testing.T, offsets []uint64) []byte {
	t.Helper()
	raw, err := typedcolumn.EncodeRawUint32OffsetsListOffsets(nil, offsets)
	if err != nil {
		t.Fatalf("EncodeRawUint32OffsetsListOffsets: %v", err)
	}
	return raw
}

func typedColumnAdapterMustOffsetsListValues(t *testing.T, values []uint32) []byte {
	t.Helper()
	raw, err := typedcolumn.EncodeRawUint32OffsetsListValues(nil, values)
	if err != nil {
		t.Fatalf("EncodeRawUint32OffsetsListValues: %v", err)
	}
	return raw
}

func assertTypedColumnAdapterNoActive(t testing.TB, mgr *mappedresource.Manager) {
	t.Helper()
	stats := mgr.Stats()
	if stats.ActiveHandles != 0 || stats.ActiveMappedBytes != 0 || stats.ActiveHeapCopyBytes != 0 || stats.ActiveDerivedMetadataBytes != 0 {
		t.Fatalf("mappedresource active stats=%+v", stats)
	}
	if pins := mgr.PinSummary(); len(pins) != 0 {
		t.Fatalf("mappedresource active pins=%+v", pins)
	}
}

func typedColumnAdapterAlignedBytes(size int, align int) []byte {
	buf := make([]byte, size+align)
	base := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	for off := 0; off < align; off++ {
		if (base+uintptr(off))%uintptr(align) == 0 {
			return buf[off : off+size]
		}
	}
	panic("no aligned offset found")
}
