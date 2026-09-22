package collections

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestColumnStorePrimitiveScalarValueTypesNormalizeAndFailClosed1929(t *testing.T) {
	for _, valueType := range primitiveScalarValueTypes1929() {
		got, err := normalizeColumnStoreValueType(valueType)
		if err != nil || got != valueType {
			t.Fatalf("normalizeColumnStoreValueType(%s)=%s err=%v", valueType, got, err)
		}
	}
	for _, valueType := range []ColumnStoreValueType{"fixed_bytes", "packed_uint_vector"} {
		if _, err := normalizeColumnStoreValueType(valueType); err == nil || !strings.Contains(err.Error(), "unsupported value_type") {
			t.Fatalf("normalizeColumnStoreValueType(%s) err=%v want unsupported", valueType, err)
		}
	}

	for _, valueType := range primitiveScalarValueTypes1929() {
		t.Run(string(valueType)+" nullable typed_column_part", func(t *testing.T) {
			cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "v", Path: "v", ValueType: valueType, Owner: TypedStorageOwnerColumnPart, Nullable: true}}}
			if _, err := normalizeColumnStoreConfig("events", cfg); err == nil || !strings.Contains(err.Error(), "typed_column_part is unsupported") {
				t.Fatalf("normalizeColumnStoreConfig nullable %s err=%v want fail-closed", valueType, err)
			}
			layout := TypedStorageLayout{Collection: "events", Fields: []TypedStorageField{{Name: "v", Path: "v", Owner: TypedStorageOwnerColumnPart, ValueType: valueType, Nullable: true}}}
			if _, err := NormalizeTypedStorageLayout(layout); err == nil || !strings.Contains(err.Error(), "typed_column_part is unsupported") {
				t.Fatalf("NormalizeTypedStorageLayout nullable %s err=%v want fail-closed", valueType, err)
			}
		})
	}

	cfg := &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "u8", Path: "u8", ValueType: ColumnStoreValueUint8, Owner: TypedStorageOwnerRowAsset, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian}}}
	if _, err := normalizeColumnStoreConfig("events", cfg); err == nil || !strings.Contains(err.Error(), "requires owner") {
		t.Fatalf("row-asset primitive fixed_width_encoding err=%v want typed_column_part-only rejection", err)
	}
}

func TestTypedColumnProductionCapabilityMapsPrimitiveScalars1929(t *testing.T) {
	cases := []struct {
		valueType    ColumnStoreValueType
		wantType     typedcolumn.ColumnType
		wantEncoding typedcolumn.Encoding
		wantStatsOff bool
		wantWidth    int
	}{
		{ColumnStoreValueInt8, typedcolumn.ColumnTypeInt8, typedcolumn.EncodingRawInt8, false, 1},
		{ColumnStoreValueUint8, typedcolumn.ColumnTypeUint8, typedcolumn.EncodingRawUint8, false, 1},
		{ColumnStoreValueInt16, typedcolumn.ColumnTypeInt16, typedcolumn.EncodingRawInt16, false, 2},
		{ColumnStoreValueUint16, typedcolumn.ColumnTypeUint16, typedcolumn.EncodingRawUint16, false, 2},
		{ColumnStoreValueInt32, typedcolumn.ColumnTypeInt32, typedcolumn.EncodingRawInt32, false, 4},
		{ColumnStoreValueUint32, typedcolumn.ColumnTypeUint32, typedcolumn.EncodingRawUint32, false, 4},
		{ColumnStoreValueUint64, typedcolumn.ColumnTypeUint64, typedcolumn.EncodingRawUint64, true, 8},
		{ColumnStoreValueFloat16, typedcolumn.ColumnTypeFloat16, typedcolumn.EncodingRawFloat16, true, 2},
		{ColumnStoreValueBFloat16, typedcolumn.ColumnTypeBFloat16, typedcolumn.EncodingRawBFloat16, true, 2},
	}
	for _, tc := range cases {
		t.Run(string(tc.valueType), func(t *testing.T) {
			field := typedColumnAdapterField("v", tc.valueType)
			def, err := typedColumnProductionDefinitionForField(field)
			if err != nil {
				t.Fatalf("typedColumnProductionDefinitionForField: %v", err)
			}
			if def.Type != tc.wantType || def.Encoding != tc.wantEncoding || def.Compression != typedcolumn.CompressionNone || def.FixedWidthElements != 0 || def.StatsDisabled != tc.wantStatsOff {
				t.Fatalf("definition=%+v want type=%s encoding=%s compression=none fixed_width_elements=0 stats_disabled=%v", def, tc.wantType, tc.wantEncoding, tc.wantStatsOff)
			}
			width, ok := columnStorePrimitiveScalarWidth(tc.valueType)
			if !ok || width != tc.wantWidth {
				t.Fatalf("columnStorePrimitiveScalarWidth(%s)=%d,%v want %d,true", tc.valueType, width, ok, tc.wantWidth)
			}
			if err := validateTypedColumnProductionDefinition(field, def); err != nil {
				t.Fatalf("validateTypedColumnProductionDefinition: %v", err)
			}
		})
	}
}

func TestTypedColumnAdapterPrimitiveScalarRoundTrip1929(t *testing.T) {
	for _, tc := range primitiveScalarRoundTripCases1929() {
		t.Run(string(tc.valueType), func(t *testing.T) {
			field := typedColumnAdapterField("v", tc.valueType)
			part := typedColumnAdapterBuildPart(t, field, tc.values)
			definition := part.Part.Columns[field.Name].Definition
			wantType, wantEncoding, _, ok := typedColumnPrimitiveScalarMapping(tc.valueType)
			if !ok {
				t.Fatalf("missing primitive mapping for %s", tc.valueType)
			}
			if definition.Type != wantType || definition.Encoding != wantEncoding || definition.FixedWidthElements != 0 {
				t.Fatalf("definition=%+v want type=%s encoding=%s fixed_width_elements=0", definition, wantType, wantEncoding)
			}
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
				t.Fatalf("scanColumnValues: %v", err)
			}
			assertPrimitiveScalarDeclaredValuesEqual1929(t, got, tc.values)
		})
	}
}

func TestTypedColumnAdapterPrimitiveScalarSelectedRows1929(t *testing.T) {
	for _, tc := range primitiveScalarRoundTripCases1929() {
		t.Run(string(tc.valueType), func(t *testing.T) {
			field := typedColumnAdapterField("v", tc.valueType)
			values := append([]columnDeclaredValue(nil), tc.values...)
			values = append(values, tc.values[0])
			part := typedColumnAdapterBuildPart(t, field, values)
			got, diag, err := part.scanColumnValuesRows(field.Name, []int{1})
			if err != nil {
				t.Fatalf("scanColumnValuesRows: %v", err)
			}
			assertPrimitiveScalarDeclaredValuesEqual1929(t, got, values[1:2])
			if diag.RowsScanned != 1 || diag.ColumnsProjected != 1 || diag.BlocksDecoded != 1 || diag.BytesDecoded == 0 {
				t.Fatalf("selected-row diagnostics=%+v want one decoded primitive scalar block", diag)
			}

			sparse, sparseDiag, err := part.scanColumnValuesRows(field.Name, []int{0, 2})
			if err != nil {
				t.Fatalf("scanColumnValuesRows sparse: %v", err)
			}
			assertPrimitiveScalarDeclaredValuesEqual1929(t, sparse, []columnDeclaredValue{values[0], values[2]})
			if sparseDiag.RowsScanned != 2 || sparseDiag.ColumnsProjected != 1 || sparseDiag.BytesDecoded == 0 {
				t.Fatalf("sparse selected-row diagnostics=%+v want two decoded primitive scalar rows", sparseDiag)
			}

			empty, emptyDiag, err := part.scanColumnValuesRows(field.Name, []int{})
			if err != nil || len(empty) != 0 || emptyDiag.RowsScanned != 0 || emptyDiag.ColumnsProjected != 1 {
				t.Fatalf("empty selected-row scan values=%+v diagnostics=%+v err=%v", empty, emptyDiag, err)
			}
		})
	}
}

func TestColumnPhysicalAssetPrimitiveScalarEncodeDecodeIdentity1929(t *testing.T) {
	columns := make([]ColumnStoreColumn, 0, len(primitiveScalarRoundTripCases1929()))
	rowValues := make([]columnDeclaredValue, 0, len(primitiveScalarRoundTripCases1929()))
	for _, tc := range primitiveScalarRoundTripCases1929() {
		columns = append(columns, ColumnStoreColumn{Name: string(tc.valueType), Path: string(tc.valueType), ValueType: tc.valueType, Owner: TypedStorageOwnerRowAsset})
		rowValues = append(rowValues, tc.values[0])
	}
	version, err := columnPhysicalAssetVersionForColumns(columns)
	if err != nil {
		t.Fatalf("columnPhysicalAssetVersionForColumns: %v", err)
	}
	if version != columnPhysicalAssetVersionV4 {
		t.Fatalf("primitive scalar row assets version=%d want v4 manifest-compatible scalar records", version)
	}
	raw, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        "events",
		Namespace:         "ns",
		Generation:        1,
		PartID:            2,
		AppliedCommandLSN: 3,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        4,
		Columns:           columns,
		Rows:              []columnDeclaredRow{{ID: []byte("r1"), Values: rowValues}},
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	if summary.RowCount != 1 || summary.ColumnCount != len(columns) || summary.PayloadBytes != int64(len(raw)) {
		t.Fatalf("summary=%+v raw bytes=%d", summary, len(raw))
	}
	asset, err := decodeColumnPhysicalAsset(raw)
	if err != nil {
		t.Fatalf("decodeColumnPhysicalAsset: %v", err)
	}
	if asset.Header.Collection != "events" || asset.Header.Generation != 1 || asset.Header.PartID != 2 || len(asset.Columns) != len(columns) || len(asset.Rows) != 1 {
		t.Fatalf("decoded asset header/shape=%+v columns=%d rows=%d", asset.Header, len(asset.Columns), len(asset.Rows))
	}
	for i, col := range columns {
		if asset.Columns[i].Name != col.Name || asset.Columns[i].Path != col.Path || asset.Columns[i].ValueType != col.ValueType {
			t.Fatalf("decoded column[%d]=%+v want %+v", i, asset.Columns[i], col)
		}
	}
	assertPrimitiveScalarDeclaredValuesEqual1929(t, asset.Rows[0].Values, rowValues)
}

func TestPrimitiveScalarTypedColumnPartCheckpointReopen1929(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createPrimitiveScalarTypedColumnCollection1929(t, d)

	ids := [][]byte{[]byte("r1"), []byte("r2")}
	docs := [][]byte{
		[]byte(`{"i8":-128,"u8":255,"i16":-32768,"u16":65535,"i32":-2147483648,"u32":4294967295,"u64":18446744073709551615,"f16":32768,"bf16":32640}`),
		[]byte(`{"i8":127,"u8":0,"i16":32767,"u16":0,"i32":2147483647,"u32":0,"u64":9223372036854775808,"f16":31744,"bf16":65408}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	assertPrimitiveScalarLatestTypedRows1929(t, d, col, []map[string]columnDeclaredValue{
		primitiveScalarExpectedRow1929(-128, 255, -32768, 65535, -2147483648, 4294967295, 18446744073709551615, 0x8000, 0x7f80),
		primitiveScalarExpectedRow1929(127, 0, 32767, 0, 2147483647, 0, 9223372036854775808, 0x7c00, 0xff80),
	})
	got, err := col.Get([]byte("r1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get r1: %v", err)
	}
	assertPrimitiveScalarJSONNumbers1929(t, got, map[string]string{"i8": "-128", "u8": "255", "i16": "-32768", "u16": "65535", "i32": "-2147483648", "u32": "4294967295", "u64": "18446744073709551615", "f16": "32768", "bf16": "32640"})

	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("primitive_scalars")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertPrimitiveScalarLatestTypedRows1929(t, reopened, reopenedCol, []map[string]columnDeclaredValue{
		primitiveScalarExpectedRow1929(-128, 255, -32768, 65535, -2147483648, 4294967295, 18446744073709551615, 0x8000, 0x7f80),
		primitiveScalarExpectedRow1929(127, 0, 32767, 0, 2147483647, 0, 9223372036854775808, 0x7c00, 0xff80),
	})
	reopenedGot, err := reopenedCol.Get([]byte("r2"))
	if err != nil {
		t.Fatalf("reopened Get r2: %v", err)
	}
	assertPrimitiveScalarJSONNumbers1929(t, reopenedGot, map[string]string{"i8": "127", "u8": "0", "i16": "32767", "u16": "0", "i32": "2147483647", "u32": "0", "u64": "9223372036854775808", "f16": "31744", "bf16": "65408"})
}

type primitiveScalarRoundTripCase1929 struct {
	valueType ColumnStoreValueType
	values    []columnDeclaredValue
}

func primitiveScalarValueTypes1929() []ColumnStoreValueType {
	return []ColumnStoreValueType{
		ColumnStoreValueInt8,
		ColumnStoreValueUint8,
		ColumnStoreValueInt16,
		ColumnStoreValueUint16,
		ColumnStoreValueInt32,
		ColumnStoreValueUint32,
		ColumnStoreValueUint64,
		ColumnStoreValueFloat16,
		ColumnStoreValueBFloat16,
	}
}

func primitiveScalarRoundTripCases1929() []primitiveScalarRoundTripCase1929 {
	return []primitiveScalarRoundTripCase1929{
		{ColumnStoreValueInt8, []columnDeclaredValue{{Type: ColumnStoreValueInt8, Present: true, Int8: -128}, {Type: ColumnStoreValueInt8, Present: true, Int8: 127}}},
		{ColumnStoreValueUint8, []columnDeclaredValue{{Type: ColumnStoreValueUint8, Present: true, Uint8: 0}, {Type: ColumnStoreValueUint8, Present: true, Uint8: 255}}},
		{ColumnStoreValueInt16, []columnDeclaredValue{{Type: ColumnStoreValueInt16, Present: true, Int16: -32768}, {Type: ColumnStoreValueInt16, Present: true, Int16: 32767}}},
		{ColumnStoreValueUint16, []columnDeclaredValue{{Type: ColumnStoreValueUint16, Present: true, Uint16: 0}, {Type: ColumnStoreValueUint16, Present: true, Uint16: 65535}}},
		{ColumnStoreValueInt32, []columnDeclaredValue{{Type: ColumnStoreValueInt32, Present: true, Int32: -2147483648}, {Type: ColumnStoreValueInt32, Present: true, Int32: 2147483647}}},
		{ColumnStoreValueUint32, []columnDeclaredValue{{Type: ColumnStoreValueUint32, Present: true, Uint32: 0}, {Type: ColumnStoreValueUint32, Present: true, Uint32: 0xffffffff}}},
		{ColumnStoreValueUint64, []columnDeclaredValue{{Type: ColumnStoreValueUint64, Present: true, Uint64: 0}, {Type: ColumnStoreValueUint64, Present: true, Uint64: 0xffffffffffffffff}}},
		{ColumnStoreValueFloat16, []columnDeclaredValue{{Type: ColumnStoreValueFloat16, Present: true, Float16: 0x8000}, {Type: ColumnStoreValueFloat16, Present: true, Float16: 0x7e01}}},
		{ColumnStoreValueBFloat16, []columnDeclaredValue{{Type: ColumnStoreValueBFloat16, Present: true, BFloat16: 0x8000}, {Type: ColumnStoreValueBFloat16, Present: true, BFloat16: 0x7fc1}}},
	}
}

func assertPrimitiveScalarDeclaredValuesEqual1929(t testing.TB, got, want []columnDeclaredValue) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values len=%d want %d got=%+v want=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Type != want[i].Type || got[i].Present != want[i].Present || got[i].Null != want[i].Null {
			t.Fatalf("value[%d]=%+v want %+v", i, got[i], want[i])
		}
		switch want[i].Type {
		case ColumnStoreValueInt8:
			if got[i].Int8 != want[i].Int8 {
				t.Fatalf("value[%d].Int8=%d want %d", i, got[i].Int8, want[i].Int8)
			}
		case ColumnStoreValueUint8:
			if got[i].Uint8 != want[i].Uint8 {
				t.Fatalf("value[%d].Uint8=%d want %d", i, got[i].Uint8, want[i].Uint8)
			}
		case ColumnStoreValueInt16:
			if got[i].Int16 != want[i].Int16 {
				t.Fatalf("value[%d].Int16=%d want %d", i, got[i].Int16, want[i].Int16)
			}
		case ColumnStoreValueUint16:
			if got[i].Uint16 != want[i].Uint16 {
				t.Fatalf("value[%d].Uint16=%d want %d", i, got[i].Uint16, want[i].Uint16)
			}
		case ColumnStoreValueInt32:
			if got[i].Int32 != want[i].Int32 {
				t.Fatalf("value[%d].Int32=%d want %d", i, got[i].Int32, want[i].Int32)
			}
		case ColumnStoreValueUint32:
			if got[i].Uint32 != want[i].Uint32 {
				t.Fatalf("value[%d].Uint32=%d want %d", i, got[i].Uint32, want[i].Uint32)
			}
		case ColumnStoreValueUint64:
			if got[i].Uint64 != want[i].Uint64 {
				t.Fatalf("value[%d].Uint64=%d want %d", i, got[i].Uint64, want[i].Uint64)
			}
		case ColumnStoreValueFloat16:
			if got[i].Float16 != want[i].Float16 {
				t.Fatalf("value[%d].Float16=%04x want %04x", i, got[i].Float16, want[i].Float16)
			}
		case ColumnStoreValueBFloat16:
			if got[i].BFloat16 != want[i].BFloat16 {
				t.Fatalf("value[%d].BFloat16=%04x want %04x", i, got[i].BFloat16, want[i].BFloat16)
			}
		default:
			t.Fatalf("value[%d] unsupported primitive type %q", i, want[i].Type)
		}
	}
}

func createPrimitiveScalarTypedColumnCollection1929(t testing.TB, d *backenddb.DB) *Collection {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.RetainedPayload = ColumnRetainedPayloadNone
	cfg.Columns = []ColumnStoreColumn{
		{Name: "i8", Path: "i8", ValueType: ColumnStoreValueInt8, Owner: TypedStorageOwnerColumnPart},
		{Name: "u8", Path: "u8", ValueType: ColumnStoreValueUint8, Owner: TypedStorageOwnerColumnPart},
		{Name: "i16", Path: "i16", ValueType: ColumnStoreValueInt16, Owner: TypedStorageOwnerColumnPart},
		{Name: "u16", Path: "u16", ValueType: ColumnStoreValueUint16, Owner: TypedStorageOwnerColumnPart},
		{Name: "i32", Path: "i32", ValueType: ColumnStoreValueInt32, Owner: TypedStorageOwnerColumnPart},
		{Name: "u32", Path: "u32", ValueType: ColumnStoreValueUint32, Owner: TypedStorageOwnerColumnPart},
		{Name: "u64", Path: "u64", ValueType: ColumnStoreValueUint64, Owner: TypedStorageOwnerColumnPart},
		{Name: "f16", Path: "f16", ValueType: ColumnStoreValueFloat16, Owner: TypedStorageOwnerColumnPart},
		{Name: "bf16", Path: "bf16", ValueType: ColumnStoreValueBFloat16, Owner: TypedStorageOwnerColumnPart},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "primitive_scalars", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("primitive_scalars")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func primitiveScalarExpectedRow1929(i8 int8, u8 uint8, i16 int16, u16 uint16, i32 int32, u32 uint32, u64 uint64, f16 uint16, bf16 uint16) map[string]columnDeclaredValue {
	return map[string]columnDeclaredValue{
		"i8":   {Type: ColumnStoreValueInt8, Present: true, Int8: i8},
		"u8":   {Type: ColumnStoreValueUint8, Present: true, Uint8: u8},
		"i16":  {Type: ColumnStoreValueInt16, Present: true, Int16: i16},
		"u16":  {Type: ColumnStoreValueUint16, Present: true, Uint16: u16},
		"i32":  {Type: ColumnStoreValueInt32, Present: true, Int32: i32},
		"u32":  {Type: ColumnStoreValueUint32, Present: true, Uint32: u32},
		"u64":  {Type: ColumnStoreValueUint64, Present: true, Uint64: u64},
		"f16":  {Type: ColumnStoreValueFloat16, Present: true, Float16: f16},
		"bf16": {Type: ColumnStoreValueBFloat16, Present: true, BFloat16: bf16},
	}
}

func assertPrimitiveScalarLatestTypedRows1929(t testing.TB, d *backenddb.DB, col *Collection, want []map[string]columnDeclaredValue) {
	t.Helper()
	refs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(refs) != 1 {
		t.Fatalf("typed-column refs=%+v want exactly one primitive scalar part", refs)
	}
	rows := typedColumnPartRowsForGeneration1778(t, d, col, refs[0].Generation)
	if len(rows) != len(want) {
		t.Fatalf("typed rows=%d want %d", len(rows), len(want))
	}
	for rowIdx, row := range rows {
		if row.PrimaryID != int64(rowIdx) {
			t.Fatalf("row[%d] primary_id=%d want %d", rowIdx, row.PrimaryID, rowIdx)
		}
		for name, wantValue := range want[rowIdx] {
			got, ok := row.Values[name]
			if !ok {
				t.Fatalf("row[%d] missing %s values=%+v", rowIdx, name, row.Values)
			}
			assertPrimitiveScalarDeclaredValuesEqual1929(t, []columnDeclaredValue{got}, []columnDeclaredValue{wantValue})
		}
	}
}

func assertPrimitiveScalarJSONNumbers1929(t testing.TB, raw []byte, want map[string]string) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var got map[string]json.Number
	if err := decoder.Decode(&got); err != nil {
		t.Fatalf("decode JSON %q: %v", raw, err)
	}
	if len(got) != len(want) {
		t.Fatalf("JSON fields=%v want %v raw=%s", got, want, raw)
	}
	for key, wantNumber := range want {
		gotNumber, ok := got[key]
		if !ok || gotNumber.String() != wantNumber {
			t.Fatalf("JSON %s=%q ok=%v want %q raw=%s", key, gotNumber.String(), ok, wantNumber, raw)
		}
	}
}

func TestPrimitiveScalarHelpersKeepInventoryInSync1929(t *testing.T) {
	if got, want := columnStorePrimitiveScalarValueTypes(), primitiveScalarValueTypes1929(); !slices.Equal(got, want) {
		t.Fatalf("columnStorePrimitiveScalarValueTypes=%v want %v", got, want)
	}
	for _, valueType := range primitiveScalarValueTypes1929() {
		if !columnStoreValueTypeIsPrimitiveScalar(valueType) {
			t.Fatalf("%s not recognized as primitive scalar", valueType)
		}
	}
	if columnStoreValueTypeIsPrimitiveScalar(ColumnStoreValueFloat32Vector) {
		t.Fatal("float32_vector must not be a primitive scalar for #1929")
	}
	if columnStoreValueTypeHasTypedColumnIntegerStats(ColumnStoreValueUint64) || columnStoreValueTypeHasTypedColumnIntegerStats(ColumnStoreValueFloat16) || columnStoreValueTypeHasTypedColumnIntegerStats(ColumnStoreValueBFloat16) {
		t.Fatal("uint64/float16/bfloat16 must not claim int64-compatible stats")
	}
	for _, valueType := range []ColumnStoreValueType{ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32} {
		if !columnStoreValueTypeHasTypedColumnIntegerStats(valueType) {
			t.Fatalf("%s should claim int64-compatible stats", valueType)
		}
	}
}

func TestPrimitiveScalarJSONConversionRejectsInvalidInputs1929(t *testing.T) {
	cases := []struct {
		name string
		col  ColumnStoreColumn
		raw  any
		want string
	}{
		{name: "int8 overflow", col: ColumnStoreColumn{Name: "v", Path: "v", ValueType: ColumnStoreValueInt8}, raw: json.Number("128"), want: "outside range"},
		{name: "uint8 negative", col: ColumnStoreColumn{Name: "v", Path: "v", ValueType: ColumnStoreValueUint8}, raw: json.Number("-1"), want: "invalid syntax"},
		{name: "uint16 overflow", col: ColumnStoreColumn{Name: "v", Path: "v", ValueType: ColumnStoreValueUint16}, raw: json.Number("65536"), want: "outside range"},
		{name: "int32 overflow", col: ColumnStoreColumn{Name: "v", Path: "v", ValueType: ColumnStoreValueInt32}, raw: json.Number("2147483648"), want: "outside range"},
		{name: "uint64 fractional", col: ColumnStoreColumn{Name: "v", Path: "v", ValueType: ColumnStoreValueUint64}, raw: json.Number("1.5"), want: "invalid syntax"},
		{name: "float16 raw bits overflow", col: ColumnStoreColumn{Name: "v", Path: "v", ValueType: ColumnStoreValueFloat16}, raw: json.Number("65536"), want: "outside range"},
		{name: "bfloat16 string", col: ColumnStoreColumn{Name: "v", Path: "v", ValueType: ColumnStoreValueBFloat16}, raw: "nan", want: "expected bfloat16 integer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convertColumnDeclaredValue(tc.col, tc.raw, true)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("convertColumnDeclaredValue err=%v want %q", err, tc.want)
			}
		})
	}
	for _, tc := range primitiveScalarRoundTripCases1929() {
		got, err := convertColumnDeclaredValue(ColumnStoreColumn{Name: "v", Path: "v", ValueType: tc.valueType}, json.Number(fmt.Sprint(primitiveScalarJSONNumberForValue1929(tc.values[0]))), true)
		if err != nil {
			t.Fatalf("convert valid %s: %v", tc.valueType, err)
		}
		assertPrimitiveScalarDeclaredValuesEqual1929(t, []columnDeclaredValue{got}, tc.values[:1])
	}
}

func primitiveScalarJSONNumberForValue1929(value columnDeclaredValue) any {
	switch value.Type {
	case ColumnStoreValueInt8:
		return value.Int8
	case ColumnStoreValueUint8:
		return value.Uint8
	case ColumnStoreValueInt16:
		return value.Int16
	case ColumnStoreValueUint16:
		return value.Uint16
	case ColumnStoreValueInt32:
		return value.Int32
	case ColumnStoreValueUint32:
		return value.Uint32
	case ColumnStoreValueUint64:
		return value.Uint64
	case ColumnStoreValueFloat16:
		return value.Float16
	case ColumnStoreValueBFloat16:
		return value.BFloat16
	default:
		return 0
	}
}
