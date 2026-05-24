package collections

import (
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

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnAdapterMapsTreeDBDeclaredTypes(t *testing.T) {
	want := map[ColumnStoreValueType]typedColumnAdapterTypeStatus{
		ColumnStoreValueBool:          typedColumnAdapterRepresented,
		ColumnStoreValueInt64:         typedColumnAdapterRepresented,
		ColumnStoreValueFloat32:       typedColumnAdapterRepresented,
		ColumnStoreValueDouble:        typedColumnAdapterRepresented,
		ColumnStoreValueString:        typedColumnAdapterRepresented,
		ColumnStoreValueFloat32Vector: typedColumnAdapterRepresented,
		ColumnStoreValueAdjacencyList: typedColumnAdapterFailClosed,
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

func TestTypedColumnAdapterVectorRepresentedAdjacencyFailsClosed(t *testing.T) {
	mapping, err := typedColumnAdapterMappingForValueType(ColumnStoreValueFloat32Vector)
	if err != nil {
		t.Fatalf("float32_vector mapping err=%v", err)
	}
	if mapping.Status != typedColumnAdapterRepresented || mapping.ColumnType != typedcolumn.ColumnTypeFloat32Vector || mapping.Encoding != typedcolumn.EncodingRawFloat32Vector {
		t.Fatalf("float32_vector mapping=%+v want dense represented", mapping)
	}

	mapping, err = typedColumnAdapterMappingForValueType(ColumnStoreValueAdjacencyList)
	if !errors.Is(err, errTypedColumnAdapterUnsupportedType) {
		t.Fatalf("adjacency_list err=%v want errTypedColumnAdapterUnsupportedType", err)
	}
	if mapping.Status != typedColumnAdapterFailClosed || mapping.Reason == "" {
		t.Fatalf("adjacency_list mapping=%+v want fail-closed reason", mapping)
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

func typedColumnAdapterField(name string, valueType ColumnStoreValueType) TypedStorageField {
	return TypedStorageField{Name: name, Path: name, Owner: TypedStorageOwnerColumnPart, ValueType: valueType}
}

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

func typedColumnAdapterAcquireBytes(t *testing.T, mgr *mappedresource.Manager, scope mappedresource.Scope, data []byte, kind string) *mappedresource.Handle {
	t.Helper()
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: scope.Namespace, Kind: kind, FileID: 1, Length: int64(len(data))}
	h, err := mgr.AcquireBytes(key, scope, mappedresource.SourceMapped, data, mappedresource.AcquireOptions{Reason: kind, ValidationMode: mappedresource.ValidationVerify})
	if err != nil {
		t.Fatalf("AcquireBytes(%s): %v", kind, err)
	}
	return h
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
