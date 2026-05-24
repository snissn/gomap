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

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

var typedColumnNullableBenchSink1784 int64

func TestTypedColumnAdapterMapsTreeDBDeclaredTypes(t *testing.T) {
	want := map[ColumnStoreValueType]typedColumnAdapterTypeStatus{
		ColumnStoreValueBool:          typedColumnAdapterRepresented,
		ColumnStoreValueInt64:         typedColumnAdapterRepresented,
		ColumnStoreValueFloat32:       typedColumnAdapterRepresented,
		ColumnStoreValueDouble:        typedColumnAdapterRepresented,
		ColumnStoreValueString:        typedColumnAdapterRepresented,
		ColumnStoreValueFloat32Vector: typedColumnAdapterRepresented,
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

func TestTypedColumnAdapterAdjacencyDenseDirectViewAndFallback(t *testing.T) {
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
	reader := typedColumnAdapterResourceReader{Manager: mgr, Image: image, Path: path, Namespace: "typed-column-adapter-adjacency", PartID: image.PartID, PreferMapped: true, AllowHeapCopy: true}
	view, err := typedColumnAdapterAcquireDenseUint32ColumnView(reader, column, image.Rows)
	if err != nil {
		t.Fatalf("AcquireDenseUint32ColumnView: %v", err)
	}
	if !view.Direct || view.Handle == nil || !slices.Equal(view.Values, []uint32{1, 2, 3, 4, 5, 6}) {
		if view.Handle != nil {
			_ = view.Handle.Release()
		}
		t.Fatalf("direct view=%+v want direct dense values", view)
	}
	if err := view.Handle.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	assertTypedColumnAdapterNoActive(t, mgr)
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
		{Name: typedColumnAdapterPrimaryIDColumn, Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone, CompressionSet: true},
		{Name: "count", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone, CompressionSet: true},
	}, typedColumnAdapterPrimaryIDColumn, 2)
	selectedEncodingMismatch := typedColumnAdapterBuildCustomInt64AggregateImage(t, []typedcolumn.ColumnDefinition{
		{Name: typedColumnAdapterPrimaryIDColumn, Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true},
		{Name: "count", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true},
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
		{name: "selected_column_encoding_mismatch", raw: selectedEncodingMismatch.Bytes, refPartID: selectedEncodingMismatch.PartID, typedRows: selectedEncodingMismatch.Rows, physicalRows: selectedEncodingMismatch.Rows, schemaHash: uint64(selectedEncodingMismatch.PartID), wantErr: "schema mismatch"},
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

func BenchmarkTypedColumnAdjacencyDenseDirectViewScan(b *testing.B) {
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
	if !view.Direct || view.Handle == nil {
		b.Fatalf("expected direct mapped view")
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
