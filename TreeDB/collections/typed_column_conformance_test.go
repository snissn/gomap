package collections

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnAdapterSemanticConformanceMatrix(t *testing.T) {
	type capabilityCheck struct {
		op     columnsemantics.Operation
		status columnsemantics.Status
		reason columnsemantics.ReasonCode
	}
	tests := []struct {
		name     string
		field    TypedStorageField
		logical  columnsemantics.LogicalType
		physical typedcolumn.ColumnType
		encoding typedcolumn.Encoding
		checks   []capabilityCheck
	}{
		{
			name:     "bool",
			field:    semanticField("flag", ColumnStoreValueBool),
			logical:  columnsemantics.LogicalBool,
			physical: typedcolumn.ColumnTypeBool,
			encoding: typedcolumn.EncodingBoolBitpackRLE,
			checks: []capabilityCheck{
				{op: columnsemantics.OpEquality, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpBoolCounts, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonBoolRangeUnsupported},
			},
		},
		{
			name:     "int64",
			field:    semanticField("count", ColumnStoreValueInt64),
			logical:  columnsemantics.LogicalInt64,
			physical: typedcolumn.ColumnTypeInt64,
			encoding: typedcolumn.EncodingDeltaVarint,
			checks: []capabilityCheck{
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpSum, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpStatsSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonStatsPayloadUnsupported},
			},
		},
		{
			name:     "float32_raw_bits",
			field:    semanticField("score32", ColumnStoreValueFloat32),
			logical:  columnsemantics.LogicalFloat32,
			physical: typedcolumn.ColumnTypeInt64,
			encoding: typedcolumn.EncodingRawInt64,
			checks: []capabilityCheck{
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpDirectScalarValueCarrier, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpEquality, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonNativeFloatLayoutMissing},
			},
		},
		{
			name:     "double_raw_bits",
			field:    semanticField("score64", ColumnStoreValueDouble),
			logical:  columnsemantics.LogicalDouble,
			physical: typedcolumn.ColumnTypeInt64,
			encoding: typedcolumn.EncodingRawInt64,
			checks: []capabilityCheck{
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpMin, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpInList, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonNativeFloatLayoutMissing},
			},
		},
		{
			name:     "string_dictionary",
			field:    semanticField("kind", ColumnStoreValueString),
			logical:  columnsemantics.LogicalString,
			physical: typedcolumn.ColumnTypeLowCardinalityCode,
			encoding: typedcolumn.EncodingLowCardinalityUint32,
			checks: []capabilityCheck{
				{op: columnsemantics.OpDictionaryEquality, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpDictionaryGroupBy, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpStringPrefix, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonDictionaryOrderUnproven},
			},
		},
		{
			name:     "float32_vector",
			field:    semanticVectorField("embedding"),
			logical:  columnsemantics.LogicalFloat32Vector,
			physical: typedcolumn.ColumnTypeFloat32Vector,
			encoding: typedcolumn.EncodingRawFloat32Vector,
			checks: []capabilityCheck{
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpEquality, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonVectorScalarOperationUnsupported},
				{op: columnsemantics.OpVectorSimilarity, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonVectorCapabilityDeferred},
			},
		},
		{
			name:     "adjacency_list",
			field:    semanticAdjacencyField("neighbors"),
			logical:  columnsemantics.LogicalAdjacencyList,
			physical: typedcolumn.ColumnTypeAdjacencyList,
			encoding: typedcolumn.EncodingRawUint32Dense,
			checks: []capabilityCheck{
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonAdjacencyScalarOperationUnsupported},
				{op: columnsemantics.OpVectorMetrics, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonAdjacencyCapabilityDeferred},
			},
		},
	}

	seen := make(map[ColumnStoreValueType]bool, len(tests))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seen[tc.field.ValueType] = true
			column, err := typedColumnAdapterMapField(tc.field)
			if err != nil {
				t.Fatalf("typedColumnAdapterMapField: %v", err)
			}
			desc, err := typedColumnAdapterSemanticDescriptor(column)
			if err != nil {
				t.Fatalf("typedColumnAdapterSemanticDescriptor: %v", err)
			}
			if desc.Logical != tc.logical || desc.Physical != tc.physical || desc.Encoding != tc.encoding {
				t.Fatalf("descriptor=%+v want logical=%s physical=%s encoding=%s", desc, tc.logical, tc.physical, tc.encoding)
			}
			for _, check := range tc.checks {
				cap, err := typedColumnAdapterCapability(column, check.op)
				if err != nil {
					t.Fatalf("typedColumnAdapterCapability(%s): %v", check.op, err)
				}
				if cap.Status != check.status || cap.Reason != check.reason || cap.Phase != columnsemantics.PhasePrepare {
					t.Fatalf("%s capability=%+v want status=%s reason=%s phase=%s", check.op, cap, check.status, check.reason, columnsemantics.PhasePrepare)
				}
				if check.status != columnsemantics.StatusSupported {
					err := requireTypedColumnAdapterCapability(column, check.op, "conformance "+tc.name)
					if !errors.Is(err, ErrColumnQueryPlanUnsupported) {
						t.Fatalf("requireTypedColumnAdapterCapability(%s) err=%v want ErrColumnQueryPlanUnsupported", check.op, err)
					}
					gotReason, ok := typedColumnSemanticCapabilityReason(err)
					if !ok || gotReason != check.reason {
						t.Fatalf("semantic reason=%s ok=%v want %s err=%v", gotReason, ok, check.reason, err)
					}
				}
			}
		})
	}
	for _, valueType := range []ColumnStoreValueType{ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueString, ColumnStoreValueFloat32Vector, ColumnStoreValueAdjacencyList} {
		if !seen[valueType] {
			t.Fatalf("value type %s missing from semantic conformance matrix", valueType)
		}
	}
}

func TestTypedColumnAdapterSemanticConformanceSmallRows(t *testing.T) {
	fields := []TypedStorageField{
		semanticField("flag", ColumnStoreValueBool),
		semanticField("count", ColumnStoreValueInt64),
		semanticField("score32", ColumnStoreValueFloat32),
		semanticField("score64", ColumnStoreValueDouble),
		semanticField("kind", ColumnStoreValueString),
		semanticVectorField("embedding"),
		semanticAdjacencyField("neighbors"),
	}
	rows := []typedColumnAdapterRow{
		{PrimaryID: 10, Values: map[string]columnDeclaredValue{
			"flag":      {Type: ColumnStoreValueBool, Present: true, Bool: true},
			"count":     {Type: ColumnStoreValueInt64, Present: true, Int64: math.MinInt64},
			"score32":   {Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x80000000)},
			"score64":   {Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x7ff8000000001234)},
			"kind":      {Type: ColumnStoreValueString, Present: true, String: "beta"},
			"embedding": {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 2, 3}},
			"neighbors": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{4, 5}},
		}},
		{PrimaryID: 11, Values: map[string]columnDeclaredValue{
			"flag":      {Type: ColumnStoreValueBool, Present: true, Bool: false},
			"count":     {Type: ColumnStoreValueInt64, Present: true, Int64: math.MaxInt64},
			"score32":   {Type: ColumnStoreValueFloat32, Present: true, Float32: math.Float32frombits(0x7fc01234)},
			"score64":   {Type: ColumnStoreValueDouble, Present: true, Double: math.Float64frombits(0x8000000000000000)},
			"kind":      {Type: ColumnStoreValueString, Present: true, String: "alpha"},
			"embedding": {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{-1, -2, -3}},
			"neighbors": {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{6, 7}},
		}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1842, RowsPerGranule: 1, Fields: fields}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	if got := part.Dictionary["kind"]; got["alpha"] != 0 || got["beta"] != 1 {
		t.Fatalf("dictionary=%+v want sorted string codes", got)
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
	if !gotRows[0].Values["flag"].Bool || gotRows[1].Values["flag"].Bool {
		t.Fatalf("bool rows=%+v", gotRows)
	}
	if gotRows[0].Values["count"].Int64 != math.MinInt64 || gotRows[1].Values["count"].Int64 != math.MaxInt64 {
		t.Fatalf("int64 rows=%+v", gotRows)
	}
	if math.Float32bits(gotRows[0].Values["score32"].Float32) != 0x80000000 || math.Float32bits(gotRows[1].Values["score32"].Float32) != 0x7fc01234 {
		t.Fatalf("float32 bit rows=%+v", gotRows)
	}
	if math.Float64bits(gotRows[0].Values["score64"].Double) != 0x7ff8000000001234 || math.Float64bits(gotRows[1].Values["score64"].Double) != 0x8000000000000000 {
		t.Fatalf("double bit rows=%+v", gotRows)
	}
	if gotRows[0].Values["kind"].String != "beta" || gotRows[1].Values["kind"].String != "alpha" {
		t.Fatalf("string rows=%+v", gotRows)
	}
	if !slices.Equal(gotRows[0].Values["embedding"].Float32Vector, []float32{1, 2, 3}) || !slices.Equal(gotRows[1].Values["embedding"].Float32Vector, []float32{-1, -2, -3}) {
		t.Fatalf("vector rows=%+v", gotRows)
	}
	if !slices.Equal(gotRows[0].Values["neighbors"].AdjacencyList, []uint32{4, 5}) || !slices.Equal(gotRows[1].Values["neighbors"].AdjacencyList, []uint32{6, 7}) {
		t.Fatalf("adjacency rows=%+v", gotRows)
	}
}

type typedColumnTestSelectionKind string

const (
	typedColumnTestSelectionEmpty  typedColumnTestSelectionKind = "empty"
	typedColumnTestSelectionAll    typedColumnTestSelectionKind = "all"
	typedColumnTestSelectionRange  typedColumnTestSelectionKind = "range"
	typedColumnTestSelectionBitmap typedColumnTestSelectionKind = "bitmap"
	typedColumnTestSelectionSparse typedColumnTestSelectionKind = "sparse"
)

type typedColumnTestSelection struct {
	kind   typedColumnTestSelectionKind
	rows   int
	start  int
	end    int
	bitmap []bool
	sparse []int
}

func typedColumnTestSelectionRows(sel typedColumnTestSelection, visible, nulls, defaults []bool) []int {
	if sel.rows <= 0 {
		return nil
	}
	out := make([]int, 0, sel.rows)
	appendIfLive := func(row int) {
		if row < 0 || row >= sel.rows {
			return
		}
		if len(visible) != 0 && !visible[row] {
			return
		}
		if len(nulls) != 0 && nulls[row] {
			return
		}
		if len(defaults) != 0 && defaults[row] {
			return
		}
		out = append(out, row)
	}
	switch sel.kind {
	case typedColumnTestSelectionEmpty:
		return nil
	case typedColumnTestSelectionAll:
		for row := 0; row < sel.rows; row++ {
			appendIfLive(row)
		}
	case typedColumnTestSelectionRange:
		start, end := sel.start, sel.end
		if start < 0 {
			start = 0
		}
		if end > sel.rows {
			end = sel.rows
		}
		for row := start; row < end; row++ {
			appendIfLive(row)
		}
	case typedColumnTestSelectionBitmap:
		limit := min(sel.rows, len(sel.bitmap))
		for row := 0; row < limit; row++ {
			if sel.bitmap[row] {
				appendIfLive(row)
			}
		}
	case typedColumnTestSelectionSparse:
		for _, row := range sel.sparse {
			appendIfLive(row)
		}
	}
	return out
}

func TestTypedColumnSelectionMaskConformancePlaceholder(t *testing.T) {
	visible := []bool{true, true, false, true, true, true, true, false}
	nulls := []bool{false, true, false, false, false, false, true, false}
	defaults := []bool{false, false, false, false, true, false, false, false}
	tests := []struct {
		name string
		sel  typedColumnTestSelection
		want []int
	}{
		{name: "empty", sel: typedColumnTestSelection{kind: typedColumnTestSelectionEmpty, rows: 8}, want: nil},
		{name: "all", sel: typedColumnTestSelection{kind: typedColumnTestSelectionAll, rows: 8}, want: []int{0, 3, 5}},
		{name: "range", sel: typedColumnTestSelection{kind: typedColumnTestSelectionRange, rows: 8, start: 2, end: 7}, want: []int{3, 5}},
		{name: "bitmap", sel: typedColumnTestSelection{kind: typedColumnTestSelectionBitmap, rows: 8, bitmap: []bool{true, true, false, false, true, true, false, true}}, want: []int{0, 5}},
		{name: "sparse", sel: typedColumnTestSelection{kind: typedColumnTestSelectionSparse, rows: 8, sparse: []int{6, 3, 0}}, want: []int{3, 0}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := typedColumnTestSelectionRows(tc.sel, visible, nulls, defaults); !slices.Equal(got, tc.want) {
				t.Fatalf("selection rows=%v want %v", got, tc.want)
			}
		})
	}
}
