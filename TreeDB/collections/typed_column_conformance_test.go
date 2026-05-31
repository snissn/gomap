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
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpCountNonNull, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpBoolCounts, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonBoolRangeUnsupported},
				{op: columnsemantics.OpSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonOperationUnsupported},
				{op: columnsemantics.OpAvg, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonOperationUnsupported},
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
				{op: columnsemantics.OpStatsSum, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
			},
		},
		{
			name:     "float32_raw_bits",
			field:    semanticField("score32", ColumnStoreValueFloat32),
			logical:  columnsemantics.LogicalFloat32,
			physical: typedcolumn.ColumnTypeInt64,
			encoding: typedcolumn.EncodingRawInt64,
			checks: []capabilityCheck{
				{op: columnsemantics.OpEquality, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonNativeFloatLayoutMissing},
				{op: columnsemantics.OpInList, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonNativeFloatLayoutMissing},
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpCountNonNull, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpMin, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpMax, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpAvg, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpStatsMinMax, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpStatsSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpPruneEquality, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpPruneOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpDirectScalarValueCarrier, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
			},
		},
		{
			name:     "double_raw_bits",
			field:    semanticField("score64", ColumnStoreValueDouble),
			logical:  columnsemantics.LogicalDouble,
			physical: typedcolumn.ColumnTypeInt64,
			encoding: typedcolumn.EncodingRawInt64,
			checks: []capabilityCheck{
				{op: columnsemantics.OpEquality, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonNativeFloatLayoutMissing},
				{op: columnsemantics.OpInList, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonNativeFloatLayoutMissing},
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpCountNonNull, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpMin, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpMax, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpAvg, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpStatsMinMax, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpStatsSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpPruneEquality, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpPruneOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
				{op: columnsemantics.OpDirectScalarValueCarrier, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonFloatRawInt64BitPattern},
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
				{op: columnsemantics.OpStringPrefix, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonDictionaryOrderUnproven},
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
				{op: columnsemantics.OpVectorDirectPayload, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpVectorSimilarity, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpVectorDotProduct, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpVectorMetrics, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpEquality, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonVectorScalarOperationUnsupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonVectorScalarOperationUnsupported},
				{op: columnsemantics.OpSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonVectorScalarOperationUnsupported},
				{op: columnsemantics.OpMin, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonVectorScalarOperationUnsupported},
				{op: columnsemantics.OpMax, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonVectorScalarOperationUnsupported},
			},
		},
		{
			name:     "uint32_list",
			field:    semanticField("tags", ColumnStoreValueUint32List),
			logical:  columnsemantics.LogicalUint32List,
			physical: typedcolumn.ColumnTypeUint32List,
			encoding: typedcolumn.EncodingRawUint32OffsetsList,
			checks: []capabilityCheck{
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpCountNonNull, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpUint32ListDirectPayload, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpEquality, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonUint32ListScalarOperationUnsupported},
				{op: columnsemantics.OpAdjacencyDirectPayload, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonOperationUnsupported},
			},
		},
		{
			name:     "bytes",
			field:    semanticField("opaque", ColumnStoreValueBytes),
			logical:  columnsemantics.LogicalBytes,
			physical: typedcolumn.ColumnTypeBytes,
			encoding: typedcolumn.EncodingRawBytesOffsets,
			checks: []capabilityCheck{
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpCountNonNull, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpBytesDirectPayload, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpEquality, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonBytesScalarOperationUnsupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonBytesScalarOperationUnsupported},
				{op: columnsemantics.OpVectorDirectPayload, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonOperationUnsupported},
			},
		},
		{
			name:     "adjacency_list_dense_compatibility",
			field:    semanticAdjacencyField("neighbors"),
			logical:  columnsemantics.LogicalAdjacencyList,
			physical: typedcolumn.ColumnTypeAdjacencyList,
			encoding: typedcolumn.EncodingRawUint32Dense,
			checks: []capabilityCheck{
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpAdjacencyDirectPayload, status: columnsemantics.StatusFallback, reason: columnsemantics.ReasonAdjacencyCapabilityDeferred},
				{op: columnsemantics.OpAdjacencyTraversal, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpAdjacencyMetrics, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonAdjacencyScalarOperationUnsupported},
				{op: columnsemantics.OpSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonAdjacencyScalarOperationUnsupported},
				{op: columnsemantics.OpMin, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonAdjacencyScalarOperationUnsupported},
				{op: columnsemantics.OpMax, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonAdjacencyScalarOperationUnsupported},
			},
		},
		{
			name:     "adjacency_list_offsets_list_adapter",
			field:    semanticAdjacencyOffsetsListField("neighbors_offsets"),
			logical:  columnsemantics.LogicalAdjacencyList,
			physical: typedcolumn.ColumnTypeAdjacencyList,
			encoding: typedcolumn.EncodingRawUint32OffsetsList,
			checks: []capabilityCheck{
				{op: columnsemantics.OpCountRows, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpAdjacencyDirectPayload, status: columnsemantics.StatusSupported, reason: columnsemantics.ReasonSupported},
				{op: columnsemantics.OpOrderedRange, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonAdjacencyScalarOperationUnsupported},
				{op: columnsemantics.OpSum, status: columnsemantics.StatusUnsupported, reason: columnsemantics.ReasonAdjacencyScalarOperationUnsupported},
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
	for _, valueType := range []ColumnStoreValueType{ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueString, ColumnStoreValueFloat32Vector, ColumnStoreValueUint32List, ColumnStoreValueBytes, ColumnStoreValueAdjacencyList} {
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
	if _, ok := part.Part.ColumnStats.Int64Column("count"); !ok {
		t.Fatalf("logical int64 column missing stats: %+v", part.Part.ColumnStats)
	}
	for _, name := range []string{"score32", "score64", typedColumnAdapterPrimaryIDColumn} {
		if _, ok := part.Part.ColumnStats.Int64Column(name); ok {
			t.Fatalf("column %q emitted int64 stats despite non-int64 semantics: %+v", name, part.Part.ColumnStats)
		}
		if _, ok := part.Part.PruningMetadata.Int64Column(name); ok {
			t.Fatalf("column %q emitted int64 pruning despite non-int64 semantics: %+v", name, part.Part.PruningMetadata)
		}
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

func TestTypedColumnSelectionMaskConformance(t *testing.T) {
	visible := mustConformanceMaskSelection(t, []bool{true, true, false, true, true, true, true, false})
	nulls := mustConformanceMaskSelection(t, []bool{false, true, false, false, false, false, true, false})
	defaults := mustConformanceMaskSelection(t, []bool{false, false, false, false, true, false, false, false})
	bitmapWords := []uint64{0}
	for _, row := range []int{0, 1, 4, 5, 7} {
		bitmapWords[0] |= uint64(1) << uint(row)
	}
	bitmapSel, err := typedcolumn.NewBitmapRowSelection(8, bitmapWords)
	if err != nil {
		t.Fatalf("NewBitmapRowSelection: %v", err)
	}
	emptySel, err := typedcolumn.NewEmptyRowSelection(8)
	if err != nil {
		t.Fatalf("NewEmptyRowSelection: %v", err)
	}
	allSel, err := typedcolumn.NewAllRowSelection(8)
	if err != nil {
		t.Fatalf("NewAllRowSelection: %v", err)
	}
	rangeSel, err := typedcolumn.NewRangeRowSelection(8, 2, 7)
	if err != nil {
		t.Fatalf("NewRangeRowSelection: %v", err)
	}
	sparseSel, err := typedcolumn.NewSparseRowSelection(8, []int{0, 3, 6})
	if err != nil {
		t.Fatalf("NewSparseRowSelection: %v", err)
	}
	tests := []struct {
		name string
		sel  typedcolumn.RowSelection
		want []int
	}{
		{name: "empty", sel: emptySel, want: nil},
		{name: "all", sel: allSel, want: []int{0, 3, 5}},
		{name: "range", sel: rangeSel, want: []int{3, 5}},
		{name: "bitmap", sel: bitmapSel, want: []int{0, 5}},
		{name: "sparse", sel: sparseSel, want: []int{0, 3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSel, err := typedcolumn.ComposeRowSelections(8, typedcolumn.RowSelectionComponents{Predicate: &tc.sel, Visibility: &visible, Nulls: &nulls, Defaults: &defaults})
			if err != nil {
				t.Fatalf("ComposeRowSelections: %v", err)
			}
			if got := gotSel.AppendRows(nil); !slices.Equal(got, tc.want) {
				t.Fatalf("selection rows=%v want %v shape=%+v", got, tc.want, gotSel.Shape())
			}
		})
	}
}

func mustConformanceMaskSelection(t *testing.T, mask []bool) typedcolumn.RowSelection {
	t.Helper()
	rows := make([]int, 0, len(mask))
	for row, ok := range mask {
		if ok {
			rows = append(rows, row)
		}
	}
	selection, err := typedcolumn.NewSparseRowSelection(len(mask), rows)
	if err != nil {
		t.Fatalf("NewSparseRowSelection mask: %v", err)
	}
	return selection
}
