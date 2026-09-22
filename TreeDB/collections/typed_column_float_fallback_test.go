package collections

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

type typedColumnFloatFallbackPredicateKind string

const (
	typedColumnFloatFallbackAll   typedColumnFloatFallbackPredicateKind = "all"
	typedColumnFloatFallbackEqual typedColumnFloatFallbackPredicateKind = "equal"
	typedColumnFloatFallbackRange typedColumnFloatFallbackPredicateKind = "range"
)

type typedColumnFloatFallbackPredicate struct {
	Kind  typedColumnFloatFallbackPredicateKind
	F32   float32
	F64   float64
	Min32 float32
	Max32 float32
	Min64 float64
	Max64 float64
}

type typedColumnFloatFallbackCell struct {
	Present bool
	Null    bool
	Visible bool
	F32     float32
	F64     float64
}

type typedColumnFloatFallbackDiagnostics struct {
	FallbackBlocks                     int64
	NativeFloatLayoutMissingFallbacks  int64
	RawInt64BitPatternRejectedFallback int64
	PhysicalBytesScanned               int64
	MappedBytes                        int64
	DecodedBytes                       int64
}

type typedColumnFloatFallbackResult struct {
	RowsScanned            int64
	RowsMatched            int64
	NonNulls               int64
	NullRows               int64
	DefaultRows            int64
	VisibilityExcludedRows int64
	NaNRows                int64
	Min                    float64
	Max                    float64
	Sum                    float64
	Avg                    float64
	HasMinMax              bool
	Diagnostics            typedColumnFloatFallbackDiagnostics
}

func TestTypedColumnFloatFallbackLogicalEdgeCases(t *testing.T) {
	for _, tc := range []struct {
		name      string
		valueType ColumnStoreValueType
	}{
		{name: "float32", valueType: ColumnStoreValueFloat32},
		{name: "double", valueType: ColumnStoreValueDouble},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cells, scannedRows := buildTypedColumnFloatFallbackEdgeCells(t, tc.valueType)
			assertTypedColumnFloatFallbackBitPreservation(t, tc.valueType, scannedRows)

			if got := typedColumnFloatFallbackMatchingRows(cells, tc.valueType, typedColumnFloatFallbackZeroPredicate(tc.valueType)); !slices.Equal(got, []int{2, 3}) {
				t.Fatalf("equality +0 rows=%v want signed zeros [2 3]", got)
			}
			if got := typedColumnFloatFallbackMatchingRows(cells, tc.valueType, typedColumnFloatFallbackNaNPredicate(tc.valueType)); len(got) != 0 {
				t.Fatalf("equality NaN rows=%v want none", got)
			}
			if got := typedColumnFloatFallbackMatchingRows(cells, tc.valueType, typedColumnFloatFallbackPositiveInfPredicate(tc.valueType)); !slices.Equal(got, []int{5}) {
				t.Fatalf("equality +Inf rows=%v want [5]", got)
			}
			if got := typedColumnFloatFallbackMatchingRows(cells, tc.valueType, typedColumnFloatFallbackNegativeOrdinaryPredicate(tc.valueType)); !slices.Equal(got, []int{1}) {
				t.Fatalf("equality ordinary negative rows=%v want [1]", got)
			}

			if got := typedColumnFloatFallbackMatchingRows(cells, tc.valueType, typedColumnFloatFallbackRangeFromZeroToPositiveInf(tc.valueType)); !slices.Equal(got, []int{2, 3, 4, 5}) {
				t.Fatalf("range [-0,+Inf] rows=%v want [-0 +0 positive +Inf]", got)
			}
			if got := typedColumnFloatFallbackMatchingRows(cells, tc.valueType, typedColumnFloatFallbackRangeFromNegativeInfToZero(tc.valueType)); !slices.Equal(got, []int{0, 1, 2, 3}) {
				t.Fatalf("range [-Inf,-0] rows=%v want [-Inf negative -0 +0]", got)
			}

			result := runTypedColumnFloatFallback(cells, tc.valueType, typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackAll})
			if result.RowsScanned != 7 || result.RowsMatched != 7 || result.NonNulls != 7 || result.NaNRows != 1 {
				t.Fatalf("aggregate rows=%+v want all non-null edge rows including one NaN", result)
			}
			if !result.HasMinMax || !math.IsInf(result.Min, -1) || !math.IsInf(result.Max, 1) {
				t.Fatalf("aggregate min/max min=%v max=%v has=%v want -Inf/+Inf ignoring NaN for ordering", result.Min, result.Max, result.HasMinMax)
			}
			if !math.IsNaN(result.Sum) || !math.IsNaN(result.Avg) {
				t.Fatalf("aggregate sum=%v avg=%v want NaN propagation", result.Sum, result.Avg)
			}
			assertTypedColumnFloatFallbackDiagnostics(t, tc.valueType, typedColumnFloatFallbackAll, result.Diagnostics)
		})
	}
}

func TestTypedColumnFloatFallbackAggregateZeroAndFinitePrecision(t *testing.T) {
	for _, tc := range []struct {
		name      string
		valueType ColumnStoreValueType
	}{
		{name: "float32", valueType: ColumnStoreValueFloat32},
		{name: "double", valueType: ColumnStoreValueDouble},
	} {
		t.Run(tc.name+"/canonical_zero", func(t *testing.T) {
			for _, values := range [][]float64{
				{math.Copysign(0, -1), 0},
				{0, math.Copysign(0, -1)},
			} {
				result := runTypedColumnFloatFallback(typedColumnFloatFallbackCellsFromFloat64Values(tc.valueType, values), tc.valueType, typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackAll})
				assertTypedColumnFloatFallbackCanonicalZeroMinMax(t, result)
			}
		})
	}

	t.Run("float32_widens_to_float64_sum", func(t *testing.T) {
		values := []float64{1 << 24, 1, -(1 << 24)}
		result := runTypedColumnFloatFallback(typedColumnFloatFallbackCellsFromFloat64Values(ColumnStoreValueFloat32, values), ColumnStoreValueFloat32, typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackAll})
		if result.Sum != 1 || result.Avg != float64(1)/3 {
			t.Fatalf("float32 fallback sum=%g avg=%g want widened float64 sum=1 avg=1/3", result.Sum, result.Avg)
		}
		if result.Min != -(1<<24) || result.Max != 1<<24 {
			t.Fatalf("float32 fallback min=%g max=%g want finite min/max", result.Min, result.Max)
		}
	})

	t.Run("double_accumulates_as_float64", func(t *testing.T) {
		values := []float64{1e16, 1, -1e16}
		var wantSum float64
		for _, value := range values {
			wantSum += value
		}
		result := runTypedColumnFloatFallback(typedColumnFloatFallbackCellsFromFloat64Values(ColumnStoreValueDouble, values), ColumnStoreValueDouble, typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackAll})
		if result.Sum != wantSum || result.Avg != wantSum/3 {
			t.Fatalf("double fallback sum=%g avg=%g want float64-accumulated sum=%g avg=%g", result.Sum, result.Avg, wantSum, wantSum/3)
		}
		if result.Min != -1e16 || result.Max != 1e16 {
			t.Fatalf("double fallback min=%g max=%g want finite min/max", result.Min, result.Max)
		}
	})
}

func TestTypedColumnFloatFallbackNativeTypedColumnCapabilitiesFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		valueType ColumnStoreValueType
	}{
		{name: "float32", valueType: ColumnStoreValueFloat32},
		{name: "double", valueType: ColumnStoreValueDouble},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := semanticField("score", tc.valueType)
			column, err := typedColumnAdapterMapField(field)
			if err != nil {
				t.Fatalf("typedColumnAdapterMapField: %v", err)
			}
			if column.Definition.Type != typedcolumn.ColumnTypeInt64 || column.Definition.Encoding != typedcolumn.EncodingRawInt64 || !column.Definition.StatsDisabled {
				t.Fatalf("definition=%+v want raw int64 carrier with stats disabled, not native float", column.Definition)
			}
			for _, op := range []columnsemantics.Operation{columnsemantics.OpEquality, columnsemantics.OpInList} {
				cap, err := typedColumnAdapterCapability(column, op)
				if err != nil || cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonNativeFloatLayoutMissing {
					t.Fatalf("%s capability=%+v err=%v want explicit native-float-missing fallback", op, cap, err)
				}
			}
			for _, op := range []columnsemantics.Operation{columnsemantics.OpOrderedRange, columnsemantics.OpSum, columnsemantics.OpAvg, columnsemantics.OpMin, columnsemantics.OpMax, columnsemantics.OpStatsMinMax, columnsemantics.OpStatsSum, columnsemantics.OpPruneEquality, columnsemantics.OpPruneOrderedRange, columnsemantics.OpDirectScalarValueCarrier} {
				cap, err := typedColumnAdapterCapability(column, op)
				if err != nil || cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonFloatRawInt64BitPattern {
					t.Fatalf("%s capability=%+v err=%v want raw-bit fail-closed", op, cap, err)
				}
				requireErr := requireTypedColumnAdapterCapability(column, op, "float native typed-column gate")
				if !errors.Is(requireErr, ErrColumnQueryPlanUnsupported) || !strings.Contains(requireErr.Error(), string(columnsemantics.ReasonFloatRawInt64BitPattern)) {
					t.Fatalf("%s require err=%v want ErrColumnQueryPlanUnsupported with raw-bit reason", op, requireErr)
				}
			}
		})
	}
}

func TestTypedColumnFloatFallbackNullableDefaultVisibilityComposition(t *testing.T) {
	for _, tc := range []struct {
		name      string
		valueType ColumnStoreValueType
	}{
		{name: "float32", valueType: ColumnStoreValueFloat32},
		{name: "double", valueType: ColumnStoreValueDouble},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cells := buildTypedColumnFloatFallbackNullableCells(t, tc.valueType)
			cells[4].Visible = false

			visible := mustConformanceMaskSelection(t, []bool{true, true, true, true, false, true})
			nulls := mustConformanceMaskSelection(t, []bool{false, true, false, false, false, false})
			defaults := mustConformanceMaskSelection(t, []bool{false, false, false, true, false, false})
			selection, err := typedcolumn.ComposeRowSelections(6, typedcolumn.RowSelectionComponents{Visibility: &visible, Nulls: &nulls, Defaults: &defaults})
			if err != nil {
				t.Fatalf("ComposeRowSelections: %v", err)
			}
			if got := selection.AppendRows(nil); !slices.Equal(got, []int{0, 2, 5}) {
				t.Fatalf("composed rows=%v want visible, non-null, non-default [0 2 5]", got)
			}

			predicate := typedColumnFloatFallbackRangeFromZeroToPositiveInf(tc.valueType)
			if got := typedColumnFloatFallbackMatchingRows(cells, tc.valueType, predicate); !slices.Equal(got, []int{2}) {
				t.Fatalf("visible nullable/default range rows=%v want only signed-zero row", got)
			}
			result := runTypedColumnFloatFallback(cells, tc.valueType, typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackAll})
			if result.RowsScanned != 6 || result.RowsMatched != 3 || result.NullRows != 1 || result.DefaultRows != 1 || result.VisibilityExcludedRows != 1 || result.NaNRows != 1 {
				t.Fatalf("nullable/default/visibility aggregate=%+v want rows matched [0 2 5] with explicit exclusions", result)
			}
			if !result.HasMinMax || result.Min != -1 || result.Max != 0 || !math.IsNaN(result.Sum) {
				t.Fatalf("aggregate min=%v max=%v sum=%v has=%v want min -1 max signed-zero and NaN sum", result.Min, result.Max, result.Sum, result.HasMinMax)
			}

			wrongDomainDefaults := mustConformanceMaskSelection(t, []bool{false, true, false, false})
			failClosed, err := typedcolumn.ComposeRowSelections(6, typedcolumn.RowSelectionComponents{Visibility: &visible, Defaults: &wrongDomainDefaults})
			if err == nil {
				t.Fatalf("mismatched default mask unexpectedly succeeded")
			}
			if got := failClosed.AppendRows(nil); len(got) != 0 {
				t.Fatalf("mismatched default mask rows=%v want empty fail-closed selection", got)
			}
		})
	}
}

func buildTypedColumnFloatFallbackEdgeCells(t *testing.T, valueType ColumnStoreValueType) ([]typedColumnFloatFallbackCell, []typedColumnAdapterRow) {
	t.Helper()
	values := []columnDeclaredValue{
		typedColumnFloatDeclaredValue(valueType, math.Inf(-1)),
		typedColumnFloatDeclaredValue(valueType, -2.5),
		typedColumnFloatDeclaredValue(valueType, math.Copysign(0, -1)),
		typedColumnFloatDeclaredValue(valueType, 0),
		typedColumnFloatDeclaredValue(valueType, 3.25),
		typedColumnFloatDeclaredValue(valueType, math.Inf(1)),
		typedColumnFloatDeclaredNaN(valueType),
	}
	rows := make([]typedColumnAdapterRow, len(values))
	for i, value := range values {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i), Values: map[string]columnDeclaredValue{"score": value}}
	}
	field := semanticField("score", valueType)
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1847, RowsPerGranule: 2, Fields: []TypedStorageField{field}}, rows)
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
	scanned, err := parsed.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	return typedColumnFloatFallbackCellsFromRows(scanned, valueType, "score"), scanned
}

func buildTypedColumnFloatFallbackNullableCells(t *testing.T, valueType ColumnStoreValueType) []typedColumnFloatFallbackCell {
	t.Helper()
	field := semanticField("score", valueType)
	field.Nullable = true
	values := []columnDeclaredValue{
		typedColumnFloatDeclaredValue(valueType, -1),
		{Type: valueType, Present: true, Null: true},
		typedColumnFloatDeclaredValue(valueType, math.Copysign(0, -1)),
		{Type: valueType, Present: false, Null: true},
		typedColumnFloatDeclaredValue(valueType, math.Inf(1)),
		typedColumnFloatDeclaredNaN(valueType),
	}
	rows := make([]typedColumnAdapterRow, len(values))
	for i, value := range values {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i), Values: map[string]columnDeclaredValue{"score": value}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 49, RowsPerGranule: 2, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	scanned, err := part.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	return typedColumnFloatFallbackCellsFromRows(scanned, valueType, "score")
}

func typedColumnFloatFallbackCellsFromRows(rows []typedColumnAdapterRow, valueType ColumnStoreValueType, column string) []typedColumnFloatFallbackCell {
	cells := make([]typedColumnFloatFallbackCell, len(rows))
	for i, row := range rows {
		value := row.Values[column]
		cell := typedColumnFloatFallbackCell{Present: value.Present, Null: value.Null, Visible: true}
		if valueType == ColumnStoreValueFloat32 {
			cell.F32 = value.Float32
		} else {
			cell.F64 = value.Double
		}
		cells[i] = cell
	}
	return cells
}

func typedColumnFloatDeclaredValue(valueType ColumnStoreValueType, value float64) columnDeclaredValue {
	out := columnDeclaredValue{Type: valueType, Present: true}
	if valueType == ColumnStoreValueFloat32 {
		out.Float32 = float32(value)
	} else {
		out.Double = value
	}
	return out
}

func typedColumnFloatDeclaredNaN(valueType ColumnStoreValueType) columnDeclaredValue {
	out := columnDeclaredValue{Type: valueType, Present: true}
	if valueType == ColumnStoreValueFloat32 {
		out.Float32 = math.Float32frombits(0x7fc01234)
	} else {
		out.Double = math.Float64frombits(0x7ff8000000001234)
	}
	return out
}

func assertTypedColumnFloatFallbackBitPreservation(t *testing.T, valueType ColumnStoreValueType, rows []typedColumnAdapterRow) {
	t.Helper()
	if valueType == ColumnStoreValueFloat32 {
		want := []uint32{math.Float32bits(float32(math.Inf(-1))), math.Float32bits(float32(-2.5)), 0x80000000, 0, math.Float32bits(float32(3.25)), math.Float32bits(float32(math.Inf(1))), 0x7fc01234}
		for i, row := range rows {
			if got := math.Float32bits(row.Values["score"].Float32); got != want[i] {
				t.Fatalf("row %d float32 bits=0x%08x want 0x%08x", i, got, want[i])
			}
		}
		return
	}
	want := []uint64{math.Float64bits(math.Inf(-1)), math.Float64bits(-2.5), 0x8000000000000000, 0, math.Float64bits(3.25), math.Float64bits(math.Inf(1)), 0x7ff8000000001234}
	for i, row := range rows {
		if got := math.Float64bits(row.Values["score"].Double); got != want[i] {
			t.Fatalf("row %d double bits=0x%016x want 0x%016x", i, got, want[i])
		}
	}
}

func typedColumnFloatFallbackMatchingRows(cells []typedColumnFloatFallbackCell, valueType ColumnStoreValueType, predicate typedColumnFloatFallbackPredicate) []int {
	var rows []int
	for i, cell := range cells {
		if !cell.Visible || !cell.Present || cell.Null {
			continue
		}
		if typedColumnFloatFallbackMatches(cell, valueType, predicate) {
			rows = append(rows, i)
		}
	}
	return rows
}

func runTypedColumnFloatFallback(cells []typedColumnFloatFallbackCell, valueType ColumnStoreValueType, predicate typedColumnFloatFallbackPredicate) typedColumnFloatFallbackResult {
	result := typedColumnFloatFallbackResult{}
	result.Diagnostics.PhysicalBytesScanned = int64(len(cells) * 8) // current scalar floats are carried as raw int64 bit patterns.
	result.Diagnostics.DecodedBytes = int64(len(cells) * typedColumnFloatLogicalWidth(valueType))
	if len(cells) > 0 {
		result.Diagnostics.FallbackBlocks = 1
	}
	switch predicate.Kind {
	case typedColumnFloatFallbackEqual:
		result.Diagnostics.NativeFloatLayoutMissingFallbacks = result.Diagnostics.FallbackBlocks
	case typedColumnFloatFallbackRange, typedColumnFloatFallbackAll:
		result.Diagnostics.RawInt64BitPatternRejectedFallback = result.Diagnostics.FallbackBlocks
	}
	for _, cell := range cells {
		result.RowsScanned++
		if !cell.Visible {
			result.VisibilityExcludedRows++
			continue
		}
		if !cell.Present {
			result.DefaultRows++
			continue
		}
		if cell.Null {
			result.NullRows++
			continue
		}
		if !typedColumnFloatFallbackMatches(cell, valueType, predicate) {
			continue
		}
		value := typedColumnFloatFallbackValue64(cell, valueType)
		result.RowsMatched++
		result.NonNulls++
		if math.IsNaN(value) {
			result.NaNRows++
		} else if !result.HasMinMax {
			result.Min = value
			result.Max = value
			result.HasMinMax = true
		} else {
			if typedColumnFloatFallbackLessForMin(value, result.Min) {
				result.Min = value
			}
			if typedColumnFloatFallbackGreaterForMax(value, result.Max) {
				result.Max = value
			}
		}
		result.Sum += value
	}
	if result.NonNulls > 0 {
		result.Avg = result.Sum / float64(result.NonNulls)
	}
	return result
}

func typedColumnFloatFallbackLessForMin(value, current float64) bool {
	return value < current || (value == 0 && current == 0 && math.Signbit(value) && !math.Signbit(current))
}

func typedColumnFloatFallbackGreaterForMax(value, current float64) bool {
	return value > current || (value == 0 && current == 0 && !math.Signbit(value) && math.Signbit(current))
}

func typedColumnFloatFallbackMatches(cell typedColumnFloatFallbackCell, valueType ColumnStoreValueType, predicate typedColumnFloatFallbackPredicate) bool {
	switch predicate.Kind {
	case typedColumnFloatFallbackAll:
		return true
	case typedColumnFloatFallbackEqual:
		if valueType == ColumnStoreValueFloat32 {
			return cell.F32 == predicate.F32
		}
		return cell.F64 == predicate.F64
	case typedColumnFloatFallbackRange:
		if valueType == ColumnStoreValueFloat32 {
			return cell.F32 >= predicate.Min32 && cell.F32 <= predicate.Max32
		}
		return cell.F64 >= predicate.Min64 && cell.F64 <= predicate.Max64
	default:
		return false
	}
}

func typedColumnFloatFallbackValue64(cell typedColumnFloatFallbackCell, valueType ColumnStoreValueType) float64 {
	if valueType == ColumnStoreValueFloat32 {
		return float64(cell.F32)
	}
	return cell.F64
}

func typedColumnFloatLogicalWidth(valueType ColumnStoreValueType) int {
	if valueType == ColumnStoreValueFloat32 {
		return 4
	}
	return 8
}

func typedColumnFloatFallbackZeroPredicate(valueType ColumnStoreValueType) typedColumnFloatFallbackPredicate {
	if valueType == ColumnStoreValueFloat32 {
		return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F32: 0}
	}
	return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F64: 0}
}

func typedColumnFloatFallbackNaNPredicate(valueType ColumnStoreValueType) typedColumnFloatFallbackPredicate {
	if valueType == ColumnStoreValueFloat32 {
		return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F32: math.Float32frombits(0x7fc01234)}
	}
	return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F64: math.Float64frombits(0x7ff8000000001234)}
}

func typedColumnFloatFallbackPositiveInfPredicate(valueType ColumnStoreValueType) typedColumnFloatFallbackPredicate {
	if valueType == ColumnStoreValueFloat32 {
		return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F32: float32(math.Inf(1))}
	}
	return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F64: math.Inf(1)}
}

func typedColumnFloatFallbackNegativeOrdinaryPredicate(valueType ColumnStoreValueType) typedColumnFloatFallbackPredicate {
	if valueType == ColumnStoreValueFloat32 {
		return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F32: -2.5}
	}
	return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F64: -2.5}
}

func typedColumnFloatFallbackRangeFromZeroToPositiveInf(valueType ColumnStoreValueType) typedColumnFloatFallbackPredicate {
	if valueType == ColumnStoreValueFloat32 {
		return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackRange, Min32: float32(math.Copysign(0, -1)), Max32: float32(math.Inf(1))}
	}
	return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackRange, Min64: math.Copysign(0, -1), Max64: math.Inf(1)}
}

func typedColumnFloatFallbackRangeFromNegativeInfToZero(valueType ColumnStoreValueType) typedColumnFloatFallbackPredicate {
	if valueType == ColumnStoreValueFloat32 {
		return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackRange, Min32: float32(math.Inf(-1)), Max32: float32(math.Copysign(0, -1))}
	}
	return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackRange, Min64: math.Inf(-1), Max64: math.Copysign(0, -1)}
}

func typedColumnFloatFallbackCellsFromFloat64Values(valueType ColumnStoreValueType, values []float64) []typedColumnFloatFallbackCell {
	cells := make([]typedColumnFloatFallbackCell, len(values))
	for i, value := range values {
		cells[i] = typedColumnFloatFallbackCell{Present: true, Visible: true}
		if valueType == ColumnStoreValueFloat32 {
			cells[i].F32 = float32(value)
		} else {
			cells[i].F64 = value
		}
	}
	return cells
}

func assertTypedColumnFloatFallbackCanonicalZeroMinMax(t *testing.T, result typedColumnFloatFallbackResult) {
	t.Helper()
	if !result.HasMinMax || result.Min != 0 || result.Max != 0 {
		t.Fatalf("zero aggregate min=%v max=%v has=%v want zero min/max", result.Min, result.Max, result.HasMinMax)
	}
	if !math.Signbit(result.Min) || math.Float64bits(result.Min) != 0x8000000000000000 {
		t.Fatalf("zero aggregate min bits=0x%016x signbit=%v want canonical -0", math.Float64bits(result.Min), math.Signbit(result.Min))
	}
	if math.Signbit(result.Max) || math.Float64bits(result.Max) != 0 {
		t.Fatalf("zero aggregate max bits=0x%016x signbit=%v want canonical +0", math.Float64bits(result.Max), math.Signbit(result.Max))
	}
}

func typedColumnFloatFallbackSameFloat64(left, right float64) bool {
	if math.IsNaN(left) || math.IsNaN(right) {
		return math.IsNaN(left) && math.IsNaN(right)
	}
	return math.Float64bits(left) == math.Float64bits(right)
}

func assertTypedColumnFloatFallbackDiagnostics(t *testing.T, valueType ColumnStoreValueType, kind typedColumnFloatFallbackPredicateKind, diag typedColumnFloatFallbackDiagnostics) {
	t.Helper()
	if diag.FallbackBlocks != 1 || diag.PhysicalBytesScanned != 56 || diag.MappedBytes != 0 || diag.DecodedBytes != int64(7*typedColumnFloatLogicalWidth(valueType)) {
		t.Fatalf("diagnostics=%+v want fallback block, physical raw-carrier bytes, zero mapped bytes, decoded logical bytes", diag)
	}
	if kind == typedColumnFloatFallbackAll && diag.RawInt64BitPatternRejectedFallback != 1 {
		t.Fatalf("diagnostics=%+v want raw-bit aggregate fallback diagnostic", diag)
	}
}
