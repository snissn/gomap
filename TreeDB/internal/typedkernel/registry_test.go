package typedkernel_test

import (
	"math"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typedkernel"
)

func TestInt64AggregateParityRowSelectionShapes(t *testing.T) {
	values := []int64{-7, 2, 0, 5, -3, 11, 4, -9, 8, 6}
	selections := map[string]typedcolumn.RowSelection{
		"all":    mustSelection(typedcolumn.NewAllRowSelection(len(values))),
		"range":  mustSelection(typedcolumn.NewRangeRowSelection(len(values), 2, 8)),
		"ranges": mustSelection(typedcolumn.NewRangesRowSelection(len(values), []typedcolumn.RowRange{{Start: 1, End: 3}, {Start: 5, End: 8}})),
		"bitmap": mustSelection(typedcolumn.NewBitmapRowSelection(len(values), []uint64{(1 << 0) | (1 << 2) | (1 << 5) | (1 << 9)})),
		"sparse": mustSelection(typedcolumn.NewSparseRowSelection(len(values), []int{0, 3, 4, 9})),
		"empty":  mustSelection(typedcolumn.NewEmptyRowSelection(len(values))),
	}
	ops := []typedkernel.AggregateOp{typedkernel.OpCountRows, typedkernel.OpCountNonNull, typedkernel.OpSum, typedkernel.OpAvg, typedkernel.OpMin, typedkernel.OpMax}
	reg := typedkernel.DefaultRegistry()
	for shape, selection := range selections {
		want := expectedInt64(values, selection)
		for _, op := range ops {
			t.Run(shape+"/"+string(op), func(t *testing.T) {
				prepared, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: op, Semantic: int64Semantic(false, typedcolumn.EncodingRawInt64), Layout: int64Layout(false, typedcolumn.EncodingRawInt64, typedcolumn.CompressionNone)})
				if err != nil {
					t.Fatalf("dispatch: %v", err)
				}
				got, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: len(values), Selection: selection, Int64Values: values}, nil)
				if err != nil {
					t.Fatalf("reduce: %v", err)
				}
				assertInt64Aggregate(t, op, got, want)
			})
		}
	}
}

func TestInt64AggregateCursorParityRowSelectionShapes(t *testing.T) {
	values := []int64{-7, 2, 0, 5, -3, 11, 4, -9, 8, 6}
	selections := map[string]typedcolumn.RowSelection{
		"all":    mustSelection(typedcolumn.NewAllRowSelection(len(values))),
		"range":  mustSelection(typedcolumn.NewRangeRowSelection(len(values), 2, 8)),
		"ranges": mustSelection(typedcolumn.NewRangesRowSelection(len(values), []typedcolumn.RowRange{{Start: 1, End: 3}, {Start: 5, End: 8}})),
		"bitmap": mustSelection(typedcolumn.NewBitmapRowSelection(len(values), []uint64{(1 << 0) | (1 << 2) | (1 << 5) | (1 << 9)})),
		"sparse": mustSelection(typedcolumn.NewSparseRowSelection(len(values), []int{0, 3, 4, 9})),
	}
	reg := typedkernel.DefaultRegistry()
	for _, enc := range []typedcolumn.Encoding{typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint} {
		enc := enc
		t.Run(enc.String(), func(t *testing.T) {
			builder := typedcolumn.NewGranuleBuilder(typedcolumn.Config{Encoding: enc, Compression: typedcolumn.CompressionNone})
			granule, err := builder.BuildInt64(values)
			if err != nil {
				t.Fatalf("BuildInt64: %v", err)
			}
			for shape, selection := range selections {
				shape := shape
				selection := selection
				t.Run(shape, func(t *testing.T) {
					prepared, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpSum, Semantic: int64Semantic(false, enc), Layout: int64Layout(false, enc, typedcolumn.CompressionNone)})
					if err != nil {
						t.Fatalf("dispatch: %v", err)
					}
					var reader typedcolumn.GranuleReader
					cursor, err := reader.Int64Cursor(granule)
					if err != nil {
						t.Fatalf("Int64Cursor: %v", err)
					}
					got, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: len(values), Selection: selection, Int64Cursor: &cursor}, nil)
					if err != nil {
						t.Fatalf("reduce cursor: %v", err)
					}
					want := expectedInt64(values, selection)
					assertInt64Aggregate(t, typedkernel.OpSum, got, want)
				})
			}
		})
	}
}

func TestInt64AggregateOverflow(t *testing.T) {
	reg := typedkernel.DefaultRegistry()
	prepared, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpSum, Semantic: int64Semantic(false, typedcolumn.EncodingRawInt64), Layout: int64Layout(false, typedcolumn.EncodingRawInt64, typedcolumn.CompressionNone)})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	selection := mustSelection(typedcolumn.NewAllRowSelection(2))
	for name, values := range map[string][]int64{
		"positive": {math.MaxInt64, 1},
		"negative": {math.MinInt64, -1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: len(values), Selection: selection, Int64Values: values}, nil)
			if err == nil || !strings.Contains(err.Error(), "overflow") {
				t.Fatalf("expected overflow error, got %v", err)
			}
		})
	}
}

func TestInt64MinMaxDoNotAccumulateOverflowingSum(t *testing.T) {
	reg := typedkernel.DefaultRegistry()
	selection := mustSelection(typedcolumn.NewAllRowSelection(2))
	tests := []struct {
		name string
		op   typedkernel.AggregateOp
		vals []int64
		want int64
	}{
		{name: "min positive sum overflow", op: typedkernel.OpMin, vals: []int64{math.MaxInt64, 1}, want: 1},
		{name: "max positive sum overflow", op: typedkernel.OpMax, vals: []int64{math.MaxInt64, 1}, want: math.MaxInt64},
		{name: "min negative sum overflow", op: typedkernel.OpMin, vals: []int64{math.MinInt64, -1}, want: math.MinInt64},
		{name: "max negative sum overflow", op: typedkernel.OpMax, vals: []int64{math.MinInt64, -1}, want: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: tc.op, Semantic: int64Semantic(false, typedcolumn.EncodingRawInt64), Layout: int64Layout(false, typedcolumn.EncodingRawInt64, typedcolumn.CompressionNone)})
			if err != nil {
				t.Fatalf("dispatch: %v", err)
			}
			got, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: len(tc.vals), Selection: selection, Int64Values: tc.vals}, nil)
			if err != nil {
				t.Fatalf("reduce: %v", err)
			}
			if !got.HasValue || got.NonNulls != 2 {
				t.Fatalf("result metadata=%+v", got)
			}
			if tc.op == typedkernel.OpMin && got.Min != tc.want {
				t.Fatalf("min=%d want %d", got.Min, tc.want)
			}
			if tc.op == typedkernel.OpMax && got.Max != tc.want {
				t.Fatalf("max=%d want %d", got.Max, tc.want)
			}
		})
	}
}

func TestInt64CountNonNullRejectsCursorWithoutConsuming(t *testing.T) {
	values := []int64{1, 2, 3}
	builder := typedcolumn.NewGranuleBuilder(typedcolumn.Config{Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone})
	granule, err := builder.BuildInt64(values)
	if err != nil {
		t.Fatalf("BuildInt64: %v", err)
	}
	granule.Payload = append(append([]byte(nil), granule.Payload...), 0)
	granule.RawBytes = len(granule.Payload)
	granule.StoredBytes = len(granule.Payload)
	granule.PayloadRef.Length = len(granule.Payload)
	var reader typedcolumn.GranuleReader
	cursor, err := reader.Int64Cursor(granule)
	if err != nil {
		t.Fatalf("Int64Cursor with trailing bytes should be constructed before Finish: %v", err)
	}
	prepared, err := typedkernel.DefaultRegistry().Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpCountNonNull, Semantic: int64Semantic(false, typedcolumn.EncodingDeltaVarint), Layout: int64Layout(false, typedcolumn.EncodingDeltaVarint, typedcolumn.CompressionNone)})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	selection := mustSelection(typedcolumn.NewAllRowSelection(len(values)))
	_, err = prepared.Reduce(typedkernel.ReduceRequest{Rows: len(values), Selection: selection, Int64Cursor: &cursor}, nil)
	if err == nil || !strings.Contains(err.Error(), "does not accept int64 cursor") {
		t.Fatalf("count non-null cursor err=%v want explicit cursor rejection", err)
	}
}

func TestInt64EmptySelectionDoesNotRequireValues(t *testing.T) {
	reg := typedkernel.DefaultRegistry()
	selection := mustSelection(typedcolumn.NewEmptyRowSelection(10))
	for _, op := range []typedkernel.AggregateOp{typedkernel.OpSum, typedkernel.OpAvg, typedkernel.OpMin, typedkernel.OpMax} {
		t.Run(string(op), func(t *testing.T) {
			prepared, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: op, Semantic: int64Semantic(false, typedcolumn.EncodingRawInt64), Layout: int64Layout(false, typedcolumn.EncodingRawInt64, typedcolumn.CompressionNone)})
			if err != nil {
				t.Fatalf("dispatch: %v", err)
			}
			got, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: 10, Selection: selection}, nil)
			if err != nil {
				t.Fatalf("reduce with nil values: %v", err)
			}
			if got.NonNulls != 0 {
				t.Fatalf("non-nulls=%d want 0", got.NonNulls)
			}
			if op == typedkernel.OpSum && !got.HasValue {
				t.Fatalf("empty sum should be additive identity: %+v", got)
			}
			if op != typedkernel.OpSum && got.HasValue {
				t.Fatalf("empty %s should not have a value: %+v", op, got)
			}
		})
	}
}

func TestInt64EmptySelectionValidatesCursorRequests(t *testing.T) {
	values := []int64{1, 2, 3}
	builder := typedcolumn.NewGranuleBuilder(typedcolumn.Config{Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone})
	granule, err := builder.BuildInt64(values)
	if err != nil {
		t.Fatalf("BuildInt64: %v", err)
	}
	var reader typedcolumn.GranuleReader
	cursor, err := reader.Int64Cursor(granule)
	if err != nil {
		t.Fatalf("Int64Cursor: %v", err)
	}
	prepared, err := typedkernel.DefaultRegistry().Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpSum, Semantic: int64Semantic(false, typedcolumn.EncodingDeltaVarint), Layout: int64Layout(false, typedcolumn.EncodingDeltaVarint, typedcolumn.CompressionNone)})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	selection := mustSelection(typedcolumn.NewEmptyRowSelection(len(values)))
	if _, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: len(values), Selection: selection, Int64Cursor: &cursor}, nil); err == nil || !strings.Contains(err.Error(), "decoded rows=0 want=3") {
		t.Fatalf("empty cursor reduce err=%v want cursor validation failure", err)
	}

	shortGranule, err := builder.BuildInt64([]int64{99})
	if err != nil {
		t.Fatalf("BuildInt64 short: %v", err)
	}
	shortCursor, err := reader.Int64Cursor(shortGranule)
	if err != nil {
		t.Fatalf("short Int64Cursor: %v", err)
	}
	if _, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: len(values), Selection: selection, Int64Cursor: &shortCursor}, nil); err == nil || !strings.Contains(err.Error(), "int64 cursor rows=1 want 3") {
		t.Fatalf("empty short-cursor reduce err=%v want cursor row-domain mismatch", err)
	}
}

func TestDispatchSemanticAndLayoutGates(t *testing.T) {
	reg := typedkernel.DefaultRegistry()
	tests := []struct {
		name    string
		op      typedkernel.AggregateOp
		sem     columnsemantics.Descriptor
		layout  columnlayout.Capabilities
		wantErr string
	}{
		{
			name:    "semantic layout descriptor mismatch fails closed",
			op:      typedkernel.OpCountRows,
			sem:     int64Semantic(false, typedcolumn.EncodingRawInt64),
			layout:  boolLayout(),
			wantErr: "descriptor mismatch",
		},
		{
			name:    "bool sum fails semantic gate",
			op:      typedkernel.OpSum,
			sem:     boolSemantic(),
			layout:  boolLayout(),
			wantErr: "semantic capability",
		},
		{
			name:    "zstd int64 sum fails layout gate",
			op:      typedkernel.OpSum,
			sem:     int64Semantic(false, typedcolumn.EncodingRawInt64),
			layout:  int64Layout(false, typedcolumn.EncodingRawInt64, typedcolumn.CompressionZSTD),
			wantErr: "layout capability",
		},
		{
			name:    "nullable int64 value aggregate remains fallback",
			op:      typedkernel.OpSum,
			sem:     int64Semantic(true, typedcolumn.EncodingNullableInt64),
			layout:  int64Layout(true, typedcolumn.EncodingNullableInt64, typedcolumn.CompressionNone),
			wantErr: "status=fallback",
		},
		{
			name:    "nullable count non-null has no value-mask kernel yet",
			op:      typedkernel.OpCountNonNull,
			sem:     int64Semantic(true, typedcolumn.EncodingNullableInt64),
			layout:  int64Layout(true, typedcolumn.EncodingNullableInt64, typedcolumn.CompressionNone),
			wantErr: "no kernel registered",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: tc.op, Semantic: tc.sem, Layout: tc.layout})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Dispatch error=%v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestCountRowsAndCountNonNullDistinct(t *testing.T) {
	reg := typedkernel.DefaultRegistry()
	selection := mustSelection(typedcolumn.NewRangeRowSelection(10, 2, 7))
	countRows, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpCountRows, Semantic: int64Semantic(true, typedcolumn.EncodingNullableInt64), Layout: int64Layout(true, typedcolumn.EncodingNullableInt64, typedcolumn.CompressionNone)})
	if err != nil {
		t.Fatalf("count rows dispatch: %v", err)
	}
	rows, err := countRows.Reduce(typedkernel.ReduceRequest{Rows: 10, Selection: selection}, nil)
	if err != nil {
		t.Fatalf("count rows reduce: %v", err)
	}
	if rows.Rows != 5 || rows.NonNulls != 0 || rows.Op != typedkernel.OpCountRows {
		t.Fatalf("count rows result=%+v", rows)
	}

	countNonNull, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpCountNonNull, Semantic: int64Semantic(false, typedcolumn.EncodingRawInt64), Layout: int64Layout(false, typedcolumn.EncodingRawInt64, typedcolumn.CompressionNone)})
	if err != nil {
		t.Fatalf("count non-null dispatch: %v", err)
	}
	nonNull, err := countNonNull.Reduce(typedkernel.ReduceRequest{Rows: 10, Selection: selection, Int64Values: make([]int64, 10)}, nil)
	if err != nil {
		t.Fatalf("count non-null reduce: %v", err)
	}
	if nonNull.NonNulls != 5 || nonNull.Rows != 0 || nonNull.Op != typedkernel.OpCountNonNull {
		t.Fatalf("count non-null result=%+v", nonNull)
	}
}

func TestGenericCountRowsDispatchesForNonInt64(t *testing.T) {
	prepared, err := typedkernel.DefaultRegistry().Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpCountRows, Semantic: boolSemantic(), Layout: boolLayout()})
	if err != nil {
		t.Fatalf("dispatch bool count rows: %v", err)
	}
	selection := mustSelection(typedcolumn.NewRangeRowSelection(8, 1, 6))
	got, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: 8, Selection: selection}, nil)
	if err != nil {
		t.Fatalf("reduce bool count rows: %v", err)
	}
	if prepared.KernelName() != "generic.count_rows.v1" || got.Rows != 5 || got.NonNulls != 0 {
		t.Fatalf("kernel=%q result=%+v want generic count rows", prepared.KernelName(), got)
	}
}

func TestBoolCountsKernelSelectionParity(t *testing.T) {
	values := []bool{true, false, true, true, false, false, true, false, true, false}
	builder := typedcolumn.NewGranuleBuilder(typedcolumn.Config{Encoding: typedcolumn.EncodingBoolBitpackRLE, Compression: typedcolumn.CompressionNone})
	granule, err := builder.BuildBool(values)
	if err != nil {
		t.Fatalf("BuildBool: %v", err)
	}
	selections := map[string]typedcolumn.RowSelection{
		"all":    mustSelection(typedcolumn.NewAllRowSelection(len(values))),
		"empty":  mustSelection(typedcolumn.NewEmptyRowSelection(len(values))),
		"range":  mustSelection(typedcolumn.NewRangeRowSelection(len(values), 2, 8)),
		"ranges": mustSelection(typedcolumn.NewRangesRowSelection(len(values), []typedcolumn.RowRange{{Start: 0, End: 2}, {Start: 5, End: 9}})),
		"bitmap": mustSelection(typedcolumn.NewBitmapRowSelection(len(values), []uint64{(1 << 0) | (1 << 3) | (1 << 4) | (1 << 9)})),
		"sparse": mustSelection(typedcolumn.NewSparseRowSelection(len(values), []int{1, 2, 7, 8})),
	}
	prepared, err := typedkernel.DefaultRegistry().Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpBoolCounts, Semantic: boolSemantic(), Layout: boolLayout()})
	if err != nil {
		t.Fatalf("dispatch bool counts: %v", err)
	}
	if prepared.KernelName() != "bool.counts.v1" {
		t.Fatalf("kernel=%q want bool.counts.v1", prepared.KernelName())
	}
	var reader typedcolumn.GranuleReader
	for shape, selection := range selections {
		t.Run(shape, func(t *testing.T) {
			wantTrue, wantFalse := expectedBoolCounts(values, selection)
			got, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: len(values), Selection: selection, BoolGranule: granule, HasBoolGranule: true, BoolReader: &reader}, nil)
			if err != nil {
				t.Fatalf("reduce bool counts: %v", err)
			}
			if got.Rows != int64(selection.Count()) || got.NonNulls != int64(selection.Count()) || got.TrueCount != int64(wantTrue) || got.FalseCount != int64(wantFalse) || !got.HasValue {
				t.Fatalf("result=%+v want rows/non_null=%d true=%d false=%d", got, selection.Count(), wantTrue, wantFalse)
			}
		})
	}
}

func TestBoolCountNonNullKernel(t *testing.T) {
	selection := mustSelection(typedcolumn.NewSparseRowSelection(8, []int{1, 4, 7}))
	prepared, err := typedkernel.DefaultRegistry().Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpCountNonNull, Semantic: boolSemantic(), Layout: boolLayout()})
	if err != nil {
		t.Fatalf("dispatch bool count non-null: %v", err)
	}
	got, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: 8, Selection: selection}, nil)
	if err != nil {
		t.Fatalf("reduce bool count non-null: %v", err)
	}
	if prepared.KernelName() != "bool.counts.v1" || got.NonNulls != 3 || got.Rows != 0 {
		t.Fatalf("kernel=%q result=%+v", prepared.KernelName(), got)
	}
}

func TestCallerOwnedFakeNonInt64KernelDispatch(t *testing.T) {
	reg, err := typedkernel.NewRegistry(nil)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	reg, err = reg.WithKernel(typedkernel.KernelSpec{
		Name:     "test.bool.count_non_null.fake",
		Logical:  columnsemantics.LogicalBool,
		Physical: typedcolumn.ColumnTypeBool,
		Ops:      []typedkernel.AggregateOp{typedkernel.OpCountNonNull},
		Reduce: func(op typedkernel.AggregateOp, req typedkernel.ReduceRequest, _ *typedkernel.Scratch) (typedkernel.AggregateResult, error) {
			return typedkernel.AggregateResult{Op: op, NonNulls: int64(req.Selection.Count()) + 1000, HasValue: true}, nil
		},
	})
	if err != nil {
		t.Fatalf("with fake kernel: %v", err)
	}
	prepared, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.OpCountNonNull, Semantic: boolSemantic(), Layout: boolLayout()})
	if err != nil {
		t.Fatalf("dispatch fake bool kernel: %v", err)
	}
	selection := mustSelection(typedcolumn.NewSparseRowSelection(8, []int{1, 4, 7}))
	got, err := prepared.Reduce(typedkernel.ReduceRequest{Rows: 8, Selection: selection}, nil)
	if err != nil {
		t.Fatalf("reduce fake bool kernel: %v", err)
	}
	if prepared.KernelName() != "test.bool.count_non_null.fake" || got.NonNulls != 1003 {
		t.Fatalf("fake kernel name=%q result=%+v", prepared.KernelName(), got)
	}
}

type expectedAggregate struct {
	count int64
	sum   int64
	avg   float64
	min   int64
	max   int64
	has   bool
}

func expectedInt64(values []int64, selection typedcolumn.RowSelection) expectedAggregate {
	rows := selection.AppendRows(nil)
	out := expectedAggregate{count: int64(len(rows))}
	for _, row := range rows {
		v := values[row]
		out.sum += v
		if !out.has || v < out.min {
			out.min = v
		}
		if !out.has || v > out.max {
			out.max = v
		}
		out.has = true
	}
	if out.count != 0 {
		out.avg = float64(out.sum) / float64(out.count)
	}
	return out
}

func assertInt64Aggregate(t *testing.T, op typedkernel.AggregateOp, got typedkernel.AggregateResult, want expectedAggregate) {
	t.Helper()
	if got.Op != op {
		t.Fatalf("op=%s want %s", got.Op, op)
	}
	switch op {
	case typedkernel.OpCountRows:
		if got.Rows != want.count || got.NonNulls != 0 {
			t.Fatalf("count rows got=%+v want count=%d", got, want.count)
		}
	case typedkernel.OpCountNonNull:
		if got.NonNulls != want.count || got.Rows != 0 {
			t.Fatalf("count non-null got=%+v want count=%d", got, want.count)
		}
	case typedkernel.OpSum:
		if got.Sum != want.sum || got.NonNulls != want.count || !got.HasValue {
			t.Fatalf("sum got=%+v want sum=%d count=%d", got, want.sum, want.count)
		}
	case typedkernel.OpAvg:
		if got.Sum != want.sum || got.NonNulls != want.count || got.Avg != want.avg || got.HasValue != want.has {
			t.Fatalf("avg got=%+v want sum=%d count=%d avg=%f has=%v", got, want.sum, want.count, want.avg, want.has)
		}
	case typedkernel.OpMin:
		if got.Min != want.min || got.NonNulls != want.count || got.HasValue != want.has {
			t.Fatalf("min got=%+v want min=%d count=%d has=%v", got, want.min, want.count, want.has)
		}
	case typedkernel.OpMax:
		if got.Max != want.max || got.NonNulls != want.count || got.HasValue != want.has {
			t.Fatalf("max got=%+v want max=%d count=%d has=%v", got, want.max, want.count, want.has)
		}
	}
}

func mustSelection(selection typedcolumn.RowSelection, err error) typedcolumn.RowSelection {
	if err != nil {
		panic(err)
	}
	return selection
}

func int64Semantic(nullable bool, encoding typedcolumn.Encoding) columnsemantics.Descriptor {
	return columnsemantics.Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: encoding, Nullable: nullable}
}

func int64Layout(nullable bool, encoding typedcolumn.Encoding, compression typedcolumn.Compression) columnlayout.Capabilities {
	return columnlayout.CapabilitiesFor(columnlayout.Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: encoding, Compression: compression, Nullable: nullable, Defaultable: nullable})
}

func boolSemantic() columnsemantics.Descriptor {
	return columnsemantics.Descriptor{Logical: columnsemantics.LogicalBool, Physical: typedcolumn.ColumnTypeBool, Encoding: typedcolumn.EncodingBoolBitpackRLE}
}

func boolLayout() columnlayout.Capabilities {
	return columnlayout.CapabilitiesFor(columnlayout.Descriptor{Logical: columnsemantics.LogicalBool, Physical: typedcolumn.ColumnTypeBool, Encoding: typedcolumn.EncodingBoolBitpackRLE, Compression: typedcolumn.CompressionNone})
}

func expectedBoolCounts(values []bool, selection typedcolumn.RowSelection) (int, int) {
	trueCount := 0
	falseCount := 0
	for _, row := range selection.AppendRows(nil) {
		if values[row] {
			trueCount++
		} else {
			falseCount++
		}
	}
	return trueCount, falseCount
}
