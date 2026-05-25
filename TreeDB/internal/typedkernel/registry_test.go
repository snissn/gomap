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
			name:    "bool sum fails semantic gate",
			op:      typedkernel.OpSum,
			sem:     boolSemantic(),
			layout:  boolLayout(),
			wantErr: "semantic capability",
		},
		{
			name:    "compressed int64 sum fails layout gate",
			op:      typedkernel.OpSum,
			sem:     int64Semantic(false, typedcolumn.EncodingRawInt64),
			layout:  int64Layout(false, typedcolumn.EncodingRawInt64, typedcolumn.CompressionSnappy),
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
