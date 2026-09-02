package typedkernel

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func reduceBoolCounts(op AggregateOp, req ReduceRequest, _ *Scratch) (AggregateResult, error) {
	rows, err := validateSelectionRows(req)
	if err != nil {
		return AggregateResult{}, err
	}
	if len(req.Int64Values) != 0 || req.Int64Cursor != nil {
		return AggregateResult{}, fmt.Errorf("typedkernel: bool reducer got int64 inputs")
	}
	switch op {
	case OpCountNonNull:
		return AggregateResult{Op: op, NonNulls: int64(req.Selection.Count()), HasValue: true}, nil
	case OpBoolCounts:
		if !req.HasBoolGranule {
			if req.Selection.IsEmpty() {
				return AggregateResult{Op: op, Rows: 0, NonNulls: 0, HasValue: true}, nil
			}
			return AggregateResult{}, fmt.Errorf("typedkernel: bool counts reducer requires bool granule")
		}
		if req.BoolGranule.Rows != rows {
			return AggregateResult{}, fmt.Errorf("typedkernel: bool granule rows=%d want %d", req.BoolGranule.Rows, rows)
		}
		reader := req.BoolReader
		if reader == nil {
			reader = &typedcolumn.GranuleReader{}
		}
		trueCount, err := reader.CountBool(req.BoolGranule, req.Selection, true)
		if err != nil {
			return AggregateResult{}, err
		}
		selected := req.Selection.Count()
		if trueCount < 0 || trueCount > selected {
			return AggregateResult{}, fmt.Errorf("typedkernel: bool true count=%d outside selected=%d", trueCount, selected)
		}
		falseCount := selected - trueCount
		return AggregateResult{Op: op, Rows: int64(selected), NonNulls: int64(selected), TrueCount: int64(trueCount), FalseCount: int64(falseCount), HasValue: true}, nil
	default:
		return AggregateResult{}, fmt.Errorf("typedkernel: bool reducer got op=%s", op)
	}
}
