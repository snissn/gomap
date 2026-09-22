package typedkernel

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

type int64Accum struct {
	count int64
	sum   int64
	min   int64
	max   int64
	has   bool
}

func reduceInt64Aggregate(op AggregateOp, req ReduceRequest, _ *Scratch) (AggregateResult, error) {
	rows, err := validateSelectionRows(req)
	if err != nil {
		return AggregateResult{}, err
	}
	if op == OpCountNonNull {
		if req.Int64Cursor != nil {
			return AggregateResult{}, fmt.Errorf("typedkernel: count non-null reducer does not accept int64 cursor")
		}
		return AggregateResult{Op: op, NonNulls: int64(req.Selection.Count()), HasValue: true}, nil
	}
	var acc int64Accum
	if req.Int64Cursor != nil && len(req.Int64Values) != 0 {
		return AggregateResult{}, fmt.Errorf("typedkernel: int64 reducer got both values and cursor")
	}
	if req.Selection.Kind() == typedcolumn.RowSelectionEmpty {
		if req.Int64Cursor != nil {
			if req.Int64Cursor.Rows() != rows {
				return AggregateResult{}, fmt.Errorf("typedkernel: int64 cursor rows=%d want %d", req.Int64Cursor.Rows(), rows)
			}
			if err := req.Int64Cursor.Finish(); err != nil {
				return AggregateResult{}, err
			}
		}
		if len(req.Int64Values) != 0 && len(req.Int64Values) != rows {
			return AggregateResult{}, fmt.Errorf("typedkernel: int64 values rows=%d want %d", len(req.Int64Values), rows)
		}
		return int64Result(op, acc)
	}
	if req.Int64Cursor != nil {
		return reduceInt64AggregateCursor(op, req, rows)
	}
	if len(req.Int64Values) != rows {
		return AggregateResult{}, fmt.Errorf("typedkernel: int64 values rows=%d want %d", len(req.Int64Values), rows)
	}
	switch req.Selection.Kind() {
	case typedcolumn.RowSelectionEmpty:
		return int64Result(op, acc)
	case typedcolumn.RowSelectionAll:
		for row := 0; row < rows; row++ {
			if err := acc.add(op, req.Int64Values[row]); err != nil {
				return AggregateResult{}, err
			}
		}
	case typedcolumn.RowSelectionRange:
		start, end, ok := req.Selection.SingleRange()
		if !ok || start < 0 || end < start || end > rows {
			return AggregateResult{}, fmt.Errorf("typedkernel: invalid int64 range selection [%d,%d) rows=%d", start, end, rows)
		}
		for row := start; row < end; row++ {
			if err := acc.add(op, req.Int64Values[row]); err != nil {
				return AggregateResult{}, err
			}
		}
	case typedcolumn.RowSelectionRanges:
		for _, r := range req.Selection.Ranges() {
			if r.Start < 0 || r.End < r.Start || r.End > rows {
				return AggregateResult{}, fmt.Errorf("typedkernel: invalid int64 ranges selection [%d,%d) rows=%d", r.Start, r.End, rows)
			}
			for row := r.Start; row < r.End; row++ {
				if err := acc.add(op, req.Int64Values[row]); err != nil {
					return AggregateResult{}, err
				}
			}
		}
	case typedcolumn.RowSelectionBitmap:
		for wordIndex, word := range req.Selection.BitmapWords() {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				row := wordIndex*64 + bit
				if row >= rows {
					break
				}
				if err := acc.add(op, req.Int64Values[row]); err != nil {
					return AggregateResult{}, err
				}
				word &^= uint64(1) << uint(bit)
			}
		}
	case typedcolumn.RowSelectionSparse:
		for _, row := range req.Selection.SparseRows() {
			if row < 0 || row >= rows {
				return AggregateResult{}, fmt.Errorf("typedkernel: invalid int64 sparse row=%d rows=%d", row, rows)
			}
			if err := acc.add(op, req.Int64Values[row]); err != nil {
				return AggregateResult{}, err
			}
		}
	default:
		return AggregateResult{}, fmt.Errorf("typedkernel: unsupported int64 row selection shape %s", req.Selection.Shape().Kind)
	}
	return int64Result(op, acc)
}

func reduceInt64AggregateCursor(op AggregateOp, req ReduceRequest, rows int) (AggregateResult, error) {
	cursor := req.Int64Cursor
	if cursor.Rows() != rows {
		return AggregateResult{}, fmt.Errorf("typedkernel: int64 cursor rows=%d want %d", cursor.Rows(), rows)
	}
	var acc int64Accum
	switch req.Selection.Kind() {
	case typedcolumn.RowSelectionAll:
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return AggregateResult{}, err
			}
			if err := acc.add(op, value); err != nil {
				return AggregateResult{}, err
			}
		}
	case typedcolumn.RowSelectionRange:
		start, end, ok := req.Selection.SingleRange()
		if !ok || start < 0 || end < start || end > rows {
			return AggregateResult{}, fmt.Errorf("typedkernel: invalid int64 cursor range selection [%d,%d) rows=%d", start, end, rows)
		}
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return AggregateResult{}, err
			}
			if row >= start && row < end {
				if err := acc.add(op, value); err != nil {
					return AggregateResult{}, err
				}
			}
		}
	case typedcolumn.RowSelectionRanges:
		ranges := req.Selection.Ranges()
		for _, r := range ranges {
			if r.Start < 0 || r.End < r.Start || r.End > rows {
				return AggregateResult{}, fmt.Errorf("typedkernel: invalid int64 cursor ranges selection [%d,%d) rows=%d", r.Start, r.End, rows)
			}
		}
		rangeIndex := 0
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return AggregateResult{}, err
			}
			for rangeIndex < len(ranges) && row >= ranges[rangeIndex].End {
				rangeIndex++
			}
			if rangeIndex < len(ranges) && row >= ranges[rangeIndex].Start {
				if err := acc.add(op, value); err != nil {
					return AggregateResult{}, err
				}
			}
		}
	case typedcolumn.RowSelectionBitmap:
		words := req.Selection.BitmapWords()
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return AggregateResult{}, err
			}
			wordIndex := row / 64
			if wordIndex < len(words) && words[wordIndex]&(uint64(1)<<uint(row%64)) != 0 {
				if err := acc.add(op, value); err != nil {
					return AggregateResult{}, err
				}
			}
		}
	case typedcolumn.RowSelectionSparse:
		sparse := req.Selection.SparseRows()
		for _, row := range sparse {
			if row < 0 || row >= rows {
				return AggregateResult{}, fmt.Errorf("typedkernel: invalid int64 cursor sparse row=%d rows=%d", row, rows)
			}
		}
		sparseIndex := 0
		for row := 0; row < rows; row++ {
			value, err := cursor.Next()
			if err != nil {
				return AggregateResult{}, err
			}
			if sparseIndex < len(sparse) && row == sparse[sparseIndex] {
				if err := acc.add(op, value); err != nil {
					return AggregateResult{}, err
				}
				sparseIndex++
			}
		}
	default:
		return AggregateResult{}, fmt.Errorf("typedkernel: unsupported int64 cursor row selection shape %s", req.Selection.Shape().Kind)
	}
	if err := cursor.Finish(); err != nil {
		return AggregateResult{}, err
	}
	return int64Result(op, acc)
}

func (a *int64Accum) add(op AggregateOp, value int64) error {
	if op == OpSum || op == OpAvg {
		if value > 0 && a.sum > math.MaxInt64-value {
			return fmt.Errorf("typedkernel: int64 sum overflow current=%d value=%d", a.sum, value)
		}
		if value < 0 && a.sum < math.MinInt64-value {
			return fmt.Errorf("typedkernel: int64 sum overflow current=%d value=%d", a.sum, value)
		}
		a.sum += value
	}
	a.count++
	if !a.has {
		a.min = value
		a.max = value
		a.has = true
		return nil
	}
	if value < a.min {
		a.min = value
	}
	if value > a.max {
		a.max = value
	}
	return nil
}

func int64Result(op AggregateOp, acc int64Accum) (AggregateResult, error) {
	out := AggregateResult{Op: op, NonNulls: acc.count, HasValue: acc.has}
	switch op {
	case OpSum:
		out.Sum = acc.sum
		// Sum over an empty selection is the additive identity and still a
		// well-formed aggregate result.
		out.HasValue = true
	case OpAvg:
		out.Sum = acc.sum
		if acc.count != 0 {
			out.Avg = float64(acc.sum) / float64(acc.count)
		}
	case OpMin:
		if acc.has {
			out.Min = acc.min
		}
	case OpMax:
		if acc.has {
			out.Max = acc.max
		}
	default:
		return AggregateResult{}, fmt.Errorf("typedkernel: int64 reducer got op=%s", op)
	}
	return out, nil
}
