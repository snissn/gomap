package typedcolumn

import (
	"errors"
	"fmt"
	"math/bits"
)

type rowSelectionKind uint8

const (
	rowSelectionEmpty rowSelectionKind = iota
	rowSelectionAll
	rowSelectionRange
	rowSelectionRanges
	rowSelectionBitmap
	rowSelectionSparse
)

type rowRange struct {
	Start int
	End   int
}

type rowSelection struct {
	rows   int
	kind   rowSelectionKind
	start  int
	end    int
	ranges []rowRange
	bitmap []uint64
	sparse []int
	count  int
}

type rowSelectionShape struct {
	Kind        string
	Rows        int
	Count       int
	Ranges      int
	BitmapWords int
	SparseRows  int
}

func makeEmptyRowSelection(rows int) (rowSelection, error) {
	if rows < 0 {
		return rowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	return rowSelection{rows: rows, kind: rowSelectionEmpty}, nil
}

func makeAllRowSelection(rows int) (rowSelection, error) {
	if rows < 0 {
		return rowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	if rows == 0 {
		return rowSelection{rows: rows, kind: rowSelectionEmpty}, nil
	}
	return rowSelection{rows: rows, kind: rowSelectionAll, count: rows}, nil
}

func makeRangeRowSelection(rows int, start int, end int) (rowSelection, error) {
	if err := validateRowRange(rows, rowRange{Start: start, End: end}); err != nil {
		return rowSelection{}, err
	}
	if start == end {
		return makeEmptyRowSelection(rows)
	}
	if start == 0 && end == rows {
		return makeAllRowSelection(rows)
	}
	return rowSelection{rows: rows, kind: rowSelectionRange, start: start, end: end, count: end - start}, nil
}

func makeRangesRowSelection(rows int, ranges []rowRange) (rowSelection, error) {
	if rows < 0 {
		return rowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	if len(ranges) == 0 {
		return makeEmptyRowSelection(rows)
	}
	merged := make([]rowRange, 0, len(ranges))
	for i, r := range ranges {
		if err := validateRowRange(rows, r); err != nil {
			return rowSelection{}, fmt.Errorf("typedcolumn: invalid row range %d: %w", i, err)
		}
		if r.Start == r.End {
			continue
		}
		if len(merged) == 0 {
			merged = append(merged, r)
			continue
		}
		last := &merged[len(merged)-1]
		if r.Start < last.End {
			return rowSelection{}, fmt.Errorf("typedcolumn: row ranges overlap at %d", i)
		}
		if r.Start == last.End {
			last.End = r.End
			continue
		}
		merged = append(merged, r)
	}
	if len(merged) == 0 {
		return makeEmptyRowSelection(rows)
	}
	count := 0
	for _, r := range merged {
		count += r.End - r.Start
	}
	if count == rows {
		return makeAllRowSelection(rows)
	}
	if len(merged) == 1 {
		return makeRangeRowSelection(rows, merged[0].Start, merged[0].End)
	}
	return rowSelection{rows: rows, kind: rowSelectionRanges, ranges: merged, count: count}, nil
}

func makeSparseRowSelection(rows int, sparse []int) (rowSelection, error) {
	if rows < 0 {
		return rowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	if len(sparse) == 0 {
		return makeEmptyRowSelection(rows)
	}
	copySparse := make([]int, len(sparse))
	for i, row := range sparse {
		if row < 0 || row >= rows {
			return rowSelection{}, fmt.Errorf("typedcolumn: sparse row %d=%d outside [0,%d)", i, row, rows)
		}
		if i > 0 && row <= sparse[i-1] {
			return rowSelection{}, fmt.Errorf("typedcolumn: sparse rows not strictly increasing at %d", i)
		}
		copySparse[i] = row
	}
	if len(copySparse) == rows {
		return makeAllRowSelection(rows)
	}
	if isContiguousSparse(copySparse) {
		return makeRangeRowSelection(rows, copySparse[0], copySparse[len(copySparse)-1]+1)
	}
	return rowSelection{rows: rows, kind: rowSelectionSparse, sparse: copySparse, count: len(copySparse)}, nil
}

func makeBitmapRowSelection(rows int, bitmap []uint64) (rowSelection, error) {
	if rows < 0 {
		return rowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	wantWords := rowSelectionBitmapWords(rows)
	if len(bitmap) != wantWords {
		return rowSelection{}, fmt.Errorf("typedcolumn: bitmap words=%d want=%d", len(bitmap), wantWords)
	}
	if wantWords == 0 {
		return makeEmptyRowSelection(rows)
	}
	copyBitmap := append([]uint64(nil), bitmap...)
	if padding := rows % 64; padding != 0 {
		valid := uint64(1<<uint(padding)) - 1
		if copyBitmap[len(copyBitmap)-1]&^valid != 0 {
			return rowSelection{}, errors.New("typedcolumn: row selection bitmap has non-zero padding bits")
		}
	}
	count := 0
	for _, word := range copyBitmap {
		count += bits.OnesCount64(word)
	}
	if count == 0 {
		return makeEmptyRowSelection(rows)
	}
	if count == rows {
		return makeAllRowSelection(rows)
	}
	if start, end, ok := contiguousBitmapRange(rows, copyBitmap); ok {
		return makeRangeRowSelection(rows, start, end)
	}
	return rowSelection{rows: rows, kind: rowSelectionBitmap, bitmap: copyBitmap, count: count}, nil
}

func (s rowSelection) rowsCount() int {
	return s.rows
}

func (s rowSelection) countRows() int {
	return s.count
}

func (s rowSelection) shape() rowSelectionShape {
	shape := rowSelectionShape{Kind: s.kind.String(), Rows: s.rows, Count: s.count}
	switch s.kind {
	case rowSelectionRanges:
		shape.Ranges = len(s.ranges)
	case rowSelectionBitmap:
		shape.BitmapWords = len(s.bitmap)
	case rowSelectionSparse:
		shape.SparseRows = len(s.sparse)
	case rowSelectionRange:
		shape.Ranges = 1
	}
	return shape
}

func (s rowSelection) contains(row int) bool {
	if row < 0 || row >= s.rows {
		return false
	}
	switch s.kind {
	case rowSelectionEmpty:
		return false
	case rowSelectionAll:
		return true
	case rowSelectionRange:
		return row >= s.start && row < s.end
	case rowSelectionRanges:
		for _, r := range s.ranges {
			if row < r.Start {
				return false
			}
			if row < r.End {
				return true
			}
		}
		return false
	case rowSelectionBitmap:
		return s.bitmap[row/64]&(uint64(1)<<uint(row%64)) != 0
	case rowSelectionSparse:
		lo, hi := 0, len(s.sparse)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if s.sparse[mid] < row {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo < len(s.sparse) && s.sparse[lo] == row
	default:
		return false
	}
}

func (s rowSelection) forEach(fn func(row int)) {
	switch s.kind {
	case rowSelectionEmpty:
		return
	case rowSelectionAll:
		for row := 0; row < s.rows; row++ {
			fn(row)
		}
	case rowSelectionRange:
		for row := s.start; row < s.end; row++ {
			fn(row)
		}
	case rowSelectionRanges:
		for _, r := range s.ranges {
			for row := r.Start; row < r.End; row++ {
				fn(row)
			}
		}
	case rowSelectionBitmap:
		for wordIndex, word := range s.bitmap {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				row := wordIndex*64 + bit
				if row < s.rows {
					fn(row)
				}
				word &^= uint64(1) << uint(bit)
			}
		}
	case rowSelectionSparse:
		for _, row := range s.sparse {
			fn(row)
		}
	}
}

func (s rowSelection) appendRows(dst []int) []int {
	switch s.kind {
	case rowSelectionEmpty:
		return dst
	case rowSelectionAll:
		for row := 0; row < s.rows; row++ {
			dst = append(dst, row)
		}
	case rowSelectionRange:
		for row := s.start; row < s.end; row++ {
			dst = append(dst, row)
		}
	case rowSelectionRanges:
		for _, r := range s.ranges {
			for row := r.Start; row < r.End; row++ {
				dst = append(dst, row)
			}
		}
	case rowSelectionBitmap:
		for wordIndex, word := range s.bitmap {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				row := wordIndex*64 + bit
				if row < s.rows {
					dst = append(dst, row)
				}
				word &^= uint64(1) << uint(bit)
			}
		}
	case rowSelectionSparse:
		dst = append(dst, s.sparse...)
	}
	return dst
}

func (s rowSelection) appendRanges(dst []rowRange) []rowRange {
	switch s.kind {
	case rowSelectionEmpty:
		return dst
	case rowSelectionAll:
		return append(dst, rowRange{Start: 0, End: s.rows})
	case rowSelectionRange:
		return append(dst, rowRange{Start: s.start, End: s.end})
	case rowSelectionRanges:
		return append(dst, s.ranges...)
	case rowSelectionBitmap:
		inRange := false
		start := 0
		for row := 0; row < s.rows; row++ {
			set := s.bitmap[row/64]&(uint64(1)<<uint(row%64)) != 0
			if set && !inRange {
				start = row
				inRange = true
				continue
			}
			if !set && inRange {
				dst = append(dst, rowRange{Start: start, End: row})
				inRange = false
			}
		}
		if inRange {
			dst = append(dst, rowRange{Start: start, End: s.rows})
		}
	case rowSelectionSparse:
		if len(s.sparse) == 0 {
			return dst
		}
		start := s.sparse[0]
		prev := start
		for _, row := range s.sparse[1:] {
			if row == prev+1 {
				prev = row
				continue
			}
			dst = append(dst, rowRange{Start: start, End: prev + 1})
			start = row
			prev = row
		}
		dst = append(dst, rowRange{Start: start, End: prev + 1})
	}
	return dst
}

func andRowSelections(a rowSelection, b rowSelection) (rowSelection, error) {
	if err := validateSameSelectionRows(a, b); err != nil {
		return failClosedSelection(a.rows, b.rows), err
	}
	if a.kind == rowSelectionEmpty || b.kind == rowSelectionEmpty {
		return makeEmptyRowSelection(a.rows)
	}
	if a.kind == rowSelectionAll {
		return b, nil
	}
	if b.kind == rowSelectionAll {
		return a, nil
	}
	if a.kind == rowSelectionBitmap && b.kind == rowSelectionBitmap {
		words := make([]uint64, len(a.bitmap))
		for i := range words {
			words[i] = a.bitmap[i] & b.bitmap[i]
		}
		return makeBitmapRowSelection(a.rows, words)
	}
	left := a.appendRanges(nil)
	right := b.appendRanges(nil)
	out := make([]rowRange, 0, min(len(left), len(right)))
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		start := max(left[i].Start, right[j].Start)
		end := min(left[i].End, right[j].End)
		if start < end {
			out = append(out, rowRange{Start: start, End: end})
		}
		if left[i].End < right[j].End {
			i++
		} else {
			j++
		}
	}
	return makeRangesRowSelection(a.rows, out)
}

func orRowSelections(a rowSelection, b rowSelection) (rowSelection, error) {
	if err := validateSameSelectionRows(a, b); err != nil {
		return failClosedSelection(a.rows, b.rows), err
	}
	if a.kind == rowSelectionAll || b.kind == rowSelectionAll {
		return makeAllRowSelection(a.rows)
	}
	if a.kind == rowSelectionEmpty {
		return b, nil
	}
	if b.kind == rowSelectionEmpty {
		return a, nil
	}
	if a.kind == rowSelectionBitmap && b.kind == rowSelectionBitmap {
		words := make([]uint64, len(a.bitmap))
		for i := range words {
			words[i] = a.bitmap[i] | b.bitmap[i]
		}
		return makeBitmapRowSelection(a.rows, words)
	}
	merged := append(a.appendRanges(nil), b.appendRanges(nil)...)
	insertionSortRanges(merged)
	return makeRangesRowSelection(a.rows, merged)
}

func notRowSelection(s rowSelection) (rowSelection, error) {
	switch s.kind {
	case rowSelectionEmpty:
		return makeAllRowSelection(s.rows)
	case rowSelectionAll:
		return makeEmptyRowSelection(s.rows)
	}
	ranges := s.appendRanges(nil)
	out := make([]rowRange, 0, len(ranges)+1)
	start := 0
	for _, r := range ranges {
		if start < r.Start {
			out = append(out, rowRange{Start: start, End: r.Start})
		}
		start = r.End
	}
	if start < s.rows {
		out = append(out, rowRange{Start: start, End: s.rows})
	}
	return makeRangesRowSelection(s.rows, out)
}

func (k rowSelectionKind) String() string {
	switch k {
	case rowSelectionEmpty:
		return "empty"
	case rowSelectionAll:
		return "all"
	case rowSelectionRange:
		return "range"
	case rowSelectionRanges:
		return "ranges"
	case rowSelectionBitmap:
		return "bitmap"
	case rowSelectionSparse:
		return "sparse"
	default:
		return "unknown"
	}
}

func validateRowRange(rows int, r rowRange) error {
	if rows < 0 {
		return fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	if r.Start < 0 || r.End < r.Start || r.End > rows {
		return fmt.Errorf("typedcolumn: row range [%d,%d) outside [0,%d)", r.Start, r.End, rows)
	}
	return nil
}

func rowSelectionBitmapWords(rows int) int {
	return (rows + 63) / 64
}

func contiguousBitmapRange(rows int, bitmap []uint64) (int, int, bool) {
	start := -1
	end := -1
	seenGap := false
	for row := 0; row < rows; row++ {
		set := bitmap[row/64]&(uint64(1)<<uint(row%64)) != 0
		switch {
		case set && start < 0:
			start = row
			end = row + 1
		case set && !seenGap:
			end = row + 1
		case set && seenGap:
			return 0, 0, false
		case !set && start >= 0:
			seenGap = true
		}
	}
	return start, end, start >= 0
}

func isContiguousSparse(rows []int) bool {
	for i := 1; i < len(rows); i++ {
		if rows[i] != rows[i-1]+1 {
			return false
		}
	}
	return true
}

func validateSameSelectionRows(a rowSelection, b rowSelection) error {
	if a.rows != b.rows {
		return fmt.Errorf("typedcolumn: row selection rows mismatch %d != %d", a.rows, b.rows)
	}
	return nil
}

func failClosedSelection(aRows int, bRows int) rowSelection {
	rows := aRows
	if bRows > rows {
		rows = bRows
	}
	if rows < 0 {
		rows = 0
	}
	return rowSelection{rows: rows, kind: rowSelectionEmpty}
}

func insertionSortRanges(ranges []rowRange) {
	for i := 1; i < len(ranges); i++ {
		r := ranges[i]
		j := i - 1
		for j >= 0 && (ranges[j].Start > r.Start || (ranges[j].Start == r.Start && ranges[j].End > r.End)) {
			ranges[j+1] = ranges[j]
			j--
		}
		ranges[j+1] = r
	}
}
