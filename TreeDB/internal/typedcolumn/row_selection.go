package typedcolumn

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"
)

// RowSelectionKind names the concrete representation used for a block-local
// row selection. Callers should prefer all/range/ranges forms when possible;
// bitmap and sparse forms are available for genuinely irregular selections.
type RowSelectionKind uint8

type rowSelectionKind = RowSelectionKind

const (
	rowSelectionEmpty RowSelectionKind = iota
	rowSelectionAll
	rowSelectionRange
	rowSelectionRanges
	rowSelectionBitmap
	rowSelectionSparse
)

const (
	RowSelectionEmpty  RowSelectionKind = rowSelectionEmpty
	RowSelectionAll    RowSelectionKind = rowSelectionAll
	RowSelectionRange  RowSelectionKind = rowSelectionRange
	RowSelectionRanges RowSelectionKind = rowSelectionRanges
	RowSelectionBitmap RowSelectionKind = rowSelectionBitmap
	RowSelectionSparse RowSelectionKind = rowSelectionSparse
)

// RowRange is a half-open [Start, End) range over a block-local row domain.
type RowRange struct {
	Start int
	End   int
}

type rowRange = RowRange

// RowSelection is an immutable block-local selection over rows [0, Rows()).
// Slice-backed selections may alias caller-owned scratch when constructed by a
// NoCopy/Into helper; callers must not mutate those slices until the selection
// is no longer used. The type deliberately has no package-global cache.
type RowSelection struct {
	rows   int
	kind   RowSelectionKind
	start  int
	end    int
	ranges []RowRange
	bitmap []uint64
	sparse []int
	count  int
}

type rowSelection = RowSelection

// RowSelectionShape is a cheap diagnostic summary that does not materialize row
// IDs.
type RowSelectionShape struct {
	Kind        string
	Rows        int
	Count       int
	Ranges      int
	BitmapWords int
	SparseRows  int
}

type rowSelectionShape = RowSelectionShape

// RowSelectionScratch owns caller-scoped temporary storage for composition and
// no-copy constructors. Returned selections may alias these slices until the
// next operation using the same scratch.
type RowSelectionScratch struct {
	leftRanges  []RowRange
	rightRanges []RowRange
	outRanges   []RowRange
	bitmap      []uint64
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
	merged := make([]RowRange, 0, len(ranges))
	for i, r := range ranges {
		if err := validateRowRange(rows, r); err != nil {
			return rowSelection{}, fmt.Errorf("typedcolumn: invalid row range %d: %w", i, err)
		}
		if r.Start != r.End {
			merged = append(merged, r)
		}
	}
	if len(merged) == 0 {
		return makeEmptyRowSelection(rows)
	}
	sortRowRanges(merged)
	return makeUnionRangesRowSelection(rows, merged)
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

// NewEmptyRowSelection returns an empty selection over rows.
func NewEmptyRowSelection(rows int) (RowSelection, error) { return makeEmptyRowSelection(rows) }

// NewAllRowSelection returns a compact all-rows selection over rows.
func NewAllRowSelection(rows int) (RowSelection, error) { return makeAllRowSelection(rows) }

// NewRangeRowSelection returns a compact half-open range selection.
func NewRangeRowSelection(rows int, start int, end int) (RowSelection, error) {
	return makeRangeRowSelection(rows, start, end)
}

// NewRangesRowSelection returns a compact multi-range selection. Ranges may be
// unsorted; overlapping and adjacent ranges are coalesced, and fully-covered
// domains collapse to all.
func NewRangesRowSelection(rows int, ranges []RowRange) (RowSelection, error) {
	return makeRangesRowSelection(rows, ranges)
}

// NewRangesRowSelectionNoCopy returns a compact multi-range selection that may
// alias ranges. Ranges must already be sorted, non-overlapping, and must not be
// mutated while the selection is in use.
func NewRangesRowSelectionNoCopy(rows int, ranges []RowRange) (RowSelection, error) {
	return makeRangesRowSelectionNoCopy(rows, ranges)
}

// NewSparseRowSelection returns a sparse selection from strictly increasing row
// indexes. The input slice is copied.
func NewSparseRowSelection(rows int, sparse []int) (RowSelection, error) {
	return makeSparseRowSelection(rows, sparse)
}

// NewSparseRowSelectionNoCopy returns a sparse selection that may alias sparse.
// The input must be strictly increasing and must not be mutated while the
// selection is in use.
func NewSparseRowSelectionNoCopy(rows int, sparse []int) (RowSelection, error) {
	return makeSparseRowSelectionNoCopy(rows, sparse)
}

// NewBitmapRowSelection returns a bitmap selection. The bitmap slice is copied
// and must have exactly ceil(rows/64) words with zero padding bits.
func NewBitmapRowSelection(rows int, bitmap []uint64) (RowSelection, error) {
	return makeBitmapRowSelection(rows, bitmap)
}

// NewBitmapRowSelectionNoCopy returns a bitmap selection that may alias bitmap.
// The bitmap must have exactly ceil(rows/64) words with zero padding bits and
// must not be mutated while the selection is in use.
func NewBitmapRowSelectionNoCopy(rows int, bitmap []uint64) (RowSelection, error) {
	return makeBitmapRowSelectionNoCopy(rows, bitmap)
}

func makeSparseRowSelectionNoCopy(rows int, sparse []int) (RowSelection, error) {
	if rows < 0 {
		return RowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	if len(sparse) == 0 {
		return makeEmptyRowSelection(rows)
	}
	for i, row := range sparse {
		if row < 0 || row >= rows {
			return RowSelection{}, fmt.Errorf("typedcolumn: sparse row %d=%d outside [0,%d)", i, row, rows)
		}
		if i > 0 && row <= sparse[i-1] {
			return RowSelection{}, fmt.Errorf("typedcolumn: sparse rows not strictly increasing at %d", i)
		}
	}
	if len(sparse) == rows {
		return makeAllRowSelection(rows)
	}
	if isContiguousSparse(sparse) {
		return makeRangeRowSelection(rows, sparse[0], sparse[len(sparse)-1]+1)
	}
	return RowSelection{rows: rows, kind: rowSelectionSparse, sparse: sparse, count: len(sparse)}, nil
}

func makeRangesRowSelectionNoCopy(rows int, ranges []RowRange) (RowSelection, error) {
	if rows < 0 {
		return RowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	if len(ranges) == 0 {
		return makeEmptyRowSelection(rows)
	}
	out := ranges[:0]
	for i, r := range ranges {
		if err := validateRowRange(rows, r); err != nil {
			return RowSelection{}, fmt.Errorf("typedcolumn: invalid row range %d: %w", i, err)
		}
		if r.Start == r.End {
			continue
		}
		if len(out) == 0 {
			out = append(out, r)
			continue
		}
		last := &out[len(out)-1]
		if r.Start < last.End {
			return RowSelection{}, fmt.Errorf("typedcolumn: row ranges overlap at %d", i)
		}
		if r.Start == last.End {
			last.End = r.End
			continue
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		return makeEmptyRowSelection(rows)
	}
	count := 0
	for _, r := range out {
		count += r.End - r.Start
	}
	if count == rows {
		return makeAllRowSelection(rows)
	}
	if len(out) == 1 {
		return makeRangeRowSelection(rows, out[0].Start, out[0].End)
	}
	return RowSelection{rows: rows, kind: rowSelectionRanges, ranges: out, count: count}, nil
}

func (s rowSelection) rowsCount() int {
	return s.rows
}

func (s rowSelection) countRows() int {
	return s.count
}

// Rows returns the row-domain size.
func (s RowSelection) Rows() int { return s.rows }

// Count returns the selected row count without materializing row IDs.
func (s RowSelection) Count() int { return s.count }

// Kind returns the compact representation shape.
func (s RowSelection) Kind() RowSelectionKind { return s.kind }

// IsEmpty reports whether no rows are selected.
func (s RowSelection) IsEmpty() bool { return s.count == 0 || s.kind == rowSelectionEmpty }

// IsAll reports whether the selection covers the whole row domain.
func (s RowSelection) IsAll() bool { return s.kind == rowSelectionAll }

// SingleRange exposes the half-open range for range selections.
func (s RowSelection) SingleRange() (start int, end int, ok bool) {
	if s.kind != rowSelectionRange {
		return 0, 0, false
	}
	return s.start, s.end, true
}

// Ranges returns a range decomposition for range/ranges/all selections. Treat
// the returned slice as read-only and copy it if it must outlive caller scratch.
// For all/range shapes this returns a fresh one-element slice; hot paths should
// prefer SingleRange or AppendRanges with caller-owned scratch.
func (s RowSelection) Ranges() []RowRange {
	switch s.kind {
	case rowSelectionRange:
		return []RowRange{{Start: s.start, End: s.end}}
	case rowSelectionRanges:
		return s.ranges
	case rowSelectionAll:
		return []RowRange{{Start: 0, End: s.rows}}
	default:
		return nil
	}
}

// SparseRows returns the internal sparse row slice for sparse selections.
// Treat the returned slice as read-only.
func (s RowSelection) SparseRows() []int {
	if s.kind != rowSelectionSparse {
		return nil
	}
	return s.sparse
}

// BitmapWords returns the internal bitmap words for bitmap selections. Treat the
// returned slice as read-only.
func (s RowSelection) BitmapWords() []uint64 {
	if s.kind != rowSelectionBitmap {
		return nil
	}
	return s.bitmap
}

// Contains reports whether row is selected.
func (s RowSelection) Contains(row int) bool { return s.contains(row) }

// ForEach calls fn once for each selected row in ascending order. It does not
// allocate, but performance-sensitive kernels should switch on Kind and use the
// concrete accessors to avoid callback overhead in inner loops.
func (s RowSelection) ForEach(fn func(row int)) { s.forEach(fn) }

// AppendRows appends selected rows in ascending order to dst.
func (s RowSelection) AppendRows(dst []int) []int { return s.appendRows(dst) }

// AppendRanges appends a range decomposition to dst without materializing rows.
func (s RowSelection) AppendRanges(dst []RowRange) []RowRange { return s.appendRanges(dst) }

// Shape returns diagnostic metadata for the selection representation.
func (s RowSelection) Shape() RowSelectionShape { return s.shape() }

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
		lo, hi := 0, len(s.ranges)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if s.ranges[mid].End <= row {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo < len(s.ranges) && row >= s.ranges[lo].Start
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

// AndRowSelections returns the intersection of a and b. Mixed-shape results are
// range-normalized and may allocate; use AndRowSelectionsInto with caller scratch
// on prepared/hot paths.
func AndRowSelections(a RowSelection, b RowSelection) (RowSelection, error) {
	return andRowSelections(a, b)
}

// AndRowSelectionsInto intersects a and b using caller-owned scratch for mixed
// range results. The returned selection may alias scratch until the next scratch
// use.
func AndRowSelectionsInto(a RowSelection, b RowSelection, scratch *RowSelectionScratch) (RowSelection, error) {
	if scratch == nil {
		return andRowSelections(a, b)
	}
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
		words := rowSelectionBitmapWords(a.rows)
		if cap(scratch.bitmap) < words {
			scratch.bitmap = make([]uint64, words)
		}
		out := scratch.bitmap[:words]
		for i := range out {
			out[i] = a.bitmap[i] & b.bitmap[i]
		}
		return makeBitmapRowSelectionNoCopy(a.rows, out)
	}
	scratch.leftRanges = a.appendRanges(scratch.leftRanges[:0])
	scratch.rightRanges = b.appendRanges(scratch.rightRanges[:0])
	scratch.outRanges = scratch.outRanges[:0]
	i, j := 0, 0
	for i < len(scratch.leftRanges) && j < len(scratch.rightRanges) {
		start := max(scratch.leftRanges[i].Start, scratch.rightRanges[j].Start)
		end := min(scratch.leftRanges[i].End, scratch.rightRanges[j].End)
		if start < end {
			scratch.outRanges = append(scratch.outRanges, RowRange{Start: start, End: end})
		}
		if scratch.leftRanges[i].End < scratch.rightRanges[j].End {
			i++
		} else {
			j++
		}
	}
	return makeRangesRowSelectionNoCopy(a.rows, scratch.outRanges)
}

func makeBitmapRowSelectionNoCopy(rows int, bitmap []uint64) (RowSelection, error) {
	if rows < 0 {
		return RowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	wantWords := rowSelectionBitmapWords(rows)
	if len(bitmap) != wantWords {
		return RowSelection{}, fmt.Errorf("typedcolumn: bitmap words=%d want=%d", len(bitmap), wantWords)
	}
	if wantWords == 0 {
		return makeEmptyRowSelection(rows)
	}
	if padding := rows % 64; padding != 0 {
		valid := uint64(1<<uint(padding)) - 1
		if bitmap[len(bitmap)-1]&^valid != 0 {
			return RowSelection{}, errors.New("typedcolumn: row selection bitmap has non-zero padding bits")
		}
	}
	count := 0
	for _, word := range bitmap {
		count += bits.OnesCount64(word)
	}
	if count == 0 {
		return makeEmptyRowSelection(rows)
	}
	if count == rows {
		return makeAllRowSelection(rows)
	}
	if start, end, ok := contiguousBitmapRange(rows, bitmap); ok {
		return makeRangeRowSelection(rows, start, end)
	}
	return RowSelection{rows: rows, kind: rowSelectionBitmap, bitmap: bitmap, count: count}, nil
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
	sortRowRanges(merged)
	return makeUnionRangesRowSelection(a.rows, merged)
}

func makeUnionRangesRowSelection(rows int, ranges []RowRange) (RowSelection, error) {
	if rows < 0 {
		return RowSelection{}, fmt.Errorf("typedcolumn: negative row selection rows %d", rows)
	}
	if len(ranges) == 0 {
		return makeEmptyRowSelection(rows)
	}
	out := ranges[:0]
	for i, r := range ranges {
		if err := validateRowRange(rows, r); err != nil {
			return RowSelection{}, fmt.Errorf("typedcolumn: invalid row range %d: %w", i, err)
		}
		if r.Start == r.End {
			continue
		}
		if len(out) == 0 {
			out = append(out, r)
			continue
		}
		last := &out[len(out)-1]
		if r.Start <= last.End {
			if r.End > last.End {
				last.End = r.End
			}
			continue
		}
		out = append(out, r)
	}
	return makeRangesRowSelectionNoCopy(rows, out)
}

// SubtractRowSelectionsInto returns a-b using caller-owned scratch. The returned
// selection may alias scratch until scratch is reused.
func SubtractRowSelectionsInto(a RowSelection, b RowSelection, scratch *RowSelectionScratch) (RowSelection, error) {
	if scratch == nil {
		var local RowSelectionScratch
		scratch = &local
	}
	if err := validateSameSelectionRows(a, b); err != nil {
		return failClosedSelection(a.rows, b.rows), err
	}
	if a.kind == rowSelectionEmpty || b.kind == rowSelectionAll {
		return makeEmptyRowSelection(a.rows)
	}
	if b.kind == rowSelectionEmpty {
		return a, nil
	}
	// Copy/decompose inputs into stable scratch slices before resetting outRanges:
	// either input may itself alias scratch.outRanges from a previous operation.
	scratch.leftRanges = a.appendRanges(scratch.leftRanges[:0])
	scratch.rightRanges = b.appendRanges(scratch.rightRanges[:0])
	scratch.outRanges = scratch.outRanges[:0]
	rightIdx := 0
	for _, left := range scratch.leftRanges {
		cursor := left.Start
		for rightIdx < len(scratch.rightRanges) && scratch.rightRanges[rightIdx].End <= left.Start {
			rightIdx++
		}
		for idx := rightIdx; idx < len(scratch.rightRanges); idx++ {
			right := scratch.rightRanges[idx]
			if right.Start >= left.End {
				break
			}
			if cursor < right.Start {
				end := min(right.Start, left.End)
				if cursor < end {
					scratch.outRanges = append(scratch.outRanges, RowRange{Start: cursor, End: end})
				}
			}
			if right.End > cursor {
				cursor = right.End
				if cursor >= left.End {
					break
				}
			}
		}
		if cursor < left.End {
			scratch.outRanges = append(scratch.outRanges, RowRange{Start: cursor, End: left.End})
		}
	}
	return makeRangesRowSelectionNoCopy(a.rows, scratch.outRanges)
}

// OrRowSelections returns the union of a and b.
func OrRowSelections(a RowSelection, b RowSelection) (RowSelection, error) {
	return orRowSelections(a, b)
}

// NotRowSelection returns the complement of s over its row domain.
func NotRowSelection(s RowSelection) (RowSelection, error) {
	return notRowSelection(s)
}

// NotRowSelectionInto returns the complement of s using caller scratch for range
// decomposition. Returned selections may alias scratch.
func NotRowSelectionInto(s RowSelection, scratch *RowSelectionScratch) (RowSelection, error) {
	if scratch == nil {
		return notRowSelection(s)
	}
	switch s.kind {
	case rowSelectionEmpty:
		return makeAllRowSelection(s.rows)
	case rowSelectionAll:
		return makeEmptyRowSelection(s.rows)
	}
	scratch.leftRanges = s.appendRanges(scratch.leftRanges[:0])
	scratch.outRanges = scratch.outRanges[:0]
	start := 0
	for _, r := range scratch.leftRanges {
		if start < r.Start {
			scratch.outRanges = append(scratch.outRanges, RowRange{Start: start, End: r.Start})
		}
		start = r.End
	}
	if start < s.rows {
		scratch.outRanges = append(scratch.outRanges, RowRange{Start: start, End: s.rows})
	}
	return makeRangesRowSelectionNoCopy(s.rows, scratch.outRanges)
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
	if rows < 0 {
		rows = 0
	}
	_ = bRows
	return rowSelection{rows: rows, kind: rowSelectionEmpty}
}

func sortRowRanges(ranges []rowRange) {
	slices.SortFunc(ranges, func(a, b rowRange) int {
		switch {
		case a.Start < b.Start:
			return -1
		case a.Start > b.Start:
			return 1
		case a.End < b.End:
			return -1
		case a.End > b.End:
			return 1
		default:
			return 0
		}
	})
}
