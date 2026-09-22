package typedcolumn

import (
	"errors"
	"fmt"
	"math/bits"
)

// Uint32CodeSelectionScratch owns caller/session-scoped temporary storage for
// low-cardinality dictionary-code predicate selections. Returned selections may
// alias this scratch until the next use.
type Uint32CodeSelectionScratch struct {
	Rows   []int
	Ranges []RowRange
	Bitmap []uint64
	Bits   []uint64
}

// CountUint32Code counts rows in selection whose low-cardinality uint32 code
// equals code. It scans encoded codes directly and does not decode dictionary
// strings.
func (r *GranuleReader) CountUint32Code(g EncodedGranule, selection RowSelection, code uint32) (int, error) {
	if err := validateUint32CodeSelectionRows(g, selection); err != nil {
		return 0, err
	}
	if selection.IsEmpty() {
		return 0, nil
	}
	header, err := r.uint32CodesHeader(g)
	if err != nil {
		return 0, err
	}
	if code >= header.cardinality {
		return 0, nil
	}
	return countUint32CodeSelection(header, selection, code)
}

// CountUint32CodesIn counts rows in selection whose low-cardinality uint32 code
// is present in codes. Out-of-cardinality query codes are ignored.
func (r *GranuleReader) CountUint32CodesIn(g EncodedGranule, selection RowSelection, codes []uint32, scratch *Uint32CodeSelectionScratch) (int, error) {
	if err := validateUint32CodeSelectionRows(g, selection); err != nil {
		return 0, err
	}
	if selection.IsEmpty() || len(codes) == 0 {
		return 0, nil
	}
	if scratch == nil {
		scratch = &Uint32CodeSelectionScratch{}
	}
	header, err := r.uint32CodesHeader(g)
	if err != nil {
		return 0, err
	}
	bits, any := prepareUint32CodeSet(codes, header.cardinality, scratch)
	if !any {
		return 0, nil
	}
	return countUint32CodeSetSelection(header, selection, bits)
}

// SelectUint32Code returns the subset of selection whose low-cardinality uint32
// code equals code. The returned selection may alias scratch when scratch is
// non-nil.
func (r *GranuleReader) SelectUint32Code(g EncodedGranule, selection RowSelection, code uint32, scratch *Uint32CodeSelectionScratch) (RowSelection, error) {
	if err := validateUint32CodeSelectionRows(g, selection); err != nil {
		return RowSelection{}, err
	}
	if selection.IsEmpty() {
		return NewEmptyRowSelection(g.Rows)
	}
	if scratch == nil {
		scratch = &Uint32CodeSelectionScratch{}
	}
	header, err := r.uint32CodesHeader(g)
	if err != nil {
		return RowSelection{}, err
	}
	if code >= header.cardinality {
		return NewEmptyRowSelection(g.Rows)
	}
	return selectUint32CodeSelection(header, g.Rows, selection, code, scratch)
}

// SelectUint32CodesIn returns the subset of selection whose low-cardinality
// uint32 code is present in codes. The returned selection may alias scratch when
// scratch is non-nil. Out-of-cardinality query codes are ignored.
func (r *GranuleReader) SelectUint32CodesIn(g EncodedGranule, selection RowSelection, codes []uint32, scratch *Uint32CodeSelectionScratch) (RowSelection, error) {
	if err := validateUint32CodeSelectionRows(g, selection); err != nil {
		return RowSelection{}, err
	}
	if selection.IsEmpty() || len(codes) == 0 {
		return NewEmptyRowSelection(g.Rows)
	}
	if scratch == nil {
		scratch = &Uint32CodeSelectionScratch{}
	}
	header, err := r.uint32CodesHeader(g)
	if err != nil {
		return RowSelection{}, err
	}
	bits, any := prepareUint32CodeSet(codes, header.cardinality, scratch)
	if !any {
		return NewEmptyRowSelection(g.Rows)
	}
	return selectUint32CodeSetSelection(header, g.Rows, selection, bits, scratch)
}

func (r *GranuleReader) uint32CodesHeader(g EncodedGranule) (uint32CodesHeader, error) {
	if g.Encoding != EncodingLowCardinalityUint32 {
		return uint32CodesHeader{}, fmt.Errorf("typedcolumn: code predicate got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return uint32CodesHeader{}, err
	}
	return parseUint32CodesHeader(raw, g.Rows)
}

func validateUint32CodeSelectionRows(g EncodedGranule, selection RowSelection) error {
	if g.Encoding != EncodingLowCardinalityUint32 {
		return fmt.Errorf("typedcolumn: code predicate got encoding %d", g.Encoding)
	}
	if err := validateGranuleDecodeRows(g.Rows); err != nil {
		return err
	}
	if selection.Rows() != g.Rows {
		return fmt.Errorf("typedcolumn: code selection rows=%d want %d", selection.Rows(), g.Rows)
	}
	return nil
}

func countUint32CodeSelection(header uint32CodesHeader, selection RowSelection, code uint32) (int, error) {
	countRange := func(start, end int) (int, error) {
		count := 0
		for row := start; row < end; row++ {
			got, err := readValidUint32Code(header, row)
			if err != nil {
				return 0, err
			}
			if got == code {
				count++
			}
		}
		return count, nil
	}
	switch selection.Kind() {
	case RowSelectionEmpty:
		return 0, nil
	case RowSelectionAll:
		return countRange(0, selection.Rows())
	case RowSelectionRange:
		start, end, _ := selection.SingleRange()
		return countRange(start, end)
	case RowSelectionRanges:
		count := 0
		for _, r := range selection.Ranges() {
			n, err := countRange(r.Start, r.End)
			if err != nil {
				return 0, err
			}
			count += n
		}
		return count, nil
	case RowSelectionBitmap:
		count := 0
		for wordIndex, word := range selection.BitmapWords() {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				row := wordIndex*64 + bit
				if row >= selection.Rows() {
					break
				}
				got, err := readValidUint32Code(header, row)
				if err != nil {
					return 0, err
				}
				if got == code {
					count++
				}
				word &^= uint64(1) << uint(bit)
			}
		}
		return count, nil
	case RowSelectionSparse:
		count := 0
		for _, row := range selection.SparseRows() {
			got, err := readValidUint32Code(header, row)
			if err != nil {
				return 0, err
			}
			if got == code {
				count++
			}
		}
		return count, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unsupported code row selection shape %s", selection.Shape().Kind)
	}
}

func countUint32CodeSetSelection(header uint32CodesHeader, selection RowSelection, codeBits []uint64) (int, error) {
	countRange := func(start, end int) (int, error) {
		count := 0
		for row := start; row < end; row++ {
			got, err := readValidUint32Code(header, row)
			if err != nil {
				return 0, err
			}
			if uint32CodeSetContains(codeBits, got) {
				count++
			}
		}
		return count, nil
	}
	switch selection.Kind() {
	case RowSelectionEmpty:
		return 0, nil
	case RowSelectionAll:
		return countRange(0, selection.Rows())
	case RowSelectionRange:
		start, end, _ := selection.SingleRange()
		return countRange(start, end)
	case RowSelectionRanges:
		count := 0
		for _, r := range selection.Ranges() {
			n, err := countRange(r.Start, r.End)
			if err != nil {
				return 0, err
			}
			count += n
		}
		return count, nil
	case RowSelectionBitmap:
		count := 0
		for wordIndex, word := range selection.BitmapWords() {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				row := wordIndex*64 + bit
				if row >= selection.Rows() {
					break
				}
				got, err := readValidUint32Code(header, row)
				if err != nil {
					return 0, err
				}
				if uint32CodeSetContains(codeBits, got) {
					count++
				}
				word &^= uint64(1) << uint(bit)
			}
		}
		return count, nil
	case RowSelectionSparse:
		count := 0
		for _, row := range selection.SparseRows() {
			got, err := readValidUint32Code(header, row)
			if err != nil {
				return 0, err
			}
			if uint32CodeSetContains(codeBits, got) {
				count++
			}
		}
		return count, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unsupported code row selection shape %s", selection.Shape().Kind)
	}
}

func selectUint32CodeSelection(header uint32CodesHeader, rows int, selection RowSelection, code uint32, scratch *Uint32CodeSelectionScratch) (RowSelection, error) {
	switch selection.Kind() {
	case RowSelectionEmpty:
		return NewEmptyRowSelection(rows)
	case RowSelectionSparse:
		scratch.Rows = scratch.Rows[:0]
		for _, row := range selection.SparseRows() {
			got, err := readValidUint32Code(header, row)
			if err != nil {
				return RowSelection{}, err
			}
			if got == code {
				scratch.Rows = append(scratch.Rows, row)
			}
		}
		return uint32CodeRowsSelection(rows, scratch)
	default:
		words := rowSelectionBitmapWords(rows)
		if cap(scratch.Bitmap) < words {
			scratch.Bitmap = make([]uint64, words)
		} else {
			scratch.Bitmap = scratch.Bitmap[:words]
			clear(scratch.Bitmap)
		}
		switch selection.Kind() {
		case RowSelectionAll:
			if err := applyCodeRangeRows(header, code, scratch.Bitmap, 0, rows); err != nil {
				return RowSelection{}, err
			}
		case RowSelectionRange:
			start, end, _ := selection.SingleRange()
			if err := applyCodeRangeRows(header, code, scratch.Bitmap, start, end); err != nil {
				return RowSelection{}, err
			}
		case RowSelectionRanges:
			for _, r := range selection.Ranges() {
				if err := applyCodeRangeRows(header, code, scratch.Bitmap, r.Start, r.End); err != nil {
					return RowSelection{}, err
				}
			}
		case RowSelectionBitmap:
			if err := applyCodeBitmapRows(header, code, rows, selection.BitmapWords(), scratch.Bitmap); err != nil {
				return RowSelection{}, err
			}
		default:
			return RowSelection{}, fmt.Errorf("typedcolumn: unsupported code row selection shape %s", selection.Shape().Kind)
		}
		return NewBitmapRowSelectionNoCopy(rows, scratch.Bitmap)
	}
}

func selectUint32CodeSetSelection(header uint32CodesHeader, rows int, selection RowSelection, codeBits []uint64, scratch *Uint32CodeSelectionScratch) (RowSelection, error) {
	switch selection.Kind() {
	case RowSelectionEmpty:
		return NewEmptyRowSelection(rows)
	case RowSelectionSparse:
		scratch.Rows = scratch.Rows[:0]
		for _, row := range selection.SparseRows() {
			got, err := readValidUint32Code(header, row)
			if err != nil {
				return RowSelection{}, err
			}
			if uint32CodeSetContains(codeBits, got) {
				scratch.Rows = append(scratch.Rows, row)
			}
		}
		return uint32CodeRowsSelection(rows, scratch)
	default:
		words := rowSelectionBitmapWords(rows)
		if cap(scratch.Bitmap) < words {
			scratch.Bitmap = make([]uint64, words)
		} else {
			scratch.Bitmap = scratch.Bitmap[:words]
			clear(scratch.Bitmap)
		}
		switch selection.Kind() {
		case RowSelectionAll:
			if err := applyCodeSetRangeRows(header, codeBits, scratch.Bitmap, 0, rows); err != nil {
				return RowSelection{}, err
			}
		case RowSelectionRange:
			start, end, _ := selection.SingleRange()
			if err := applyCodeSetRangeRows(header, codeBits, scratch.Bitmap, start, end); err != nil {
				return RowSelection{}, err
			}
		case RowSelectionRanges:
			for _, r := range selection.Ranges() {
				if err := applyCodeSetRangeRows(header, codeBits, scratch.Bitmap, r.Start, r.End); err != nil {
					return RowSelection{}, err
				}
			}
		case RowSelectionBitmap:
			if err := applyCodeSetBitmapRows(header, codeBits, rows, selection.BitmapWords(), scratch.Bitmap); err != nil {
				return RowSelection{}, err
			}
		default:
			return RowSelection{}, fmt.Errorf("typedcolumn: unsupported code row selection shape %s", selection.Shape().Kind)
		}
		return NewBitmapRowSelectionNoCopy(rows, scratch.Bitmap)
	}
}

func applyCodeRangeRows(header uint32CodesHeader, code uint32, out []uint64, start int, end int) error {
	for row := start; row < end; row++ {
		got, err := readValidUint32Code(header, row)
		if err != nil {
			return err
		}
		if got == code {
			out[row/64] |= uint64(1) << uint(row%64)
		}
	}
	return nil
}

func applyCodeBitmapRows(header uint32CodesHeader, code uint32, rows int, selectedWords []uint64, out []uint64) error {
	for wordIndex, word := range selectedWords {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			row := wordIndex*64 + bit
			if row >= rows {
				break
			}
			got, err := readValidUint32Code(header, row)
			if err != nil {
				return err
			}
			if got == code {
				out[wordIndex] |= uint64(1) << uint(bit)
			}
			word &^= uint64(1) << uint(bit)
		}
	}
	return nil
}

func applyCodeSetRangeRows(header uint32CodesHeader, codeBits []uint64, out []uint64, start int, end int) error {
	for row := start; row < end; row++ {
		got, err := readValidUint32Code(header, row)
		if err != nil {
			return err
		}
		if uint32CodeSetContains(codeBits, got) {
			out[row/64] |= uint64(1) << uint(row%64)
		}
	}
	return nil
}

func applyCodeSetBitmapRows(header uint32CodesHeader, codeBits []uint64, rows int, selectedWords []uint64, out []uint64) error {
	for wordIndex, word := range selectedWords {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			row := wordIndex*64 + bit
			if row >= rows {
				break
			}
			got, err := readValidUint32Code(header, row)
			if err != nil {
				return err
			}
			if uint32CodeSetContains(codeBits, got) {
				out[wordIndex] |= uint64(1) << uint(bit)
			}
			word &^= uint64(1) << uint(bit)
		}
	}
	return nil
}

func readValidUint32Code(header uint32CodesHeader, row int) (uint32, error) {
	code := readUint32Code(header.data, header.width, row)
	if code >= header.cardinality {
		return 0, fmt.Errorf("typedcolumn: code %d outside cardinality %d", code, header.cardinality)
	}
	return code, nil
}

const uint32CodeSelectionRangeLimit = 8

func uint32CodeRowsSelection(rows int, scratch *Uint32CodeSelectionScratch) (RowSelection, error) {
	selected := scratch.Rows
	if len(selected) == 0 {
		return NewEmptyRowSelection(rows)
	}
	if len(selected) == rows {
		return NewAllRowSelection(rows)
	}
	if isContiguousBoolRows(selected) {
		return NewRangeRowSelection(rows, selected[0], selected[len(selected)-1]+1)
	}
	scratch.Ranges = scratch.Ranges[:0]
	start, prev := selected[0], selected[0]
	for _, row := range selected[1:] {
		if row == prev+1 {
			prev = row
			continue
		}
		scratch.Ranges = append(scratch.Ranges, RowRange{Start: start, End: prev + 1})
		start, prev = row, row
	}
	scratch.Ranges = append(scratch.Ranges, RowRange{Start: start, End: prev + 1})
	if len(scratch.Ranges) <= uint32CodeSelectionRangeLimit {
		return NewRangesRowSelectionNoCopy(rows, scratch.Ranges)
	}
	if len(selected) >= 64 && len(selected)*4 >= rows*3 {
		words := rowSelectionBitmapWords(rows)
		if cap(scratch.Bitmap) < words {
			scratch.Bitmap = make([]uint64, words)
		} else {
			scratch.Bitmap = scratch.Bitmap[:words]
			clear(scratch.Bitmap)
		}
		for _, row := range selected {
			scratch.Bitmap[row/64] |= uint64(1) << uint(row%64)
		}
		return NewBitmapRowSelectionNoCopy(rows, scratch.Bitmap)
	}
	return NewSparseRowSelectionNoCopy(rows, selected)
}

func prepareUint32CodeSet(codes []uint32, cardinality uint32, scratch *Uint32CodeSelectionScratch) ([]uint64, bool) {
	if cardinality == 0 {
		return nil, false
	}
	words := (int(cardinality) + 63) / 64
	if cap(scratch.Bits) < words {
		scratch.Bits = make([]uint64, words)
	} else {
		scratch.Bits = scratch.Bits[:words]
		clear(scratch.Bits)
	}
	any := false
	for _, code := range codes {
		if code >= cardinality {
			continue
		}
		scratch.Bits[code/64] |= uint64(1) << uint(code%64)
		any = true
	}
	return scratch.Bits, any
}

func uint32CodeSetContains(codeBits []uint64, code uint32) bool {
	word := int(code / 64)
	return word < len(codeBits) && codeBits[word]&(uint64(1)<<uint(code%64)) != 0
}

func validateUint32CodeSelectedGranules(granules []EncodedGranule, selections []RowSelection) error {
	if len(granules) != len(selections) {
		return fmt.Errorf("typedcolumn: code granules=%d selections=%d", len(granules), len(selections))
	}
	if len(granules) == 0 {
		return errors.New("typedcolumn: empty code granules")
	}
	for i, g := range granules {
		if err := validateUint32CodeSelectionRows(g, selections[i]); err != nil {
			return fmt.Errorf("typedcolumn: code granule %d: %w", i, err)
		}
	}
	return nil
}
