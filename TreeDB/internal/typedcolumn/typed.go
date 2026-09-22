package typedcolumn

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sort"
)

const (
	boolPayloadBitpack byte = 1
	boolPayloadRLE     byte = 2

	nullableInt64HeaderBytes = 21
	maxCodeCardinality       = 1 << 20
)

// BuildBool returns a granule whose payload aliases builder-owned scratch until
// the next builder Build* or Reset call.
func (b *GranuleBuilder) BuildBool(values []bool) (EncodedGranule, error) {
	if len(values) == 0 {
		return EncodedGranule{}, errors.New("typedcolumn: empty granule")
	}
	if err := validateGranuleDecodeRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	raw := encodeBoolPayload(b.raw[:0], values)
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingBoolBitpackRLE, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	min, max := boolMinMax(values)
	return newEncodedGranule(len(values), min, max, true, EncodingBoolBitpackRLE, selection), nil
}

func (r *GranuleReader) DecodeBool(g EncodedGranule) ([]bool, error) {
	values, err := r.DecodeBoolInto(r.bools[:0], g)
	if err != nil {
		return nil, err
	}
	r.bools = values
	return values, nil
}

func (r *GranuleReader) DecodeBoolInto(dst []bool, g EncodedGranule) ([]bool, error) {
	if g.Encoding != EncodingBoolBitpackRLE {
		return nil, fmt.Errorf("typedcolumn: bool decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return decodeBoolPayload(dst, raw, g.Rows)
}

func (r *GranuleReader) CountTrueBool(g EncodedGranule) (int, error) {
	if g.Encoding != EncodingBoolBitpackRLE {
		return 0, fmt.Errorf("typedcolumn: bool count got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return 0, err
	}
	return countTrueBoolPayload(raw, g.Rows)
}

// BoolSelectionScratch owns caller/session-scoped temporary storage for bool
// predicate selections. Returned selections from SelectBool may alias this
// scratch until the next use.
type BoolSelectionScratch struct {
	Rows   []int
	Ranges []RowRange
	Bitmap []uint64
}

// CountBool counts rows in selection whose bool payload equals value. Bitpack
// payloads use popcount over selected bit ranges/words; RLE payloads aggregate
// matching runs without materializing []bool.
func (r *GranuleReader) CountBool(g EncodedGranule, selection RowSelection, value bool) (int, error) {
	if g.Encoding != EncodingBoolBitpackRLE {
		return 0, fmt.Errorf("typedcolumn: bool count got encoding %d", g.Encoding)
	}
	if err := validateBoolSelectionRows(g.Rows, selection); err != nil {
		return 0, err
	}
	if selection.IsEmpty() {
		return 0, nil
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return 0, err
	}
	return countBoolPayloadSelection(raw, g.Rows, selection, value)
}

// SelectBool returns the subset of selection whose bool payload equals value.
// The returned selection may alias scratch when scratch is non-nil.
func (r *GranuleReader) SelectBool(g EncodedGranule, selection RowSelection, value bool, scratch *BoolSelectionScratch) (RowSelection, error) {
	if g.Encoding != EncodingBoolBitpackRLE {
		return RowSelection{}, fmt.Errorf("typedcolumn: bool select got encoding %d", g.Encoding)
	}
	if err := validateBoolSelectionRows(g.Rows, selection); err != nil {
		return RowSelection{}, err
	}
	if selection.IsEmpty() {
		return NewEmptyRowSelection(g.Rows)
	}
	if scratch == nil {
		scratch = &BoolSelectionScratch{}
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return RowSelection{}, err
	}
	return selectBoolPayload(raw, g.Rows, selection, value, scratch)
}

// BuildNullableInt64 returns a granule whose payload aliases builder-owned
// scratch until the next builder Build* or Reset call.
func (b *GranuleBuilder) BuildNullableInt64(values []int64, nulls []bool, defaults []bool, defaultValue int64) (EncodedGranule, error) {
	if len(values) == 0 {
		return EncodedGranule{}, errors.New("typedcolumn: empty granule")
	}
	if err := validateGranuleDecodeRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	valueEncoding := b.cfg.Encoding
	if valueEncoding == EncodingNullableInt64 {
		valueEncoding = EncodingRawInt64
	}
	valueEncoding, err := nullableValueEncoding(valueEncoding)
	if err != nil {
		return EncodedGranule{}, err
	}
	if err := validateOptionalBoolRows("nulls", len(values), nulls); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateOptionalBoolRows("defaults", len(values), defaults); err != nil {
		return EncodedGranule{}, err
	}

	b.values64 = b.values64[:0]
	nullCount := 0
	defaultCount := 0
	hasMinMax := false
	min, max := int64(0), int64(0)
	for i, v := range values {
		isNull := boolAt(nulls, i)
		isDefault := boolAt(defaults, i)
		if isNull && isDefault {
			return EncodedGranule{}, fmt.Errorf("typedcolumn: row %d is both null and default", i)
		}
		switch {
		case isNull:
			nullCount++
		case isDefault:
			defaultCount++
		default:
			b.values64 = append(b.values64, v)
			min, max, hasMinMax = updateOptionalMinMax(min, max, hasMinMax, v)
		}
	}

	encodedValues, err := encodeInt64Payload(b.encoded[:0], b.values64, valueEncoding)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.encoded = encodedValues

	nullMaskLen := bitmapBytes(len(values))
	defaultMaskLen := bitmapBytes(len(values))
	raw := b.raw[:0]
	raw = append(raw, make([]byte, nullableInt64HeaderBytes)...)
	raw[0] = byte(valueEncoding)
	binary.LittleEndian.PutUint64(raw[1:9], uint64(defaultValue))
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(b.values64)))
	binary.LittleEndian.PutUint32(raw[13:17], uint32(nullMaskLen))
	binary.LittleEndian.PutUint32(raw[17:21], uint32(defaultMaskLen))
	raw = appendBitmap(raw, len(values), nulls)
	raw = appendBitmap(raw, len(values), defaults)
	raw = append(raw, encodedValues...)
	b.raw = raw

	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingNullableInt64, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	g := newEncodedGranule(len(values), min, max, hasMinMax, EncodingNullableInt64, selection)
	g.NullCount = nullCount
	g.DefaultCount = defaultCount
	return g, nil
}

func (r *GranuleReader) DecodeNullableInt64(g EncodedGranule) ([]int64, []bool, []bool, error) {
	values, nulls, defaults, err := r.DecodeNullableInt64Into(r.values[:0], r.nulls[:0], r.defaults[:0], g)
	if err != nil {
		return nil, nil, nil, err
	}
	r.values = values
	r.nulls = nulls
	r.defaults = defaults
	return values, nulls, defaults, nil
}

func (r *GranuleReader) DecodeNullableInt64Into(dst []int64, nulls []bool, defaults []bool, g EncodedGranule) ([]int64, []bool, []bool, error) {
	if g.Encoding != EncodingNullableInt64 {
		return nil, nil, nil, fmt.Errorf("typedcolumn: nullable int64 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, nil, nil, err
	}
	header, err := parseNullableInt64Header(raw, g.Rows)
	if err != nil {
		return nil, nil, nil, err
	}
	nullCount, defaultCount, overlap, err := nullableBitmapCounts(header.nullMask, header.defaultMask, g.Rows)
	if err != nil {
		return nil, nil, nil, err
	}
	if overlap >= 0 {
		return nil, nil, nil, fmt.Errorf("typedcolumn: nullable int64 row %d is both null and default", overlap)
	}
	if nullCount != g.NullCount {
		return nil, nil, nil, fmt.Errorf("typedcolumn: nullable int64 null count=%d want metadata=%d", nullCount, g.NullCount)
	}
	if defaultCount != g.DefaultCount {
		return nil, nil, nil, fmt.Errorf("typedcolumn: nullable int64 default count=%d want metadata=%d", defaultCount, g.DefaultCount)
	}
	wantStoredRows := g.Rows - nullCount - defaultCount
	if header.storedRows != wantStoredRows {
		return nil, nil, nil, fmt.Errorf("typedcolumn: nullable int64 stored rows=%d want=%d", header.storedRows, wantStoredRows)
	}
	storedGranule := EncodedGranule{
		Rows:        header.storedRows,
		HasMinMax:   false,
		Encoding:    header.valueEncoding,
		Compression: CompressionNone,
		RawBytes:    len(header.encodedValues),
		StoredBytes: len(header.encodedValues),
		PayloadRef:  PayloadRef{Kind: PayloadRefInline, Length: len(header.encodedValues)},
		Payload:     header.encodedValues,
	}
	stored, err := decodeInt64Payload(r.stored64[:0], header.encodedValues, storedGranule)
	if err != nil {
		return nil, nil, nil, err
	}
	r.stored64 = stored

	out := ensureInt64Len(dst, g.Rows)
	nullOut := ensureBoolLen(nulls, g.Rows)
	defaultOut := ensureBoolLen(defaults, g.Rows)
	storedIndex := 0
	for i := 0; i < g.Rows; i++ {
		isNull := bitmapBit(header.nullMask, i)
		isDefault := bitmapBit(header.defaultMask, i)
		nullOut[i] = isNull
		defaultOut[i] = isDefault
		switch {
		case isNull:
			out[i] = 0
		case isDefault:
			out[i] = header.defaultValue
		default:
			if storedIndex >= len(stored) {
				return nil, nil, nil, errors.New("typedcolumn: nullable int64 stored values underflow")
			}
			out[i] = stored[storedIndex]
			storedIndex++
		}
	}
	if storedIndex != len(stored) {
		return nil, nil, nil, errors.New("typedcolumn: nullable int64 stored values overflow")
	}
	return out, nullOut, defaultOut, nil
}

// BuildUint32Codes returns a granule whose payload aliases builder-owned
// scratch until the next builder Build* or Reset call.
func (b *GranuleBuilder) BuildUint32Codes(codes []uint32, cardinality uint32) (EncodedGranule, error) {
	if len(codes) == 0 {
		return EncodedGranule{}, errors.New("typedcolumn: empty granule")
	}
	if err := validateGranuleDecodeRows(len(codes)); err != nil {
		return EncodedGranule{}, err
	}
	minCode, maxCode := minMaxUint32(codes)
	if cardinality == 0 {
		if maxCode == ^uint32(0) {
			return EncodedGranule{}, errors.New("typedcolumn: inferred cardinality overflows uint32")
		}
		cardinality = maxCode + 1
	}
	if cardinality > maxCodeCardinality {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: cardinality %d exceeds cap %d", cardinality, maxCodeCardinality)
	}
	if maxCode >= cardinality {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: code %d outside cardinality %d", maxCode, cardinality)
	}
	raw := encodeUint32CodesPayload(b.raw[:0], codes, cardinality, maxCode)
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingLowCardinalityUint32, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(len(codes), int64(minCode), int64(maxCode), true, EncodingLowCardinalityUint32, selection), nil
}

func (r *GranuleReader) DecodeUint32Codes(g EncodedGranule) ([]uint32, error) {
	codes, err := r.DecodeUint32CodesInto(r.codes[:0], g)
	if err != nil {
		return nil, err
	}
	r.codes = codes
	return codes, nil
}

func (r *GranuleReader) DecodeUint32CodesInto(dst []uint32, g EncodedGranule) ([]uint32, error) {
	if g.Encoding != EncodingLowCardinalityUint32 {
		return nil, fmt.Errorf("typedcolumn: code decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	header, err := parseUint32CodesHeader(raw, g.Rows)
	if err != nil {
		return nil, err
	}
	out := ensureUint32Len(dst, g.Rows)
	for i := 0; i < g.Rows; i++ {
		code := readUint32Code(header.data, header.width, i)
		if code >= header.cardinality {
			return nil, fmt.Errorf("typedcolumn: code %d outside cardinality %d", code, header.cardinality)
		}
		out[i] = code
	}
	return out, nil
}

func (r *GranuleReader) CountUint32Codes(g EncodedGranule, counts []int) ([]int, error) {
	if g.Encoding != EncodingLowCardinalityUint32 {
		return nil, fmt.Errorf("typedcolumn: code count got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	header, err := parseUint32CodesHeader(raw, g.Rows)
	if err != nil {
		return nil, err
	}
	out := ensureIntLen(counts, int(header.cardinality))
	clear(out)
	for i := 0; i < g.Rows; i++ {
		code := readUint32Code(header.data, header.width, i)
		if code >= header.cardinality {
			return nil, fmt.Errorf("typedcolumn: code %d outside cardinality %d", code, header.cardinality)
		}
		out[code]++
	}
	return out, nil
}

func encodeBoolPayload(dst []byte, values []bool) []byte {
	bitpackLen := 1 + bitmapBytes(len(values))
	rleLen := boolRLEPayloadLen(values)
	if rleLen < bitpackLen {
		dst = appendBoolRLE(dst[:0], values)
		return dst
	}
	dst = append(dst[:0], boolPayloadBitpack)
	return appendBitmap(dst, len(values), values)
}

func decodeBoolPayload(dst []bool, raw []byte, rows int) ([]bool, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, errors.New("typedcolumn: missing bool payload mode")
	}
	switch raw[0] {
	case boolPayloadBitpack:
		mask := raw[1:]
		need, err := bitmapBytesChecked(rows)
		if err != nil {
			return nil, err
		}
		if len(mask) != need {
			return nil, fmt.Errorf("typedcolumn: bool bitpack bytes=%d want=%d", len(mask), need)
		}
		out := ensureBoolLen(dst, rows)
		for i := 0; i < rows; i++ {
			out[i] = bitmapBit(mask, i)
		}
		return out, nil
	case boolPayloadRLE:
		value, data, err := parseBoolRLEHeader(raw)
		if err != nil {
			return nil, err
		}
		if err := validateBoolRLERuns(data, rows); err != nil {
			return nil, err
		}
		out := ensureBoolLen(dst, rows)
		row := 0
		for len(data) > 0 && row < rows {
			run, n := binary.Uvarint(data)
			for i := 0; i < int(run); i++ {
				out[row+i] = value
			}
			row += int(run)
			value = !value
			data = data[n:]
		}
		return out, nil
	default:
		return nil, fmt.Errorf("typedcolumn: unsupported bool payload mode %d", raw[0])
	}
}

func countTrueBoolPayload(raw []byte, rows int) (int, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		return 0, errors.New("typedcolumn: missing bool payload mode")
	}
	switch raw[0] {
	case boolPayloadBitpack:
		mask := raw[1:]
		need, err := bitmapBytesChecked(rows)
		if err != nil {
			return 0, err
		}
		if len(mask) != need {
			return 0, fmt.Errorf("typedcolumn: bool bitpack bytes=%d want=%d", len(mask), need)
		}
		return countBoolBitpackRange(mask, 0, rows), nil
	case boolPayloadRLE:
		value, data, err := parseBoolRLEHeader(raw)
		if err != nil {
			return 0, err
		}
		row := 0
		count := 0
		for len(data) > 0 && row < rows {
			run, n := binary.Uvarint(data)
			if n <= 0 {
				return 0, errors.New("typedcolumn: malformed bool rle run")
			}
			if run == 0 || run > uint64(rows-row) {
				return 0, errors.New("typedcolumn: invalid bool rle run")
			}
			if value {
				count += int(run)
			}
			row += int(run)
			value = !value
			data = data[n:]
		}
		if row != rows {
			return 0, fmt.Errorf("typedcolumn: bool rle rows=%d want=%d", row, rows)
		}
		if len(data) != 0 {
			return 0, errors.New("typedcolumn: trailing bool rle bytes")
		}
		return count, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unsupported bool payload mode %d", raw[0])
	}
}

func validateBoolSelectionRows(rows int, selection RowSelection) error {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return err
	}
	if selection.Rows() != rows {
		return fmt.Errorf("typedcolumn: bool selection rows=%d want %d", selection.Rows(), rows)
	}
	return nil
}

func countBoolPayloadSelection(raw []byte, rows int, selection RowSelection, value bool) (int, error) {
	if len(raw) == 0 {
		return 0, errors.New("typedcolumn: missing bool payload mode")
	}
	switch raw[0] {
	case boolPayloadBitpack:
		mask := raw[1:]
		need, err := bitmapBytesChecked(rows)
		if err != nil {
			return 0, err
		}
		if len(mask) != need {
			return 0, fmt.Errorf("typedcolumn: bool bitpack bytes=%d want=%d", len(mask), need)
		}
		trueCount := countBoolBitpackSelection(mask, rows, selection)
		if value {
			return trueCount, nil
		}
		return selection.Count() - trueCount, nil
	case boolPayloadRLE:
		return countBoolRLESelection(raw, rows, selection, value)
	default:
		return 0, fmt.Errorf("typedcolumn: unsupported bool payload mode %d", raw[0])
	}
}

func countBoolBitpackSelection(mask []byte, rows int, selection RowSelection) int {
	switch selection.Kind() {
	case RowSelectionEmpty:
		return 0
	case RowSelectionAll:
		return countBoolBitpackRange(mask, 0, rows)
	case RowSelectionRange:
		start, end, _ := selection.SingleRange()
		return countBoolBitpackRange(mask, start, end)
	case RowSelectionRanges:
		count := 0
		for _, r := range selection.Ranges() {
			count += countBoolBitpackRange(mask, r.Start, r.End)
		}
		return count
	case RowSelectionBitmap:
		count := 0
		for wordIndex, selected := range selection.BitmapWords() {
			if selected == 0 {
				continue
			}
			word := boolBitpackWord(mask, wordIndex, rows)
			count += bits.OnesCount64(word & selected)
		}
		return count
	case RowSelectionSparse:
		count := 0
		for _, row := range selection.SparseRows() {
			if bitmapBit(mask, row) {
				count++
			}
		}
		return count
	default:
		return 0
	}
}

func countBoolBitpackRange(mask []byte, start int, end int) int {
	if start < 0 || end < start {
		return 0
	}
	count := 0
	for start < end && start%8 != 0 {
		if bitmapBit(mask, start) {
			count++
		}
		start++
	}
	for start+8 <= end {
		count += bits.OnesCount8(mask[start/8])
		start += 8
	}
	for start < end {
		if bitmapBit(mask, start) {
			count++
		}
		start++
	}
	return count
}

func boolBitpackWord(mask []byte, wordIndex int, rows int) uint64 {
	byteStart := wordIndex * 8
	if byteStart >= len(mask) {
		return 0
	}
	byteEnd := min(byteStart+8, len(mask))
	var word uint64
	for i, b := range mask[byteStart:byteEnd] {
		word |= uint64(b) << uint(i*8)
	}
	if padding := rows % 64; padding != 0 && wordIndex == rowSelectionBitmapWords(rows)-1 {
		word &= uint64(1<<uint(padding)) - 1
	}
	return word
}

func countBoolRLESelection(raw []byte, rows int, selection RowSelection, want bool) (int, error) {
	value, data, err := parseBoolRLEHeader(raw)
	if err != nil {
		return 0, err
	}
	row := 0
	count := 0
	for len(data) > 0 && row < rows {
		run, n := binary.Uvarint(data)
		if n <= 0 {
			return 0, errors.New("typedcolumn: malformed bool rle run")
		}
		if run == 0 || run > uint64(rows-row) {
			return 0, errors.New("typedcolumn: invalid bool rle run")
		}
		runStart := row
		runEnd := row + int(run)
		if value == want {
			count += countSelectionInRange(selection, runStart, runEnd)
		}
		row = runEnd
		value = !value
		data = data[n:]
	}
	if row != rows {
		return 0, fmt.Errorf("typedcolumn: bool rle rows=%d want=%d", row, rows)
	}
	if len(data) != 0 {
		return 0, errors.New("typedcolumn: trailing bool rle bytes")
	}
	return count, nil
}

func countSelectionInRange(selection RowSelection, start int, end int) int {
	if end <= start || selection.IsEmpty() {
		return 0
	}
	switch selection.Kind() {
	case RowSelectionAll:
		return end - start
	case RowSelectionRange:
		selStart, selEnd, _ := selection.SingleRange()
		return max(0, min(end, selEnd)-max(start, selStart))
	case RowSelectionRanges:
		count := 0
		for _, r := range selection.Ranges() {
			if r.End <= start {
				continue
			}
			if r.Start >= end {
				break
			}
			count += max(0, min(end, r.End)-max(start, r.Start))
		}
		return count
	case RowSelectionBitmap:
		return countBitmapSelectionRange(selection.BitmapWords(), start, end)
	case RowSelectionSparse:
		sparse := selection.SparseRows()
		lo := sort.SearchInts(sparse, start)
		hi := sort.SearchInts(sparse, end)
		return hi - lo
	default:
		return 0
	}
}

func countBitmapSelectionRange(words []uint64, start int, end int) int {
	if end <= start {
		return 0
	}
	count := 0
	for start < end && start%64 != 0 {
		if words[start/64]&(uint64(1)<<uint(start%64)) != 0 {
			count++
		}
		start++
	}
	for start+64 <= end {
		count += bits.OnesCount64(words[start/64])
		start += 64
	}
	for start < end {
		if words[start/64]&(uint64(1)<<uint(start%64)) != 0 {
			count++
		}
		start++
	}
	return count
}

func selectBoolPayload(raw []byte, rows int, selection RowSelection, value bool, scratch *BoolSelectionScratch) (RowSelection, error) {
	if len(raw) == 0 {
		return RowSelection{}, errors.New("typedcolumn: missing bool payload mode")
	}
	switch raw[0] {
	case boolPayloadBitpack:
		mask := raw[1:]
		need, err := bitmapBytesChecked(rows)
		if err != nil {
			return RowSelection{}, err
		}
		if len(mask) != need {
			return RowSelection{}, fmt.Errorf("typedcolumn: bool bitpack bytes=%d want=%d", len(mask), need)
		}
		return selectBoolBitpack(mask, rows, selection, value, scratch)
	case boolPayloadRLE:
		return selectBoolRLE(raw, rows, selection, value, scratch)
	default:
		return RowSelection{}, fmt.Errorf("typedcolumn: unsupported bool payload mode %d", raw[0])
	}
}

func selectBoolBitpack(mask []byte, rows int, selection RowSelection, value bool, scratch *BoolSelectionScratch) (RowSelection, error) {
	switch selection.Kind() {
	case RowSelectionEmpty:
		return NewEmptyRowSelection(rows)
	case RowSelectionSparse:
		scratch.Rows = scratch.Rows[:0]
		for _, row := range selection.SparseRows() {
			if bitmapBit(mask, row) == value {
				scratch.Rows = append(scratch.Rows, row)
			}
		}
		return boolRowsSelection(rows, scratch)
	default:
		words := rowSelectionBitmapWords(rows)
		if cap(scratch.Bitmap) < words {
			scratch.Bitmap = make([]uint64, words)
		} else {
			scratch.Bitmap = scratch.Bitmap[:words]
			for i := range scratch.Bitmap {
				scratch.Bitmap[i] = 0
			}
		}
		applyBoolBitpackSelection(mask, rows, selection, value, scratch.Bitmap)
		return NewBitmapRowSelectionNoCopy(rows, scratch.Bitmap)
	}
}

func applyBoolBitpackSelection(mask []byte, rows int, selection RowSelection, value bool, out []uint64) {
	applyRange := func(start, end int) {
		for row := start; row < end; row++ {
			if bitmapBit(mask, row) == value {
				out[row/64] |= uint64(1) << uint(row%64)
			}
		}
	}
	switch selection.Kind() {
	case RowSelectionAll:
		applyRange(0, rows)
	case RowSelectionRange:
		start, end, _ := selection.SingleRange()
		applyRange(start, end)
	case RowSelectionRanges:
		for _, r := range selection.Ranges() {
			applyRange(r.Start, r.End)
		}
	case RowSelectionBitmap:
		for wordIndex, selected := range selection.BitmapWords() {
			if selected == 0 {
				continue
			}
			word := boolBitpackWord(mask, wordIndex, rows)
			if !value {
				word = ^word
				if padding := rows % 64; padding != 0 && wordIndex == rowSelectionBitmapWords(rows)-1 {
					word &= uint64(1<<uint(padding)) - 1
				}
			}
			out[wordIndex] = selected & word
		}
	}
}

func selectBoolRLE(raw []byte, rows int, selection RowSelection, want bool, scratch *BoolSelectionScratch) (RowSelection, error) {
	value, data, err := parseBoolRLEHeader(raw)
	if err != nil {
		return RowSelection{}, err
	}
	if selection.Kind() == RowSelectionBitmap || selection.Kind() == RowSelectionSparse {
		return selectBoolRLEByRows(value, data, rows, selection, want, scratch)
	}
	scratch.Ranges = scratch.Ranges[:0]
	row := 0
	for len(data) > 0 && row < rows {
		run, n := binary.Uvarint(data)
		if n <= 0 {
			return RowSelection{}, errors.New("typedcolumn: malformed bool rle run")
		}
		if run == 0 || run > uint64(rows-row) {
			return RowSelection{}, errors.New("typedcolumn: invalid bool rle run")
		}
		runStart := row
		runEnd := row + int(run)
		if value == want {
			appendSelectionRangeIntersections(selection, runStart, runEnd, scratch)
		}
		row = runEnd
		value = !value
		data = data[n:]
	}
	if row != rows {
		return RowSelection{}, fmt.Errorf("typedcolumn: bool rle rows=%d want=%d", row, rows)
	}
	if len(data) != 0 {
		return RowSelection{}, errors.New("typedcolumn: trailing bool rle bytes")
	}
	return NewRangesRowSelectionNoCopy(rows, scratch.Ranges)
}

func appendSelectionRangeIntersections(selection RowSelection, start int, end int, scratch *BoolSelectionScratch) {
	appendRange := func(a, b int) {
		if a < b {
			scratch.Ranges = append(scratch.Ranges, RowRange{Start: a, End: b})
		}
	}
	switch selection.Kind() {
	case RowSelectionAll:
		appendRange(start, end)
	case RowSelectionRange:
		selStart, selEnd, _ := selection.SingleRange()
		appendRange(max(start, selStart), min(end, selEnd))
	case RowSelectionRanges:
		for _, r := range selection.Ranges() {
			if r.End <= start {
				continue
			}
			if r.Start >= end {
				break
			}
			appendRange(max(start, r.Start), min(end, r.End))
		}
	}
}

func selectBoolRLEByRows(value bool, data []byte, rows int, selection RowSelection, want bool, scratch *BoolSelectionScratch) (RowSelection, error) {
	if selection.Kind() == RowSelectionBitmap {
		words := rowSelectionBitmapWords(rows)
		if cap(scratch.Bitmap) < words {
			scratch.Bitmap = make([]uint64, words)
		} else {
			scratch.Bitmap = scratch.Bitmap[:words]
			for i := range scratch.Bitmap {
				scratch.Bitmap[i] = 0
			}
		}
		row := 0
		for len(data) > 0 && row < rows {
			run, n := binary.Uvarint(data)
			if n <= 0 {
				return RowSelection{}, errors.New("typedcolumn: malformed bool rle run")
			}
			if run == 0 || run > uint64(rows-row) {
				return RowSelection{}, errors.New("typedcolumn: invalid bool rle run")
			}
			runEnd := row + int(run)
			if value == want {
				copyBitmapRangeIntersection(scratch.Bitmap, selection.BitmapWords(), row, runEnd)
			}
			row = runEnd
			value = !value
			data = data[n:]
		}
		if row != rows {
			return RowSelection{}, fmt.Errorf("typedcolumn: bool rle rows=%d want=%d", row, rows)
		}
		if len(data) != 0 {
			return RowSelection{}, errors.New("typedcolumn: trailing bool rle bytes")
		}
		return NewBitmapRowSelectionNoCopy(rows, scratch.Bitmap)
	}
	scratch.Rows = scratch.Rows[:0]
	row := 0
	sparse := selection.SparseRows()
	sparseIndex := 0
	for len(data) > 0 && row < rows {
		run, n := binary.Uvarint(data)
		if n <= 0 {
			return RowSelection{}, errors.New("typedcolumn: malformed bool rle run")
		}
		if run == 0 || run > uint64(rows-row) {
			return RowSelection{}, errors.New("typedcolumn: invalid bool rle run")
		}
		runEnd := row + int(run)
		for sparseIndex < len(sparse) && sparse[sparseIndex] < row {
			sparseIndex++
		}
		if value == want {
			for sparseIndex < len(sparse) && sparse[sparseIndex] < runEnd {
				scratch.Rows = append(scratch.Rows, sparse[sparseIndex])
				sparseIndex++
			}
		}
		row = runEnd
		value = !value
		data = data[n:]
	}
	if row != rows {
		return RowSelection{}, fmt.Errorf("typedcolumn: bool rle rows=%d want=%d", row, rows)
	}
	if len(data) != 0 {
		return RowSelection{}, errors.New("typedcolumn: trailing bool rle bytes")
	}
	return boolRowsSelection(rows, scratch)
}

func copyBitmapRangeIntersection(dst []uint64, src []uint64, start int, end int) {
	for row := start; row < end; row++ {
		word := row / 64
		bit := uint(row % 64)
		if src[word]&(uint64(1)<<bit) != 0 {
			dst[word] |= uint64(1) << bit
		}
	}
}

const boolSelectionRangeLimit = 8

func boolRowsSelection(rows int, scratch *BoolSelectionScratch) (RowSelection, error) {
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
	if len(scratch.Ranges) <= boolSelectionRangeLimit {
		return NewRangesRowSelectionNoCopy(rows, scratch.Ranges)
	}
	if len(selected) >= 64 && len(selected)*4 >= rows*3 {
		words := rowSelectionBitmapWords(rows)
		if cap(scratch.Bitmap) < words {
			scratch.Bitmap = make([]uint64, words)
		} else {
			scratch.Bitmap = scratch.Bitmap[:words]
			for i := range scratch.Bitmap {
				scratch.Bitmap[i] = 0
			}
		}
		for _, row := range selected {
			scratch.Bitmap[row/64] |= uint64(1) << uint(row%64)
		}
		return NewBitmapRowSelectionNoCopy(rows, scratch.Bitmap)
	}
	return NewSparseRowSelectionNoCopy(rows, selected)
}

func isContiguousBoolRows(rows []int) bool {
	for i := 1; i < len(rows); i++ {
		if rows[i] != rows[i-1]+1 {
			return false
		}
	}
	return len(rows) != 0
}

func boolRLEPayloadLen(values []bool) int {
	if len(values) == 0 {
		return 0
	}
	var buf [binary.MaxVarintLen64]byte
	n := 2
	run := uint64(1)
	prev := values[0]
	for _, v := range values[1:] {
		if v == prev {
			run++
			continue
		}
		n += binary.PutUvarint(buf[:], run)
		run = 1
		prev = v
	}
	n += binary.PutUvarint(buf[:], run)
	return n
}

func appendBoolRLE(dst []byte, values []bool) []byte {
	dst = append(dst, boolPayloadRLE)
	if values[0] {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	var buf [binary.MaxVarintLen64]byte
	run := uint64(1)
	prev := values[0]
	for _, v := range values[1:] {
		if v == prev {
			run++
			continue
		}
		n := binary.PutUvarint(buf[:], run)
		dst = append(dst, buf[:n]...)
		run = 1
		prev = v
	}
	n := binary.PutUvarint(buf[:], run)
	return append(dst, buf[:n]...)
}

func encodeUint32CodesPayload(dst []byte, codes []uint32, cardinality uint32, maxCode uint32) []byte {
	width := uint32CodeWidth(maxCode)
	dst = append(dst[:0], width)
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(cardinality))
	dst = append(dst, buf[:n]...)
	switch width {
	case 1:
		for _, code := range codes {
			dst = append(dst, byte(code))
		}
	case 2:
		oldLen := len(dst)
		dst = append(dst, make([]byte, len(codes)*2)...)
		for i, code := range codes {
			binary.LittleEndian.PutUint16(dst[oldLen+i*2:], uint16(code))
		}
	case 4:
		oldLen := len(dst)
		dst = append(dst, make([]byte, len(codes)*4)...)
		for i, code := range codes {
			binary.LittleEndian.PutUint32(dst[oldLen+i*4:], code)
		}
	}
	return dst
}

type uint32CodesHeader struct {
	width       byte
	cardinality uint32
	data        []byte
}

func parseUint32CodesHeader(raw []byte, rows int) (uint32CodesHeader, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return uint32CodesHeader{}, err
	}
	if len(raw) < 2 {
		return uint32CodesHeader{}, errors.New("typedcolumn: truncated code header")
	}
	width := raw[0]
	if width != 1 && width != 2 && width != 4 {
		return uint32CodesHeader{}, fmt.Errorf("typedcolumn: unsupported code width %d", width)
	}
	cardinality64, n := binary.Uvarint(raw[1:])
	if n <= 0 {
		return uint32CodesHeader{}, errors.New("typedcolumn: malformed code cardinality")
	}
	if cardinality64 == 0 || cardinality64 > maxCodeCardinality {
		return uint32CodesHeader{}, fmt.Errorf("typedcolumn: invalid code cardinality %d", cardinality64)
	}
	data := raw[1+n:]
	if rows > int(^uint(0)>>1)/int(width) {
		return uint32CodesHeader{}, fmt.Errorf("typedcolumn: code rows=%d width=%d overflow", rows, width)
	}
	need := rows * int(width)
	if len(data) != need {
		return uint32CodesHeader{}, fmt.Errorf("typedcolumn: code data bytes=%d want=%d", len(data), need)
	}
	return uint32CodesHeader{width: width, cardinality: uint32(cardinality64), data: data}, nil
}

func readUint32Code(data []byte, width byte, row int) uint32 {
	switch width {
	case 1:
		return uint32(data[row])
	case 2:
		return uint32(binary.LittleEndian.Uint16(data[row*2:]))
	default:
		return binary.LittleEndian.Uint32(data[row*4:])
	}
}

type nullableInt64Header struct {
	valueEncoding Encoding
	defaultValue  int64
	storedRows    int
	nullMask      []byte
	defaultMask   []byte
	encodedValues []byte
}

func parseNullableInt64Header(raw []byte, rows int) (nullableInt64Header, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return nullableInt64Header{}, err
	}
	if len(raw) < nullableInt64HeaderBytes {
		return nullableInt64Header{}, errors.New("typedcolumn: truncated nullable int64 header")
	}
	valueEncoding := Encoding(raw[0])
	if _, err := nullableValueEncoding(valueEncoding); err != nil {
		return nullableInt64Header{}, err
	}
	defaultValue := int64(binary.LittleEndian.Uint64(raw[1:9]))
	storedRows := int(binary.LittleEndian.Uint32(raw[9:13]))
	if storedRows > rows {
		return nullableInt64Header{}, fmt.Errorf("typedcolumn: nullable stored rows=%d exceeds rows=%d", storedRows, rows)
	}
	nullMaskLen := int(binary.LittleEndian.Uint32(raw[13:17]))
	defaultMaskLen := int(binary.LittleEndian.Uint32(raw[17:21]))
	wantMaskLen, err := bitmapBytesChecked(rows)
	if err != nil {
		return nullableInt64Header{}, err
	}
	if nullMaskLen != wantMaskLen {
		return nullableInt64Header{}, fmt.Errorf("typedcolumn: null mask bytes=%d want=%d", nullMaskLen, wantMaskLen)
	}
	if defaultMaskLen != wantMaskLen {
		return nullableInt64Header{}, fmt.Errorf("typedcolumn: default mask bytes=%d want=%d", defaultMaskLen, wantMaskLen)
	}
	need := nullableInt64HeaderBytes + nullMaskLen + defaultMaskLen
	if need > len(raw) {
		return nullableInt64Header{}, errors.New("typedcolumn: truncated nullable int64 masks")
	}
	return nullableInt64Header{
		valueEncoding: valueEncoding,
		defaultValue:  defaultValue,
		storedRows:    storedRows,
		nullMask:      raw[nullableInt64HeaderBytes : nullableInt64HeaderBytes+nullMaskLen],
		defaultMask:   raw[nullableInt64HeaderBytes+nullMaskLen : need],
		encodedValues: raw[need:],
	}, nil
}

func nullableValueEncoding(encoding Encoding) (Encoding, error) {
	switch encoding {
	case EncodingRawInt64, EncodingDeltaVarint, EncodingDoubleDeltaVarint:
		return encoding, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unsupported nullable int64 value encoding %d", encoding)
	}
}

func appendBitmap(dst []byte, rows int, values []bool) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, bitmapBytes(rows))...)
	for i := 0; i < rows; i++ {
		if boolAt(values, i) {
			dst[start+i/8] |= 1 << uint(i%8)
		}
	}
	return dst
}

func bitmapBytes(rows int) int {
	return (rows + 7) / 8
}

func bitmapBytesChecked(rows int) (int, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return 0, err
	}
	return bitmapBytes(rows), nil
}

func parseBoolRLEHeader(raw []byte) (bool, []byte, error) {
	if len(raw) < 2 {
		return false, nil, errors.New("typedcolumn: truncated bool rle header")
	}
	switch raw[1] {
	case 0:
		return false, raw[2:], nil
	case 1:
		return true, raw[2:], nil
	default:
		return false, nil, fmt.Errorf("typedcolumn: invalid bool rle start value %d", raw[1])
	}
}

func validateBoolRLERuns(data []byte, rows int) error {
	row := 0
	for len(data) > 0 && row < rows {
		run, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("typedcolumn: malformed bool rle run")
		}
		if run == 0 || run > uint64(rows-row) {
			return errors.New("typedcolumn: invalid bool rle run")
		}
		row += int(run)
		data = data[n:]
	}
	if row != rows {
		return fmt.Errorf("typedcolumn: bool rle rows=%d want=%d", row, rows)
	}
	if len(data) != 0 {
		return errors.New("typedcolumn: trailing bool rle bytes")
	}
	return nil
}

func nullableBitmapCounts(nullMask []byte, defaultMask []byte, rows int) (int, int, int, error) {
	if len(nullMask) != len(defaultMask) {
		return 0, 0, -1, fmt.Errorf("typedcolumn: nullable int64 mask length mismatch null=%d default=%d", len(nullMask), len(defaultMask))
	}
	nulls := 0
	defaults := 0
	overlap := -1
	fullBytes := rows / 8
	for i := 0; i < fullBytes; i++ {
		nullByte := nullMask[i]
		defaultByte := defaultMask[i]
		nulls += bits.OnesCount8(nullByte)
		defaults += bits.OnesCount8(defaultByte)
		if both := nullByte & defaultByte; both != 0 && overlap < 0 {
			overlap = i*8 + bits.TrailingZeros8(both)
		}
	}
	if rows%8 != 0 {
		validBits := uint(rows % 8)
		validMask := byte((1 << validBits) - 1)
		nullByte := nullMask[fullBytes]
		defaultByte := defaultMask[fullBytes]
		if nullByte&^validMask != 0 {
			return 0, 0, -1, errors.New("typedcolumn: null mask has non-zero padding bits")
		}
		if defaultByte&^validMask != 0 {
			return 0, 0, -1, errors.New("typedcolumn: default mask has non-zero padding bits")
		}
		nullByte &= validMask
		defaultByte &= validMask
		nulls += bits.OnesCount8(nullByte)
		defaults += bits.OnesCount8(defaultByte)
		if both := nullByte & defaultByte; both != 0 && overlap < 0 {
			overlap = fullBytes*8 + bits.TrailingZeros8(both)
		}
	}
	return nulls, defaults, overlap, nil
}

func bitmapBit(mask []byte, row int) bool {
	return mask[row/8]&(1<<uint(row%8)) != 0
}

func boolAt(values []bool, i int) bool {
	return i >= 0 && i < len(values) && values[i]
}

func validateOptionalBoolRows(name string, rows int, values []bool) error {
	if len(values) != 0 && len(values) != rows {
		return fmt.Errorf("typedcolumn: %s rows=%d want 0 or %d", name, len(values), rows)
	}
	return nil
}

func updateOptionalMinMax(min int64, max int64, has bool, v int64) (int64, int64, bool) {
	if !has {
		return v, v, true
	}
	if v < min {
		min = v
	}
	if v > max {
		max = v
	}
	return min, max, true
}

func boolMinMax(values []bool) (int64, int64) {
	seenFalse := false
	seenTrue := false
	for _, v := range values {
		if v {
			seenTrue = true
		} else {
			seenFalse = true
		}
	}
	switch {
	case seenFalse && seenTrue:
		return 0, 1
	case seenTrue:
		return 1, 1
	default:
		return 0, 0
	}
}

func minMaxUint32(values []uint32) (uint32, uint32) {
	min, max := values[0], values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func uint32CodeWidth(maxCode uint32) byte {
	switch {
	case maxCode <= 0xff:
		return 1
	case maxCode <= 0xffff:
		return 2
	default:
		return 4
	}
}

func ensureInt64Len(dst []int64, n int) []int64 {
	if cap(dst) < n {
		return make([]int64, n)
	}
	return dst[:n]
}

func ensureBoolLen(dst []bool, n int) []bool {
	if cap(dst) < n {
		return make([]bool, n)
	}
	return dst[:n]
}

func ensureUint32Len(dst []uint32, n int) []uint32 {
	if cap(dst) < n {
		return make([]uint32, n)
	}
	return dst[:n]
}

func ensureIntLen(dst []int, n int) []int {
	if cap(dst) < n {
		return make([]int, n)
	}
	return dst[:n]
}
