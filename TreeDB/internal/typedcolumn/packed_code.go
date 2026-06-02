package typedcolumn

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// FixedBytesRows is a row-major fixed-byte payload. Row slices alias Values and
// therefore inherit the caller-owned/mapped-resource lifetime of Values.
type FixedBytesRows struct {
	Rows        int
	BytesPerRow int
	Values      []byte
}

// PackedUintRows is a row-major sub-byte unsigned integer payload. Elements are
// packed LSB-first within each byte. Multi-byte word views are little-endian.
type PackedUintRows struct {
	Rows           int
	ElementsPerRow int
	BitsPerElement int
	BytesPerRow    int
	Values         []byte
}

var nativePackedCodeLittleEndian = func() bool {
	var value uint16 = 1
	return *(*byte)(unsafe.Pointer(&value)) == 1
}()

// NewFixedBytesRows validates a fixed-byte row payload and returns the view.
func NewFixedBytesRows(rows int, bytesPerRow int, values []byte) (FixedBytesRows, error) {
	out := FixedBytesRows{Rows: rows, BytesPerRow: bytesPerRow, Values: values}
	if err := out.Validate(); err != nil {
		return FixedBytesRows{}, err
	}
	return out, nil
}

// FixedBytesPayloadBytes returns rows*bytesPerRow after shape/overflow checks.
func FixedBytesPayloadBytes(rows int, bytesPerRow int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("typedcolumn: fixed_bytes rows=%d", rows)
	}
	if bytesPerRow <= 0 {
		return 0, fmt.Errorf("typedcolumn: fixed_bytes requires positive bytes_per_row, got %d", bytesPerRow)
	}
	return checkedMulInt(rows, bytesPerRow, "fixed_bytes payload bytes")
}

// Validate checks the fixed-byte payload shape.
func (c FixedBytesRows) Validate() error {
	want, err := FixedBytesPayloadBytes(c.Rows, c.BytesPerRow)
	if err != nil {
		return err
	}
	if len(c.Values) != want {
		return fmt.Errorf("typedcolumn: fixed_bytes values bytes=%d want rows=%d*bytes_per_row=%d (%d)", len(c.Values), c.Rows, c.BytesPerRow, want)
	}
	return nil
}

// Row returns Values for row without allocating. The returned slice aliases
// Values until the backing resource is released or mutated by the caller.
func (c FixedBytesRows) Row(row int) ([]byte, error) {
	if row < 0 || row >= c.Rows {
		return nil, fmt.Errorf("typedcolumn: fixed_bytes row=%d outside rows=%d", row, c.Rows)
	}
	start, err := checkedMulInt(row, c.BytesPerRow, "fixed_bytes row offset")
	if err != nil {
		return nil, err
	}
	end := start + c.BytesPerRow
	if start < 0 || end < start || end > len(c.Values) {
		return nil, fmt.Errorf("typedcolumn: fixed_bytes row %d range [%d,%d) outside values=%d", row, start, end, len(c.Values))
	}
	return c.Values[start:end], nil
}

// WordCount returns ceil(BytesPerRow/8), the scratch-word count for RowWords.
func (c FixedBytesRows) WordCount() int {
	return wordCountForBytes(c.BytesPerRow)
}

// RowWords returns the row as little-endian uint64 words. If the row is 8-byte
// aligned, host-endian-compatible, and its length is a multiple of 8, the
// returned words alias Values and direct is true. Otherwise words aliases or
// allocates caller scratch and the final partial word is zero-padded.
func (c FixedBytesRows) RowWords(row int, scratch []uint64) (words []uint64, direct bool, err error) {
	rowBytes, err := c.Row(row)
	if err != nil {
		return nil, false, err
	}
	return littleEndianRowWords(rowBytes, scratch)
}

// NewPackedUintRows validates a packed uint payload and returns the view.
func NewPackedUintRows(rows int, elementsPerRow int, bitsPerElement int, values []byte) (PackedUintRows, error) {
	bytesPerRow, err := PackedUintRowBytes(elementsPerRow, bitsPerElement)
	if err != nil {
		return PackedUintRows{}, err
	}
	out := PackedUintRows{Rows: rows, ElementsPerRow: elementsPerRow, BitsPerElement: bitsPerElement, BytesPerRow: bytesPerRow, Values: values}
	if err := out.Validate(); err != nil {
		return PackedUintRows{}, err
	}
	return out, nil
}

// PackedUintRowBytes returns ceil(elementsPerRow*bitsPerElement/8).
func PackedUintRowBytes(elementsPerRow int, bitsPerElement int) (int, error) {
	if elementsPerRow <= 0 {
		return 0, fmt.Errorf("typedcolumn: packed_uint requires positive elements_per_row, got %d", elementsPerRow)
	}
	if err := validatePackedBitsPerElement(bitsPerElement); err != nil {
		return 0, err
	}
	logicalBits, err := checkedMulInt(elementsPerRow, bitsPerElement, "packed_uint logical bits")
	if err != nil {
		return 0, err
	}
	withPadding, err := checkedAddInt(logicalBits, 7, "packed_uint rounded bits")
	if err != nil {
		return 0, err
	}
	return withPadding / 8, nil
}

// PackedUintPayloadBytes returns the physical bytes for a packed payload.
func PackedUintPayloadBytes(rows int, elementsPerRow int, bitsPerElement int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("typedcolumn: packed_uint rows=%d", rows)
	}
	rowBytes, err := PackedUintRowBytes(elementsPerRow, bitsPerElement)
	if err != nil {
		return 0, err
	}
	return checkedMulInt(rows, rowBytes, "packed_uint payload bytes")
}

// PackedUintRowWords returns ceil(row_bytes/8), the scratch-word count for one row.
func PackedUintRowWords(elementsPerRow int, bitsPerElement int) (int, error) {
	rowBytes, err := PackedUintRowBytes(elementsPerRow, bitsPerElement)
	if err != nil {
		return 0, err
	}
	return wordCountForBytes(rowBytes), nil
}

// Validate checks shape and zero padding bits.
func (p PackedUintRows) Validate() error {
	rowBytes, err := PackedUintRowBytes(p.ElementsPerRow, p.BitsPerElement)
	if err != nil {
		return err
	}
	if p.BytesPerRow != 0 && p.BytesPerRow != rowBytes {
		return fmt.Errorf("typedcolumn: packed_uint bytes_per_row=%d want %d for elements_per_row=%d bits_per_element=%d", p.BytesPerRow, rowBytes, p.ElementsPerRow, p.BitsPerElement)
	}
	want, err := PackedUintPayloadBytes(p.Rows, p.ElementsPerRow, p.BitsPerElement)
	if err != nil {
		return err
	}
	if len(p.Values) != want {
		return fmt.Errorf("typedcolumn: packed_uint values bytes=%d want rows=%d*bytes_per_row=%d (%d)", len(p.Values), p.Rows, rowBytes, want)
	}
	return p.ValidatePadding()
}

// ValidatePadding verifies that unused high bits in the final byte of every row
// are zero. It is intended for writer/open integrity validation; hot row access
// can rely on this once the immutable payload is certified.
func (p PackedUintRows) ValidatePadding() error {
	rowBytes, err := PackedUintRowBytes(p.ElementsPerRow, p.BitsPerElement)
	if err != nil {
		return err
	}
	want, err := checkedMulInt(p.Rows, rowBytes, "packed_uint payload bytes")
	if err != nil {
		return err
	}
	if len(p.Values) != want {
		return fmt.Errorf("typedcolumn: packed_uint values bytes=%d want rows=%d*bytes_per_row=%d (%d)", len(p.Values), p.Rows, rowBytes, want)
	}
	for row := 0; row < p.Rows; row++ {
		start := row * rowBytes
		if err := ValidatePackedUintRowPadding(p.Values[start:start+rowBytes], p.ElementsPerRow, p.BitsPerElement); err != nil {
			return fmt.Errorf("typedcolumn: packed_uint row %d: %w", row, err)
		}
	}
	return nil
}

// RowBytes returns packed bytes for row without allocating. The returned slice
// aliases Values and uses LSB-first element order within each byte.
func (p PackedUintRows) RowBytes(row int) ([]byte, error) {
	if row < 0 || row >= p.Rows {
		return nil, fmt.Errorf("typedcolumn: packed_uint row=%d outside rows=%d", row, p.Rows)
	}
	rowBytes, err := PackedUintRowBytes(p.ElementsPerRow, p.BitsPerElement)
	if err != nil {
		return nil, err
	}
	start, err := checkedMulInt(row, rowBytes, "packed_uint row offset")
	if err != nil {
		return nil, err
	}
	end := start + rowBytes
	if start < 0 || end < start || end > len(p.Values) {
		return nil, fmt.Errorf("typedcolumn: packed_uint row %d range [%d,%d) outside values=%d", row, start, end, len(p.Values))
	}
	return p.Values[start:end], nil
}

// Element returns one unpacked element from row/element without allocation.
func (p PackedUintRows) Element(row int, element int) (uint8, error) {
	if element < 0 || element >= p.ElementsPerRow {
		return 0, fmt.Errorf("typedcolumn: packed_uint element=%d outside elements_per_row=%d", element, p.ElementsPerRow)
	}
	rowBytes, err := p.RowBytes(row)
	if err != nil {
		return 0, err
	}
	return packedUintElement(rowBytes, element, p.BitsPerElement), nil
}

// WordCount returns ceil(BytesPerRow/8), the scratch-word count for RowWords.
func (p PackedUintRows) WordCount() int {
	if p.BytesPerRow > 0 {
		return wordCountForBytes(p.BytesPerRow)
	}
	rowBytes, err := PackedUintRowBytes(p.ElementsPerRow, p.BitsPerElement)
	if err != nil {
		return 0
	}
	return wordCountForBytes(rowBytes)
}

// RowWords returns the packed row as little-endian uint64 words. If the row is
// 8-byte aligned, host-endian-compatible, and its physical length is a multiple
// of 8, the returned words alias Values and direct is true. Otherwise words
// aliases or allocates caller scratch and the final partial word is zero-padded.
func (p PackedUintRows) RowWords(row int, scratch []uint64) (words []uint64, direct bool, err error) {
	rowBytes, err := p.RowBytes(row)
	if err != nil {
		return nil, false, err
	}
	return littleEndianRowWords(rowBytes, scratch)
}

// EncodePackedUintRows packs flattened row-major element values into bytes.
// Bits per element must be 1, 2, or 4. Padding bits are written as zero.
func EncodePackedUintRows(dst []byte, rows int, elementsPerRow int, bitsPerElement int, values []uint8) ([]byte, error) {
	if rows < 0 {
		return nil, fmt.Errorf("typedcolumn: packed_uint rows=%d", rows)
	}
	rowBytes, err := PackedUintRowBytes(elementsPerRow, bitsPerElement)
	if err != nil {
		return nil, err
	}
	elements, err := checkedMulInt(rows, elementsPerRow, "packed_uint elements")
	if err != nil {
		return nil, err
	}
	if len(values) != elements {
		return nil, fmt.Errorf("typedcolumn: packed_uint values=%d want rows=%d*elements_per_row=%d (%d)", len(values), rows, elementsPerRow, elements)
	}
	total, err := checkedMulInt(rows, rowBytes, "packed_uint payload bytes")
	if err != nil {
		return nil, err
	}
	out := dst
	if cap(out) < total {
		out = make([]byte, total)
	} else {
		out = out[:total]
		clear(out)
	}
	for row := 0; row < rows; row++ {
		rowStart := row * rowBytes
		elementStart := row * elementsPerRow
		if _, err := PackUintRow(out[rowStart:rowStart+rowBytes], values[elementStart:elementStart+elementsPerRow], bitsPerElement); err != nil {
			return nil, fmt.Errorf("typedcolumn: packed_uint row %d: %w", row, err)
		}
	}
	return out, nil
}

// PackUintRow packs one row into dst. The returned slice may alias dst.
func PackUintRow(dst []byte, values []uint8, bitsPerElement int) ([]byte, error) {
	rowBytes, err := PackedUintRowBytes(len(values), bitsPerElement)
	if err != nil {
		return nil, err
	}
	out := dst
	if cap(out) < rowBytes {
		out = make([]byte, rowBytes)
	} else {
		out = out[:rowBytes]
		clear(out)
	}
	maxValue := uint8((1 << uint(bitsPerElement)) - 1)
	for element, value := range values {
		if value > maxValue {
			return nil, fmt.Errorf("packed_uint element %d value=%d exceeds %d bits", element, value, bitsPerElement)
		}
		setPackedUintElement(out, element, bitsPerElement, value)
	}
	return out, nil
}

// UnpackUintRow unpacks one row into dst after validating row shape and padding.
// The returned slice may alias dst.
func UnpackUintRow(dst []uint8, row []byte, elementsPerRow int, bitsPerElement int) ([]uint8, error) {
	rowBytes, err := PackedUintRowBytes(elementsPerRow, bitsPerElement)
	if err != nil {
		return nil, err
	}
	if len(row) != rowBytes {
		return nil, fmt.Errorf("typedcolumn: packed_uint row bytes=%d want %d", len(row), rowBytes)
	}
	if err := ValidatePackedUintRowPadding(row, elementsPerRow, bitsPerElement); err != nil {
		return nil, err
	}
	out := dst
	if cap(out) < elementsPerRow {
		out = make([]uint8, elementsPerRow)
	} else {
		out = out[:elementsPerRow]
	}
	for element := 0; element < elementsPerRow; element++ {
		out[element] = packedUintElement(row, element, bitsPerElement)
	}
	return out, nil
}

// ValidatePackedUintRowPadding validates zero unused high bits in the row's
// final physical byte.
func ValidatePackedUintRowPadding(row []byte, elementsPerRow int, bitsPerElement int) error {
	rowBytes, err := PackedUintRowBytes(elementsPerRow, bitsPerElement)
	if err != nil {
		return err
	}
	if len(row) != rowBytes {
		return fmt.Errorf("packed_uint row bytes=%d want %d", len(row), rowBytes)
	}
	logicalBits, err := checkedMulInt(elementsPerRow, bitsPerElement, "packed_uint logical bits")
	if err != nil {
		return err
	}
	paddingBits := rowBytes*8 - logicalBits
	if paddingBits == 0 {
		return nil
	}
	validBits := 8 - paddingBits
	validMask := byte((1 << uint(validBits)) - 1)
	if row[len(row)-1]&^validMask != 0 {
		return fmt.Errorf("non-zero padding bits final_byte=0x%02x valid_mask=0x%02x", row[len(row)-1], validMask)
	}
	return nil
}

func validatePackedBitsPerElement(bitsPerElement int) error {
	switch bitsPerElement {
	case 1, 2, 4:
		return nil
	default:
		return fmt.Errorf("typedcolumn: packed_uint bits_per_element=%d want 1, 2, or 4", bitsPerElement)
	}
}

func packedUintElement(row []byte, element int, bitsPerElement int) uint8 {
	bitOffset := element * bitsPerElement
	byteIndex := bitOffset / 8
	shift := uint(bitOffset % 8)
	mask := uint8((1 << uint(bitsPerElement)) - 1)
	return (row[byteIndex] >> shift) & mask
}

func setPackedUintElement(row []byte, element int, bitsPerElement int, value uint8) {
	bitOffset := element * bitsPerElement
	byteIndex := bitOffset / 8
	shift := uint(bitOffset % 8)
	row[byteIndex] |= value << shift
}

func littleEndianRowWords(rowBytes []byte, scratch []uint64) ([]uint64, bool, error) {
	if len(rowBytes)%8 == 0 {
		if words, ok := directUint64View(rowBytes); ok {
			return words, true, nil
		}
	}
	wordCount := wordCountForBytes(len(rowBytes))
	out := scratch
	if cap(out) < wordCount {
		out = make([]uint64, wordCount)
	} else {
		out = out[:wordCount]
		clear(out)
	}
	for word := 0; word < wordCount; word++ {
		start := word * 8
		end := min(start+8, len(rowBytes))
		if end-start == 8 {
			out[word] = binary.LittleEndian.Uint64(rowBytes[start:end])
			continue
		}
		var value uint64
		for i, b := range rowBytes[start:end] {
			value |= uint64(b) << uint(i*8)
		}
		out[word] = value
	}
	return out, false, nil
}

func directUint64View(data []byte) ([]uint64, bool) {
	if !nativePackedCodeLittleEndian || len(data)%8 != 0 {
		return nil, false
	}
	if len(data) == 0 {
		return nil, true
	}
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	if addr%unsafe.Alignof(uint64(0)) != 0 {
		return nil, false
	}
	return unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(data))), len(data)/8), true
}

func wordCountForBytes(bytes int) int {
	if bytes <= 0 {
		return 0
	}
	return (bytes + 7) / 8
}
