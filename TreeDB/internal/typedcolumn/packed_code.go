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

func FixedBytesEncoding(t ColumnType) (Encoding, bool) {
	if t == ColumnTypeFixedBytes {
		return EncodingRawFixedBytes, true
	}
	return 0, false
}

func PackedUintVectorBits(t ColumnType) (int, bool) {
	switch t {
	case ColumnTypePackedBitVector:
		return 1, true
	case ColumnTypePackedUint2Vector:
		return 2, true
	case ColumnTypePackedUint4Vector:
		return 4, true
	default:
		return 0, false
	}
}

func PackedUintVectorTypeForBits(bitsPerElement int) (ColumnType, bool) {
	switch bitsPerElement {
	case 1:
		return ColumnTypePackedBitVector, true
	case 2:
		return ColumnTypePackedUint2Vector, true
	case 4:
		return ColumnTypePackedUint4Vector, true
	default:
		return "", false
	}
}

func PackedUintVectorEncoding(t ColumnType) (Encoding, bool) {
	switch t {
	case ColumnTypePackedBitVector:
		return EncodingRawPackedBitVector, true
	case ColumnTypePackedUint2Vector:
		return EncodingRawPackedUint2Vector, true
	case ColumnTypePackedUint4Vector:
		return EncodingRawPackedUint4Vector, true
	default:
		return 0, false
	}
}

func PackedUintVectorEncodingForBits(bitsPerElement int) (Encoding, bool) {
	switch bitsPerElement {
	case 1:
		return EncodingRawPackedBitVector, true
	case 2:
		return EncodingRawPackedUint2Vector, true
	case 4:
		return EncodingRawPackedUint4Vector, true
	default:
		return 0, false
	}
}

func PackedUintEncodingBits(encoding Encoding) (int, bool) {
	switch encoding {
	case EncodingRawPackedBitVector:
		return 1, true
	case EncodingRawPackedUint2Vector:
		return 2, true
	case EncodingRawPackedUint4Vector:
		return 4, true
	default:
		return 0, false
	}
}

func IsPackedUintVectorColumnType(t ColumnType) bool {
	_, ok := PackedUintVectorBits(t)
	return ok
}

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

func DecodeRawFixedBytesPayload(dst []byte, raw []byte, rows int, bytesPerRow int) ([]byte, error) {
	if _, err := NewFixedBytesRows(rows, bytesPerRow, raw); err != nil {
		return nil, err
	}
	out := ensureByteLen(dst[:0], len(raw))
	copy(out, raw)
	return out, nil
}

func FixedBytesViewFromBytes(raw []byte, rows int, bytesPerRow int) (FixedBytesRows, error) {
	return NewFixedBytesRows(rows, bytesPerRow, raw)
}

func DecodeRawPackedUintPayload(dst []byte, raw []byte, rows int, elementsPerRow int, bitsPerElement int) ([]byte, error) {
	if _, err := NewPackedUintRows(rows, elementsPerRow, bitsPerElement, raw); err != nil {
		return nil, err
	}
	out := ensureByteLen(dst[:0], len(raw))
	copy(out, raw)
	return out, nil
}

func PackedUintViewFromBytes(raw []byte, rows int, elementsPerRow int, bitsPerElement int) (PackedUintRows, error) {
	return NewPackedUintRows(rows, elementsPerRow, bitsPerElement, raw)
}

func (b *ColumnPartBuilder) gatherFixedBytes(source FixedBytesRows, def ColumnDefinition, start int, end int) ([]byte, error) {
	if def.Type != ColumnTypeFixedBytes {
		return nil, fmt.Errorf("typedcolumn: column %s type=%s is not fixed_bytes", def.Name, def.Type)
	}
	if source.Rows < 0 || source.BytesPerRow != def.FixedWidthElements {
		return nil, fmt.Errorf("typedcolumn: fixed_bytes column %s metadata rows=%d bytes_per_row=%d want bytes_per_row=%d", def.Name, source.Rows, source.BytesPerRow, def.FixedWidthElements)
	}
	if err := source.Validate(); err != nil {
		return nil, err
	}
	count, err := checkedMulInt(end-start, def.FixedWidthElements, def.Name+" block bytes")
	if err != nil {
		return nil, err
	}
	b.packedRaw = ensureByteLen(b.packedRaw[:0], count)
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		sourceStart, err := checkedMulInt(sourceRow, def.FixedWidthElements, def.Name+" source byte offset")
		if err != nil {
			return nil, err
		}
		if sourceStart > len(source.Values)-def.FixedWidthElements {
			return nil, fmt.Errorf("typedcolumn: fixed_bytes column %s row %d outside source bytes=%d", def.Name, sourceRow, len(source.Values))
		}
		copy(b.packedRaw[(row-start)*def.FixedWidthElements:], source.Values[sourceStart:sourceStart+def.FixedWidthElements])
	}
	return b.packedRaw, nil
}

func (b *ColumnPartBuilder) gatherPackedUint(source PackedUintRows, def ColumnDefinition, start int, end int) ([]byte, error) {
	bitsPerElement, ok := PackedUintVectorBits(def.Type)
	if !ok {
		return nil, fmt.Errorf("typedcolumn: column %s type=%s is not packed_uint vector", def.Name, def.Type)
	}
	if def.BitsPerElement != bitsPerElement {
		return nil, fmt.Errorf("typedcolumn: packed_uint column %s bits_per_element=%d want %d", def.Name, def.BitsPerElement, bitsPerElement)
	}
	rowBytes, err := PackedUintRowBytes(def.FixedWidthElements, bitsPerElement)
	if err != nil {
		return nil, err
	}
	if source.Rows < 0 || source.ElementsPerRow != def.FixedWidthElements || source.BitsPerElement != bitsPerElement || source.BytesPerRow != rowBytes {
		return nil, fmt.Errorf("typedcolumn: packed_uint column %s metadata rows=%d elements_per_row=%d bits=%d bytes_per_row=%d want elements_per_row=%d bits=%d bytes_per_row=%d", def.Name, source.Rows, source.ElementsPerRow, source.BitsPerElement, source.BytesPerRow, def.FixedWidthElements, bitsPerElement, rowBytes)
	}
	if err := source.Validate(); err != nil {
		return nil, err
	}
	count, err := checkedMulInt(end-start, rowBytes, def.Name+" block bytes")
	if err != nil {
		return nil, err
	}
	b.packedRaw = ensureByteLen(b.packedRaw[:0], count)
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		sourceStart, err := checkedMulInt(sourceRow, rowBytes, def.Name+" source byte offset")
		if err != nil {
			return nil, err
		}
		if sourceStart > len(source.Values)-rowBytes {
			return nil, fmt.Errorf("typedcolumn: packed_uint column %s row %d outside source bytes=%d", def.Name, sourceRow, len(source.Values))
		}
		copy(b.packedRaw[(row-start)*rowBytes:], source.Values[sourceStart:sourceStart+rowBytes])
	}
	return b.packedRaw, nil
}

func (b *GranuleBuilder) BuildFixedBytes(values []byte, rows int, bytesPerRow int) (EncodedGranule, error) {
	if b.cfg.Encoding != 0 && b.cfg.Encoding != EncodingRawFixedBytes {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: fixed_bytes encoding=%s want %s", b.cfg.Encoding, EncodingRawFixedBytes)
	}
	if b.cfg.Compression != CompressionNone {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: fixed_bytes sections require compression=none, got %s", b.cfg.Compression)
	}
	if rows <= 0 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: invalid fixed_bytes rows %d", rows)
	}
	if _, err := NewFixedBytesRows(rows, bytesPerRow, values); err != nil {
		return EncodedGranule{}, err
	}
	b.raw = ensureByteLen(b.raw[:0], len(values))
	copy(b.raw, values)
	selection, err := admitCompressionInto(b.compressed[:0], b.raw, EncodingRawFixedBytes, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, EncodingRawFixedBytes, selection), nil
}

func (b *GranuleBuilder) BuildPackedUint(values []byte, rows int, elementsPerRow int, bitsPerElement int) (EncodedGranule, error) {
	wantEncoding, ok := PackedUintVectorEncodingForBits(bitsPerElement)
	if !ok {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: unsupported packed_uint bits_per_element=%d", bitsPerElement)
	}
	if b.cfg.Encoding != 0 && b.cfg.Encoding != wantEncoding {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: packed_uint encoding=%s want %s", b.cfg.Encoding, wantEncoding)
	}
	if b.cfg.Compression != CompressionNone {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: packed_uint sections require compression=none, got %s", b.cfg.Compression)
	}
	if rows <= 0 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: invalid packed_uint rows %d", rows)
	}
	if _, err := NewPackedUintRows(rows, elementsPerRow, bitsPerElement, values); err != nil {
		return EncodedGranule{}, err
	}
	b.raw = ensureByteLen(b.raw[:0], len(values))
	copy(b.raw, values)
	selection, err := admitCompressionInto(b.compressed[:0], b.raw, wantEncoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, wantEncoding, selection), nil
}

func (r *GranuleReader) DecodeFixedBytesInto(dst []byte, g EncodedGranule, bytesPerRow int) ([]byte, error) {
	if g.Encoding != EncodingRawFixedBytes {
		return nil, fmt.Errorf("typedcolumn: fixed_bytes encoding=%s want %s", g.Encoding, EncodingRawFixedBytes)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawFixedBytesPayload(dst, raw, g.Rows, bytesPerRow)
}

func (r *GranuleReader) DecodePackedUintInto(dst []byte, g EncodedGranule, elementsPerRow int, bitsPerElement int) ([]byte, error) {
	wantEncoding, ok := PackedUintVectorEncodingForBits(bitsPerElement)
	if !ok {
		return nil, fmt.Errorf("typedcolumn: unsupported packed_uint bits_per_element=%d", bitsPerElement)
	}
	if g.Encoding != wantEncoding {
		return nil, fmt.Errorf("typedcolumn: packed_uint encoding=%s want %s", g.Encoding, wantEncoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawPackedUintPayload(dst, raw, g.Rows, elementsPerRow, bitsPerElement)
}

func (p *ColumnPart) FixedBytesColumn(name string, dst []byte) (FixedBytesRows, error) {
	column, ok := p.Columns[name]
	if !ok {
		return FixedBytesRows{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	if column.Definition.Type != ColumnTypeFixedBytes || column.Definition.Encoding != EncodingRawFixedBytes {
		return FixedBytesRows{}, fmt.Errorf("typedcolumn: column %s type/encoding=(%s,%s) is not fixed_bytes/raw_fixed_bytes", name, column.Definition.Type, column.Definition.Encoding)
	}
	out, err := fixedBytesColumnInto(dst, p.Descriptor.RowCount, column)
	if err != nil {
		return FixedBytesRows{}, err
	}
	return FixedBytesRows{Rows: p.Descriptor.RowCount, BytesPerRow: column.Definition.FixedWidthElements, Values: out}, nil
}

func fixedBytesColumnInto(dst []byte, rows int, column ColumnPartColumn) ([]byte, error) {
	bytes, err := FixedBytesPayloadBytes(rows, column.Definition.FixedWidthElements)
	if err != nil {
		return nil, err
	}
	out := ensureByteLen(dst[:0], bytes)
	var reader GranuleReader
	var scratch []byte
	expectedFirstRow := 0
	for _, block := range column.Blocks {
		if block.Descriptor.FirstRow != expectedFirstRow {
			return nil, fmt.Errorf("typedcolumn: fixed_bytes block first_row=%d want %d", block.Descriptor.FirstRow, expectedFirstRow)
		}
		values, err := reader.DecodeFixedBytesInto(scratch[:0], block.Granule, column.Definition.FixedWidthElements)
		if err != nil {
			return nil, err
		}
		scratch = values
		wantBytes, err := FixedBytesPayloadBytes(block.Descriptor.RowCount, column.Definition.FixedWidthElements)
		if err != nil {
			return nil, err
		}
		if len(values) != wantBytes {
			return nil, fmt.Errorf("typedcolumn: block rows=%d decoded fixed_bytes bytes=%d want %d", block.Descriptor.RowCount, len(values), wantBytes)
		}
		start, err := checkedMulInt(block.Descriptor.FirstRow, column.Definition.FixedWidthElements, "fixed_bytes block offset bytes")
		if err != nil {
			return nil, err
		}
		if start > len(out)-len(values) {
			return nil, fmt.Errorf("typedcolumn: fixed_bytes block first_row=%d bytes=%d outside column bytes=%d", block.Descriptor.FirstRow, len(values), len(out))
		}
		copy(out[start:start+len(values)], values)
		expectedFirstRow += block.Descriptor.RowCount
	}
	if expectedFirstRow != rows {
		return nil, fmt.Errorf("typedcolumn: fixed_bytes column covers %d rows, want %d", expectedFirstRow, rows)
	}
	return out, nil
}

func (p *ColumnPart) PackedUintColumn(name string, dst []byte) (PackedUintRows, error) {
	column, ok := p.Columns[name]
	if !ok {
		return PackedUintRows{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	bitsPerElement, ok := PackedUintVectorBits(column.Definition.Type)
	if !ok {
		return PackedUintRows{}, fmt.Errorf("typedcolumn: column %s type=%s is not packed_uint vector", name, column.Definition.Type)
	}
	wantEncoding, _ := PackedUintVectorEncodingForBits(bitsPerElement)
	if column.Definition.Encoding != wantEncoding || column.Definition.BitsPerElement != bitsPerElement {
		return PackedUintRows{}, fmt.Errorf("typedcolumn: column %s packed_uint encoding/bits=(%s,%d) want (%s,%d)", name, column.Definition.Encoding, column.Definition.BitsPerElement, wantEncoding, bitsPerElement)
	}
	out, err := packedUintColumnInto(dst, p.Descriptor.RowCount, column, bitsPerElement)
	if err != nil {
		return PackedUintRows{}, err
	}
	bytesPerRow, _ := PackedUintRowBytes(column.Definition.FixedWidthElements, bitsPerElement)
	return PackedUintRows{Rows: p.Descriptor.RowCount, ElementsPerRow: column.Definition.FixedWidthElements, BitsPerElement: bitsPerElement, BytesPerRow: bytesPerRow, Values: out}, nil
}

func packedUintColumnInto(dst []byte, rows int, column ColumnPartColumn, bitsPerElement int) ([]byte, error) {
	bytes, err := PackedUintPayloadBytes(rows, column.Definition.FixedWidthElements, bitsPerElement)
	if err != nil {
		return nil, err
	}
	out := ensureByteLen(dst[:0], bytes)
	rowBytes, err := PackedUintRowBytes(column.Definition.FixedWidthElements, bitsPerElement)
	if err != nil {
		return nil, err
	}
	var reader GranuleReader
	var scratch []byte
	expectedFirstRow := 0
	for _, block := range column.Blocks {
		if block.Descriptor.FirstRow != expectedFirstRow {
			return nil, fmt.Errorf("typedcolumn: packed_uint block first_row=%d want %d", block.Descriptor.FirstRow, expectedFirstRow)
		}
		values, err := reader.DecodePackedUintInto(scratch[:0], block.Granule, column.Definition.FixedWidthElements, bitsPerElement)
		if err != nil {
			return nil, err
		}
		scratch = values
		wantBytes, err := PackedUintPayloadBytes(block.Descriptor.RowCount, column.Definition.FixedWidthElements, bitsPerElement)
		if err != nil {
			return nil, err
		}
		if len(values) != wantBytes {
			return nil, fmt.Errorf("typedcolumn: block rows=%d decoded packed_uint bytes=%d want %d", block.Descriptor.RowCount, len(values), wantBytes)
		}
		start, err := checkedMulInt(block.Descriptor.FirstRow, rowBytes, "packed_uint block offset bytes")
		if err != nil {
			return nil, err
		}
		if start > len(out)-len(values) {
			return nil, fmt.Errorf("typedcolumn: packed_uint block first_row=%d bytes=%d outside column bytes=%d", block.Descriptor.FirstRow, len(values), len(out))
		}
		copy(out[start:start+len(values)], values)
		expectedFirstRow += block.Descriptor.RowCount
	}
	if expectedFirstRow != rows {
		return nil, fmt.Errorf("typedcolumn: packed_uint column covers %d rows, want %d", expectedFirstRow, rows)
	}
	return out, nil
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
