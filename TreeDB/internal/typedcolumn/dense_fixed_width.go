package typedcolumn

import "fmt"

// RawDenseFixedWidth is a row-major fixed-width dense vector/matrix payload.
// Values are already encoded in the column type's raw storage byte order: one
// byte for 1-byte elements and little-endian for 2/4/8-byte elements.
type RawDenseFixedWidth struct {
	Rows              int
	ElementsPerRow    int
	ElementWidthBytes int
	Values            []byte
}

// DenseFixedWidthColumn is a row-major fixed-width byte view. Values may alias
// a mapped/session-owned section when Direct is true; callers must copy before
// retaining row slices beyond that handle/session lifetime.
type DenseFixedWidthColumn struct {
	Rows              int
	ElementsPerRow    int
	ElementWidthBytes int
	Values            []byte
	Direct            bool
}

func (c DenseFixedWidthColumn) RowBytes(row int) ([]byte, error) {
	if row < 0 || row >= c.Rows {
		return nil, fmt.Errorf("typedcolumn: dense row %d outside rows=%d", row, c.Rows)
	}
	rowBytes, err := checkedMulInt(c.ElementsPerRow, c.ElementWidthBytes, "dense row bytes")
	if err != nil {
		return nil, err
	}
	start, err := checkedMulInt(row, rowBytes, "dense row offset")
	if err != nil {
		return nil, err
	}
	if start > len(c.Values)-rowBytes {
		return nil, fmt.Errorf("typedcolumn: dense row %d outside values bytes=%d", row, len(c.Values))
	}
	return c.Values[start : start+rowBytes], nil
}

func (c DenseFixedWidthColumn) RowRangeBytes(startRow, endRow int) ([]byte, error) {
	if startRow < 0 || endRow < startRow || endRow > c.Rows {
		return nil, fmt.Errorf("typedcolumn: dense row range [%d,%d) outside rows=%d", startRow, endRow, c.Rows)
	}
	rowBytes, err := checkedMulInt(c.ElementsPerRow, c.ElementWidthBytes, "dense row bytes")
	if err != nil {
		return nil, err
	}
	start, err := checkedMulInt(startRow, rowBytes, "dense row-range offset")
	if err != nil {
		return nil, err
	}
	end, err := checkedMulInt(endRow, rowBytes, "dense row-range end")
	if err != nil {
		return nil, err
	}
	if start > len(c.Values) || end > len(c.Values) {
		return nil, fmt.Errorf("typedcolumn: dense row range [%d,%d) outside values bytes=%d", startRow, endRow, len(c.Values))
	}
	return c.Values[start:end], nil
}

func DenseFixedWidthVectorElementWidth(t ColumnType) (int, bool) {
	switch t {
	case ColumnTypeUint8Vector, ColumnTypeInt8Vector:
		return 1, true
	case ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector:
		return 2, true
	case ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeFloat32Vector:
		return 4, true
	case ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat64Vector:
		return 8, true
	default:
		return 0, false
	}
}

func DenseFixedWidthVectorEncoding(t ColumnType) (Encoding, bool) {
	switch t {
	case ColumnTypeUint8Vector:
		return EncodingRawUint8Vector, true
	case ColumnTypeInt8Vector:
		return EncodingRawInt8Vector, true
	case ColumnTypeUint16Vector:
		return EncodingRawUint16Vector, true
	case ColumnTypeInt16Vector:
		return EncodingRawInt16Vector, true
	case ColumnTypeUint32Vector:
		return EncodingRawUint32Vector, true
	case ColumnTypeInt32Vector:
		return EncodingRawInt32Vector, true
	case ColumnTypeUint64Vector:
		return EncodingRawUint64Vector, true
	case ColumnTypeInt64Vector:
		return EncodingRawInt64Vector, true
	case ColumnTypeFloat16Vector:
		return EncodingRawFloat16Vector, true
	case ColumnTypeBFloat16Vector:
		return EncodingRawBFloat16Vector, true
	case ColumnTypeFloat32Vector:
		return EncodingRawFloat32Vector, true
	case ColumnTypeFloat64Vector:
		return EncodingRawFloat64Vector, true
	default:
		return 0, false
	}
}

func DenseFixedWidthEncodingElementWidth(encoding Encoding) (int, bool) {
	switch encoding {
	case EncodingRawUint8Vector, EncodingRawInt8Vector:
		return 1, true
	case EncodingRawUint16Vector, EncodingRawInt16Vector, EncodingRawFloat16Vector, EncodingRawBFloat16Vector:
		return 2, true
	case EncodingRawUint32Vector, EncodingRawInt32Vector, EncodingRawFloat32Vector:
		return 4, true
	case EncodingRawUint64Vector, EncodingRawInt64Vector, EncodingRawFloat64Vector:
		return 8, true
	default:
		return 0, false
	}
}

func IsDenseFixedWidthVectorColumnType(t ColumnType) bool {
	_, ok := DenseFixedWidthVectorElementWidth(t)
	return ok
}

func IsGenericDenseFixedWidthVectorColumnType(t ColumnType) bool {
	return t != ColumnTypeFloat32Vector && IsDenseFixedWidthVectorColumnType(t)
}

func validateRawDenseFixedWidth(source RawDenseFixedWidth, name string) error {
	return validateDensePayloadBytes(len(source.Values), source.Rows, source.ElementsPerRow, source.ElementWidthBytes, name)
}

func (b *ColumnPartBuilder) gatherDenseFixedWidth(source RawDenseFixedWidth, def ColumnDefinition, start int, end int) ([]byte, error) {
	elementWidth, ok := DenseFixedWidthVectorElementWidth(def.Type)
	if !ok || def.Type == ColumnTypeFloat32Vector {
		return nil, fmt.Errorf("typedcolumn: column %s type=%s is not generic dense fixed-width vector", def.Name, def.Type)
	}
	if source.Rows < 0 || source.ElementsPerRow != def.FixedWidthElements || source.ElementWidthBytes != elementWidth {
		return nil, fmt.Errorf("typedcolumn: dense column %s metadata rows=%d elements_per_row=%d width=%d want elements_per_row=%d width=%d", def.Name, source.Rows, source.ElementsPerRow, source.ElementWidthBytes, def.FixedWidthElements, elementWidth)
	}
	if err := validateRawDenseFixedWidth(source, def.Name); err != nil {
		return nil, err
	}
	rowBytes, err := checkedMulInt(def.FixedWidthElements, elementWidth, def.Name+" row bytes")
	if err != nil {
		return nil, err
	}
	count, err := checkedMulInt(end-start, rowBytes, def.Name+" block bytes")
	if err != nil {
		return nil, err
	}
	b.denseRaw = ensureByteLen(b.denseRaw[:0], count)
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		sourceStart, err := checkedMulInt(sourceRow, rowBytes, def.Name+" source byte offset")
		if err != nil {
			return nil, err
		}
		if sourceStart > len(source.Values)-rowBytes {
			return nil, fmt.Errorf("typedcolumn: dense column %s row %d outside source bytes=%d", def.Name, sourceRow, len(source.Values))
		}
		copy(b.denseRaw[(row-start)*rowBytes:], source.Values[sourceStart:sourceStart+rowBytes])
	}
	return b.denseRaw, nil
}

func (b *GranuleBuilder) BuildDenseFixedWidth(values []byte, rows int, elementsPerRow int, elementBytes int) (EncodedGranule, error) {
	wantWidth, ok := DenseFixedWidthEncodingElementWidth(b.cfg.Encoding)
	if !ok || b.cfg.Encoding == EncodingRawFloat32Vector {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: dense fixed-width encoding=%s is unsupported", b.cfg.Encoding)
	}
	if elementBytes != wantWidth {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: dense fixed-width encoding=%s element_bytes=%d want %d", b.cfg.Encoding, elementBytes, wantWidth)
	}
	if b.cfg.Compression != CompressionNone {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: dense fixed-width sections require compression=none, got %s", b.cfg.Compression)
	}
	if rows <= 0 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: invalid dense fixed-width rows %d", rows)
	}
	if err := validateDensePayloadBytes(len(values), rows, elementsPerRow, elementBytes, b.cfg.Encoding.String()); err != nil {
		return EncodedGranule{}, err
	}
	b.raw = ensureByteLen(b.raw[:0], len(values))
	copy(b.raw, values)
	selection, err := admitCompressionInto(b.compressed[:0], b.raw, b.cfg.Encoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, b.cfg.Encoding, selection), nil
}

func DecodeRawDenseFixedWidthPayload(dst []byte, raw []byte, rows int, elementsPerRow int, elementBytes int) ([]byte, error) {
	if err := validateDensePayloadBytes(len(raw), rows, elementsPerRow, elementBytes, "dense fixed-width"); err != nil {
		return nil, err
	}
	out := ensureByteLen(dst[:0], len(raw))
	copy(out, raw)
	return out, nil
}

func DenseFixedWidthViewFromBytes(raw []byte, rows int, elementsPerRow int, elementBytes int) (DenseFixedWidthColumn, error) {
	if err := validateDensePayloadBytes(len(raw), rows, elementsPerRow, elementBytes, "dense fixed-width"); err != nil {
		return DenseFixedWidthColumn{}, err
	}
	return DenseFixedWidthColumn{Rows: rows, ElementsPerRow: elementsPerRow, ElementWidthBytes: elementBytes, Values: raw, Direct: true}, nil
}

func (r *GranuleReader) DecodeDenseFixedWidthInto(dst []byte, g EncodedGranule, elementsPerRow int, elementBytes int) ([]byte, error) {
	wantWidth, ok := DenseFixedWidthEncodingElementWidth(g.Encoding)
	if !ok || g.Encoding == EncodingRawFloat32Vector {
		return nil, fmt.Errorf("typedcolumn: dense fixed-width encoding=%s is unsupported", g.Encoding)
	}
	if elementBytes != wantWidth {
		return nil, fmt.Errorf("typedcolumn: dense fixed-width encoding=%s element_bytes=%d want %d", g.Encoding, elementBytes, wantWidth)
	}
	raw, err := r.decompressDensePayload(g, elementsPerRow, elementBytes, "dense fixed-width")
	if err != nil {
		return nil, err
	}
	return DecodeRawDenseFixedWidthPayload(dst, raw, g.Rows, elementsPerRow, elementBytes)
}

func (p *ColumnPart) DenseFixedWidthColumn(name string, dst []byte) (DenseFixedWidthColumn, error) {
	column, ok := p.Columns[name]
	if !ok {
		return DenseFixedWidthColumn{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	elementWidth, ok := DenseFixedWidthVectorElementWidth(column.Definition.Type)
	if !ok || column.Definition.Type == ColumnTypeFloat32Vector {
		return DenseFixedWidthColumn{}, fmt.Errorf("typedcolumn: column %s type=%s is not generic dense fixed-width vector", name, column.Definition.Type)
	}
	out, err := denseFixedWidthColumnInto(dst, p.Descriptor.RowCount, column, elementWidth)
	if err != nil {
		return DenseFixedWidthColumn{}, err
	}
	return DenseFixedWidthColumn{Rows: p.Descriptor.RowCount, ElementsPerRow: column.Definition.FixedWidthElements, ElementWidthBytes: elementWidth, Values: out}, nil
}

func denseFixedWidthColumnInto(dst []byte, rows int, column ColumnPartColumn, elementWidth int) ([]byte, error) {
	elements, err := checkedMulInt(rows, column.Definition.FixedWidthElements, "dense fixed-width column elements")
	if err != nil {
		return nil, err
	}
	bytes, err := checkedMulInt(elements, elementWidth, "dense fixed-width column bytes")
	if err != nil {
		return nil, err
	}
	out := ensureByteLen(dst[:0], bytes)
	var reader GranuleReader
	var scratch []byte
	for _, block := range column.Blocks {
		values, err := reader.DecodeDenseFixedWidthInto(scratch[:0], block.Granule, column.Definition.FixedWidthElements, elementWidth)
		if err != nil {
			return nil, err
		}
		scratch = values
		wantBytes, err := checkedMulInt(block.Descriptor.RowCount, column.Definition.FixedWidthElements, "dense fixed-width block elements")
		if err != nil {
			return nil, err
		}
		wantBytes, err = checkedMulInt(wantBytes, elementWidth, "dense fixed-width block bytes")
		if err != nil {
			return nil, err
		}
		if len(values) != wantBytes {
			return nil, fmt.Errorf("typedcolumn: block rows=%d decoded bytes=%d", block.Descriptor.RowCount, len(values))
		}
		start, err := checkedMulInt(block.Descriptor.FirstRow, column.Definition.FixedWidthElements, "dense fixed-width block offset elements")
		if err != nil {
			return nil, err
		}
		start, err = checkedMulInt(start, elementWidth, "dense fixed-width block offset bytes")
		if err != nil {
			return nil, err
		}
		if start > len(out)-len(values) {
			return nil, fmt.Errorf("typedcolumn: dense fixed-width block first_row=%d bytes=%d outside column bytes=%d", block.Descriptor.FirstRow, len(values), len(out))
		}
		copy(out[start:start+len(values)], values)
	}
	return out, nil
}

func ensureByteLen(dst []byte, n int) []byte {
	if cap(dst) < n {
		return make([]byte, n)
	}
	return dst[:n]
}
