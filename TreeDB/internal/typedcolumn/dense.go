package typedcolumn

import "fmt"

// DenseFloat32Column is a row-major fixed-width float32 matrix section.
type DenseFloat32Column struct {
	Rows           int
	ElementsPerRow int
	Values         []float32
	Direct         bool
}

// DenseUint32Column is a row-major fixed-width uint32 matrix section.
type DenseUint32Column struct {
	Rows           int
	ElementsPerRow int
	Values         []uint32
	Direct         bool
}

func denseRowsForValues(values int, elementsPerRow int, name string) (int, error) {
	if elementsPerRow <= 0 {
		return 0, fmt.Errorf("typedcolumn: dense column %s requires positive fixed-width elements", name)
	}
	if values%elementsPerRow != 0 {
		return 0, fmt.Errorf("typedcolumn: dense column %s values=%d not divisible by fixed-width elements=%d", name, values, elementsPerRow)
	}
	rows := values / elementsPerRow
	return rows, nil
}

func (b *ColumnPartBuilder) gatherFloat32Dense(source []float32, elementsPerRow int, start int, end int) ([]float32, error) {
	count, err := checkedMulInt(end-start, elementsPerRow, "float32_vector block elements")
	if err != nil {
		return nil, err
	}
	b.float32s = ensureFloat32Len(b.float32s[:0], count)
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		sourceStart, err := checkedMulInt(sourceRow, elementsPerRow, "float32_vector source offset")
		if err != nil {
			return nil, err
		}
		if sourceStart > len(source)-elementsPerRow {
			return nil, fmt.Errorf("typedcolumn: float32_vector row %d outside source elements=%d", sourceRow, len(source))
		}
		copy(b.float32s[(row-start)*elementsPerRow:], source[sourceStart:sourceStart+elementsPerRow])
	}
	return b.float32s, nil
}

func (b *ColumnPartBuilder) gatherUint32Dense(source []uint32, elementsPerRow int, start int, end int) ([]uint32, error) {
	count, err := checkedMulInt(end-start, elementsPerRow, "uint32 dense block elements")
	if err != nil {
		return nil, err
	}
	b.u32dense = ensureUint32Len(b.u32dense[:0], count)
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		sourceStart, err := checkedMulInt(sourceRow, elementsPerRow, "uint32 dense source offset")
		if err != nil {
			return nil, err
		}
		if sourceStart > len(source)-elementsPerRow {
			return nil, fmt.Errorf("typedcolumn: uint32 dense row %d outside source elements=%d", sourceRow, len(source))
		}
		copy(b.u32dense[(row-start)*elementsPerRow:], source[sourceStart:sourceStart+elementsPerRow])
	}
	return b.u32dense, nil
}

func (b *ColumnPartBuilder) gatherUint32OffsetsList(source RawUint32OffsetsList, start int, end int) (RawUint32OffsetsList, error) {
	if err := ValidateRawUint32OffsetsListShape(source.Rows, source.Offsets, uint64(len(source.Values))); err != nil {
		return RawUint32OffsetsList{}, err
	}
	rows := end - start
	b.u32offset = resizeFixedWidthValues(b.u32offset[:0], rows+1)
	b.u32offset[0] = 0
	b.u32dense = b.u32dense[:0]
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		if sourceRow < 0 || sourceRow >= source.Rows {
			return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: uint32 offsets-list source row %d outside rows=%d", sourceRow, source.Rows)
		}
		begin := source.Offsets[sourceRow]
		finish := source.Offsets[sourceRow+1]
		if begin > maxHostIntUint64() || finish > maxHostIntUint64() {
			return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: uint32 offsets-list row %d offset outside host int", sourceRow)
		}
		if finish < begin {
			return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: uint32 offsets-list row %d non-monotonic offsets", sourceRow)
		}
		beginInt := int(begin)
		finishInt := int(finish)
		if beginInt > len(source.Values) || finishInt > len(source.Values) {
			return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: uint32 offsets-list row %d values range [%d,%d) outside values=%d", sourceRow, begin, finish, len(source.Values))
		}
		b.u32dense = append(b.u32dense, source.Values[beginInt:finishInt]...)
		b.u32offset[row-start+1] = uint64(len(b.u32dense))
	}
	return RawUint32OffsetsList{Rows: rows, Offsets: b.u32offset, Values: b.u32dense}, nil
}

func (b *ColumnPartBuilder) gatherBytesOffsets(source RawBytesOffsets, start int, end int) (RawBytesOffsets, error) {
	if err := ValidateRawBytesOffsetsShape(source.Rows, source.Offsets, uint64(len(source.Values))); err != nil {
		return RawBytesOffsets{}, err
	}
	rows := end - start
	b.bytesOff = resizeFixedWidthValues(b.bytesOff[:0], rows+1)
	b.bytesOff[0] = 0
	b.bytesData = b.bytesData[:0]
	for row := start; row < end; row++ {
		sourceRow := b.order[row]
		if sourceRow < 0 || sourceRow >= source.Rows {
			return RawBytesOffsets{}, fmt.Errorf("typedcolumn: bytes source row %d outside rows=%d", sourceRow, source.Rows)
		}
		begin := source.Offsets[sourceRow]
		finish := source.Offsets[sourceRow+1]
		if begin > maxHostIntUint64() || finish > maxHostIntUint64() {
			return RawBytesOffsets{}, fmt.Errorf("typedcolumn: bytes row %d offset outside host int", sourceRow)
		}
		if finish < begin {
			return RawBytesOffsets{}, fmt.Errorf("typedcolumn: bytes row %d non-monotonic offsets", sourceRow)
		}
		beginInt := int(begin)
		finishInt := int(finish)
		if beginInt > len(source.Values) || finishInt > len(source.Values) {
			return RawBytesOffsets{}, fmt.Errorf("typedcolumn: bytes row %d values range [%d,%d) outside values=%d", sourceRow, begin, finish, len(source.Values))
		}
		b.bytesData = append(b.bytesData, source.Values[beginInt:finishInt]...)
		b.bytesOff[row-start+1] = uint64(len(b.bytesData))
	}
	return RawBytesOffsets{Rows: rows, Offsets: b.bytesOff, Values: b.bytesData}, nil
}

func (b *GranuleBuilder) BuildFloat32Vector(values []float32, rows int, elementsPerRow int) (EncodedGranule, error) {
	if b.cfg.Encoding != 0 && b.cfg.Encoding != EncodingRawFloat32Vector {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: float32_vector encoding=%s want %s", b.cfg.Encoding, EncodingRawFloat32Vector)
	}
	if b.cfg.Compression != CompressionNone {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: float32_vector dense sections require compression=none, got %s", b.cfg.Compression)
	}
	if rows <= 0 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: invalid float32_vector rows %d", rows)
	}
	if err := validateDenseElementCount(len(values), rows, elementsPerRow, "float32_vector"); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeFloat32DensePayload(b.raw[:0], values)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawFloat32Vector, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, EncodingRawFloat32Vector, selection), nil
}

func (b *GranuleBuilder) BuildUint32Dense(values []uint32, rows int, elementsPerRow int) (EncodedGranule, error) {
	if b.cfg.Encoding != 0 && b.cfg.Encoding != EncodingRawUint32Dense {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: uint32 dense encoding=%s want %s", b.cfg.Encoding, EncodingRawUint32Dense)
	}
	if b.cfg.Compression != CompressionNone {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: uint32 dense sections require compression=none, got %s", b.cfg.Compression)
	}
	if rows <= 0 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: invalid uint32 dense rows %d", rows)
	}
	if err := validateDenseElementCount(len(values), rows, elementsPerRow, "uint32 dense"); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeUint32DensePayload(b.raw[:0], values)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawUint32Dense, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, EncodingRawUint32Dense, selection), nil
}

func validateDenseElementCount(values int, rows int, elementsPerRow int, name string) error {
	if elementsPerRow <= 0 {
		return fmt.Errorf("typedcolumn: %s requires positive fixed-width elements", name)
	}
	if err := validateGranuleDecodeRows(rows); err != nil {
		return err
	}
	want, err := checkedMulInt(rows, elementsPerRow, name+" elements")
	if err != nil {
		return err
	}
	if values != want {
		return fmt.Errorf("typedcolumn: %s elements=%d want rows=%d*elements=%d", name, values, rows, elementsPerRow)
	}
	return nil
}

func encodeFloat32DensePayload(dst []byte, values []float32) ([]byte, error) {
	return encodeFloat32Payload(dst, values)
}

func encodeUint32DensePayload(dst []byte, values []uint32) ([]byte, error) {
	return encodeLittleEndian4Payload(dst, values, "uint32 dense")
}

func DecodeRawFloat32VectorPayload(dst []float32, raw []byte, rows int, elementsPerRow int) ([]float32, error) {
	if err := validateDensePayloadBytes(len(raw), rows, elementsPerRow, 4, "float32_vector"); err != nil {
		return nil, err
	}
	elements, err := checkedMulInt(rows, elementsPerRow, "float32_vector elements")
	if err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, elements)
	for i := range out {
		out[i] = float32FromLittleEndian(raw[i*4:])
	}
	return out, nil
}

func DecodeRawUint32DensePayload(dst []uint32, raw []byte, rows int, elementsPerRow int) ([]uint32, error) {
	if err := validateDensePayloadBytes(len(raw), rows, elementsPerRow, 4, "uint32 dense"); err != nil {
		return nil, err
	}
	elements, err := checkedMulInt(rows, elementsPerRow, "uint32 dense elements")
	if err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, elements)
	for i := range out {
		out[i] = readLittleEndianUint32(raw[i*4:])
	}
	return out, nil
}

func validateDensePayloadBytes(rawBytes int, rows int, elementsPerRow int, elementBytes int, name string) error {
	if rawBytes < 0 {
		return fmt.Errorf("typedcolumn: %s raw bytes=%d", name, rawBytes)
	}
	if rows <= 0 {
		return fmt.Errorf("typedcolumn: invalid %s rows %d", name, rows)
	}
	if elementsPerRow <= 0 {
		return fmt.Errorf("typedcolumn: %s requires positive fixed-width elements", name)
	}
	if elementBytes <= 0 {
		return fmt.Errorf("typedcolumn: %s requires positive element bytes", name)
	}
	if err := validateGranuleDecodeRows(rows); err != nil {
		return err
	}
	elements, err := checkedMulInt(rows, elementsPerRow, name+" elements")
	if err != nil {
		return err
	}
	wantBytes, err := checkedMulInt(elements, elementBytes, name+" raw bytes")
	if err != nil {
		return err
	}
	if rawBytes != wantBytes {
		return fmt.Errorf("typedcolumn: %s raw bytes=%d want=%d", name, rawBytes, wantBytes)
	}
	return nil
}

func (r *GranuleReader) DecodeFloat32VectorInto(dst []float32, g EncodedGranule, elementsPerRow int) ([]float32, error) {
	if g.Encoding != EncodingRawFloat32Vector {
		return nil, fmt.Errorf("typedcolumn: float32_vector encoding=%s want %s", g.Encoding, EncodingRawFloat32Vector)
	}
	raw, err := r.decompressDensePayload(g, elementsPerRow, 4, "float32_vector")
	if err != nil {
		return nil, err
	}
	return DecodeRawFloat32VectorPayload(dst, raw, g.Rows, elementsPerRow)
}

func (r *GranuleReader) DecodeUint32DenseInto(dst []uint32, g EncodedGranule, elementsPerRow int) ([]uint32, error) {
	if g.Encoding != EncodingRawUint32Dense {
		return nil, fmt.Errorf("typedcolumn: uint32 dense encoding=%s want %s", g.Encoding, EncodingRawUint32Dense)
	}
	raw, err := r.decompressDensePayload(g, elementsPerRow, 4, "uint32 dense")
	if err != nil {
		return nil, err
	}
	return DecodeRawUint32DensePayload(dst, raw, g.Rows, elementsPerRow)
}

func (r *GranuleReader) decompressDensePayload(g EncodedGranule, elementsPerRow int, elementBytes int, name string) ([]byte, error) {
	if err := validateDenseGranule(g, elementsPerRow, elementBytes, name); err != nil {
		return nil, err
	}
	return g.Payload, nil
}

func validateDenseGranule(g EncodedGranule, elementsPerRow int, elementBytes int, name string) error {
	if g.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: %s dense direct section requires compression=none, got %s", name, g.Compression)
	}
	if g.NullCount != 0 || g.DefaultCount != 0 {
		return fmt.Errorf("typedcolumn: %s dense section has null/default counts", name)
	}
	if g.HasMinMax {
		return fmt.Errorf("typedcolumn: %s dense section unexpectedly has min/max", name)
	}
	if err := validateDensePayloadBytes(g.RawBytes, g.Rows, elementsPerRow, elementBytes, name); err != nil {
		return err
	}
	if g.StoredBytes != g.RawBytes || len(g.Payload) != g.RawBytes {
		return fmt.Errorf("typedcolumn: %s stored bytes=%d payload=%d raw=%d", name, g.StoredBytes, len(g.Payload), g.RawBytes)
	}
	if g.PayloadRef.Kind != PayloadRefInline || g.PayloadRef.Offset != 0 || g.PayloadRef.Length != len(g.Payload) {
		return fmt.Errorf("typedcolumn: %s payload ref kind=%s offset=%d length=%d payload=%d", name, g.PayloadRef.Kind, g.PayloadRef.Offset, g.PayloadRef.Length, len(g.Payload))
	}
	return nil
}

func (p *ColumnPart) DenseFloat32VectorColumn(name string, dst []float32) (DenseFloat32Column, error) {
	column, ok := p.Columns[name]
	if !ok {
		return DenseFloat32Column{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	if column.Definition.Type != ColumnTypeFloat32Vector {
		return DenseFloat32Column{}, fmt.Errorf("typedcolumn: column %s type=%s is not float32_vector", name, column.Definition.Type)
	}
	out, err := denseFloat32ColumnInto(dst, p.Descriptor.RowCount, column)
	if err != nil {
		return DenseFloat32Column{}, err
	}
	return DenseFloat32Column{Rows: p.Descriptor.RowCount, ElementsPerRow: column.Definition.FixedWidthElements, Values: out}, nil
}

func (p *ColumnPart) DenseUint32Column(name string, dst []uint32) (DenseUint32Column, error) {
	column, ok := p.Columns[name]
	if !ok {
		return DenseUint32Column{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	if column.Definition.Type != ColumnTypeAdjacencyList {
		return DenseUint32Column{}, fmt.Errorf("typedcolumn: column %s type=%s is not adjacency_list", name, column.Definition.Type)
	}
	out, err := denseUint32ColumnInto(dst, p.Descriptor.RowCount, column)
	if err != nil {
		return DenseUint32Column{}, err
	}
	return DenseUint32Column{Rows: p.Descriptor.RowCount, ElementsPerRow: column.Definition.FixedWidthElements, Values: out}, nil
}

func (p *ColumnPart) Uint32ListColumn(name string, offsetsDst []uint64, valuesDst []uint32) (Uint32List, error) {
	column, ok := p.Columns[name]
	if !ok {
		return Uint32List{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	if column.Definition.Type != ColumnTypeUint32List || column.Definition.Encoding != EncodingRawUint32OffsetsList {
		return Uint32List{}, fmt.Errorf("typedcolumn: column %s type/encoding=(%s,%s) is not uint32_list/raw_uint32_offsets_list", name, column.Definition.Type, column.Definition.Encoding)
	}
	return uint32OffsetsListColumnInto(offsetsDst, valuesDst, p.Descriptor.RowCount, column)
}

// Uint32OffsetsListColumn is the physical-encoding compatibility reader. It
// accepts the generic uint32_list type and the legacy adjacency_list offsets-list
// selector while new callers should prefer Uint32ListColumn for generic data.
func (p *ColumnPart) Uint32OffsetsListColumn(name string, offsetsDst []uint64, valuesDst []uint32) (RawUint32OffsetsList, error) {
	column, ok := p.Columns[name]
	if !ok {
		return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	if column.Definition.Encoding != EncodingRawUint32OffsetsList || (column.Definition.Type != ColumnTypeUint32List && column.Definition.Type != ColumnTypeAdjacencyList) {
		return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: column %s type/encoding=(%s,%s) is not raw_uint32_offsets_list", name, column.Definition.Type, column.Definition.Encoding)
	}
	return uint32OffsetsListColumnInto(offsetsDst, valuesDst, p.Descriptor.RowCount, column)
}

func (p *ColumnPart) BytesColumn(name string, offsetsDst []uint64, valuesDst []byte) (BytesColumn, error) {
	column, ok := p.Columns[name]
	if !ok {
		return BytesColumn{}, fmt.Errorf("typedcolumn: missing column %s", name)
	}
	if column.Definition.Type != ColumnTypeBytes || column.Definition.Encoding != EncodingRawBytesOffsets {
		return BytesColumn{}, fmt.Errorf("typedcolumn: column %s type/encoding=(%s,%s) is not bytes/raw_bytes_offsets", name, column.Definition.Type, column.Definition.Encoding)
	}
	return bytesColumnInto(offsetsDst, valuesDst, p.Descriptor.RowCount, column)
}

func denseFloat32ColumnInto(dst []float32, rows int, column ColumnPartColumn) ([]float32, error) {
	elements, err := checkedMulInt(rows, column.Definition.FixedWidthElements, "float32_vector column elements")
	if err != nil {
		return nil, err
	}
	out := dst[:0]
	if cap(out) < elements {
		out = make([]float32, elements)
	} else {
		out = out[:elements]
	}
	var reader GranuleReader
	for _, block := range column.Blocks {
		values, err := reader.DecodeFloat32VectorInto(nil, block.Granule, column.Definition.FixedWidthElements)
		if err != nil {
			return nil, err
		}
		wantElements, err := checkedMulInt(block.Descriptor.RowCount, column.Definition.FixedWidthElements, "float32_vector block elements")
		if err != nil {
			return nil, err
		}
		if len(values) != wantElements {
			return nil, fmt.Errorf("typedcolumn: block rows=%d decoded elements=%d", block.Descriptor.RowCount, len(values))
		}
		start, err := checkedMulInt(block.Descriptor.FirstRow, column.Definition.FixedWidthElements, "float32_vector block offset")
		if err != nil {
			return nil, err
		}
		if start > len(out)-len(values) {
			return nil, fmt.Errorf("typedcolumn: float32_vector block first_row=%d elements=%d outside column elements=%d", block.Descriptor.FirstRow, len(values), len(out))
		}
		copy(out[start:start+len(values)], values)
	}
	return out, nil
}

func denseUint32ColumnInto(dst []uint32, rows int, column ColumnPartColumn) ([]uint32, error) {
	elements, err := checkedMulInt(rows, column.Definition.FixedWidthElements, "uint32 dense column elements")
	if err != nil {
		return nil, err
	}
	out := dst[:0]
	if cap(out) < elements {
		out = make([]uint32, elements)
	} else {
		out = out[:elements]
	}
	var reader GranuleReader
	for _, block := range column.Blocks {
		values, err := reader.DecodeUint32DenseInto(nil, block.Granule, column.Definition.FixedWidthElements)
		if err != nil {
			return nil, err
		}
		wantElements, err := checkedMulInt(block.Descriptor.RowCount, column.Definition.FixedWidthElements, "uint32 dense block elements")
		if err != nil {
			return nil, err
		}
		if len(values) != wantElements {
			return nil, fmt.Errorf("typedcolumn: block rows=%d decoded elements=%d", block.Descriptor.RowCount, len(values))
		}
		start, err := checkedMulInt(block.Descriptor.FirstRow, column.Definition.FixedWidthElements, "uint32 dense block offset")
		if err != nil {
			return nil, err
		}
		if start > len(out)-len(values) {
			return nil, fmt.Errorf("typedcolumn: uint32 dense block first_row=%d elements=%d outside column elements=%d", block.Descriptor.FirstRow, len(values), len(out))
		}
		copy(out[start:start+len(values)], values)
	}
	return out, nil
}

func uint32OffsetsListColumnInto(offsetsDst []uint64, valuesDst []uint32, rows int, column ColumnPartColumn) (RawUint32OffsetsList, error) {
	outOffsets := offsetsDst[:0]
	if cap(outOffsets) < rows+1 {
		outOffsets = make([]uint64, rows+1)
	} else {
		outOffsets = outOffsets[:rows+1]
	}
	for i := range outOffsets {
		outOffsets[i] = 0
	}
	outValues := valuesDst[:0]
	var reader GranuleReader
	var offsetsScratch []uint64
	var valuesScratch []uint32
	for _, block := range column.Blocks {
		decoded, err := reader.DecodeUint32OffsetsListInto(offsetsScratch[:0], valuesScratch[:0], block.Granule)
		if err != nil {
			return RawUint32OffsetsList{}, err
		}
		if decoded.Rows != block.Descriptor.RowCount || len(decoded.Offsets) != block.Descriptor.RowCount+1 {
			return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: offsets-list block rows=%d decoded rows=%d offsets=%d", block.Descriptor.RowCount, decoded.Rows, len(decoded.Offsets))
		}
		first := block.Descriptor.FirstRow
		if first < 0 || first > rows-decoded.Rows {
			return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: offsets-list block first_row=%d rows=%d outside column rows=%d", first, decoded.Rows, rows)
		}
		base := uint64(len(outValues))
		if base > maxHostIntUint64() {
			return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: offsets-list values exceed host int")
		}
		outValues = append(outValues, decoded.Values...)
		offsetsScratch = decoded.Offsets
		valuesScratch = decoded.Values
		for i := 0; i < decoded.Rows; i++ {
			outOffsets[first+i] = base + decoded.Offsets[i]
		}
		outOffsets[first+decoded.Rows] = uint64(len(outValues))
	}
	if err := ValidateRawUint32OffsetsListShape(rows, outOffsets, uint64(len(outValues))); err != nil {
		return RawUint32OffsetsList{}, err
	}
	return RawUint32OffsetsList{Rows: rows, Offsets: outOffsets, Values: outValues}, nil
}

func bytesColumnInto(offsetsDst []uint64, valuesDst []byte, rows int, column ColumnPartColumn) (RawBytesOffsets, error) {
	outOffsets := offsetsDst[:0]
	if cap(outOffsets) < rows+1 {
		outOffsets = make([]uint64, rows+1)
	} else {
		outOffsets = outOffsets[:rows+1]
	}
	for i := range outOffsets {
		outOffsets[i] = 0
	}
	outValues := valuesDst[:0]
	var reader GranuleReader
	var offsetsScratch []uint64
	var valuesScratch []byte
	for _, block := range column.Blocks {
		decoded, err := reader.DecodeBytesInto(offsetsScratch[:0], valuesScratch[:0], block.Granule)
		if err != nil {
			return RawBytesOffsets{}, err
		}
		if decoded.Rows != block.Descriptor.RowCount || len(decoded.Offsets) != block.Descriptor.RowCount+1 {
			return RawBytesOffsets{}, fmt.Errorf("typedcolumn: bytes block rows=%d decoded rows=%d offsets=%d", block.Descriptor.RowCount, decoded.Rows, len(decoded.Offsets))
		}
		first := block.Descriptor.FirstRow
		if first < 0 || first > rows-decoded.Rows {
			return RawBytesOffsets{}, fmt.Errorf("typedcolumn: bytes block first_row=%d rows=%d outside column rows=%d", first, decoded.Rows, rows)
		}
		base := uint64(len(outValues))
		if base > maxHostIntUint64() {
			return RawBytesOffsets{}, fmt.Errorf("typedcolumn: bytes values exceed host int")
		}
		outValues = append(outValues, decoded.Values...)
		offsetsScratch = decoded.Offsets
		valuesScratch = decoded.Values
		for i := 0; i < decoded.Rows; i++ {
			outOffsets[first+i] = base + decoded.Offsets[i]
		}
		outOffsets[first+decoded.Rows] = uint64(len(outValues))
	}
	if err := ValidateRawBytesOffsetsShape(rows, outOffsets, uint64(len(outValues))); err != nil {
		return RawBytesOffsets{}, err
	}
	return RawBytesOffsets{Rows: rows, Offsets: outOffsets, Values: outValues}, nil
}

func ensureFloat32Len(dst []float32, n int) []float32 {
	if cap(dst) < n {
		return make([]float32, n)
	}
	return dst[:n]
}
