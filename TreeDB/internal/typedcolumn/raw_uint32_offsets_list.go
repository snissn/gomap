package typedcolumn

import "fmt"

// Uint32List is the generic owned representation for the v1 non-null
// variable-length uint32_list primitive. It is consumer-neutral storage
// machinery: HNSW adjacency may consume it, but graph-specific semantics belong
// above this layer. Offsets has Rows+1 entries and Values stores all row values
// concatenated; row i spans Values[Offsets[i]:Offsets[i+1]].
type Uint32List struct {
	Rows    int
	Offsets []uint64
	Values  []uint32
}

// RawUint32OffsetsList is retained as the physical-encoding compatibility name
// for existing raw_uint32_offsets_list code. New logical APIs should prefer
// Uint32List.
type RawUint32OffsetsList = Uint32List

// Uint32ListView is the same row-slicing contract used by direct views: Offsets
// and Values may alias a mmap/heap resource whose lifetime is owned elsewhere.
type Uint32ListView = Uint32List

// Validate checks the complete offsets/value shape for this uint32_list value.
func (l Uint32List) Validate() error {
	return ValidateRawUint32OffsetsListShape(l.Rows, l.Offsets, uint64(len(l.Values)))
}

// Row returns the row slice Values[Offsets[row]:Offsets[row+1]] after validating
// row bounds and host-int conversions. The returned slice aliases Values.
func (l Uint32List) Row(row int) ([]uint32, error) {
	if row < 0 || row >= l.Rows {
		return nil, fmt.Errorf("typedcolumn: uint32_list row=%d outside rows=%d", row, l.Rows)
	}
	if len(l.Offsets) != l.Rows+1 {
		return nil, fmt.Errorf("typedcolumn: uint32_list offsets=%d want row_count+1=%d", len(l.Offsets), l.Rows+1)
	}
	begin := l.Offsets[row]
	end := l.Offsets[row+1]
	if begin > maxHostIntUint64() || end > maxHostIntUint64() || begin > end {
		return nil, fmt.Errorf("typedcolumn: uint32_list row %d invalid offsets [%d,%d)", row, begin, end)
	}
	beginInt := int(begin)
	endInt := int(end)
	if beginInt > len(l.Values) || endInt > len(l.Values) {
		return nil, fmt.Errorf("typedcolumn: uint32_list row %d values range [%d,%d) outside values=%d", row, begin, end, len(l.Values))
	}
	return l.Values[beginInt:endInt], nil
}

// EncodeRawUint32OffsetsListOffsets writes offsets as little-endian uint64
// values. The returned slice may alias dst.
func EncodeRawUint32OffsetsListOffsets(dst []byte, offsets []uint64) ([]byte, error) {
	return encodeLittleEndian8Payload(dst, offsets, "raw uint32 offsets-list offsets")
}

// EncodeRawUint32OffsetsListValues writes values as little-endian uint32
// values. The returned slice may alias dst.
func EncodeRawUint32OffsetsListValues(dst []byte, values []uint32) ([]byte, error) {
	return encodeLittleEndian4Payload(dst, values, "raw uint32 offsets-list values")
}

// EncodeRawUint32OffsetsListPayload writes the canonical fallback block payload:
// offsets bytes followed by values bytes. Image writers split this combined
// block payload into independent offsets and values sections.
func EncodeRawUint32OffsetsListPayload(dst []byte, rows int, offsets []uint64, values []uint32) ([]byte, error) {
	if err := ValidateRawUint32OffsetsListShape(rows, offsets, uint64(len(values))); err != nil {
		return nil, err
	}
	offsetsBytes, err := checkedMulInt(rows+1, 8, "raw uint32 offsets-list offsets bytes")
	if err != nil {
		return nil, err
	}
	valuesBytes, err := checkedMulInt(len(values), 4, "raw uint32 offsets-list values bytes")
	if err != nil {
		return nil, err
	}
	total, err := checkedAddInt(offsetsBytes, valuesBytes, "raw uint32 offsets-list payload bytes")
	if err != nil {
		return nil, err
	}
	out := dst
	if cap(out) < total {
		out = make([]byte, total)
	} else {
		out = out[:total]
	}
	if _, err := EncodeRawUint32OffsetsListOffsets(out[:0], offsets); err != nil {
		return nil, err
	}
	if _, err := EncodeRawUint32OffsetsListValues(out[offsetsBytes:offsetsBytes], values); err != nil {
		return nil, err
	}
	return out, nil
}

// DecodeRawUint32OffsetsListFallback decodes the split offsets and values
// sections into owned Go slices. The returned slices never alias offsetsRaw or
// valuesRaw, but they may reuse caller-provided destination slices.
func DecodeRawUint32OffsetsListFallback(offsetsDst []uint64, valuesDst []uint32, offsetsRaw []byte, valuesRaw []byte, rows int) (RawUint32OffsetsList, error) {
	valuesCount, err := validateRawUint32OffsetsListBytes(offsetsRaw, valuesRaw, rows)
	if err != nil {
		return RawUint32OffsetsList{}, err
	}
	offsetCount := rows + 1
	offsets := resizeFixedWidthValues(offsetsDst, offsetCount)
	for i := range offsets {
		offsets[i] = readLittleEndianUint64(offsetsRaw[i*8:])
	}
	values := resizeFixedWidthValues(valuesDst, valuesCount)
	for i := range values {
		values[i] = readLittleEndianUint32(valuesRaw[i*4:])
	}
	return RawUint32OffsetsList{Rows: rows, Offsets: offsets, Values: values}, nil
}

// DecodeRawUint32OffsetsListPayload decodes the combined block payload produced
// by EncodeRawUint32OffsetsListPayload into owned Go slices.
func DecodeRawUint32OffsetsListPayload(offsetsDst []uint64, valuesDst []uint32, raw []byte, rows int) (RawUint32OffsetsList, error) {
	offsetsBytes, valuesBytes, err := RawUint32OffsetsListBlockPayloadBytes(rows, len(raw))
	if err != nil {
		return RawUint32OffsetsList{}, err
	}
	return DecodeRawUint32OffsetsListFallback(offsetsDst, valuesDst, raw[:offsetsBytes], raw[offsetsBytes:offsetsBytes+valuesBytes], rows)
}

// ValidateRawUint32OffsetsListShape validates owned offset/value counts before
// encoding or row slicing. values is the number of uint32 values, not bytes.
func ValidateRawUint32OffsetsListShape(rows int, offsets []uint64, values uint64) error {
	if err := ValidateRawUint32OffsetsListOffsets(rows, offsets); err != nil {
		return err
	}
	if values > maxHostIntUint64() {
		return fmt.Errorf("typedcolumn: offsets-list values=%d exceeds host int", values)
	}
	if final := offsets[rows]; final != values {
		return fmt.Errorf("typedcolumn: offsets-list final offset=%d values=%d", final, values)
	}
	return nil
}

// ValidateRawUint32OffsetsListOffsets validates the length-only offsets
// substream shape without requiring the values substream. It is intended for
// row-length APIs and offset-only integrity checks; full value integrity still
// requires ValidateRawUint32OffsetsListShape or section validation.
func ValidateRawUint32OffsetsListOffsets(rows int, offsets []uint64) error {
	if rows < 0 {
		return fmt.Errorf("typedcolumn: negative offsets-list rows=%d", rows)
	}
	maxInt := int(^uint(0) >> 1)
	if rows == maxInt {
		return fmt.Errorf("typedcolumn: offsets-list row_count+1 overflows int")
	}
	wantOffsets := rows + 1
	if len(offsets) != wantOffsets {
		return fmt.Errorf("typedcolumn: offsets-list offsets=%d want row_count+1=%d", len(offsets), wantOffsets)
	}
	if len(offsets) == 0 {
		return fmt.Errorf("typedcolumn: offsets-list missing offsets")
	}
	if offsets[0] != 0 {
		return fmt.Errorf("typedcolumn: offsets-list offsets[0]=%d want 0", offsets[0])
	}
	maxIndex := uint64(maxInt)
	prev := uint64(0)
	for i, offset := range offsets {
		if offset > maxIndex {
			return fmt.Errorf("typedcolumn: offsets-list offsets[%d]=%d exceeds host int", i, offset)
		}
		if i > 0 && offset < prev {
			return fmt.Errorf("typedcolumn: offsets-list offsets[%d]=%d before previous=%d", i, offset, prev)
		}
		prev = offset
	}
	return nil
}

// DecodeRawUint32OffsetsListOffsetsFallback decodes only the little-endian
// uint64 offsets substream. It validates rows+1 shape, offsets[0]==0,
// monotonicity, and host-int bounds, but intentionally does not verify the final
// offset against values bytes.
func DecodeRawUint32OffsetsListOffsetsFallback(dst []uint64, offsetsRaw []byte, rows int) ([]uint64, error) {
	offsetCount, err := ValidateRawUint32OffsetsListOffsetsBytes(offsetsRaw, rows)
	if err != nil {
		return nil, err
	}
	offsets := resizeFixedWidthValues(dst, offsetCount)
	for i := range offsets {
		offsets[i] = readLittleEndianUint64(offsetsRaw[i*8:])
	}
	if err := ValidateRawUint32OffsetsListOffsets(rows, offsets); err != nil {
		return nil, err
	}
	return offsets, nil
}

// ValidateRawUint32OffsetsListOffsetsBytes validates only the byte length of the
// offsets substream and returns its uint64 element count.
func ValidateRawUint32OffsetsListOffsetsBytes(offsetsRaw []byte, rows int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("typedcolumn: negative offsets-list rows=%d", rows)
	}
	offsetCount, err := checkedAddInt(rows, 1, "raw uint32 offsets-list offset count")
	if err != nil {
		return 0, err
	}
	wantOffsetsBytes, err := checkedMulInt(offsetCount, 8, "raw uint32 offsets-list offsets bytes")
	if err != nil {
		return 0, err
	}
	if len(offsetsRaw) != wantOffsetsBytes {
		return 0, fmt.Errorf("typedcolumn: offsets-list offsets bytes=%d want=%d", len(offsetsRaw), wantOffsetsBytes)
	}
	return offsetCount, nil
}

// RawUint32OffsetsListBlockPayloadBytes returns the split byte lengths for a
// combined block payload. rawBytes must be exactly offsets bytes plus a uint32
// values byte multiple.
func RawUint32OffsetsListBlockPayloadBytes(rows int, rawBytes int) (offsetsBytes int, valuesBytes int, err error) {
	if rows < 0 {
		return 0, 0, fmt.Errorf("typedcolumn: negative offsets-list rows=%d", rows)
	}
	offsetCount, err := checkedAddInt(rows, 1, "raw uint32 offsets-list offset count")
	if err != nil {
		return 0, 0, err
	}
	offsetsBytes, err = checkedMulInt(offsetCount, 8, "raw uint32 offsets-list offsets bytes")
	if err != nil {
		return 0, 0, err
	}
	if rawBytes < offsetsBytes {
		return 0, 0, fmt.Errorf("typedcolumn: offsets-list raw bytes=%d shorter than offsets bytes=%d", rawBytes, offsetsBytes)
	}
	valuesBytes = rawBytes - offsetsBytes
	if valuesBytes%4 != 0 {
		return 0, 0, fmt.Errorf("typedcolumn: offsets-list values bytes=%d not multiple of 4", valuesBytes)
	}
	return offsetsBytes, valuesBytes, nil
}

// ValidateRawUint32OffsetsListSections validates split image section metadata
// and payload shape for raw_uint32_offsets_list without exposing unsafe direct
// views. It fails closed on any metadata or byte-shape mismatch.
func ValidateRawUint32OffsetsListSections(offsetsSection ColumnPartImageSection, valuesSection ColumnPartImageSection, offsetsRaw []byte, valuesRaw []byte, rows int) error {
	if offsetsSection.Kind != ColumnPartImageSectionColumnOffsets {
		return fmt.Errorf("typedcolumn: offsets-list offsets section kind=%s want %s", offsetsSection.Kind, ColumnPartImageSectionColumnOffsets)
	}
	if valuesSection.Kind != ColumnPartImageSectionColumnValues {
		return fmt.Errorf("typedcolumn: offsets-list values section kind=%s want %s", valuesSection.Kind, ColumnPartImageSectionColumnValues)
	}
	if offsetsSection.Category != ColumnPartImageCategoryDeclaredColumnOffsets {
		return fmt.Errorf("typedcolumn: offsets-list offsets section category=%s want %s", offsetsSection.Category, ColumnPartImageCategoryDeclaredColumnOffsets)
	}
	if valuesSection.Category != ColumnPartImageCategoryDeclaredColumnValues {
		return fmt.Errorf("typedcolumn: offsets-list values section category=%s want %s", valuesSection.Category, ColumnPartImageCategoryDeclaredColumnValues)
	}
	if offsetsSection.Column == "" || valuesSection.Column == "" || offsetsSection.Column != valuesSection.Column {
		return fmt.Errorf("typedcolumn: offsets-list column mismatch offsets=%q values=%q", offsetsSection.Column, valuesSection.Column)
	}
	if offsetsSection.Rows != rows || valuesSection.Rows != rows {
		return fmt.Errorf("typedcolumn: offsets-list section rows offsets=%d values=%d want %d", offsetsSection.Rows, valuesSection.Rows, rows)
	}
	if offsetsSection.Encoding != EncodingRawUint32OffsetsList || valuesSection.Encoding != EncodingRawUint32OffsetsList {
		return fmt.Errorf("typedcolumn: offsets-list section encoding offsets=%s values=%s want %s", offsetsSection.Encoding, valuesSection.Encoding, EncodingRawUint32OffsetsList)
	}
	if offsetsSection.Compression != CompressionNone || valuesSection.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: offsets-list section compression offsets=%s values=%s want %s", offsetsSection.Compression, valuesSection.Compression, CompressionNone)
	}
	if offsetsSection.Length != len(offsetsRaw) || valuesSection.Length != len(valuesRaw) {
		return fmt.Errorf("typedcolumn: offsets-list section lengths offsets=%d/%d values=%d/%d", offsetsSection.Length, len(offsetsRaw), valuesSection.Length, len(valuesRaw))
	}
	_, err := validateRawUint32OffsetsListBytes(offsetsRaw, valuesRaw, rows)
	return err
}

// NewRawUint32OffsetsListImageSections returns image-directory metadata for the
// split offsets and values sections. Offsets and values byte lengths are checked
// for exact element sizing; offsets length must be (rows+1)*8.
func NewRawUint32OffsetsListImageSections(column string, rows int, offsetsBytes int, valuesBytes int) (ColumnPartImageSection, ColumnPartImageSection, error) {
	if column == "" {
		return ColumnPartImageSection{}, ColumnPartImageSection{}, fmt.Errorf("typedcolumn: empty offsets-list column")
	}
	if err := validateRawUint32OffsetsListSectionLengths(offsetsBytes, valuesBytes, rows); err != nil {
		return ColumnPartImageSection{}, ColumnPartImageSection{}, err
	}
	offsets := ColumnPartImageSection{
		Kind:        ColumnPartImageSectionColumnOffsets,
		Category:    ColumnPartImageCategoryDeclaredColumnOffsets,
		Name:        "offsets",
		Column:      column,
		Length:      offsetsBytes,
		Rows:        rows,
		Encoding:    EncodingRawUint32OffsetsList,
		Compression: CompressionNone,
	}
	values := ColumnPartImageSection{
		Kind:        ColumnPartImageSectionColumnValues,
		Category:    ColumnPartImageCategoryDeclaredColumnValues,
		Name:        "values",
		Column:      column,
		Length:      valuesBytes,
		Rows:        rows,
		Encoding:    EncodingRawUint32OffsetsList,
		Compression: CompressionNone,
	}
	return offsets, values, nil
}

func validateRawUint32OffsetsListBytes(offsetsRaw []byte, valuesRaw []byte, rows int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("typedcolumn: negative offsets-list rows=%d", rows)
	}
	if err := validateRawUint32OffsetsListSectionLengths(len(offsetsRaw), len(valuesRaw), rows); err != nil {
		return 0, err
	}
	if len(offsetsRaw) == 0 {
		return 0, fmt.Errorf("typedcolumn: offsets-list missing offsets")
	}
	first := readLittleEndianUint64(offsetsRaw)
	if first != 0 {
		return 0, fmt.Errorf("typedcolumn: offsets-list offsets[0]=%d want 0", first)
	}
	prev := first
	for i := 1; i <= rows; i++ {
		current := readLittleEndianUint64(offsetsRaw[i*8:])
		if current > maxHostIntUint64() {
			return 0, fmt.Errorf("typedcolumn: offsets-list offsets[%d]=%d exceeds host int", i, current)
		}
		if current < prev {
			return 0, fmt.Errorf("typedcolumn: offsets-list offsets[%d]=%d before previous=%d", i, current, prev)
		}
		prev = current
	}
	if prev > maxHostIntUint64() {
		return 0, fmt.Errorf("typedcolumn: offsets-list final offset=%d exceeds host int", prev)
	}
	valueCount := len(valuesRaw) / 4
	if int(prev) != valueCount {
		return 0, fmt.Errorf("typedcolumn: offsets-list final offset=%d values=%d", prev, valueCount)
	}
	return valueCount, nil
}

func validateRawUint32OffsetsListSectionLengths(offsetsBytes int, valuesBytes int, rows int) error {
	if rows < 0 {
		return fmt.Errorf("typedcolumn: negative offsets-list rows=%d", rows)
	}
	offsetCount, err := checkedAddInt(rows, 1, "raw uint32 offsets-list offset count")
	if err != nil {
		return err
	}
	wantOffsetsBytes, err := checkedMulInt(offsetCount, 8, "raw uint32 offsets-list offsets bytes")
	if err != nil {
		return err
	}
	if offsetsBytes != wantOffsetsBytes {
		return fmt.Errorf("typedcolumn: offsets-list offsets bytes=%d want=%d", offsetsBytes, wantOffsetsBytes)
	}
	if valuesBytes < 0 {
		return fmt.Errorf("typedcolumn: offsets-list values bytes=%d", valuesBytes)
	}
	if valuesBytes%4 != 0 {
		return fmt.Errorf("typedcolumn: offsets-list values bytes=%d not multiple of 4", valuesBytes)
	}
	return nil
}

func maxHostIntUint64() uint64 {
	return uint64(^uint(0) >> 1)
}

// BuildUint32OffsetsList builds an uncompressed raw_uint32_offsets_list granule.
// The granule payload is the canonical combined block payload: offsets bytes
// followed by values bytes. Image serialization splits that payload into
// independently checksummed offsets and values sections.
func (b *GranuleBuilder) BuildUint32OffsetsList(rows int, offsets []uint64, values []uint32) (EncodedGranule, error) {
	if b.cfg.Encoding != 0 && b.cfg.Encoding != EncodingRawUint32OffsetsList {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: uint32 offsets-list encoding=%s want %s", b.cfg.Encoding, EncodingRawUint32OffsetsList)
	}
	if b.cfg.Compression != CompressionNone {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: uint32 offsets-list sections require compression=none, got %s", b.cfg.Compression)
	}
	if rows <= 0 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: invalid uint32 offsets-list rows %d", rows)
	}
	raw, err := EncodeRawUint32OffsetsListPayload(b.raw[:0], rows, offsets, values)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawUint32OffsetsList, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, EncodingRawUint32OffsetsList, selection), nil
}

// DecodeUint32OffsetsListInto decodes a raw_uint32_offsets_list granule into
// owned offsets and values slices. It does not expose direct/mmap views.
func (r *GranuleReader) DecodeUint32OffsetsListInto(offsetsDst []uint64, valuesDst []uint32, g EncodedGranule) (RawUint32OffsetsList, error) {
	if g.Encoding != EncodingRawUint32OffsetsList {
		return RawUint32OffsetsList{}, fmt.Errorf("typedcolumn: uint32 offsets-list encoding=%s want %s", g.Encoding, EncodingRawUint32OffsetsList)
	}
	if err := validateUint32OffsetsListGranule(g); err != nil {
		return RawUint32OffsetsList{}, err
	}
	return DecodeRawUint32OffsetsListPayload(offsetsDst, valuesDst, g.Payload, g.Rows)
}

func validateUint32OffsetsListGranule(g EncodedGranule) error {
	if g.Rows <= 0 {
		return fmt.Errorf("typedcolumn: invalid uint32 offsets-list rows %d", g.Rows)
	}
	if err := validateGranuleDecodeRows(g.Rows); err != nil {
		return err
	}
	if g.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: uint32 offsets-list section requires compression=none, got %s", g.Compression)
	}
	if g.NullCount != 0 || g.DefaultCount != 0 {
		return fmt.Errorf("typedcolumn: uint32 offsets-list section has null/default counts")
	}
	if g.HasMinMax {
		return fmt.Errorf("typedcolumn: uint32 offsets-list section unexpectedly has min/max")
	}
	if g.StoredBytes != g.RawBytes || len(g.Payload) != g.RawBytes {
		return fmt.Errorf("typedcolumn: uint32 offsets-list stored bytes=%d payload=%d raw=%d", g.StoredBytes, len(g.Payload), g.RawBytes)
	}
	if g.PayloadRef.Kind != PayloadRefInline || g.PayloadRef.Offset != 0 || g.PayloadRef.Length != len(g.Payload) {
		return fmt.Errorf("typedcolumn: uint32 offsets-list payload ref kind=%s offset=%d length=%d payload=%d", g.PayloadRef.Kind, g.PayloadRef.Offset, g.PayloadRef.Length, len(g.Payload))
	}
	_, _, err := RawUint32OffsetsListBlockPayloadBytes(g.Rows, g.RawBytes)
	return err
}
