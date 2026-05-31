package typedcolumn

import "fmt"

// BytesColumn is the generic owned representation for the v1 non-null opaque
// bytes primitive. It is consumer-neutral storage machinery: every row owns one
// exact byte string, and callers decide higher-level meaning above this layer.
// Offsets has Rows+1 entries and Values stores all row payload bytes
// concatenated; row i spans Values[Offsets[i]:Offsets[i+1]].
type BytesColumn struct {
	Rows    int
	Offsets []uint64
	Values  []byte
}

// RawBytesOffsets is the physical-encoding compatibility name for the
// raw_bytes_offsets layout.
type RawBytesOffsets = BytesColumn

// BytesColumnView is the same row-slicing contract used by direct views:
// Offsets and Values may alias a mmap/heap resource whose lifetime is owned
// elsewhere.
type BytesColumnView = BytesColumn

// Validate checks the complete offsets/value shape for this bytes column.
func (b BytesColumn) Validate() error {
	return ValidateRawBytesOffsetsShape(b.Rows, b.Offsets, uint64(len(b.Values)))
}

// Row returns the row slice Values[Offsets[row]:Offsets[row+1]] after validating
// row bounds and host-int conversions. The returned slice aliases Values.
func (b BytesColumn) Row(row int) ([]byte, error) {
	if row < 0 || row >= b.Rows {
		return nil, fmt.Errorf("typedcolumn: bytes row=%d outside rows=%d", row, b.Rows)
	}
	if len(b.Offsets) != b.Rows+1 {
		return nil, fmt.Errorf("typedcolumn: bytes offsets=%d want row_count+1=%d", len(b.Offsets), b.Rows+1)
	}
	begin := b.Offsets[row]
	end := b.Offsets[row+1]
	if begin > maxHostIntUint64() || end > maxHostIntUint64() || begin > end {
		return nil, fmt.Errorf("typedcolumn: bytes row %d invalid offsets [%d,%d)", row, begin, end)
	}
	beginInt := int(begin)
	endInt := int(end)
	if beginInt > len(b.Values) || endInt > len(b.Values) {
		return nil, fmt.Errorf("typedcolumn: bytes row %d values range [%d,%d) outside values=%d", row, begin, end, len(b.Values))
	}
	return b.Values[beginInt:endInt], nil
}

// EncodeRawBytesOffsetsOffsets writes offsets as little-endian uint64 values.
// The returned slice may alias dst.
func EncodeRawBytesOffsetsOffsets(dst []byte, offsets []uint64) ([]byte, error) {
	return encodeLittleEndian8Payload(dst, offsets, "raw bytes offsets")
}

// EncodeRawBytesOffsetsValues writes value bytes exactly as supplied. The
// returned slice may alias dst but never interprets bytes as text.
func EncodeRawBytesOffsetsValues(dst []byte, values []byte) ([]byte, error) {
	out := dst
	if cap(out) < len(values) {
		out = make([]byte, len(values))
	} else {
		out = out[:len(values)]
	}
	copy(out, values)
	return out, nil
}

// EncodeRawBytesOffsetsPayload writes the canonical fallback block payload:
// offsets bytes followed by value bytes. Image writers split this combined block
// payload into independent offsets and values sections.
func EncodeRawBytesOffsetsPayload(dst []byte, rows int, offsets []uint64, values []byte) ([]byte, error) {
	if err := ValidateRawBytesOffsetsShape(rows, offsets, uint64(len(values))); err != nil {
		return nil, err
	}
	offsetsBytes, err := checkedMulInt(rows+1, 8, "raw bytes offsets bytes")
	if err != nil {
		return nil, err
	}
	total, err := checkedAddInt(offsetsBytes, len(values), "raw bytes payload bytes")
	if err != nil {
		return nil, err
	}
	out := dst
	if cap(out) < total {
		out = make([]byte, total)
	} else {
		out = out[:total]
	}
	if _, err := EncodeRawBytesOffsetsOffsets(out[:0], offsets); err != nil {
		return nil, err
	}
	copy(out[offsetsBytes:], values)
	return out, nil
}

// DecodeRawBytesOffsetsFallback decodes the split offsets and values sections
// into owned Go slices. The returned slices never alias offsetsRaw or valuesRaw,
// but they may reuse caller-provided destination slices.
func DecodeRawBytesOffsetsFallback(offsetsDst []uint64, valuesDst []byte, offsetsRaw []byte, valuesRaw []byte, rows int) (RawBytesOffsets, error) {
	if err := validateRawBytesOffsetsBytes(offsetsRaw, valuesRaw, rows); err != nil {
		return RawBytesOffsets{}, err
	}
	offsetCount := rows + 1
	offsets := resizeFixedWidthValues(offsetsDst, offsetCount)
	for i := range offsets {
		offsets[i] = readLittleEndianUint64(offsetsRaw[i*8:])
	}
	values := valuesDst
	if cap(values) < len(valuesRaw) {
		values = make([]byte, len(valuesRaw))
	} else {
		values = values[:len(valuesRaw)]
	}
	copy(values, valuesRaw)
	return RawBytesOffsets{Rows: rows, Offsets: offsets, Values: values}, nil
}

// DecodeRawBytesOffsetsPayload decodes the combined block payload produced by
// EncodeRawBytesOffsetsPayload into owned Go slices.
func DecodeRawBytesOffsetsPayload(offsetsDst []uint64, valuesDst []byte, raw []byte, rows int) (RawBytesOffsets, error) {
	offsetsBytes, valuesBytes, err := RawBytesOffsetsBlockPayloadBytes(rows, len(raw))
	if err != nil {
		return RawBytesOffsets{}, err
	}
	return DecodeRawBytesOffsetsFallback(offsetsDst, valuesDst, raw[:offsetsBytes], raw[offsetsBytes:offsetsBytes+valuesBytes], rows)
}

// ValidateRawBytesOffsetsShape validates owned offset/value counts before
// encoding or row slicing. values is the number of payload bytes.
func ValidateRawBytesOffsetsShape(rows int, offsets []uint64, values uint64) error {
	if err := ValidateRawBytesOffsetsOffsets(rows, offsets); err != nil {
		return err
	}
	if values > maxHostIntUint64() {
		return fmt.Errorf("typedcolumn: bytes values=%d exceeds host int", values)
	}
	if final := offsets[rows]; final != values {
		return fmt.Errorf("typedcolumn: bytes final offset=%d values=%d", final, values)
	}
	return nil
}

// ValidateRawBytesOffsetsOffsets validates the length-only offsets substream
// shape without requiring the values substream. It is intended for row-length
// APIs and offset-only integrity checks; full value integrity still requires
// ValidateRawBytesOffsetsShape or section validation.
func ValidateRawBytesOffsetsOffsets(rows int, offsets []uint64) error {
	if rows < 0 {
		return fmt.Errorf("typedcolumn: negative bytes rows=%d", rows)
	}
	maxInt := int(^uint(0) >> 1)
	if rows == maxInt {
		return fmt.Errorf("typedcolumn: bytes row_count+1 overflows int")
	}
	wantOffsets := rows + 1
	if len(offsets) != wantOffsets {
		return fmt.Errorf("typedcolumn: bytes offsets=%d want row_count+1=%d", len(offsets), wantOffsets)
	}
	if len(offsets) == 0 {
		return fmt.Errorf("typedcolumn: bytes missing offsets")
	}
	if offsets[0] != 0 {
		return fmt.Errorf("typedcolumn: bytes offsets[0]=%d want 0", offsets[0])
	}
	maxIndex := uint64(maxInt)
	prev := uint64(0)
	for i, offset := range offsets {
		if offset > maxIndex {
			return fmt.Errorf("typedcolumn: bytes offsets[%d]=%d exceeds host int", i, offset)
		}
		if i > 0 && offset < prev {
			return fmt.Errorf("typedcolumn: bytes offsets[%d]=%d before previous=%d", i, offset, prev)
		}
		prev = offset
	}
	return nil
}

// DecodeRawBytesOffsetsOffsetsFallback decodes only the little-endian uint64
// offsets substream.
func DecodeRawBytesOffsetsOffsetsFallback(dst []uint64, offsetsRaw []byte, rows int) ([]uint64, error) {
	offsetCount, err := ValidateRawBytesOffsetsOffsetsBytes(offsetsRaw, rows)
	if err != nil {
		return nil, err
	}
	offsets := resizeFixedWidthValues(dst, offsetCount)
	for i := range offsets {
		offsets[i] = readLittleEndianUint64(offsetsRaw[i*8:])
	}
	if err := ValidateRawBytesOffsetsOffsets(rows, offsets); err != nil {
		return nil, err
	}
	return offsets, nil
}

// ValidateRawBytesOffsetsOffsetsBytes validates only the byte length of the
// offsets substream and returns its uint64 element count.
func ValidateRawBytesOffsetsOffsetsBytes(offsetsRaw []byte, rows int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("typedcolumn: negative bytes rows=%d", rows)
	}
	offsetCount, err := checkedAddInt(rows, 1, "raw bytes offset count")
	if err != nil {
		return 0, err
	}
	wantOffsetsBytes, err := checkedMulInt(offsetCount, 8, "raw bytes offsets bytes")
	if err != nil {
		return 0, err
	}
	if len(offsetsRaw) != wantOffsetsBytes {
		return 0, fmt.Errorf("typedcolumn: bytes offsets bytes=%d want=%d", len(offsetsRaw), wantOffsetsBytes)
	}
	return offsetCount, nil
}

// RawBytesOffsetsBlockPayloadBytes returns the split byte lengths for a
// combined block payload. rawBytes must be exactly offsets bytes plus arbitrary
// value bytes.
func RawBytesOffsetsBlockPayloadBytes(rows int, rawBytes int) (offsetsBytes int, valuesBytes int, err error) {
	if rows < 0 {
		return 0, 0, fmt.Errorf("typedcolumn: negative bytes rows=%d", rows)
	}
	offsetCount, err := checkedAddInt(rows, 1, "raw bytes offset count")
	if err != nil {
		return 0, 0, err
	}
	offsetsBytes, err = checkedMulInt(offsetCount, 8, "raw bytes offsets bytes")
	if err != nil {
		return 0, 0, err
	}
	if rawBytes < offsetsBytes {
		return 0, 0, fmt.Errorf("typedcolumn: bytes raw bytes=%d shorter than offsets bytes=%d", rawBytes, offsetsBytes)
	}
	valuesBytes = rawBytes - offsetsBytes
	return offsetsBytes, valuesBytes, nil
}

// ValidateRawBytesOffsetsSections validates split image section metadata and
// payload shape for raw_bytes_offsets without exposing unsafe direct views. It
// fails closed on any metadata or byte-shape mismatch.
func ValidateRawBytesOffsetsSections(offsetsSection ColumnPartImageSection, valuesSection ColumnPartImageSection, offsetsRaw []byte, valuesRaw []byte, rows int) error {
	if offsetsSection.Kind != ColumnPartImageSectionColumnOffsets {
		return fmt.Errorf("typedcolumn: bytes offsets section kind=%s want %s", offsetsSection.Kind, ColumnPartImageSectionColumnOffsets)
	}
	if valuesSection.Kind != ColumnPartImageSectionColumnValues {
		return fmt.Errorf("typedcolumn: bytes values section kind=%s want %s", valuesSection.Kind, ColumnPartImageSectionColumnValues)
	}
	if offsetsSection.Category != ColumnPartImageCategoryDeclaredColumnOffsets {
		return fmt.Errorf("typedcolumn: bytes offsets section category=%s want %s", offsetsSection.Category, ColumnPartImageCategoryDeclaredColumnOffsets)
	}
	if valuesSection.Category != ColumnPartImageCategoryDeclaredColumnValues {
		return fmt.Errorf("typedcolumn: bytes values section category=%s want %s", valuesSection.Category, ColumnPartImageCategoryDeclaredColumnValues)
	}
	if offsetsSection.Column == "" || valuesSection.Column == "" || offsetsSection.Column != valuesSection.Column {
		return fmt.Errorf("typedcolumn: bytes column mismatch offsets=%q values=%q", offsetsSection.Column, valuesSection.Column)
	}
	if offsetsSection.Rows != rows || valuesSection.Rows != rows {
		return fmt.Errorf("typedcolumn: bytes section rows offsets=%d values=%d want %d", offsetsSection.Rows, valuesSection.Rows, rows)
	}
	if offsetsSection.Encoding != EncodingRawBytesOffsets || valuesSection.Encoding != EncodingRawBytesOffsets {
		return fmt.Errorf("typedcolumn: bytes section encoding offsets=%s values=%s want %s", offsetsSection.Encoding, valuesSection.Encoding, EncodingRawBytesOffsets)
	}
	if offsetsSection.Compression != CompressionNone || valuesSection.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: bytes section compression offsets=%s values=%s want %s", offsetsSection.Compression, valuesSection.Compression, CompressionNone)
	}
	if offsetsSection.Length != len(offsetsRaw) || valuesSection.Length != len(valuesRaw) {
		return fmt.Errorf("typedcolumn: bytes section lengths offsets=%d/%d values=%d/%d", offsetsSection.Length, len(offsetsRaw), valuesSection.Length, len(valuesRaw))
	}
	return validateRawBytesOffsetsBytes(offsetsRaw, valuesRaw, rows)
}

// NewRawBytesOffsetsImageSections returns image-directory metadata for the split
// offsets and values sections. Offsets length must be (rows+1)*8. Values bytes
// may be any length, including zero.
func NewRawBytesOffsetsImageSections(column string, rows int, offsetsBytes int, valuesBytes int) (ColumnPartImageSection, ColumnPartImageSection, error) {
	if column == "" {
		return ColumnPartImageSection{}, ColumnPartImageSection{}, fmt.Errorf("typedcolumn: empty bytes column")
	}
	if err := validateRawBytesOffsetsSectionLengths(offsetsBytes, valuesBytes, rows); err != nil {
		return ColumnPartImageSection{}, ColumnPartImageSection{}, err
	}
	offsets := ColumnPartImageSection{
		Kind:        ColumnPartImageSectionColumnOffsets,
		Category:    ColumnPartImageCategoryDeclaredColumnOffsets,
		Name:        "offsets",
		Column:      column,
		Length:      offsetsBytes,
		Rows:        rows,
		Encoding:    EncodingRawBytesOffsets,
		Compression: CompressionNone,
	}
	values := ColumnPartImageSection{
		Kind:        ColumnPartImageSectionColumnValues,
		Category:    ColumnPartImageCategoryDeclaredColumnValues,
		Name:        "values",
		Column:      column,
		Length:      valuesBytes,
		Rows:        rows,
		Encoding:    EncodingRawBytesOffsets,
		Compression: CompressionNone,
	}
	return offsets, values, nil
}

func validateRawBytesOffsetsBytes(offsetsRaw []byte, valuesRaw []byte, rows int) error {
	if rows < 0 {
		return fmt.Errorf("typedcolumn: negative bytes rows=%d", rows)
	}
	if err := validateRawBytesOffsetsSectionLengths(len(offsetsRaw), len(valuesRaw), rows); err != nil {
		return err
	}
	if len(offsetsRaw) == 0 {
		return fmt.Errorf("typedcolumn: bytes missing offsets")
	}
	first := readLittleEndianUint64(offsetsRaw)
	if first != 0 {
		return fmt.Errorf("typedcolumn: bytes offsets[0]=%d want 0", first)
	}
	prev := first
	for i := 1; i <= rows; i++ {
		current := readLittleEndianUint64(offsetsRaw[i*8:])
		if current > maxHostIntUint64() {
			return fmt.Errorf("typedcolumn: bytes offsets[%d]=%d exceeds host int", i, current)
		}
		if current < prev {
			return fmt.Errorf("typedcolumn: bytes offsets[%d]=%d before previous=%d", i, current, prev)
		}
		prev = current
	}
	if prev > maxHostIntUint64() {
		return fmt.Errorf("typedcolumn: bytes final offset=%d exceeds host int", prev)
	}
	if int(prev) != len(valuesRaw) {
		return fmt.Errorf("typedcolumn: bytes final offset=%d values=%d", prev, len(valuesRaw))
	}
	return nil
}

func validateRawBytesOffsetsSectionLengths(offsetsBytes int, valuesBytes int, rows int) error {
	if rows < 0 {
		return fmt.Errorf("typedcolumn: negative bytes rows=%d", rows)
	}
	offsetCount, err := checkedAddInt(rows, 1, "raw bytes offset count")
	if err != nil {
		return err
	}
	wantOffsetsBytes, err := checkedMulInt(offsetCount, 8, "raw bytes offsets bytes")
	if err != nil {
		return err
	}
	if offsetsBytes != wantOffsetsBytes {
		return fmt.Errorf("typedcolumn: bytes offsets bytes=%d want=%d", offsetsBytes, wantOffsetsBytes)
	}
	if valuesBytes < 0 {
		return fmt.Errorf("typedcolumn: bytes values bytes=%d", valuesBytes)
	}
	return nil
}

// BuildBytes builds an uncompressed raw_bytes_offsets granule. The granule
// payload is the canonical combined block payload: offsets bytes followed by
// values bytes. Image serialization splits that payload into independently
// checksummed offsets and values sections.
func (b *GranuleBuilder) BuildBytes(rows int, offsets []uint64, values []byte) (EncodedGranule, error) {
	if b.cfg.Encoding != 0 && b.cfg.Encoding != EncodingRawBytesOffsets {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: bytes encoding=%s want %s", b.cfg.Encoding, EncodingRawBytesOffsets)
	}
	if b.cfg.Compression != CompressionNone {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: bytes sections require compression=none, got %s", b.cfg.Compression)
	}
	if rows <= 0 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: invalid bytes rows %d", rows)
	}
	raw, err := EncodeRawBytesOffsetsPayload(b.raw[:0], rows, offsets, values)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawBytesOffsets, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, EncodingRawBytesOffsets, selection), nil
}

// DecodeBytesInto decodes a raw_bytes_offsets granule into owned offsets and
// value byte slices. It does not expose direct/mmap views.
func (r *GranuleReader) DecodeBytesInto(offsetsDst []uint64, valuesDst []byte, g EncodedGranule) (RawBytesOffsets, error) {
	if g.Encoding != EncodingRawBytesOffsets {
		return RawBytesOffsets{}, fmt.Errorf("typedcolumn: bytes encoding=%s want %s", g.Encoding, EncodingRawBytesOffsets)
	}
	if err := validateBytesGranule(g); err != nil {
		return RawBytesOffsets{}, err
	}
	return DecodeRawBytesOffsetsPayload(offsetsDst, valuesDst, g.Payload, g.Rows)
}

func validateBytesGranule(g EncodedGranule) error {
	if g.Rows <= 0 {
		return fmt.Errorf("typedcolumn: invalid bytes rows %d", g.Rows)
	}
	if err := validateGranuleDecodeRows(g.Rows); err != nil {
		return err
	}
	if g.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: bytes section requires compression=none, got %s", g.Compression)
	}
	if g.NullCount != 0 || g.DefaultCount != 0 {
		return fmt.Errorf("typedcolumn: bytes section has null/default counts")
	}
	if g.HasMinMax {
		return fmt.Errorf("typedcolumn: bytes section unexpectedly has min/max")
	}
	if g.StoredBytes != g.RawBytes || len(g.Payload) != g.RawBytes {
		return fmt.Errorf("typedcolumn: bytes stored bytes=%d payload=%d raw=%d", g.StoredBytes, len(g.Payload), g.RawBytes)
	}
	if g.PayloadRef.Kind != PayloadRefInline || g.PayloadRef.Offset != 0 || g.PayloadRef.Length != len(g.Payload) {
		return fmt.Errorf("typedcolumn: bytes payload ref kind=%s offset=%d length=%d payload=%d", g.PayloadRef.Kind, g.PayloadRef.Offset, g.PayloadRef.Length, len(g.Payload))
	}
	_, _, err := RawBytesOffsetsBlockPayloadBytes(g.Rows, g.RawBytes)
	return err
}
