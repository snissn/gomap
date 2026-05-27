package typedcolumn

import "fmt"

// RawUint32OffsetsList is the owned fallback representation for the v1
// variable-length uint32 list primitive. Offsets has Rows+1 entries and Values
// stores all row values concatenated; row i spans Values[Offsets[i]:Offsets[i+1]].
type RawUint32OffsetsList struct {
	Rows    int
	Offsets []uint64
	Values  []uint32
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

// DecodeRawUint32OffsetsListFallback decodes the split offsets and values
// sections into owned Go slices. The returned slices never alias offsetsRaw or
// valuesRaw, but they may reuse caller-provided destination slices.
func DecodeRawUint32OffsetsListFallback(offsetsDst []uint64, valuesDst []uint32, offsetsRaw []byte, valuesRaw []byte, rows int) (RawUint32OffsetsList, error) {
	if err := validateRawUint32OffsetsListBytes(offsetsRaw, valuesRaw, rows); err != nil {
		return RawUint32OffsetsList{}, err
	}
	offsetCount := rows + 1
	offsets := resizeFixedWidthValues(offsetsDst, offsetCount)
	for i := range offsets {
		offsets[i] = readLittleEndianUint64(offsetsRaw[i*8:])
	}
	valuesCount, err := validateRawUint32OffsetsListOffsets(offsets, valuesRaw)
	if err != nil {
		return RawUint32OffsetsList{}, err
	}
	values := resizeFixedWidthValues(valuesDst, valuesCount)
	for i := range values {
		values[i] = readLittleEndianUint32(valuesRaw[i*4:])
	}
	return RawUint32OffsetsList{Rows: rows, Offsets: offsets, Values: values}, nil
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
	return validateRawUint32OffsetsListBytes(offsetsRaw, valuesRaw, rows)
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
		Column:      column,
		Length:      offsetsBytes,
		Rows:        rows,
		Encoding:    EncodingRawUint32OffsetsList,
		Compression: CompressionNone,
	}
	values := ColumnPartImageSection{
		Kind:        ColumnPartImageSectionColumnValues,
		Category:    ColumnPartImageCategoryDeclaredColumnValues,
		Column:      column,
		Length:      valuesBytes,
		Rows:        rows,
		Encoding:    EncodingRawUint32OffsetsList,
		Compression: CompressionNone,
	}
	return offsets, values, nil
}

func validateRawUint32OffsetsListBytes(offsetsRaw []byte, valuesRaw []byte, rows int) error {
	if rows < 0 {
		return fmt.Errorf("typedcolumn: negative offsets-list rows=%d", rows)
	}
	if err := validateRawUint32OffsetsListSectionLengths(len(offsetsRaw), len(valuesRaw), rows); err != nil {
		return err
	}
	if len(offsetsRaw) == 0 {
		return fmt.Errorf("typedcolumn: offsets-list missing offsets")
	}
	first := readLittleEndianUint64(offsetsRaw)
	if first != 0 {
		return fmt.Errorf("typedcolumn: offsets-list offsets[0]=%d want 0", first)
	}
	prev := first
	for i := 1; i <= rows; i++ {
		current := readLittleEndianUint64(offsetsRaw[i*8:])
		if current < prev {
			return fmt.Errorf("typedcolumn: offsets-list offsets[%d]=%d before previous=%d", i, current, prev)
		}
		prev = current
	}
	if prev > maxHostIntUint64() {
		return fmt.Errorf("typedcolumn: offsets-list final offset=%d exceeds host int", prev)
	}
	valueCount := len(valuesRaw) / 4
	if int(prev) != valueCount {
		return fmt.Errorf("typedcolumn: offsets-list final offset=%d values=%d", prev, valueCount)
	}
	return nil
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

func validateRawUint32OffsetsListOffsets(offsets []uint64, valuesRaw []byte) (int, error) {
	if len(offsets) == 0 {
		return 0, fmt.Errorf("typedcolumn: offsets-list missing offsets")
	}
	if offsets[0] != 0 {
		return 0, fmt.Errorf("typedcolumn: offsets-list offsets[0]=%d want 0", offsets[0])
	}
	prev := offsets[0]
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < prev {
			return 0, fmt.Errorf("typedcolumn: offsets-list offsets[%d]=%d before previous=%d", i, offsets[i], prev)
		}
		prev = offsets[i]
	}
	if prev > maxHostIntUint64() {
		return 0, fmt.Errorf("typedcolumn: offsets-list final offset=%d exceeds host int", prev)
	}
	valuesCount := len(valuesRaw) / 4
	if int(prev) != valuesCount {
		return 0, fmt.Errorf("typedcolumn: offsets-list final offset=%d values=%d", prev, valuesCount)
	}
	return valuesCount, nil
}
