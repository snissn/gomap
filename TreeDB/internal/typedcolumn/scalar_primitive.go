package typedcolumn

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func rawScalarEncodingForColumnType(t ColumnType) Encoding {
	switch t {
	case ColumnTypeInt8:
		return EncodingRawInt8
	case ColumnTypeUint8:
		return EncodingRawUint8
	case ColumnTypeInt16:
		return EncodingRawInt16
	case ColumnTypeUint16:
		return EncodingRawUint16
	case ColumnTypeInt32:
		return EncodingRawInt32
	case ColumnTypeUint32:
		return EncodingRawUint32
	case ColumnTypeUint64:
		return EncodingRawUint64
	case ColumnTypeFloat16:
		return EncodingRawFloat16
	case ColumnTypeBFloat16:
		return EncodingRawBFloat16
	default:
		return 0
	}
}

func rawScalarWidthForColumnType(t ColumnType) int {
	switch t {
	case ColumnTypeInt8, ColumnTypeUint8:
		return 1
	case ColumnTypeInt16, ColumnTypeUint16, ColumnTypeFloat16, ColumnTypeBFloat16:
		return 2
	case ColumnTypeInt32, ColumnTypeUint32:
		return 4
	case ColumnTypeUint64:
		return 8
	default:
		return 0
	}
}

func integerStatsPayloadColumnType(t ColumnType) bool {
	switch t {
	case ColumnTypeInt64, ColumnTypeInt8, ColumnTypeUint8, ColumnTypeInt16, ColumnTypeUint16, ColumnTypeInt32, ColumnTypeUint32:
		return true
	default:
		return false
	}
}

func (b *GranuleBuilder) BuildInt8(values []int8) (EncodedGranule, error) {
	return b.buildRawInt8(values, EncodingRawInt8, "int8")
}

func (b *GranuleBuilder) BuildUint8(values []uint8) (EncodedGranule, error) {
	return b.buildRawUint8(values, EncodingRawUint8, "uint8")
}

func (b *GranuleBuilder) BuildInt16(values []int16) (EncodedGranule, error) {
	return b.buildRawInt16(values, EncodingRawInt16, "int16")
}

func (b *GranuleBuilder) BuildUint16(values []uint16) (EncodedGranule, error) {
	return b.buildRawUint16(values, EncodingRawUint16, "uint16", true)
}

func (b *GranuleBuilder) BuildInt32(values []int32) (EncodedGranule, error) {
	return b.buildRawInt32(values, EncodingRawInt32, "int32")
}

func (b *GranuleBuilder) BuildUint32(values []uint32) (EncodedGranule, error) {
	return b.buildRawUint32(values, EncodingRawUint32, "uint32", true)
}

func (b *GranuleBuilder) BuildUint64(values []uint64) (EncodedGranule, error) {
	if err := validateRawScalarBuildRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateRawScalarBuildConfig(b.cfg, EncodingRawUint64, "uint64"); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeLittleEndian8Payload(b.raw[:0], values, "uint64")
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawUint64, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	// The descriptor min/max fields are signed int64, so full-range uint64 cannot
	// advertise ordered min/max without losing ordering for values above MaxInt64.
	return newEncodedGranule(len(values), 0, 0, false, EncodingRawUint64, selection), nil
}

// BuildFloat16Bits stores IEEE binary16 raw uint16 bits without arithmetic
// interpretation. NaN payloads, infinities, and signed zero bits are preserved.
func (b *GranuleBuilder) BuildFloat16Bits(values []uint16) (EncodedGranule, error) {
	return b.buildRawUint16(values, EncodingRawFloat16, "float16", false)
}

// BuildBFloat16Bits stores bfloat16 raw uint16 bits without arithmetic
// interpretation. NaN payloads, infinities, and signed zero bits are preserved.
func (b *GranuleBuilder) BuildBFloat16Bits(values []uint16) (EncodedGranule, error) {
	return b.buildRawUint16(values, EncodingRawBFloat16, "bfloat16", false)
}

func (b *GranuleBuilder) buildRawInt8(values []int8, encoding Encoding, name string) (EncodedGranule, error) {
	if err := validateRawScalarBuildRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateRawScalarBuildConfig(b.cfg, encoding, name); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeInt8Payload(b.raw[:0], values, name)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, encoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	min, max := minMaxInt8(values)
	return newEncodedGranule(len(values), int64(min), int64(max), true, encoding, selection), nil
}

func (b *GranuleBuilder) buildRawUint8(values []uint8, encoding Encoding, name string) (EncodedGranule, error) {
	if err := validateRawScalarBuildRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateRawScalarBuildConfig(b.cfg, encoding, name); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeUint8Payload(b.raw[:0], values, name)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, encoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	min, max := minMaxUint8(values)
	return newEncodedGranule(len(values), int64(min), int64(max), true, encoding, selection), nil
}

func (b *GranuleBuilder) buildRawInt16(values []int16, encoding Encoding, name string) (EncodedGranule, error) {
	if err := validateRawScalarBuildRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateRawScalarBuildConfig(b.cfg, encoding, name); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeLittleEndian2Payload(b.raw[:0], values, name)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, encoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	min, max := minMaxInt16(values)
	return newEncodedGranule(len(values), int64(min), int64(max), true, encoding, selection), nil
}

func (b *GranuleBuilder) buildRawUint16(values []uint16, encoding Encoding, name string, hasMinMax bool) (EncodedGranule, error) {
	if err := validateRawScalarBuildRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateRawScalarBuildConfig(b.cfg, encoding, name); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeLittleEndian2Payload(b.raw[:0], values, name)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, encoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	if !hasMinMax {
		return newEncodedGranule(len(values), 0, 0, false, encoding, selection), nil
	}
	min, max := minMaxUint16(values)
	return newEncodedGranule(len(values), int64(min), int64(max), true, encoding, selection), nil
}

func (b *GranuleBuilder) buildRawInt32(values []int32, encoding Encoding, name string) (EncodedGranule, error) {
	if err := validateRawScalarBuildRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateRawScalarBuildConfig(b.cfg, encoding, name); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeLittleEndian4Payload(b.raw[:0], values, name)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, encoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	min, max := minMaxInt32(values)
	return newEncodedGranule(len(values), int64(min), int64(max), true, encoding, selection), nil
}

func (b *GranuleBuilder) buildRawUint32(values []uint32, encoding Encoding, name string, hasMinMax bool) (EncodedGranule, error) {
	if err := validateRawScalarBuildRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateRawScalarBuildConfig(b.cfg, encoding, name); err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeLittleEndian4Payload(b.raw[:0], values, name)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, encoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	if !hasMinMax {
		return newEncodedGranule(len(values), 0, 0, false, encoding, selection), nil
	}
	min, max := minMaxUint32(values)
	return newEncodedGranule(len(values), int64(min), int64(max), true, encoding, selection), nil
}

func validateRawScalarBuildRows(rows int) error {
	if rows == 0 {
		return errors.New("typedcolumn: empty granule")
	}
	return validateGranuleDecodeRows(rows)
}

func validateRawScalarBuildConfig(cfg Config, want Encoding, name string) error {
	if cfg.Encoding != 0 && cfg.Encoding != want {
		return fmt.Errorf("typedcolumn: %s encoding=%s want %s", name, cfg.Encoding, want)
	}
	if err := validateCompression(cfg.Compression); err != nil {
		return fmt.Errorf("typedcolumn: unsupported %s compression %s", name, cfg.Compression)
	}
	return nil
}

func encodeInt8Payload(dst []byte, values []int8, name string) ([]byte, error) {
	out, err := resizeFixedWidthPayload(dst, len(values), 1, name)
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		out[i] = byte(value)
	}
	return out, nil
}

func encodeUint8Payload(dst []byte, values []uint8, name string) ([]byte, error) {
	out, err := resizeFixedWidthPayload(dst, len(values), 1, name)
	if err != nil {
		return nil, err
	}
	copy(out, values)
	return out, nil
}

func (r *GranuleReader) DecodeInt8(g EncodedGranule) ([]int8, error) {
	values, err := r.DecodeInt8Into(r.int8s[:0], g)
	if err != nil {
		return nil, err
	}
	r.int8s = values
	return values, nil
}

func (r *GranuleReader) DecodeInt8Into(dst []int8, g EncodedGranule) ([]int8, error) {
	if g.Encoding != EncodingRawInt8 {
		return nil, fmt.Errorf("typedcolumn: int8 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawInt8Payload(dst, raw, g.Rows)
}

func (r *GranuleReader) DecodeUint8(g EncodedGranule) ([]uint8, error) {
	values, err := r.DecodeUint8Into(r.uint8s[:0], g)
	if err != nil {
		return nil, err
	}
	r.uint8s = values
	return values, nil
}

func (r *GranuleReader) DecodeUint8Into(dst []uint8, g EncodedGranule) ([]uint8, error) {
	if g.Encoding != EncodingRawUint8 {
		return nil, fmt.Errorf("typedcolumn: uint8 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawUint8Payload(dst, raw, g.Rows)
}

func (r *GranuleReader) DecodeInt16(g EncodedGranule) ([]int16, error) {
	values, err := r.DecodeInt16Into(r.int16s[:0], g)
	if err != nil {
		return nil, err
	}
	r.int16s = values
	return values, nil
}

func (r *GranuleReader) DecodeInt16Into(dst []int16, g EncodedGranule) ([]int16, error) {
	if g.Encoding != EncodingRawInt16 {
		return nil, fmt.Errorf("typedcolumn: int16 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawInt16Payload(dst, raw, g.Rows)
}

func (r *GranuleReader) DecodeUint16(g EncodedGranule) ([]uint16, error) {
	values, err := r.DecodeUint16Into(r.uint16s[:0], g)
	if err != nil {
		return nil, err
	}
	r.uint16s = values
	return values, nil
}

func (r *GranuleReader) DecodeUint16Into(dst []uint16, g EncodedGranule) ([]uint16, error) {
	if g.Encoding != EncodingRawUint16 {
		return nil, fmt.Errorf("typedcolumn: uint16 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawUint16Payload(dst, raw, g.Rows, "uint16")
}

func (r *GranuleReader) DecodeInt32(g EncodedGranule) ([]int32, error) {
	values, err := r.DecodeInt32Into(r.int32s[:0], g)
	if err != nil {
		return nil, err
	}
	r.int32s = values
	return values, nil
}

func (r *GranuleReader) DecodeInt32Into(dst []int32, g EncodedGranule) ([]int32, error) {
	if g.Encoding != EncodingRawInt32 {
		return nil, fmt.Errorf("typedcolumn: int32 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawInt32Payload(dst, raw, g.Rows)
}

func (r *GranuleReader) DecodeUint32(g EncodedGranule) ([]uint32, error) {
	values, err := r.DecodeUint32Into(r.uint32s[:0], g)
	if err != nil {
		return nil, err
	}
	r.uint32s = values
	return values, nil
}

func (r *GranuleReader) DecodeUint32Into(dst []uint32, g EncodedGranule) ([]uint32, error) {
	if g.Encoding != EncodingRawUint32 {
		return nil, fmt.Errorf("typedcolumn: uint32 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawUint32Payload(dst, raw, g.Rows, "uint32")
}

func (r *GranuleReader) DecodeUint64(g EncodedGranule) ([]uint64, error) {
	values, err := r.DecodeUint64Into(r.uint64s[:0], g)
	if err != nil {
		return nil, err
	}
	r.uint64s = values
	return values, nil
}

func (r *GranuleReader) DecodeUint64Into(dst []uint64, g EncodedGranule) ([]uint64, error) {
	if g.Encoding != EncodingRawUint64 {
		return nil, fmt.Errorf("typedcolumn: uint64 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawUint64Payload(dst, raw, g.Rows)
}

func (r *GranuleReader) DecodeFloat16Bits(g EncodedGranule) ([]uint16, error) {
	values, err := r.DecodeFloat16BitsInto(r.float16s[:0], g)
	if err != nil {
		return nil, err
	}
	r.float16s = values
	return values, nil
}

func (r *GranuleReader) DecodeFloat16BitsInto(dst []uint16, g EncodedGranule) ([]uint16, error) {
	if g.Encoding != EncodingRawFloat16 {
		return nil, fmt.Errorf("typedcolumn: float16 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawUint16Payload(dst, raw, g.Rows, "float16")
}

func (r *GranuleReader) DecodeBFloat16Bits(g EncodedGranule) ([]uint16, error) {
	values, err := r.DecodeBFloat16BitsInto(r.bfloat16s[:0], g)
	if err != nil {
		return nil, err
	}
	r.bfloat16s = values
	return values, nil
}

func (r *GranuleReader) DecodeBFloat16BitsInto(dst []uint16, g EncodedGranule) ([]uint16, error) {
	if g.Encoding != EncodingRawBFloat16 {
		return nil, fmt.Errorf("typedcolumn: bfloat16 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawUint16Payload(dst, raw, g.Rows, "bfloat16")
}

func (r *GranuleReader) DecodeIntegerAsInt64Into(dst []int64, columnType ColumnType, g EncodedGranule) ([]int64, error) {
	out := ensureInt64Len(dst[:0], g.Rows)
	switch columnType {
	case ColumnTypeInt64:
		return r.DecodeInt64Into(out[:0], g)
	case ColumnTypeInt8:
		values, err := r.DecodeInt8(g)
		if err != nil {
			return nil, err
		}
		for i, value := range values {
			out[i] = int64(value)
		}
	case ColumnTypeUint8:
		values, err := r.DecodeUint8(g)
		if err != nil {
			return nil, err
		}
		for i, value := range values {
			out[i] = int64(value)
		}
	case ColumnTypeInt16:
		values, err := r.DecodeInt16(g)
		if err != nil {
			return nil, err
		}
		for i, value := range values {
			out[i] = int64(value)
		}
	case ColumnTypeUint16:
		values, err := r.DecodeUint16(g)
		if err != nil {
			return nil, err
		}
		for i, value := range values {
			out[i] = int64(value)
		}
	case ColumnTypeInt32:
		values, err := r.DecodeInt32(g)
		if err != nil {
			return nil, err
		}
		for i, value := range values {
			out[i] = int64(value)
		}
	case ColumnTypeUint32:
		values, err := r.DecodeUint32(g)
		if err != nil {
			return nil, err
		}
		for i, value := range values {
			out[i] = int64(value)
		}
	default:
		return nil, fmt.Errorf("typedcolumn: %s cannot decode as int64 integer", columnType)
	}
	return out, nil
}

func DecodeRawInt8Payload(dst []int8, raw []byte, rows int) ([]int8, error) {
	if err := validateFixedWidthPayloadBytes(len(raw), rows, 1, "int8"); err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, rows)
	for i := range out {
		out[i] = int8(raw[i])
	}
	return out, nil
}

func DecodeRawUint8Payload(dst []uint8, raw []byte, rows int) ([]uint8, error) {
	if err := validateFixedWidthPayloadBytes(len(raw), rows, 1, "uint8"); err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, rows)
	copy(out, raw)
	return out, nil
}

func DecodeRawInt16Payload(dst []int16, raw []byte, rows int) ([]int16, error) {
	if err := validateFixedWidthPayloadBytes(len(raw), rows, 2, "int16"); err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, rows)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
	}
	return out, nil
}

func DecodeRawUint16Payload(dst []uint16, raw []byte, rows int, name string) ([]uint16, error) {
	if err := validateFixedWidthPayloadBytes(len(raw), rows, 2, name); err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, rows)
	for i := range out {
		out[i] = binary.LittleEndian.Uint16(raw[i*2:])
	}
	return out, nil
}

func DecodeRawInt32Payload(dst []int32, raw []byte, rows int) ([]int32, error) {
	return decodeLittleEndian4Payload(dst, raw, rows, "int32")
}

func DecodeRawUint32Payload(dst []uint32, raw []byte, rows int, name string) ([]uint32, error) {
	return decodeLittleEndian4Payload(dst, raw, rows, name)
}

func DecodeRawUint64Payload(dst []uint64, raw []byte, rows int) ([]uint64, error) {
	return decodeLittleEndian8Payload(dst, raw, rows, "uint64")
}

func ensureInt8Len(dst []int8, n int) []int8       { return resizeFixedWidthValues(dst, n) }
func ensureUint8Len(dst []uint8, n int) []uint8    { return resizeFixedWidthValues(dst, n) }
func ensureInt16Len(dst []int16, n int) []int16    { return resizeFixedWidthValues(dst, n) }
func ensureUint16Len(dst []uint16, n int) []uint16 { return resizeFixedWidthValues(dst, n) }
func ensureInt32Len(dst []int32, n int) []int32    { return resizeFixedWidthValues(dst, n) }
func ensureUint64Len(dst []uint64, n int) []uint64 { return resizeFixedWidthValues(dst, n) }

func minMaxInt8(values []int8) (int8, int8) {
	min, max := values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max
}

func minMaxUint8(values []uint8) (uint8, uint8) {
	min, max := values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max
}

func minMaxInt16(values []int16) (int16, int16) {
	min, max := values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max
}

func minMaxUint16(values []uint16) (uint16, uint16) {
	min, max := values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max
}

func minMaxInt32(values []int32) (int32, int32) {
	min, max := values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max
}
