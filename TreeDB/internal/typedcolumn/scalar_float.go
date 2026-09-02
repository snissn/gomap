package typedcolumn

import (
	"errors"
	"fmt"
	"math"
)

// BuildFloat32 returns a granule whose payload aliases builder-owned scratch until
// the next builder Build* or Reset call. Payload values are raw little-endian
// IEEE-754 float32 bits; NaN payloads and signed zeroes are preserved exactly.
func (b *GranuleBuilder) BuildFloat32(values []float32) (EncodedGranule, error) {
	if len(values) == 0 {
		return EncodedGranule{}, errors.New("typedcolumn: empty granule")
	}
	if err := validateGranuleDecodeRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if b.cfg.Encoding != 0 && b.cfg.Encoding != EncodingRawFloat32 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: float32 encoding=%s want %s", b.cfg.Encoding, EncodingRawFloat32)
	}
	if err := validateCompression(b.cfg.Compression); err != nil {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: unsupported float32 compression %s", b.cfg.Compression)
	}
	raw, err := encodeFloat32Payload(b.raw[:0], values)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawFloat32, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(len(values), 0, 0, false, EncodingRawFloat32, selection), nil
}

// BuildFloat64 returns a granule whose payload aliases builder-owned scratch until
// the next builder Build* or Reset call. Payload values are raw little-endian
// IEEE-754 float64 bits; NaN payloads and signed zeroes are preserved exactly.
func (b *GranuleBuilder) BuildFloat64(values []float64) (EncodedGranule, error) {
	if len(values) == 0 {
		return EncodedGranule{}, errors.New("typedcolumn: empty granule")
	}
	if err := validateGranuleDecodeRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	if b.cfg.Encoding != 0 && b.cfg.Encoding != EncodingRawFloat64 {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: float64 encoding=%s want %s", b.cfg.Encoding, EncodingRawFloat64)
	}
	if err := validateCompression(b.cfg.Compression); err != nil {
		return EncodedGranule{}, fmt.Errorf("typedcolumn: unsupported float64 compression %s", b.cfg.Compression)
	}
	raw, err := encodeFloat64Payload(b.raw[:0], values)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawFloat64, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(len(values), 0, 0, false, EncodingRawFloat64, selection), nil
}

func (r *GranuleReader) DecodeFloat32(g EncodedGranule) ([]float32, error) {
	values, err := r.DecodeFloat32Into(r.float32s[:0], g)
	if err != nil {
		return nil, err
	}
	r.float32s = values
	return values, nil
}

func (r *GranuleReader) DecodeFloat32Into(dst []float32, g EncodedGranule) ([]float32, error) {
	if g.Encoding != EncodingRawFloat32 {
		return nil, fmt.Errorf("typedcolumn: float32 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawFloat32Payload(dst, raw, g.Rows)
}

func (r *GranuleReader) DecodeFloat64(g EncodedGranule) ([]float64, error) {
	values, err := r.DecodeFloat64Into(r.float64s[:0], g)
	if err != nil {
		return nil, err
	}
	r.float64s = values
	return values, nil
}

func (r *GranuleReader) DecodeFloat64Into(dst []float64, g EncodedGranule) ([]float64, error) {
	if g.Encoding != EncodingRawFloat64 {
		return nil, fmt.Errorf("typedcolumn: float64 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return DecodeRawFloat64Payload(dst, raw, g.Rows)
}

func encodeFloat32Payload(dst []byte, values []float32) ([]byte, error) {
	out, err := resizeFixedWidthPayload(dst, len(values), 4, "float32")
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		putLittleEndianFloat32(out[i*4:], value)
	}
	return out, nil
}

func encodeFloat64Payload(dst []byte, values []float64) ([]byte, error) {
	out, err := resizeFixedWidthPayload(dst, len(values), 8, "float64")
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		putLittleEndianFloat64(out[i*8:], value)
	}
	return out, nil
}

func DecodeRawFloat32Payload(dst []float32, raw []byte, rows int) ([]float32, error) {
	if err := validateFixedWidthPayloadBytes(len(raw), rows, 4, "float32"); err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, rows)
	for i := range out {
		out[i] = math.Float32frombits(readLittleEndianUint32(raw[i*4:]))
	}
	return out, nil
}

func DecodeRawFloat64Payload(dst []float64, raw []byte, rows int) ([]float64, error) {
	if err := validateFixedWidthPayloadBytes(len(raw), rows, 8, "float64"); err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, rows)
	for i := range out {
		out[i] = math.Float64frombits(readLittleEndianUint64(raw[i*8:]))
	}
	return out, nil
}

func ensureFloat64Len(dst []float64, n int) []float64 {
	if cap(dst) < n {
		return make([]float64, n)
	}
	return dst[:n]
}
