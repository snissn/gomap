package typedcolumn

import (
	"encoding/binary"
	"fmt"
	"math"
)

type littleEndian2Scalar interface {
	~uint16 | ~int16
}

type littleEndian4Scalar interface {
	~uint32 | ~int32
}

type littleEndian8Scalar interface {
	~uint64 | ~int64
}

func encodeLittleEndian2Payload[T littleEndian2Scalar](dst []byte, values []T, name string) ([]byte, error) {
	out, err := resizeFixedWidthPayload(dst, len(values), 2, name)
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(value))
	}
	return out, nil
}

func encodeLittleEndian4Payload[T littleEndian4Scalar](dst []byte, values []T, name string) ([]byte, error) {
	out, err := resizeFixedWidthPayload(dst, len(values), 4, name)
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		binary.LittleEndian.PutUint32(out[i*4:], uint32(value))
	}
	return out, nil
}

func encodeLittleEndian8Payload[T littleEndian8Scalar](dst []byte, values []T, name string) ([]byte, error) {
	out, err := resizeFixedWidthPayload(dst, len(values), 8, name)
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		binary.LittleEndian.PutUint64(out[i*8:], uint64(value))
	}
	return out, nil
}

func decodeLittleEndian4Payload[T littleEndian4Scalar](dst []T, raw []byte, rows int, name string) ([]T, error) {
	if err := validateFixedWidthPayloadBytes(len(raw), rows, 4, name); err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, rows)
	for i := range out {
		out[i] = T(binary.LittleEndian.Uint32(raw[i*4:]))
	}
	return out, nil
}

func decodeLittleEndian8Payload[T littleEndian8Scalar](dst []T, raw []byte, rows int, name string) ([]T, error) {
	if err := validateFixedWidthPayloadBytes(len(raw), rows, 8, name); err != nil {
		return nil, err
	}
	out := resizeFixedWidthValues(dst, rows)
	for i := range out {
		out[i] = T(binary.LittleEndian.Uint64(raw[i*8:]))
	}
	return out, nil
}

func readLittleEndianUint32(raw []byte) uint32 {
	return binary.LittleEndian.Uint32(raw)
}

func readLittleEndianUint64(raw []byte) uint64 {
	return binary.LittleEndian.Uint64(raw)
}

func float32FromLittleEndian(raw []byte) float32 {
	return math.Float32frombits(readLittleEndianUint32(raw))
}

func float64FromLittleEndian(raw []byte) float64 {
	return math.Float64frombits(readLittleEndianUint64(raw))
}

func putLittleEndianFloat32(dst []byte, value float32) {
	binary.LittleEndian.PutUint32(dst, math.Float32bits(value))
}

func putLittleEndianFloat64(dst []byte, value float64) {
	binary.LittleEndian.PutUint64(dst, math.Float64bits(value))
}

func resizeFixedWidthPayload(dst []byte, values int, width int, name string) ([]byte, error) {
	need, err := checkedMulInt(values, width, name)
	if err != nil {
		return nil, err
	}
	if cap(dst) < need {
		return make([]byte, need), nil
	}
	return dst[:need], nil
}

func validateFixedWidthPayloadBytes(rawBytes int, rows int, width int, name string) error {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return err
	}
	if width <= 0 {
		return fmt.Errorf("typedcolumn: %s invalid fixed-width element bytes=%d", name, width)
	}
	want, err := checkedMulInt(rows, width, name)
	if err != nil {
		return err
	}
	if rawBytes != want {
		return fmt.Errorf("typedcolumn: %s raw bytes=%d want=%d", name, rawBytes, want)
	}
	return nil
}

func resizeFixedWidthValues[T any](dst []T, n int) []T {
	if cap(dst) < n {
		return make([]T, n)
	}
	return dst[:n]
}
