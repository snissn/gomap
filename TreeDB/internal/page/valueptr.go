package page

import (
	"encoding/binary"
	"errors"
)

// ValuePtr points to an out-of-line value stored in a slab file.
// Its in-memory layout matches the 16-byte on-disk encoding.
type ValuePtr struct {
	Offset uint64 // bytes 0-7
	Length uint32 // bytes 8-11
	FileID uint32 // bytes 12-15
}

var ErrValuePtrTooSmall = errors.New("value pointer buffer too small")

// EncodeLE writes the little-endian on-disk encoding of p into dst.
func (p ValuePtr) EncodeLE(dst []byte) error {
	if len(dst) < ValuePtrSize {
		return ErrValuePtrTooSmall
	}
	binary.LittleEndian.PutUint64(dst[0:8], p.Offset)
	binary.LittleEndian.PutUint32(dst[8:12], p.Length)
	binary.LittleEndian.PutUint32(dst[12:16], p.FileID)
	return nil
}

// DecodeValuePtrLE parses a ValuePtr from its little-endian on-disk encoding.
func DecodeValuePtrLE(src []byte) (ValuePtr, error) {
	if len(src) < ValuePtrSize {
		return ValuePtr{}, ErrValuePtrTooSmall
	}
	return ValuePtr{
		Offset: binary.LittleEndian.Uint64(src[0:8]),
		Length: binary.LittleEndian.Uint32(src[8:12]),
		FileID: binary.LittleEndian.Uint32(src[12:16]),
	}, nil
}

