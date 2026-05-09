package nativewire

import (
	"encoding/binary"
	"math/bits"
)

func appendUvarint(dst []byte, v uint64) []byte {
	return binary.AppendUvarint(dst, v)
}

func readUvarint(src []byte) (uint64, int, error) {
	v, n := binary.Uvarint(src)
	switch {
	case n > 0:
		if !isMinimalUvarint(v, n) {
			return 0, 0, protocolError(ErrMalformedFrame, "non-minimal uvarint")
		}
		return v, n, nil
	case n == 0:
		return 0, 0, protocolError(ErrMalformedFrame, "truncated uvarint")
	default:
		return 0, 0, protocolError(ErrMalformedFrame, "uvarint overflow")
	}
}

func uvarintLen(v uint64) int {
	if v == 0 {
		return 1
	}
	return (bits.Len64(v) + 6) / 7
}

func isMinimalUvarint(v uint64, n int) bool {
	return n == uvarintLen(v)
}
