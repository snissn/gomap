package nativewire

import "encoding/binary"

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
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

func isMinimalUvarint(v uint64, n int) bool {
	return n == uvarintLen(v)
}
