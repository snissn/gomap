package slab

import "encoding/binary"

const (
	deltaXorMagic    byte = 0xD1
	deltaXorWordSize      = 8
)

// encodeDeltaXor returns a compact xor delta for fixed-size values.
// It is a prototype for value-delta feasibility experiments.
func encodeDeltaXor(prev, curr []byte) ([]byte, bool) {
	if len(prev) == 0 || len(prev) != len(curr) || len(prev)%deltaXorWordSize != 0 {
		return nil, false
	}
	words := len(curr) / deltaXorWordSize
	maskLen := (words + 7) / 8
	mask := make([]byte, maskLen)
	xors := make([]byte, 0, words*deltaXorWordSize)

	for i := 0; i < words; i++ {
		off := i * deltaXorWordSize
		prevWord := binary.LittleEndian.Uint64(prev[off:])
		currWord := binary.LittleEndian.Uint64(curr[off:])
		delta := prevWord ^ currWord
		if delta == 0 {
			continue
		}
		mask[i/8] |= 1 << uint(i%8)
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], delta)
		xors = append(xors, buf[:]...)
	}

	if len(xors) == 0 {
		return nil, false
	}
	encodedLen := 2 + len(mask) + len(xors)
	if encodedLen >= len(curr) {
		return nil, false
	}

	out := make([]byte, 0, encodedLen)
	out = append(out, deltaXorMagic, byte(maskLen))
	out = append(out, mask...)
	out = append(out, xors...)
	return out, true
}

func applyDeltaXor(prev, delta []byte) ([]byte, bool) {
	if len(prev) == 0 || len(prev)%deltaXorWordSize != 0 {
		return nil, false
	}
	if len(delta) < 2 || delta[0] != deltaXorMagic {
		return nil, false
	}
	maskLen := int(delta[1])
	words := len(prev) / deltaXorWordSize
	if maskLen != (words+7)/8 || len(delta) < 2+maskLen {
		return nil, false
	}
	payload := delta[2+maskLen:]

	out := make([]byte, len(prev))
	copy(out, prev)

	xorOff := 0
	for i := 0; i < words; i++ {
		maskByte := delta[2+(i/8)]
		if (maskByte & (1 << uint(i%8))) == 0 {
			continue
		}
		if xorOff+deltaXorWordSize > len(payload) {
			return nil, false
		}
		off := i * deltaXorWordSize
		prevWord := binary.LittleEndian.Uint64(out[off:])
		deltaWord := binary.LittleEndian.Uint64(payload[xorOff:])
		xorOff += deltaXorWordSize
		binary.LittleEndian.PutUint64(out[off:], prevWord^deltaWord)
	}
	if xorOff != len(payload) {
		return nil, false
	}
	return out, true
}
