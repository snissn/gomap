package node

import (
	"encoding/binary"
	"math/bits"
	"testing"
	"unsafe"
)

var (
	benchSinkU16 uint16
	benchSinkU64 uint64
)

func getUint16AtShift(b []byte, off int) uint16 {
	return uint16(b[off]) | uint16(b[off+1])<<8
}

func getUint16AtStdlib(b []byte, off int) uint16 {
	return binary.LittleEndian.Uint16(b[off : off+2])
}

func getUint16AtUnsafe(b []byte, off int) uint16 {
	return *(*uint16)(unsafe.Pointer(&b[off]))
}

func getUint64BEAtShift(b []byte, off int) uint64 {
	return uint64(b[off])<<56 |
		uint64(b[off+1])<<48 |
		uint64(b[off+2])<<40 |
		uint64(b[off+3])<<32 |
		uint64(b[off+4])<<24 |
		uint64(b[off+5])<<16 |
		uint64(b[off+6])<<8 |
		uint64(b[off+7])
}

func getUint64BEAtStdlib(b []byte, off int) uint64 {
	return binary.BigEndian.Uint64(b[off : off+8])
}

func getUint64BEAtUnsafe(b []byte, off int) uint64 {
	return bits.ReverseBytes64(*(*uint64)(unsafe.Pointer(&b[off])))
}

func benchmarkUint16At(b *testing.B, fn func([]byte, int) uint16) {
	buf := make([]byte, 8192)
	for i := range buf {
		buf[i] = byte(i*31 + 7)
	}
	off := 0
	const maxOff = 8190
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off += 2
		if off > maxOff {
			off = 0
		}
		benchSinkU16 = fn(buf, off)
	}
}

func benchmarkUint64BEAt(b *testing.B, fn func([]byte, int) uint64) {
	buf := make([]byte, 8192)
	for i := range buf {
		buf[i] = byte(i*17 + 3)
	}
	off := 0
	const maxOff = 8184
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off += 8
		if off > maxOff {
			off = 0
		}
		benchSinkU64 = fn(buf, off)
	}
}

func BenchmarkGetUint16At_Shift(b *testing.B) {
	benchmarkUint16At(b, getUint16AtShift)
}

func BenchmarkGetUint16At_Stdlib(b *testing.B) {
	benchmarkUint16At(b, getUint16AtStdlib)
}

func BenchmarkGetUint16At_Unsafe(b *testing.B) {
	benchmarkUint16At(b, getUint16AtUnsafe)
}

func BenchmarkGetUint64BEAt_Shift(b *testing.B) {
	benchmarkUint64BEAt(b, getUint64BEAtShift)
}

func BenchmarkGetUint64BEAt_Stdlib(b *testing.B) {
	benchmarkUint64BEAt(b, getUint64BEAtStdlib)
}

func BenchmarkGetUint64BEAt_Unsafe(b *testing.B) {
	benchmarkUint64BEAt(b, getUint64BEAtUnsafe)
}
