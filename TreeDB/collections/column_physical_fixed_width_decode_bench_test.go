package collections

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"unsafe"
)

// columnPhysicalFixedWidthDecodeBenchSink prevents benchmark work from being
// optimized away.
var columnPhysicalFixedWidthDecodeBenchSink uint64

// These helpers are benchmark-only evidence for the M1C encoding decision. They
// intentionally do not change the production physical column format.
func TestColumnPhysicalFixedWidthEndianDecodeBenchHelpersV1(t *testing.T) {
	floatBits := []uint32{
		0,
		0x80000000,
		math.Float32bits(1.25),
		math.Float32bits(-2.5),
		0x7fc12345,
	}
	floatValues := make([]float32, len(floatBits))
	for i, bits := range floatBits {
		floatValues[i] = math.Float32frombits(bits)
	}
	for _, tc := range []struct {
		name   string
		raw    []byte
		decode func([]float32, []byte) []float32
	}{
		{
			name:   "big_endian",
			raw:    encodeFloat32FixedWidthForBenchmark(floatValues, binary.BigEndian),
			decode: appendFloat32FixedWidthBigEndianForBenchmark,
		},
		{
			name:   "little_endian",
			raw:    encodeFloat32FixedWidthForBenchmark(floatValues, binary.LittleEndian),
			decode: appendFloat32FixedWidthLittleEndianForBenchmark,
		},
	} {
		t.Run("float32_"+tc.name, func(t *testing.T) {
			got := tc.decode([]float32{42}, tc.raw)
			if len(got) != len(floatValues)+1 || got[0] != 42 {
				t.Fatalf("got len=%d first=%v", len(got), got[0])
			}
			for i, wantBits := range floatBits {
				if gotBits := math.Float32bits(got[i+1]); gotBits != wantBits {
					t.Fatalf("got bits[%d]=0x%08x want 0x%08x", i, gotBits, wantBits)
				}
			}
		})
	}

	uintValues := []uint32{0, 1, 17, 1024, math.MaxUint16 + 1, math.MaxUint32}
	for _, tc := range []struct {
		name   string
		raw    []byte
		decode func([]uint32, []byte) []uint32
	}{
		{
			name:   "big_endian",
			raw:    encodeUint32FixedWidthForBenchmark(uintValues, binary.BigEndian),
			decode: appendUint32FixedWidthBigEndianForBenchmark,
		},
		{
			name:   "little_endian",
			raw:    encodeUint32FixedWidthForBenchmark(uintValues, binary.LittleEndian),
			decode: appendUint32FixedWidthLittleEndianForBenchmark,
		},
	} {
		t.Run("uint32_"+tc.name, func(t *testing.T) {
			got := tc.decode([]uint32{42}, tc.raw)
			if len(got) != len(uintValues)+1 || got[0] != 42 {
				t.Fatalf("got len=%d first=%v", len(got), got[0])
			}
			for i, want := range uintValues {
				if got[i+1] != want {
					t.Fatalf("got[%d]=%d want %d", i+1, got[i+1], want)
				}
			}
		})
	}

	if columnPhysicalBenchmarkHostLittleEndian() {
		raw := encodeFloat32FixedWidthForBenchmark(floatValues, binary.LittleEndian)
		view, ok := float32FixedWidthLittleEndianViewForBenchmark(raw)
		if !ok {
			t.Fatalf("aligned little-endian float32 view unavailable")
		}
		for i, wantBits := range floatBits {
			if gotBits := math.Float32bits(view[i]); gotBits != wantBits {
				t.Fatalf("direct view bits[%d]=0x%08x want 0x%08x", i, gotBits, wantBits)
			}
		}
		if _, ok := float32FixedWidthLittleEndianViewForBenchmark(append([]byte{0}, raw...)[1:]); ok {
			t.Fatalf("misaligned float32 direct view unexpectedly available")
		}
		if _, ok := float32FixedWidthLittleEndianViewForBenchmark(nil); ok {
			t.Fatalf("empty float32 direct view unexpectedly available")
		}

		uintRaw := encodeUint32FixedWidthForBenchmark(uintValues, binary.LittleEndian)
		uintView, ok := uint32FixedWidthLittleEndianViewForBenchmark(uintRaw)
		if !ok {
			t.Fatalf("aligned little-endian uint32 view unavailable")
		}
		for i, want := range uintValues {
			if uintView[i] != want {
				t.Fatalf("direct view uint32[%d]=%d want %d", i, uintView[i], want)
			}
		}
		if _, ok := uint32FixedWidthLittleEndianViewForBenchmark(append([]byte{0}, uintRaw...)[1:]); ok {
			t.Fatalf("misaligned uint32 direct view unexpectedly available")
		}
		if _, ok := uint32FixedWidthLittleEndianViewForBenchmark(nil); ok {
			t.Fatalf("empty uint32 direct view unexpectedly available")
		}
	}

	mustPanicColumnPhysicalFixedWidthBenchmark(t, func() {
		columnPhysicalCopyLittleEndianFloat32Bytes(make([]float32, 1), []byte{1, 2, 3})
	})

	for _, tc := range []struct {
		name string
		fn   func()
	}{
		{
			name: "float32_big_endian_truncated",
			fn: func() {
				appendFloat32FixedWidthBigEndianForBenchmark(nil, []byte{1, 2, 3})
			},
		},
		{
			name: "float32_little_endian_truncated",
			fn: func() {
				appendFloat32FixedWidthLittleEndianForBenchmark(nil, []byte{1, 2, 3})
			},
		},
		{
			name: "uint32_big_endian_truncated",
			fn: func() {
				appendUint32FixedWidthBigEndianForBenchmark(nil, []byte{1, 2, 3})
			},
		},
		{
			name: "uint32_little_endian_truncated",
			fn: func() {
				appendUint32FixedWidthLittleEndianForBenchmark(nil, []byte{1, 2, 3})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mustPanicColumnPhysicalFixedWidthBenchmark(t, tc.fn)
		})
	}
}

func BenchmarkColumnPhysicalFixedWidthEndianDecodeV1(b *testing.B) {
	for _, dims := range []int{96, 128} {
		values := makeBenchmarkFloat32Values(dims)
		bigEndianRaw := encodeFloat32FixedWidthForBenchmark(values, binary.BigEndian)
		littleEndianRaw := encodeFloat32FixedWidthForBenchmark(values, binary.LittleEndian)
		b.Run(fmt.Sprintf("float32_%dd/big_endian_decode_copy", dims), func(b *testing.B) {
			benchmarkFloat32FixedWidthDecodeCopy(b, bigEndianRaw, dims, appendFloat32FixedWidthBigEndianForBenchmark)
		})
		b.Run(fmt.Sprintf("float32_%dd/little_endian_decode_copy", dims), func(b *testing.B) {
			benchmarkFloat32FixedWidthDecodeCopy(b, littleEndianRaw, dims, appendFloat32FixedWidthLittleEndianForBenchmark)
		})
		b.Run(fmt.Sprintf("float32_%dd/little_endian_direct_set_copy", dims), func(b *testing.B) {
			if !columnPhysicalNativeLittleEndian {
				b.Skip("direct-set copy requires a little-endian host")
			}
			benchmarkFloat32FixedWidthDecodeCopy(b, littleEndianRaw, dims, appendFloat32FixedWidthLittleEndianDirectSetForBenchmark)
		})
		b.Run(fmt.Sprintf("float32_%dd/little_endian_direct_view_setup", dims), func(b *testing.B) {
			benchmarkFloat32FixedWidthDirectViewSetup(b, littleEndianRaw, dims)
		})
		b.Run(fmt.Sprintf("float32_%dd/little_endian_direct_view_full_scan", dims), func(b *testing.B) {
			benchmarkFloat32FixedWidthDirectViewFullScan(b, littleEndianRaw, dims)
		})
	}
	for _, degree := range []int{16, 64} {
		values := makeBenchmarkUint32Values(degree)
		bigEndianRaw := encodeUint32FixedWidthForBenchmark(values, binary.BigEndian)
		littleEndianRaw := encodeUint32FixedWidthForBenchmark(values, binary.LittleEndian)
		b.Run(fmt.Sprintf("uint32_%d/big_endian_decode_copy", degree), func(b *testing.B) {
			benchmarkUint32FixedWidthDecodeCopy(b, bigEndianRaw, degree, appendUint32FixedWidthBigEndianForBenchmark)
		})
		b.Run(fmt.Sprintf("uint32_%d/little_endian_decode_copy", degree), func(b *testing.B) {
			benchmarkUint32FixedWidthDecodeCopy(b, littleEndianRaw, degree, appendUint32FixedWidthLittleEndianForBenchmark)
		})
		b.Run(fmt.Sprintf("uint32_%d/little_endian_direct_view_setup", degree), func(b *testing.B) {
			benchmarkUint32FixedWidthDirectViewSetup(b, littleEndianRaw, degree)
		})
		b.Run(fmt.Sprintf("uint32_%d/little_endian_direct_view_full_scan", degree), func(b *testing.B) {
			benchmarkUint32FixedWidthDirectViewFullScan(b, littleEndianRaw, degree)
		})
	}
}

func benchmarkFloat32FixedWidthDecodeCopy(b *testing.B, raw []byte, valuesPerOp int, decode func([]float32, []byte) []float32) {
	dst := make([]float32, 0, valuesPerOp)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ReportMetric(float64(valuesPerOp), "values/op")
	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		dst = decode(dst, raw)
		sum += uint64(math.Float32bits(dst[i%len(dst)]))
	}
	columnPhysicalFixedWidthDecodeBenchSink += sum
}

func benchmarkUint32FixedWidthDecodeCopy(b *testing.B, raw []byte, valuesPerOp int, decode func([]uint32, []byte) []uint32) {
	dst := make([]uint32, 0, valuesPerOp)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ReportMetric(float64(valuesPerOp), "values/op")
	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		dst = decode(dst, raw)
		sum += uint64(dst[i%len(dst)])
	}
	columnPhysicalFixedWidthDecodeBenchSink += sum
}

func benchmarkFloat32FixedWidthDirectViewSetup(b *testing.B, raw []byte, valuesPerOp int) {
	if !columnPhysicalBenchmarkHostLittleEndian() {
		b.Skip("direct little-endian view requires little-endian host")
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(raw)), "payload_B/op")
	b.ReportMetric(float64(valuesPerOp), "values/op")
	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		view, ok := float32FixedWidthLittleEndianViewForBenchmark(raw)
		if !ok {
			b.Fatalf("direct view unavailable")
		}
		sum += uint64(math.Float32bits(view[i%len(view)]))
	}
	columnPhysicalFixedWidthDecodeBenchSink += sum
}

func benchmarkUint32FixedWidthDirectViewSetup(b *testing.B, raw []byte, valuesPerOp int) {
	if !columnPhysicalBenchmarkHostLittleEndian() {
		b.Skip("direct little-endian view requires little-endian host")
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(raw)), "payload_B/op")
	b.ReportMetric(float64(valuesPerOp), "values/op")
	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		view, ok := uint32FixedWidthLittleEndianViewForBenchmark(raw)
		if !ok {
			b.Fatalf("direct view unavailable")
		}
		sum += uint64(view[i%len(view)])
	}
	columnPhysicalFixedWidthDecodeBenchSink += sum
}

func benchmarkFloat32FixedWidthDirectViewFullScan(b *testing.B, raw []byte, valuesPerOp int) {
	if !columnPhysicalBenchmarkHostLittleEndian() {
		b.Skip("direct little-endian view requires little-endian host")
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ReportMetric(float64(valuesPerOp), "values/op")
	view, ok := float32FixedWidthLittleEndianViewForBenchmark(raw)
	if !ok {
		b.Fatalf("direct view unavailable")
	}
	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		for _, value := range view {
			sum += uint64(math.Float32bits(value))
		}
	}
	columnPhysicalFixedWidthDecodeBenchSink += sum
}

func benchmarkUint32FixedWidthDirectViewFullScan(b *testing.B, raw []byte, valuesPerOp int) {
	if !columnPhysicalBenchmarkHostLittleEndian() {
		b.Skip("direct little-endian view requires little-endian host")
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ReportMetric(float64(valuesPerOp), "values/op")
	view, ok := uint32FixedWidthLittleEndianViewForBenchmark(raw)
	if !ok {
		b.Fatalf("direct view unavailable")
	}
	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		for _, value := range view {
			sum += uint64(value)
		}
	}
	columnPhysicalFixedWidthDecodeBenchSink += sum
}

func makeBenchmarkFloat32Values(n int) []float32 {
	values := make([]float32, n)
	for i := range values {
		values[i] = float32(i%17-8) * 0.03125
	}
	return values
}

func makeBenchmarkUint32Values(n int) []uint32 {
	values := make([]uint32, n)
	for i := range values {
		values[i] = uint32(i*17 + 3)
	}
	return values
}

func encodeFloat32FixedWidthForBenchmark(values []float32, order binary.ByteOrder) []byte {
	raw := make([]byte, len(values)*4)
	for i, value := range values {
		order.PutUint32(raw[i*4:], math.Float32bits(value))
	}
	return raw
}

func encodeUint32FixedWidthForBenchmark(values []uint32, order binary.ByteOrder) []byte {
	raw := make([]byte, len(values)*4)
	for i, value := range values {
		order.PutUint32(raw[i*4:], value)
	}
	return raw
}

func appendFloat32FixedWidthBigEndianForBenchmark(dst []float32, raw []byte) []float32 {
	requireColumnPhysicalFixedWidth4ForBenchmark(raw)
	base := len(dst)
	need := base + len(raw)/4
	if cap(dst) < need {
		next := make([]float32, need)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:need]
	}
	if len(raw) > 0 {
		_ = raw[len(raw)-1]
	}
	pos := 0
	i := base
	// i indexes the extended dst slice; pos indexes raw and advances in lockstep.
	for ; i+4 <= need; i += 4 {
		_ = raw[pos+15]
		dst[i] = math.Float32frombits(uint32(raw[pos])<<24 | uint32(raw[pos+1])<<16 | uint32(raw[pos+2])<<8 | uint32(raw[pos+3]))
		dst[i+1] = math.Float32frombits(uint32(raw[pos+4])<<24 | uint32(raw[pos+5])<<16 | uint32(raw[pos+6])<<8 | uint32(raw[pos+7]))
		dst[i+2] = math.Float32frombits(uint32(raw[pos+8])<<24 | uint32(raw[pos+9])<<16 | uint32(raw[pos+10])<<8 | uint32(raw[pos+11]))
		dst[i+3] = math.Float32frombits(uint32(raw[pos+12])<<24 | uint32(raw[pos+13])<<16 | uint32(raw[pos+14])<<8 | uint32(raw[pos+15]))
		pos += 16
	}
	for ; i < need; i++ {
		dst[i] = math.Float32frombits(uint32(raw[pos])<<24 | uint32(raw[pos+1])<<16 | uint32(raw[pos+2])<<8 | uint32(raw[pos+3]))
		pos += 4
	}
	return dst
}

func appendFloat32FixedWidthLittleEndianForBenchmark(dst []float32, raw []byte) []float32 {
	requireColumnPhysicalFixedWidth4ForBenchmark(raw)
	base := len(dst)
	need := base + len(raw)/4
	if cap(dst) < need {
		next := make([]float32, need)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:need]
	}
	if len(raw) > 0 {
		_ = raw[len(raw)-1]
	}
	pos := 0
	i := base
	// i indexes the extended dst slice; pos indexes raw and advances in lockstep.
	for ; i+4 <= need; i += 4 {
		_ = raw[pos+15]
		dst[i] = math.Float32frombits(uint32(raw[pos]) | uint32(raw[pos+1])<<8 | uint32(raw[pos+2])<<16 | uint32(raw[pos+3])<<24)
		dst[i+1] = math.Float32frombits(uint32(raw[pos+4]) | uint32(raw[pos+5])<<8 | uint32(raw[pos+6])<<16 | uint32(raw[pos+7])<<24)
		dst[i+2] = math.Float32frombits(uint32(raw[pos+8]) | uint32(raw[pos+9])<<8 | uint32(raw[pos+10])<<16 | uint32(raw[pos+11])<<24)
		dst[i+3] = math.Float32frombits(uint32(raw[pos+12]) | uint32(raw[pos+13])<<8 | uint32(raw[pos+14])<<16 | uint32(raw[pos+15])<<24)
		pos += 16
	}
	for ; i < need; i++ {
		dst[i] = math.Float32frombits(uint32(raw[pos]) | uint32(raw[pos+1])<<8 | uint32(raw[pos+2])<<16 | uint32(raw[pos+3])<<24)
		pos += 4
	}
	return dst
}

func appendFloat32FixedWidthLittleEndianDirectSetForBenchmark(dst []float32, raw []byte) []float32 {
	requireColumnPhysicalFixedWidth4ForBenchmark(raw)
	if !columnPhysicalNativeLittleEndian {
		return appendFloat32FixedWidthLittleEndianForBenchmark(dst, raw)
	}
	base := len(dst)
	need := base + len(raw)/4
	if cap(dst) < need {
		next := make([]float32, need)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:need]
	}
	columnPhysicalCopyLittleEndianFloat32Bytes(dst[base:need], raw)
	return dst
}

func appendUint32FixedWidthBigEndianForBenchmark(dst []uint32, raw []byte) []uint32 {
	requireColumnPhysicalFixedWidth4ForBenchmark(raw)
	base := len(dst)
	need := base + len(raw)/4
	if cap(dst) < need {
		next := make([]uint32, need)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:need]
	}
	if len(raw) > 0 {
		_ = raw[len(raw)-1]
	}
	pos := 0
	i := base
	// i indexes the extended dst slice; pos indexes raw and advances in lockstep.
	for ; i+4 <= need; i += 4 {
		_ = raw[pos+15]
		dst[i] = uint32(raw[pos])<<24 | uint32(raw[pos+1])<<16 | uint32(raw[pos+2])<<8 | uint32(raw[pos+3])
		dst[i+1] = uint32(raw[pos+4])<<24 | uint32(raw[pos+5])<<16 | uint32(raw[pos+6])<<8 | uint32(raw[pos+7])
		dst[i+2] = uint32(raw[pos+8])<<24 | uint32(raw[pos+9])<<16 | uint32(raw[pos+10])<<8 | uint32(raw[pos+11])
		dst[i+3] = uint32(raw[pos+12])<<24 | uint32(raw[pos+13])<<16 | uint32(raw[pos+14])<<8 | uint32(raw[pos+15])
		pos += 16
	}
	for ; i < need; i++ {
		dst[i] = uint32(raw[pos])<<24 | uint32(raw[pos+1])<<16 | uint32(raw[pos+2])<<8 | uint32(raw[pos+3])
		pos += 4
	}
	return dst
}

func appendUint32FixedWidthLittleEndianForBenchmark(dst []uint32, raw []byte) []uint32 {
	requireColumnPhysicalFixedWidth4ForBenchmark(raw)
	base := len(dst)
	need := base + len(raw)/4
	if cap(dst) < need {
		next := make([]uint32, need)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:need]
	}
	if len(raw) > 0 {
		_ = raw[len(raw)-1]
	}
	pos := 0
	i := base
	// i indexes the extended dst slice; pos indexes raw and advances in lockstep.
	for ; i+4 <= need; i += 4 {
		_ = raw[pos+15]
		dst[i] = uint32(raw[pos]) | uint32(raw[pos+1])<<8 | uint32(raw[pos+2])<<16 | uint32(raw[pos+3])<<24
		dst[i+1] = uint32(raw[pos+4]) | uint32(raw[pos+5])<<8 | uint32(raw[pos+6])<<16 | uint32(raw[pos+7])<<24
		dst[i+2] = uint32(raw[pos+8]) | uint32(raw[pos+9])<<8 | uint32(raw[pos+10])<<16 | uint32(raw[pos+11])<<24
		dst[i+3] = uint32(raw[pos+12]) | uint32(raw[pos+13])<<8 | uint32(raw[pos+14])<<16 | uint32(raw[pos+15])<<24
		pos += 16
	}
	for ; i < need; i++ {
		dst[i] = uint32(raw[pos]) | uint32(raw[pos+1])<<8 | uint32(raw[pos+2])<<16 | uint32(raw[pos+3])<<24
		pos += 4
	}
	return dst
}

func float32FixedWidthLittleEndianViewForBenchmark(raw []byte) ([]float32, bool) {
	if len(raw)%4 != 0 || !columnPhysicalBenchmarkHostLittleEndian() {
		return nil, false
	}
	if len(raw) == 0 {
		return nil, false
	}
	ptr := unsafe.Pointer(unsafe.SliceData(raw))
	if uintptr(ptr)%unsafe.Alignof(float32(0)) != 0 {
		return nil, false
	}
	return unsafe.Slice((*float32)(ptr), len(raw)/4), true
}

func uint32FixedWidthLittleEndianViewForBenchmark(raw []byte) ([]uint32, bool) {
	if len(raw)%4 != 0 || !columnPhysicalBenchmarkHostLittleEndian() {
		return nil, false
	}
	if len(raw) == 0 {
		return nil, false
	}
	ptr := unsafe.Pointer(unsafe.SliceData(raw))
	if uintptr(ptr)%unsafe.Alignof(uint32(0)) != 0 {
		return nil, false
	}
	return unsafe.Slice((*uint32)(ptr), len(raw)/4), true
}

func columnPhysicalBenchmarkHostLittleEndian() bool {
	var value uint16 = 1
	return *(*byte)(unsafe.Pointer(&value)) == 1
}

func requireColumnPhysicalFixedWidth4ForBenchmark(raw []byte) {
	if len(raw)%4 != 0 {
		panic("collections: benchmark fixed-width payload length is not divisible by 4")
	}
}

func mustPanicColumnPhysicalFixedWidthBenchmark(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	fn()
}
