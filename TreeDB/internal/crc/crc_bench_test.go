package crc

import (
	"fmt"
	"testing"
)

var benchmarkCRCSink uint32

func BenchmarkCRCChecksumSizes(b *testing.B) {
	for _, size := range []int{
		64,
		256,
		4 << 10,
		16 << 10,
		64 << 10,
		1 << 20,
		4 << 20,
	} {
		b.Run(formatCRCSize(size), func(b *testing.B) {
			buf := deterministicCRCBuf(size)
			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()
			var sum uint32
			for i := 0; i < b.N; i++ {
				sum ^= Checksum(buf)
			}
			benchmarkCRCSink = sum
		})
	}
}

func BenchmarkCRCUpdateChunked(b *testing.B) {
	buf := deterministicCRCBuf(4 << 20)
	const chunk = 16 << 10
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	var sum uint32
	for i := 0; i < b.N; i++ {
		var crc uint32
		for off := 0; off < len(buf); off += chunk {
			end := off + chunk
			if end > len(buf) {
				end = len(buf)
			}
			crc = Update(crc, buf[off:end])
		}
		sum ^= crc
	}
	benchmarkCRCSink = sum
}

func BenchmarkCRCChecksumParts(b *testing.B) {
	header := deterministicCRCBuf(16)
	payload := deterministicCRCBuf(64 << 10)
	b.SetBytes(int64(len(header) + len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	var sum uint32
	for i := 0; i < b.N; i++ {
		sum ^= ChecksumParts(header, payload)
	}
	benchmarkCRCSink = sum
}

func deterministicCRCBuf(size int) []byte {
	buf := make([]byte, size)
	var x uint32 = 0x1851cafe
	for i := range buf {
		x = x*1664525 + 1013904223
		buf[i] = byte(x >> 24)
	}
	return buf
}

func formatCRCSize(size int) string {
	if size >= 1<<20 && size%(1<<20) == 0 {
		return fmt.Sprintf("%dMiB", size>>20)
	}
	if size >= 1<<10 && size%(1<<10) == 0 {
		return fmt.Sprintf("%dKiB", size>>10)
	}
	return fmt.Sprintf("%dB", size)
}
