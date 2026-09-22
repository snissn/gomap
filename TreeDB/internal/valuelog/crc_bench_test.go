package valuelog

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

var benchmarkValueLogCRCSink uint32

func BenchmarkValueLogRecordChecksumParts(b *testing.B) {
	var header [HeaderSize]byte
	payload := deterministicValueLogCRCBuf(64 << 10)
	header[4] = Version
	binary.LittleEndian.PutUint64(header[8:16], 0x1851)
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(payload)))
	want := crc.ChecksumParts(header[4:], payload)

	b.SetBytes(int64(len(header[4:]) + len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	var sum uint32
	for i := 0; i < b.N; i++ {
		got := crc.ChecksumParts(header[4:], payload)
		if got != want {
			b.Fatalf("checksum=0x%08x want 0x%08x", got, want)
		}
		sum ^= got
	}
	benchmarkValueLogCRCSink = sum
}

func deterministicValueLogCRCBuf(size int) []byte {
	buf := make([]byte, size)
	var x uint64 = 0x1851f00d12345678
	for i := range buf {
		x = x*2862933555777941757 + 3037000493
		buf[i] = byte(x >> 56)
	}
	return buf
}
