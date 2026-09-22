package page

import "testing"

var benchmarkPageCRCSink uint32
var benchmarkPageBoolSink bool

func BenchmarkPageChecksumUpdateVerify(b *testing.B) {
	buf := deterministicPageCRCBuf(PageSize)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	var sum uint32
	var ok bool
	for i := 0; i < b.N; i++ {
		buf[16] = byte(i)
		sum ^= UpdateChecksum(buf)
		ok = VerifyChecksumNonMutating(buf)
		if !ok {
			b.Fatal("checksum verification failed")
		}
	}
	b.StopTimer()
	benchmarkPageCRCSink = sum
	benchmarkPageBoolSink = ok
}

func BenchmarkPageChecksumVerifyOnly(b *testing.B) {
	buf := deterministicPageCRCBuf(PageSize)
	UpdateChecksum(buf)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		ok = VerifyChecksumNonMutating(buf)
		if !ok {
			b.Fatal("checksum verification failed")
		}
	}
	b.StopTimer()
	benchmarkPageBoolSink = ok
}

func deterministicPageCRCBuf(size int) []byte {
	buf := make([]byte, size)
	var x uint32 = 0x9e3779b9
	for i := range buf {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		buf[i] = byte(x)
	}
	return buf
}
