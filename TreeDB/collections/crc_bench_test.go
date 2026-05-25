package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

var benchmarkColumnAssetCRCSink uint32
var benchmarkColumnAssetBoolSink bool

func BenchmarkColumnPhysicalAssetChecksumVerify(b *testing.B) {
	raw := deterministicColumnAssetCRCBuf(4 << 20)
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1TypedColumnPart,
		Namespace:  "crc-bench",
		Generation: 1,
		PartID:     1851,
		FileID:     1,
		Length:     int64(len(raw)),
		Checksum:   page.Checksum(raw),
	}
	if ref.Checksum == 0 {
		b.Fatal("unexpected zero checksum for deterministic asset payload")
	}

	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	var sum uint32
	var ok bool
	for i := 0; i < b.N; i++ {
		got := page.Checksum(raw)
		ok = got == ref.Checksum
		if !ok {
			b.Fatalf("column physical asset checksum=%d want %d", got, ref.Checksum)
		}
		sum ^= got
	}
	benchmarkColumnAssetCRCSink = sum
	benchmarkColumnAssetBoolSink = ok
}

func deterministicColumnAssetCRCBuf(size int) []byte {
	buf := make([]byte, size)
	var x uint32 = 0x51ed1851
	for i := range buf {
		x += 0x9e3779b9
		x ^= x >> 16
		x *= 0x85ebca6b
		x ^= x >> 13
		buf[i] = byte(x >> 8)
	}
	return buf
}
