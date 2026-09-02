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

	b.Run("strict_verify", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw, ref, true, ColumnAssetReadIntegrityVerify, "", columnAssetVerifiedChecksumFileIdentity{}); err != nil {
				b.Fatal(err)
			}
		}
		benchmarkColumnAssetCRCSink = ref.Checksum
		benchmarkColumnAssetBoolSink = true
	})

	b.Run("cached_verify_prepare_miss", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			identity := columnAssetVerifiedChecksumFileIdentity{
				dev:             1,
				ino:             2,
				size:            int64(len(raw)),
				modTimeUnixNano: int64(i + 1),
				valid:           true,
			}
			if err := verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw, ref, true, ColumnAssetReadIntegrityCachedVerify, "crc-bench-root", identity); err != nil {
				b.Fatal(err)
			}
		}
		benchmarkColumnAssetCRCSink = ref.Checksum
		benchmarkColumnAssetBoolSink = true
	})
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
