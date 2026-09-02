package hashtournament

import (
	"crypto/sha256"
	crc32 "github.com/snissn/go-crc32-asm"
	"hash/crc64"
	"hash/fnv"
	"hash/maphash"
	"testing"

	"github.com/cespare/xxhash/v2"
	farm "github.com/dgryski/go-farm"
	"github.com/zeebo/xxh3"
)

var (
	crc32Table   = crc32.MakeTable(crc32.IEEE)
	koopmanTable = crc32.MakeTable(crc32.Koopman)
	crc64ECMA    = crc64.MakeTable(crc64.ECMA)
	crc64ISO     = crc64.MakeTable(crc64.ISO)
	mapSeed      = maphash.MakeSeed()
	sink64       uint64
	sink32       uint32
	sinkByte     byte
	sinkBytes28  [28]byte
	sinkBytes    [32]byte
)

const (
	// Deterministic seeds keep seeded benchmark variants reproducible.
	seed64A uint64 = 0x9e3779b97f4a7c15
	seed64B uint64 = 0xbf58476d1ce4e5b9
	seed32A uint32 = 0x9e3779b9
)

// inputSize names one payload length used by the benchmark matrix.
type inputSize struct {
	name string
	n    int
}

// inputSizes covers the block sizes used by the content hash tournament.
var inputSizes = []inputSize{
	{name: "64KiB_ClickHouseMinCompressBlock", n: 64 << 10},
	{name: "256KiB_Middle", n: 256 << 10},
	{name: "512KiB_WideRowsOneMark", n: 512 << 10},
	{name: "1MiB_ClickHouseMaxCompressBlock", n: 1 << 20},
}

// makeInput builds deterministic pseudo-random input data for each benchmark size.
func makeInput(n int) []byte {
	data := make([]byte, n)
	x := seed64A
	for i := range data {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		data[i] = byte(x)
	}
	return data
}

// BenchmarkContentHashTournament compares content hash throughput across block-sized inputs.
func BenchmarkContentHashTournament(b *testing.B) {
	for _, size := range inputSizes {
		data := makeInput(size.n)

		b.Run(size.name+"/CRC32_IEEE_Table_TreeDB", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink32 ^= crc32.Checksum(data, crc32Table)
			}
		})

		b.Run(size.name+"/CRC32_IEEE", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink32 ^= crc32.ChecksumIEEE(data)
			}
		})

		b.Run(size.name+"/CRC32_Koopman", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink32 ^= crc32.Checksum(data, koopmanTable)
			}
		})

		b.Run(size.name+"/CRC64_ECMA", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= crc64.Checksum(data, crc64ECMA)
			}
		})

		b.Run(size.name+"/CRC64_ISO", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= crc64.Checksum(data, crc64ISO)
			}
		})

		b.Run(size.name+"/XXHash64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= xxhash.Sum64(data)
			}
		})

		b.Run(size.name+"/XXHash64_Digest", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := xxhash.New()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink64 ^= h.Sum64()
			}
		})

		b.Run(size.name+"/XXHash64_DigestSeeded", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := xxhash.NewWithSeed(seed64A)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink64 ^= h.Sum64()
			}
		})

		b.Run(size.name+"/XXH3_64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= xxh3.Hash(data)
			}
		})

		b.Run(size.name+"/XXH3_64Seeded", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= xxh3.HashSeed(data, seed64A)
			}
		})

		b.Run(size.name+"/XXH3_128", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sum := xxh3.Hash128(data)
				sink64 ^= sum.Lo ^ sum.Hi
			}
		})

		b.Run(size.name+"/XXH3_128Seeded", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sum := xxh3.Hash128Seed(data, seed64A)
				sink64 ^= sum.Lo ^ sum.Hi
			}
		})

		b.Run(size.name+"/XXH3_64_Hasher", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := xxh3.New()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink64 ^= h.Sum64()
			}
		})

		b.Run(size.name+"/XXH3_64_HasherSeeded", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := xxh3.NewSeed(seed64A)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink64 ^= h.Sum64()
			}
		})

		b.Run(size.name+"/XXH3_128_Hasher", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := xxh3.New()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sum := h.Sum128()
				sink64 ^= sum.Lo ^ sum.Hi
			}
		})

		b.Run(size.name+"/XXH3_128_HasherSeeded", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := xxh3.NewSeed(seed64A)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sum := h.Sum128()
				sink64 ^= sum.Lo ^ sum.Hi
			}
		})

		b.Run(size.name+"/FarmHash64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= farm.Hash64(data)
			}
		})

		b.Run(size.name+"/FarmHash64Seeded", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= farm.Hash64WithSeed(data, seed64A)
			}
		})

		b.Run(size.name+"/FarmHash64TwoSeeds", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= farm.Hash64WithSeeds(data, seed64A, seed64B)
			}
		})

		b.Run(size.name+"/FarmFingerprint64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= farm.Fingerprint64(data)
			}
		})

		b.Run(size.name+"/FarmHash128", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				lo, hi := farm.Hash128(data)
				sink64 ^= lo ^ hi
			}
		})

		b.Run(size.name+"/FarmHash128Seeded", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				lo, hi := farm.Hash128WithSeed(data, seed64A, seed64B)
				sink64 ^= lo ^ hi
			}
		})

		b.Run(size.name+"/FarmFingerprint128", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				lo, hi := farm.Fingerprint128(data)
				sink64 ^= lo ^ hi
			}
		})

		b.Run(size.name+"/FarmHash32", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink32 ^= farm.Hash32(data)
			}
		})

		b.Run(size.name+"/FarmHash32Seeded", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink32 ^= farm.Hash32WithSeed(data, seed32A)
			}
		})

		b.Run(size.name+"/FarmFingerprint32", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink32 ^= farm.Fingerprint32(data)
			}
		})

		b.Run(size.name+"/MapHash_ProcessLocal", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= maphash.Bytes(mapSeed, data)
			}
		})

		b.Run(size.name+"/FNV1_64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := fnv.New64()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink64 ^= h.Sum64()
			}
		})

		b.Run(size.name+"/FNV1a_64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := fnv.New64a()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink64 ^= h.Sum64()
			}
		})

		b.Run(size.name+"/FNV1_32", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := fnv.New32()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink32 ^= h.Sum32()
			}
		})

		b.Run(size.name+"/FNV1a_32", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := fnv.New32a()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink32 ^= h.Sum32()
			}
		})

		b.Run(size.name+"/FNV1_128", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := fnv.New128()
			sum := make([]byte, 0, h.Size())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sum = h.Sum(sum[:0])
				sinkByte ^= sum[0]
			}
		})

		b.Run(size.name+"/FNV1a_128", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := fnv.New128a()
			sum := make([]byte, 0, h.Size())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sum = h.Sum(sum[:0])
				sinkByte ^= sum[0]
			}
		})

		b.Run(size.name+"/SHA224", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sinkBytes28 = sha256.Sum224(data)
			}
		})

		b.Run(size.name+"/SHA256", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sinkBytes = sha256.Sum256(data)
			}
		})
	}
}
