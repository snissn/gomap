package hashtournament

import (
	"crypto/sha256"
	"hash/crc32"
	"hash/fnv"
	"hash/maphash"
	"testing"

	"github.com/cespare/xxhash/v2"
	farm "github.com/dgryski/go-farm"
	"github.com/zeebo/xxh3"
)

var (
	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
	mapSeed     = maphash.MakeSeed()
	sink64      uint64
	sink32      uint32
	sinkBytes   [32]byte
)

type inputSize struct {
	name string
	n    int
}

var inputSizes = []inputSize{
	{name: "64KiB_ClickHouseMinCompressBlock", n: 64 << 10},
	{name: "256KiB_Middle", n: 256 << 10},
	{name: "512KiB_WideRowsOneMark", n: 512 << 10},
	{name: "1MiB_ClickHouseMaxCompressBlock", n: 1 << 20},
}

func makeInput(n int) []byte {
	data := make([]byte, n)
	var x uint64 = 0x9e3779b97f4a7c15
	for i := range data {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		data[i] = byte(x)
	}
	return data
}

func BenchmarkContentHashTournament(b *testing.B) {
	for _, size := range inputSizes {
		data := makeInput(size.n)

		b.Run(size.name+"/CRC32C_Castagnoli_TreeDB", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink32 ^= crc32.Checksum(data, crc32cTable)
			}
		})

		b.Run(size.name+"/CRC32_IEEE", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink32 ^= crc32.ChecksumIEEE(data)
			}
		})

		b.Run(size.name+"/XXHash64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= xxhash.Sum64(data)
			}
		})

		b.Run(size.name+"/XXH3_64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= xxh3.Hash(data)
			}
		})

		b.Run(size.name+"/FarmHash64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= farm.Hash64(data)
			}
		})

		b.Run(size.name+"/MapHash_ProcessLocal", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				sink64 ^= maphash.Bytes(mapSeed, data)
			}
		})

		b.Run(size.name+"/FNV1a_64", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			h := fnv.New64a()
			for i := 0; i < b.N; i++ {
				h.Reset()
				_, _ = h.Write(data)
				sink64 ^= h.Sum64()
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
