package valuelog

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/snissn/compress/zstd"
)

func BenchmarkValueLogDict_EncodeAllPartsVsConcat_NoIO(b *testing.B) {
	dict, err := buildFallbackBenchDict(1)
	if err != nil {
		b.Fatalf("build dict: %v", err)
	}
	codecs := getDictCodecs(1, dict)
	if codecs == nil || codecs.encPool == nil {
		b.Fatalf("missing codecs")
	}
	enc := codecs.encPool.Get().(*zstd.Encoder)
	b.Cleanup(func() { codecs.encPool.Put(enc) })

	type tc struct {
		name      string
		valueSize int
		k         int
	}
	cases := []tc{
		{name: "val=1k/k=4", valueSize: 1 << 10, k: 4},
		{name: "val=4k/k=32", valueSize: 4 << 10, k: 32},
		{name: "val=8k/k=32", valueSize: 8 << 10, k: 32},
		{name: "val=16k/k=4", valueSize: 16 << 10, k: 4},
		{name: "val=32k/k=4", valueSize: 32 << 10, k: 4},
		{name: "val=64k/k=4", valueSize: 64 << 10, k: 4},
		{name: "val=256k/k=4", valueSize: 256 << 10, k: 4},
		{name: "val=16k/k=32", valueSize: 16 << 10, k: 32},
	}

	rng := rand.New(rand.NewSource(1))
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			if c.k <= 0 || c.k > MaxFrameK {
				b.Skip("invalid k")
			}
			values := make([][]byte, c.k)
			rawPayloadBytes := 0
			for i := 0; i < c.k; i++ {
				buf := make([]byte, c.valueSize)
				copy(buf, bytes.Repeat([]byte("{\"key\":\"value\",\"active\":true}"), (c.valueSize/24)+1))
				if len(buf) > 64 {
					_, _ = rng.Read(buf[len(buf)-64:])
				}
				values[i] = buf
				rawPayloadBytes += len(buf)
			}

			payload := make([]byte, rawPayloadBytes)
			dst := make([]byte, 0, enc.MaxEncodedSize(rawPayloadBytes)+3*(c.k-1))
			var parts [MaxFrameK][]byte
			for i := 0; i < c.k; i++ {
				parts[i] = values[i]
			}

			b.SetBytes(int64(rawPayloadBytes))

			b.Run("concat+EncodeAll", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					off := 0
					for j := 0; j < c.k; j++ {
						off += copy(payload[off:], values[j])
					}
					dst = enc.EncodeAll(payload, dst[:0])
				}
				_ = dst
			})

			b.Run("EncodeAllParts", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dst = enc.EncodeAllParts(parts[:c.k], dst[:0])
				}
				_ = dst
			})
		})
	}
}
