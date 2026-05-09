package colgranule

import (
	"fmt"
	"math/rand"
	"testing"
)

var benchSink int64

type fixture struct {
	name   string
	values []int64
}

func benchmarkFixtures() []fixture {
	const n = DefaultRowsPerGranule
	return []fixture{
		{name: "monotonic", values: makeMonotonic(n)},
		{name: "timestamp_jitter", values: makeTimestampJitter(n)},
		{name: "low_cardinality", values: makeLowCardinality(n)},
		{name: "random", values: makeRandom(n)},
	}
}

func benchmarkConfigs() []Config {
	var configs []Config
	for _, encoding := range []Encoding{EncodingRawInt64, EncodingDeltaVarint} {
		for _, compression := range []Compression{CompressionNone, CompressionSnappy, CompressionLZ4} {
			configs = append(configs, Config{Encoding: encoding, Compression: compression})
		}
	}
	return configs
}

func BenchmarkEncodeInt64Granule(b *testing.B) {
	for _, fx := range benchmarkFixtures() {
		for _, cfg := range benchmarkConfigs() {
			name := fmt.Sprintf("%s/%s/%s", fx.name, cfg.Encoding, cfg.Compression)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(fx.values) * 8))
				for i := 0; i < b.N; i++ {
					g, err := EncodeInt64(nil, fx.values, cfg)
					if err != nil {
						b.Fatal(err)
					}
					benchSink += int64(len(g.Payload))
				}
			})
		}
	}
}

func BenchmarkDecodeInt64Granule(b *testing.B) {
	for _, fx := range benchmarkFixtures() {
		for _, cfg := range benchmarkConfigs() {
			g, err := EncodeInt64(nil, fx.values, cfg)
			if err != nil {
				b.Fatal(err)
			}
			name := fmt.Sprintf("%s/%s/%s/stored_%dB/raw_%dB", fx.name, cfg.Encoding, g.Compression, len(g.Payload), g.RawBytes)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(fx.values) * 8))
				scratch := make([]int64, 0, len(fx.values))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					values, err := DecodeInt64(scratch, g)
					if err != nil {
						b.Fatal(err)
					}
					benchSink += values[len(values)-1]
				}
			})
		}
	}
}

func BenchmarkRangeScanInt64Granule(b *testing.B) {
	for _, fx := range benchmarkFixtures() {
		for _, cfg := range benchmarkConfigs() {
			g, err := EncodeInt64(nil, fx.values, cfg)
			if err != nil {
				b.Fatal(err)
			}
			low := fx.values[len(fx.values)/2]
			high := low + 32
			name := fmt.Sprintf("%s/%s/%s/hit/stored_%dB/raw_%dB", fx.name, cfg.Encoding, g.Compression, len(g.Payload), g.RawBytes)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(fx.values) * 8))
				scratch := make([]int64, 0, len(fx.values))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count, out, err := RangeScanCount(g, low, high, scratch)
					if err != nil {
						b.Fatal(err)
					}
					scratch = out
					benchSink += int64(count)
				}
			})
			b.Run(fmt.Sprintf("%s/%s/%s/minmax_skip", fx.name, cfg.Encoding, g.Compression), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					count, _, err := RangeScanCount(g, g.Max+1, g.Max+100, nil)
					if err != nil {
						b.Fatal(err)
					}
					benchSink += int64(count)
				}
			})
		}
	}
}

func TestCompressionRatios(t *testing.T) {
	for _, fx := range benchmarkFixtures() {
		t.Logf("fixture=%s rows=%d raw_values_bytes=%d", fx.name, len(fx.values), len(fx.values)*8)
		for _, cfg := range benchmarkConfigs() {
			g, err := EncodeInt64(nil, fx.values, cfg)
			if err != nil {
				t.Fatalf("EncodeInt64(%s,%s,%s): %v", fx.name, cfg.Encoding, cfg.Compression, err)
			}
			t.Logf("fixture=%s encoding=%s requested_compression=%s actual_compression=%s encoded_raw_bytes=%d stored_bytes=%d ratio_vs_values=%.4f ratio_vs_encoded=%.4f min=%d max=%d",
				fx.name, cfg.Encoding, cfg.Compression, g.Compression, g.RawBytes, len(g.Payload),
				float64(len(g.Payload))/float64(len(fx.values)*8),
				float64(len(g.Payload))/float64(g.RawBytes),
				g.Min, g.Max)
		}
	}
}

func makeMonotonic(n int) []int64 {
	values := make([]int64, n)
	for i := range values {
		values[i] = int64(i)
	}
	return values
}

func makeTimestampJitter(n int) []int64 {
	values := make([]int64, n)
	v := int64(1_700_000_000_000_000)
	for i := range values {
		v += 1000 + int64((i%17)-8)
		values[i] = v
	}
	return values
}

func makeLowCardinality(n int) []int64 {
	values := make([]int64, n)
	for i := range values {
		values[i] = int64((i / 64) % 16)
	}
	return values
}

func makeRandom(n int) []int64 {
	r := rand.New(rand.NewSource(1))
	values := make([]int64, n)
	for i := range values {
		values[i] = r.Int63()
	}
	return values
}
