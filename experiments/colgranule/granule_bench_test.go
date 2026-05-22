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
				builder := NewGranuleBuilder(cfg)
				g, err := builder.BuildInt64(fx.values)
				if err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					g, err = builder.BuildInt64(fx.values)
					if err != nil {
						b.Fatal(err)
					}
					benchSink += int64(g.StoredBytes)
				}
				reportGranuleBenchMetrics(b, len(fx.values), len(fx.values)*8, g.StoredBytes)
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
			name := fmt.Sprintf("%s/%s/requested_%s/actual_%s/stored_%dB/raw_%dB", fx.name, cfg.Encoding, cfg.Compression, g.Compression, g.StoredBytes, g.RawBytes)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(fx.values) * 8))
				var reader GranuleReader
				values, err := reader.DecodeInt64(g)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += values[len(values)-1]
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					values, err := reader.DecodeInt64(g)
					if err != nil {
						b.Fatal(err)
					}
					benchSink += values[len(values)-1]
				}
				reportGranuleBenchMetrics(b, len(fx.values), len(fx.values)*8, g.StoredBytes)
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
			name := fmt.Sprintf("%s/%s/requested_%s/actual_%s/hit/stored_%dB/raw_%dB", fx.name, cfg.Encoding, cfg.Compression, g.Compression, g.StoredBytes, g.RawBytes)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(fx.values) * 8))
				var reader GranuleReader
				count, err := reader.RangeScanCountInt64(g, low, high)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(count)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count, err := reader.RangeScanCountInt64(g, low, high)
					if err != nil {
						b.Fatal(err)
					}
					benchSink += int64(count)
				}
				reportGranuleBenchMetrics(b, len(fx.values), len(fx.values)*8, g.StoredBytes)
			})
			b.Run(fmt.Sprintf("%s/%s/requested_%s/actual_%s/minmax_skip", fx.name, cfg.Encoding, cfg.Compression, g.Compression), func(b *testing.B) {
				b.ReportAllocs()
				var reader GranuleReader
				for i := 0; i < b.N; i++ {
					count, err := reader.RangeScanCountInt64(g, g.Max+1, g.Max+100)
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
				fx.name, cfg.Encoding, cfg.Compression, g.Compression, g.RawBytes, g.StoredBytes,
				float64(g.StoredBytes)/float64(len(fx.values)*8),
				float64(g.StoredBytes)/float64(g.RawBytes),
				g.Min, g.Max)
		}
	}
}

func reportGranuleBenchMetrics(b *testing.B, rows int, valueBytes int, storedBytes int) {
	b.Helper()
	if b.N == 0 || rows == 0 {
		return
	}
	elapsed := b.Elapsed()
	totalRows := float64(b.N) * float64(rows)
	if elapsed > 0 {
		seconds := elapsed.Seconds()
		b.ReportMetric(totalRows/seconds, "rows/s")
		b.ReportMetric(totalRows/seconds, "values/s")
		b.ReportMetric(float64(b.N)*float64(valueBytes)/seconds/(1024*1024), "MiB/s")
		b.ReportMetric(float64(elapsed.Nanoseconds())/totalRows, "ns/row")
	}
	b.ReportMetric(float64(valueBytes)/float64(rows), "value_B/row")
	b.ReportMetric(float64(storedBytes)/float64(rows), "stored_B/row")
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
