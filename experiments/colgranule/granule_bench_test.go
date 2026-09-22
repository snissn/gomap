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
	for _, encoding := range []Encoding{EncodingRawInt64, EncodingDeltaVarint, EncodingDoubleDeltaVarint} {
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

func BenchmarkDecodeNullableInt64Granule(b *testing.B) {
	values := makeTimestampJitter(DefaultRowsPerGranule)
	nulls := makeEveryNthBool(DefaultRowsPerGranule, 17)
	defaults := makeEveryNthBool(DefaultRowsPerGranule, 29)
	for i := range defaults {
		if nulls[i] {
			defaults[i] = false
		}
	}
	for _, cfg := range []Config{
		{Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
		{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionLZ4},
	} {
		builder := NewGranuleBuilder(cfg)
		g, err := builder.BuildNullableInt64(values, nulls, defaults, 1_700_000_000_000_000)
		if err != nil {
			b.Fatal(err)
		}
		name := fmt.Sprintf("%s/requested_%s/actual_%s/nulls_%d/defaults_%d/stored_%dB/raw_%dB", cfg.Encoding, cfg.Compression, g.Compression, g.NullCount, g.DefaultCount, g.StoredBytes, g.RawBytes)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(values) * 8))
			var reader GranuleReader
			decoded, _, _, err := reader.DecodeNullableInt64(g)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += decoded[len(decoded)-1]
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decoded, _, _, err := reader.DecodeNullableInt64(g)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += decoded[len(decoded)-1]
			}
			reportGranuleBenchMetrics(b, len(values), len(values)*8, g.StoredBytes)
		})
	}
}

func BenchmarkCountBoolGranule(b *testing.B) {
	fixtures := []struct {
		name   string
		values []bool
	}{
		{name: "alternating", values: makeAlternatingBool(DefaultRowsPerGranule)},
		{name: "runs", values: makeBoolRuns(DefaultRowsPerGranule)},
	}
	for _, fx := range fixtures {
		for _, compression := range []Compression{CompressionNone, CompressionSnappy, CompressionLZ4} {
			builder := NewGranuleBuilder(Config{Compression: compression})
			g, err := builder.BuildBool(fx.values)
			if err != nil {
				b.Fatal(err)
			}
			name := fmt.Sprintf("%s/requested_%s/actual_%s/stored_%dB/raw_%dB", fx.name, compression, g.Compression, g.StoredBytes, g.RawBytes)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(fx.values)))
				var reader GranuleReader
				count, err := reader.CountTrueBool(g)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(count)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count, err := reader.CountTrueBool(g)
					if err != nil {
						b.Fatal(err)
					}
					benchSink += int64(count)
				}
				reportGranuleBenchMetrics(b, len(fx.values), len(fx.values), g.StoredBytes)
			})
		}
	}
}

func BenchmarkCountUint32CodesGranule(b *testing.B) {
	codes := makeUint32Codes(DefaultRowsPerGranule, 16)
	for _, compression := range []Compression{CompressionNone, CompressionSnappy, CompressionLZ4} {
		builder := NewGranuleBuilder(Config{Compression: compression})
		g, err := builder.BuildUint32Codes(codes, 16)
		if err != nil {
			b.Fatal(err)
		}
		name := fmt.Sprintf("cardinality_16/requested_%s/actual_%s/stored_%dB/raw_%dB", compression, g.Compression, g.StoredBytes, g.RawBytes)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(codes) * 4))
			var reader GranuleReader
			counts, err := reader.CountUint32Codes(g, nil)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(counts[0])
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				counts, err = reader.CountUint32Codes(g, counts)
				if err != nil {
					b.Fatal(err)
				}
				benchSink += int64(counts[0])
			}
			reportGranuleBenchMetrics(b, len(codes), len(codes)*4, g.StoredBytes)
		})
	}
}

func BenchmarkPredicatePruningInt64Granules(b *testing.B) {
	const granulesN = 100
	granules, marks, err := buildPredicateBenchmarkGranules(granulesN, DefaultRowsPerGranule)
	if err != nil {
		b.Fatal(err)
	}
	totalRows := granulesN * DefaultRowsPerGranule
	low := int64(totalRows * 9 / 10)
	high := int64(totalRows - 1)
	plan := PredicatePlan{
		Filter:        Int64RangePredicate{Column: "time_us", Low: low, High: high},
		SortKeyRanges: []Int64RangePredicate{{Column: "time_us", Low: low, High: high}},
	}
	b.Run("unpruned_projected_scan", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(totalRows * 8))
		var reader GranuleReader
		count, err := countInt64RangeUnpruned(&reader, granules, low, high)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(count)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count, err = countInt64RangeUnpruned(&reader, granules, low, high)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(count)
		}
		reportGranuleBenchMetrics(b, totalRows, totalRows*8, totalStoredBytes(granules))
	})
	b.Run("pruned_by_sort_key_mark", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(totalRows * 8))
		var reader GranuleReader
		count, diagnostics, err := reader.CountInt64RangeWithDiagnostics(granules, marks, plan)
		if err != nil {
			b.Fatal(err)
		}
		if diagnostics.SkippedByMark < granulesN*9/10 {
			b.Fatalf("skipped_by_mark=%d want at least %d", diagnostics.SkippedByMark, granulesN*9/10)
		}
		benchSink += int64(count + diagnostics.SkippedByMark)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count, diagnostics, err = reader.CountInt64RangeWithDiagnostics(granules, marks, plan)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(count + diagnostics.SkippedByMark)
		}
		reportGranuleBenchMetrics(b, totalRows, totalRows*8, totalStoredBytes(granules))
	})
}

func BenchmarkAggregateGroupedCountCodes(b *testing.B) {
	const granulesN = 100
	const cardinality = 16
	codeGranules, err := buildAggregateCodeGranules(granulesN, DefaultRowsPerGranule, cardinality)
	if err != nil {
		b.Fatal(err)
	}
	totalRows := granulesN * DefaultRowsPerGranule
	b.Run("encoded_kernel", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(totalRows * 4))
		var arena AggregateArena
		counts, err := arena.GroupedCountCodes(codeGranules, cardinality)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(counts[0])
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			counts, err = arena.GroupedCountCodes(codeGranules, cardinality)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(counts[0])
		}
		reportGranuleBenchMetrics(b, totalRows, totalRows*4, totalStoredBytes(codeGranules))
	})
	b.Run("materialized_decode_loop", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(totalRows * 4))
		var reader GranuleReader
		counts := make([]uint64, cardinality)
		if err := countCodesMaterialized(&reader, codeGranules, counts); err != nil {
			b.Fatal(err)
		}
		benchSink += int64(counts[0])
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := countCodesMaterialized(&reader, codeGranules, counts); err != nil {
				b.Fatal(err)
			}
			benchSink += int64(counts[0])
		}
		reportGranuleBenchMetrics(b, totalRows, totalRows*4, totalStoredBytes(codeGranules))
	})
}

func BenchmarkAggregateFilteredGroupedCountCodes(b *testing.B) {
	const granulesN = 100
	const cardinality = 16
	codeGranules, timeGranules, err := buildAggregateCodeTimeGranules(granulesN, DefaultRowsPerGranule, cardinality)
	if err != nil {
		b.Fatal(err)
	}
	totalRows := granulesN * DefaultRowsPerGranule
	filter := Int64RangePredicate{Column: "time_us", Low: int64(totalRows * 9 / 10), High: int64(totalRows - 1)}
	b.ReportAllocs()
	b.SetBytes(int64(totalRows * 12))
	var arena AggregateArena
	counts, diagnostics, err := arena.FilteredGroupedCountCodes(codeGranules, timeGranules, filter, cardinality)
	if err != nil {
		b.Fatal(err)
	}
	benchSink += int64(counts[0] + uint64(diagnostics.SkippedByMinMax))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		counts, diagnostics, err = arena.FilteredGroupedCountCodes(codeGranules, timeGranules, filter, cardinality)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(counts[0] + uint64(diagnostics.SkippedByMinMax))
	}
	reportGranuleBenchMetrics(b, totalRows, totalRows*12, totalStoredBytes(codeGranules)+totalStoredBytes(timeGranules))
}

func BenchmarkAggregateExactDistinctInt64(b *testing.B) {
	const granulesN = 16
	const rowsPerGranule = DefaultRowsPerGranule
	idGranules, err := buildDistinctIDGranules(granulesN, rowsPerGranule)
	if err != nil {
		b.Fatal(err)
	}
	totalRows := granulesN * rowsPerGranule
	b.ReportAllocs()
	b.SetBytes(int64(totalRows * 8))
	var arena AggregateArena
	distinct, err := arena.ExactDistinctInt64(idGranules)
	if err != nil {
		b.Fatal(err)
	}
	benchSink += int64(distinct)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		distinct, err = arena.ExactDistinctInt64(idGranules)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(distinct)
	}
	reportGranuleBenchMetrics(b, totalRows, totalRows*8, totalStoredBytes(idGranules))
}

func BenchmarkColumnPartBuildAndProjectedScan(b *testing.B) {
	const rows = DefaultRowsPerGranule * 100
	batch := buildPartBenchmarkBatch(rows)
	opts := partBenchmarkOptions()
	part, err := BuildColumnPart(1, opts, batch)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("build_in_memory_part", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(rows * len(opts.Columns) * 8))
		builder, err := NewColumnPartBuilder(opts)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		var built *ColumnPart
		for i := 0; i < b.N; i++ {
			built, err = builder.Build(uint64(i+1), batch)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += int64(built.Descriptor.RowCount)
		}
		reportGranuleBenchMetrics(b, rows, rows*len(opts.Columns)*8, totalPartStoredBytes(built))
	})
	b.Run("projected_scan_one_column", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(rows * 8))
		scanner := part.NewScanner()
		projection := []string{"time_us"}
		scanScratch := make(map[string][]int64, len(projection))
		scan, err := scanner.ScanProjectedInto(scanScratch, projection)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += scan.Columns["time_us"][0]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			scan, err = scanner.ScanProjectedInto(scanScratch, projection)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += scan.Columns["time_us"][0]
		}
		reportGranuleBenchMetrics(b, rows, rows*8, totalPartProjectedStoredBytes(part, []string{"time_us"}))
	})
	b.Run("projected_scan_three_columns", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(rows * 3 * 8))
		scanner := part.NewScanner()
		projection := []string{"time_us", "kind_code", "has_reply"}
		scanScratch := make(map[string][]int64, len(projection))
		scan, err := scanner.ScanProjectedInto(scanScratch, projection)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += scan.Columns["time_us"][0] + scan.Columns["kind_code"][0] + scan.Columns["has_reply"][0]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			scan, err = scanner.ScanProjectedInto(scanScratch, projection)
			if err != nil {
				b.Fatal(err)
			}
			benchSink += scan.Columns["time_us"][0] + scan.Columns["kind_code"][0] + scan.Columns["has_reply"][0]
		}
		reportGranuleBenchMetrics(b, rows, rows*3*8, totalPartProjectedStoredBytes(part, projection))
	})
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
	reportGranuleBenchMetrics64(b, rows, int64(valueBytes), int64(storedBytes))
}

func reportGranuleBenchMetrics64(b *testing.B, rows int, valueBytes int64, storedBytes int64) {
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

func makeEveryNthBool(n int, step int) []bool {
	values := make([]bool, n)
	for i := range values {
		values[i] = i%step == 0
	}
	return values
}

func makeAlternatingBool(n int) []bool {
	values := make([]bool, n)
	for i := range values {
		values[i] = i%2 == 0
	}
	return values
}

func makeBoolRuns(n int) []bool {
	values := make([]bool, n)
	for i := range values {
		values[i] = (i/256)%2 == 1
	}
	return values
}

func makeUint32Codes(n int, cardinality uint32) []uint32 {
	values := make([]uint32, n)
	for i := range values {
		values[i] = uint32((i / 32) % int(cardinality))
	}
	return values
}

func buildPredicateBenchmarkGranules(granulesN int, rowsPerGranule int) ([]EncodedGranule, []SortKeyMark, error) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionLZ4})
	granules := make([]EncodedGranule, 0, granulesN)
	marks := make([]SortKeyMark, 0, granulesN)
	for granuleIndex := 0; granuleIndex < granulesN; granuleIndex++ {
		values := make([]int64, rowsPerGranule)
		for i := range values {
			values[i] = int64(granuleIndex*rowsPerGranule + i)
		}
		g, err := builder.BuildInt64(values)
		if err != nil {
			return nil, nil, err
		}
		owned := g
		owned.Payload = append([]byte(nil), g.Payload...)
		mark, err := BuildSortKeyMark([]SortKeyColumnValues{{Name: "time_us", Values: values}})
		if err != nil {
			return nil, nil, err
		}
		granules = append(granules, owned)
		marks = append(marks, mark)
	}
	return granules, marks, nil
}

func countInt64RangeUnpruned(reader *GranuleReader, granules []EncodedGranule, low int64, high int64) (int, error) {
	count := 0
	for _, g := range granules {
		values, err := reader.DecodeInt64(g)
		if err != nil {
			return 0, err
		}
		for _, v := range values {
			if v >= low && v <= high {
				count++
			}
		}
	}
	return count, nil
}

func totalStoredBytes(granules []EncodedGranule) int {
	total := 0
	for _, g := range granules {
		total += g.StoredBytes
	}
	return total
}

func buildAggregateCodeGranules(granulesN int, rowsPerGranule int, cardinality uint32) ([]EncodedGranule, error) {
	builder := NewGranuleBuilder(Config{Compression: CompressionLZ4})
	granules := make([]EncodedGranule, 0, granulesN)
	for granuleIndex := 0; granuleIndex < granulesN; granuleIndex++ {
		codes := makeUint32Codes(rowsPerGranule, cardinality)
		for i := range codes {
			codes[i] = (codes[i] + uint32(granuleIndex)) % cardinality
		}
		g, err := builder.BuildUint32Codes(codes, cardinality)
		if err != nil {
			return nil, err
		}
		owned := g
		owned.Payload = append([]byte(nil), g.Payload...)
		granules = append(granules, owned)
	}
	return granules, nil
}

func buildAggregateCodeTimeGranules(granulesN int, rowsPerGranule int, cardinality uint32) ([]EncodedGranule, []EncodedGranule, error) {
	codeBuilder := NewGranuleBuilder(Config{Compression: CompressionLZ4})
	timeBuilder := NewGranuleBuilder(Config{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionLZ4})
	codeGranules := make([]EncodedGranule, 0, granulesN)
	timeGranules := make([]EncodedGranule, 0, granulesN)
	for granuleIndex := 0; granuleIndex < granulesN; granuleIndex++ {
		codes := makeUint32Codes(rowsPerGranule, cardinality)
		times := make([]int64, rowsPerGranule)
		for i := range times {
			codes[i] = (codes[i] + uint32(granuleIndex)) % cardinality
			times[i] = int64(granuleIndex*rowsPerGranule + i)
		}
		codeGranule, err := codeBuilder.BuildUint32Codes(codes, cardinality)
		if err != nil {
			return nil, nil, err
		}
		timeGranule, err := timeBuilder.BuildInt64(times)
		if err != nil {
			return nil, nil, err
		}
		codeOwned := codeGranule
		timeOwned := timeGranule
		codeOwned.Payload = append([]byte(nil), codeGranule.Payload...)
		timeOwned.Payload = append([]byte(nil), timeGranule.Payload...)
		codeGranules = append(codeGranules, codeOwned)
		timeGranules = append(timeGranules, timeOwned)
	}
	return codeGranules, timeGranules, nil
}

func buildDistinctIDGranules(granulesN int, rowsPerGranule int) ([]EncodedGranule, error) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionLZ4})
	granules := make([]EncodedGranule, 0, granulesN)
	for granuleIndex := 0; granuleIndex < granulesN; granuleIndex++ {
		values := make([]int64, rowsPerGranule)
		for i := range values {
			values[i] = int64(granuleIndex*rowsPerGranule + i)
		}
		g, err := builder.BuildInt64(values)
		if err != nil {
			return nil, err
		}
		owned := g
		owned.Payload = append([]byte(nil), g.Payload...)
		granules = append(granules, owned)
	}
	return granules, nil
}

func countCodesMaterialized(reader *GranuleReader, granules []EncodedGranule, counts []uint64) error {
	clear(counts)
	for _, g := range granules {
		codes, err := reader.DecodeUint32Codes(g)
		if err != nil {
			return err
		}
		for _, code := range codes {
			counts[code]++
		}
	}
	return nil
}

func buildPartBenchmarkBatch(rows int) ColumnBatch {
	columns := map[string][]int64{
		"id":        make([]int64, rows),
		"time_us":   make([]int64, rows),
		"value":     make([]int64, rows),
		"kind_code": make([]int64, rows),
		"has_reply": make([]int64, rows),
	}
	for i := 0; i < rows; i++ {
		columns["id"][i] = int64(rows - i)
		columns["time_us"][i] = int64(i * 1000)
		columns["value"][i] = int64((i * 17) % 1_000_003)
		columns["kind_code"][i] = int64((i / 97) % 16)
		columns["has_reply"][i] = int64((i / 11) & 1)
	}
	return ColumnBatch{Rows: rows, Columns: columns}
}

func partBenchmarkOptions() ColumnStoreOptions {
	return ColumnStoreOptions{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, Compression: CompressionLZ4},
			{Name: "time_us", Type: ColumnTypeInt64, Encoding: EncodingDoubleDeltaVarint, Compression: CompressionLZ4},
			{Name: "value", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, Compression: CompressionLZ4},
			{Name: "kind_code", Type: ColumnTypeLowCardinalityCode, Compression: CompressionLZ4, Cardinality: 16},
			{Name: "has_reply", Type: ColumnTypeBool, Compression: CompressionLZ4},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "time_us"}}},
		PartPolicy: ColumnPartPolicy{
			RowsPerGranule:        DefaultRowsPerGranule,
			DefaultCodecBlockRows: DefaultRowsPerGranule,
		},
	}
}

func totalPartStoredBytes(part *ColumnPart) int {
	if part == nil {
		return 0
	}
	total := 0
	for _, column := range part.Columns {
		for _, block := range column.Blocks {
			total += block.Granule.StoredBytes
		}
	}
	return total
}

func totalPartProjectedStoredBytes(part *ColumnPart, projection []string) int {
	total := 0
	for _, name := range projection {
		column := part.Columns[name]
		for _, block := range column.Blocks {
			total += block.Granule.StoredBytes
		}
	}
	return total
}
