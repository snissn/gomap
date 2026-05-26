package collections

import (
	"fmt"
	"math"
	"testing"
	"time"
)

var typedColumnFloatFallbackBenchSink typedColumnFloatFallbackResult

func BenchmarkTypedColumnFloatFallback(b *testing.B) {
	const rows = 65536
	for _, tc := range []struct {
		name      string
		valueType ColumnStoreValueType
	}{
		{name: "float32", valueType: ColumnStoreValueFloat32},
		{name: "double", valueType: ColumnStoreValueDouble},
	} {
		cells := typedColumnFloatFallbackBenchmarkCells(tc.valueType, rows)
		for _, bc := range []struct {
			name      string
			predicate typedColumnFloatFallbackPredicate
		}{
			{name: "equality", predicate: typedColumnFloatFallbackBenchmarkEqualityPredicate(tc.valueType)},
			{name: "range", predicate: typedColumnFloatFallbackBenchmarkRangePredicate(tc.valueType)},
			{name: "aggregate_all", predicate: typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackAll}},
		} {
			b.Run(fmt.Sprintf("%s/%s", tc.name, bc.name), func(b *testing.B) {
				preview := runTypedColumnFloatFallback(cells, tc.valueType, bc.predicate)
				if preview.RowsScanned != rows || preview.Diagnostics.FallbackBlocks == 0 {
					b.Fatalf("preview=%+v want rows and fallback diagnostics", preview)
				}
				b.ReportAllocs()
				b.ResetTimer()
				start := time.Now()
				var result typedColumnFloatFallbackResult
				for i := 0; i < b.N; i++ {
					result = runTypedColumnFloatFallback(cells, tc.valueType, bc.predicate)
				}
				b.StopTimer()
				elapsed := time.Since(start)
				typedColumnFloatFallbackBenchSink = result
				if result.RowsScanned != preview.RowsScanned || result.RowsMatched != preview.RowsMatched || result.NaNRows != preview.NaNRows {
					b.Fatalf("result=%+v preview=%+v", result, preview)
				}
				if bc.predicate.Kind == typedColumnFloatFallbackAll {
					if result.NonNulls != preview.NonNulls || result.HasMinMax != preview.HasMinMax ||
						!typedColumnFloatFallbackSameFloat64(result.Min, preview.Min) ||
						!typedColumnFloatFallbackSameFloat64(result.Max, preview.Max) ||
						!typedColumnFloatFallbackSameFloat64(result.Sum, preview.Sum) ||
						!typedColumnFloatFallbackSameFloat64(result.Avg, preview.Avg) {
						b.Fatalf("aggregate result=%+v preview=%+v", result, preview)
					}
				}
				reportTypedColumnFloatFallbackBenchMetrics(b, result, elapsed, b.N)
			})
		}
	}
}

func typedColumnFloatFallbackBenchmarkCells(valueType ColumnStoreValueType, rows int) []typedColumnFloatFallbackCell {
	cells := make([]typedColumnFloatFallbackCell, rows)
	for i := range cells {
		cell := typedColumnFloatFallbackCell{Present: true, Visible: true}
		switch i % 64 {
		case 0:
			if valueType == ColumnStoreValueFloat32 {
				cell.F32 = float32(math.Inf(-1))
			} else {
				cell.F64 = math.Inf(-1)
			}
		case 1:
			if valueType == ColumnStoreValueFloat32 {
				cell.F32 = float32(math.Copysign(0, -1))
			} else {
				cell.F64 = math.Copysign(0, -1)
			}
		case 2:
			// Equality target.
			if valueType == ColumnStoreValueFloat32 {
				cell.F32 = 42.25
			} else {
				cell.F64 = 42.25
			}
		case 3:
			if valueType == ColumnStoreValueFloat32 {
				cell.F32 = float32(math.Inf(1))
			} else {
				cell.F64 = math.Inf(1)
			}
		case 4:
			if valueType == ColumnStoreValueFloat32 {
				cell.F32 = math.Float32frombits(0x7fc01234)
			} else {
				cell.F64 = math.Float64frombits(0x7ff8000000001234)
			}
		default:
			value := float64((i%200)-100) / 2
			if valueType == ColumnStoreValueFloat32 {
				cell.F32 = float32(value)
			} else {
				cell.F64 = value
			}
		}
		cells[i] = cell
	}
	return cells
}

func typedColumnFloatFallbackBenchmarkEqualityPredicate(valueType ColumnStoreValueType) typedColumnFloatFallbackPredicate {
	if valueType == ColumnStoreValueFloat32 {
		return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F32: 42.25}
	}
	return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackEqual, F64: 42.25}
}

func typedColumnFloatFallbackBenchmarkRangePredicate(valueType ColumnStoreValueType) typedColumnFloatFallbackPredicate {
	if valueType == ColumnStoreValueFloat32 {
		return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackRange, Min32: -10, Max32: 10}
	}
	return typedColumnFloatFallbackPredicate{Kind: typedColumnFloatFallbackRange, Min64: -10, Max64: 10}
}

func reportTypedColumnFloatFallbackBenchMetrics(b *testing.B, result typedColumnFloatFallbackResult, elapsed time.Duration, iterations int) {
	b.Helper()
	if elapsed > 0 && iterations > 0 {
		b.ReportMetric(float64(iterations)/elapsed.Seconds(), "ops/sec")
		b.ReportMetric(float64(result.RowsScanned*int64(iterations))/elapsed.Seconds(), "rows/sec")
		b.ReportMetric(float64(result.RowsMatched*int64(iterations))/elapsed.Seconds(), "matches/sec")
	}
	b.ReportMetric(float64(result.RowsScanned), "rows_scanned/op")
	b.ReportMetric(float64(result.RowsMatched), "rows_matched/op")
	b.ReportMetric(float64(result.NonNulls), "non_nulls/op")
	b.ReportMetric(float64(result.NaNRows), "nan_rows/op")
	b.ReportMetric(float64(result.NullRows), "null_rows/op")
	b.ReportMetric(float64(result.DefaultRows), "default_rows/op")
	b.ReportMetric(float64(result.VisibilityExcludedRows), "visibility_excluded_rows/op")
	diag := result.Diagnostics
	b.ReportMetric(float64(diag.PhysicalBytesScanned), "physical_bytes_scanned/op")
	b.ReportMetric(float64(diag.MappedBytes), "mapped_bytes/op")
	b.ReportMetric(float64(diag.DecodedBytes), "decoded_bytes/op")
	b.ReportMetric(float64(diag.FallbackBlocks), "fallback_blocks/op")
	b.ReportMetric(float64(diag.NativeFloatLayoutMissingFallbacks), "fallback_native_float_layout_missing/op")
	b.ReportMetric(float64(diag.RawInt64BitPatternRejectedFallback), "fallback_float_raw_int64_bit_pattern/op")
}
