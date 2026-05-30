package typedcolumn

import (
	"math"
	"testing"
)

func TestGranuleReaderInt64CursorDeltaAndDoubleDelta(t *testing.T) {
	values := []int64{10, 12, 17, 17, -3, 100}
	for _, enc := range []Encoding{EncodingDeltaVarint, EncodingDoubleDeltaVarint, EncodingRawInt64} {
		t.Run(enc.String(), func(t *testing.T) {
			builder := NewGranuleBuilder(Config{Encoding: enc, Compression: CompressionNone})
			g, err := builder.BuildInt64(values)
			if err != nil {
				t.Fatalf("BuildInt64: %v", err)
			}
			var reader GranuleReader
			cursor, err := reader.Int64Cursor(g)
			if err != nil {
				t.Fatalf("Int64Cursor: %v", err)
			}
			for i, want := range values {
				got, err := cursor.Next()
				if err != nil {
					t.Fatalf("Next[%d]: %v", i, err)
				}
				if got != want {
					t.Fatalf("Next[%d]=%d want %d", i, got, want)
				}
			}
			if err := cursor.Finish(); err != nil {
				t.Fatalf("Finish: %v", err)
			}
			count, sum, err := reader.CountSumInt64(g)
			if err != nil {
				t.Fatalf("CountSumInt64: %v", err)
			}
			if count != len(values) || sum != 153 {
				t.Fatalf("CountSumInt64 count=%d sum=%d want count=%d sum=153", count, sum, len(values))
			}
		})
	}
}

func TestGranuleReaderInt64CursorPreservesEncoderWraparound(t *testing.T) {
	values := []int64{math.MaxInt64, math.MinInt64}
	for _, enc := range []Encoding{EncodingDeltaVarint, EncodingDoubleDeltaVarint} {
		t.Run(enc.String(), func(t *testing.T) {
			builder := NewGranuleBuilder(Config{Encoding: enc, Compression: CompressionNone})
			g, err := builder.BuildInt64(values)
			if err != nil {
				t.Fatalf("BuildInt64: %v", err)
			}
			var reader GranuleReader
			cursor, err := reader.Int64Cursor(g)
			if err != nil {
				t.Fatalf("Int64Cursor: %v", err)
			}
			for i, want := range values {
				got, err := cursor.Next()
				if err != nil {
					t.Fatalf("Next[%d]: %v", i, err)
				}
				if got != want {
					t.Fatalf("Next[%d]=%d want %d", i, got, want)
				}
			}
			if err := cursor.Finish(); err != nil {
				t.Fatalf("Finish: %v", err)
			}
			count, sum, err := reader.CountSumInt64(g)
			if err != nil {
				t.Fatalf("CountSumInt64: %v", err)
			}
			if count != len(values) || sum != -1 {
				t.Fatalf("CountSumInt64 count=%d sum=%d want count=%d sum=-1", count, sum, len(values))
			}
		})
	}
}

func TestGranuleReaderInt64CursorFinishDetectsShortRead(t *testing.T) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	g, err := builder.BuildInt64([]int64{1, 2, 3})
	if err != nil {
		t.Fatalf("BuildInt64: %v", err)
	}
	var reader GranuleReader
	cursor, err := reader.Int64Cursor(g)
	if err != nil {
		t.Fatalf("Int64Cursor: %v", err)
	}
	if _, err := cursor.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := cursor.Finish(); err == nil {
		t.Fatal("Finish err=nil want short-read failure")
	}
}

func BenchmarkGranuleReaderInt64DeltaCursorVsDecode(b *testing.B) {
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(i*i/7 - i/3)
	}
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	g, err := builder.BuildInt64(values)
	if err != nil {
		b.Fatalf("BuildInt64: %v", err)
	}
	b.Run("streaming_cursor", func(b *testing.B) {
		var reader GranuleReader
		b.ReportAllocs()
		b.SetBytes(int64(g.RawBytes))
		b.ReportMetric(float64(len(values)), "values/op")
		b.ResetTimer()
		var sum int64
		for i := 0; i < b.N; i++ {
			cursor, err := reader.Int64Cursor(g)
			if err != nil {
				b.Fatalf("Int64Cursor: %v", err)
			}
			for row := 0; row < cursor.Rows(); row++ {
				v, err := cursor.Next()
				if err != nil {
					b.Fatalf("Next: %v", err)
				}
				sum += v
			}
			if err := cursor.Finish(); err != nil {
				b.Fatalf("Finish: %v", err)
			}
		}
		if sum == 42 {
			b.Fatal(sum)
		}
	})
	b.Run("streaming_count_sum", func(b *testing.B) {
		var reader GranuleReader
		b.ReportAllocs()
		b.SetBytes(int64(g.RawBytes))
		b.ReportMetric(float64(len(values)), "values/op")
		b.ResetTimer()
		var total int64
		for i := 0; i < b.N; i++ {
			count, sum, err := reader.CountSumInt64(g)
			if err != nil {
				b.Fatalf("CountSumInt64: %v", err)
			}
			if count != len(values) {
				b.Fatalf("CountSumInt64 count=%d want %d", count, len(values))
			}
			total += sum
		}
		if total == 42 {
			b.Fatal(total)
		}
	})
	b.Run("decode_into_scratch", func(b *testing.B) {
		var reader GranuleReader
		scratch := make([]int64, 0, len(values))
		b.ReportAllocs()
		b.SetBytes(int64(g.RawBytes))
		b.ReportMetric(float64(len(values)), "values/op")
		b.ResetTimer()
		var sum int64
		for i := 0; i < b.N; i++ {
			out, err := reader.DecodeInt64Into(scratch[:0], g)
			if err != nil {
				b.Fatalf("DecodeInt64Into: %v", err)
			}
			for _, v := range out {
				sum += v
			}
		}
		if sum == 42 {
			b.Fatal(sum)
		}
	})
}
