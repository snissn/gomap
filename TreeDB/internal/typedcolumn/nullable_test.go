package typedcolumn

import (
	"encoding/binary"
	"strings"
	"testing"
)

var nullableInt64BenchmarkSink int64

func TestNullableInt64RoundTripAndFailClosedMetadata(t *testing.T) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingNullableInt64, Compression: CompressionNone})
	values := []int64{10, 20, 30, 40}
	nulls := []bool{false, true, false, false}
	defaults := []bool{false, false, true, false}
	g, err := builder.BuildNullableInt64(values, nulls, defaults, 99)
	if err != nil {
		t.Fatalf("BuildNullableInt64: %v", err)
	}
	var reader GranuleReader
	got, gotNulls, gotDefaults, err := reader.DecodeNullableInt64(g)
	if err != nil {
		t.Fatalf("DecodeNullableInt64: %v", err)
	}
	if got[0] != 10 || got[1] != 0 || got[2] != 99 || got[3] != 40 {
		t.Fatalf("values=%v", got)
	}
	if !gotNulls[1] || !gotDefaults[2] {
		t.Fatalf("nulls=%v defaults=%v", gotNulls, gotDefaults)
	}

	tests := []struct {
		name string
		mut  func(EncodedGranule) EncodedGranule
		want string
	}{
		{
			name: "null_default_overlap",
			mut: func(g EncodedGranule) EncodedGranule {
				g.Payload = append([]byte(nil), g.Payload...)
				g.Payload[nullableInt64HeaderBytes+1] |= 1 << 1
				return g
			},
			want: "both null and default",
		},
		{
			name: "bad_null_mask_length",
			mut: func(g EncodedGranule) EncodedGranule {
				g.Payload = append([]byte(nil), g.Payload...)
				binary.LittleEndian.PutUint32(g.Payload[13:17], 2)
				g.RawBytes = len(g.Payload)
				g.StoredBytes = len(g.Payload)
				return g
			},
			want: "null mask bytes",
		},
		{
			name: "stored_rows_mismatch",
			mut: func(g EncodedGranule) EncodedGranule {
				g.Payload = append([]byte(nil), g.Payload...)
				binary.LittleEndian.PutUint32(g.Payload[9:13], 1)
				return g
			},
			want: "stored rows",
		},
		{
			name: "descriptor_null_count_mismatch",
			mut: func(g EncodedGranule) EncodedGranule {
				g.NullCount++
				return g
			},
			want: "null count",
		},
		{
			name: "non_zero_padding",
			mut: func(g EncodedGranule) EncodedGranule {
				g.Payload = append([]byte(nil), g.Payload...)
				g.Payload[nullableInt64HeaderBytes] |= 1 << 7
				return g
			},
			want: "padding",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := reader.DecodeNullableInt64(tt.mut(g))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("DecodeNullableInt64 err=%v want containing %q", err, tt.want)
			}
		})
	}
}

func TestBuildColumnPartRejectsNullableMetadataForNonNullableColumn(t *testing.T) {
	opts := Options{
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, CompressionSet: true},
			{Name: "value", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
	}
	base := Batch{Rows: 2, Columns: map[string][]int64{
		"id":    {0, 1},
		"value": {10, 20},
	}}
	cases := []struct {
		name string
		mut  func(*Batch)
	}{
		{name: "nulls", mut: func(b *Batch) { b.Nulls = map[string][]bool{"value": nil} }},
		{name: "defaults", mut: func(b *Batch) { b.Defaults = map[string][]bool{"value": nil} }},
		{name: "default_value", mut: func(b *Batch) { b.DefaultValues = map[string]int64{"value": 7} }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			batch := base
			tt.mut(&batch)
			_, err := BuildColumnPart(1, opts, batch)
			if err == nil || !strings.Contains(err.Error(), "nullable metadata supplied for non-nullable column value") {
				t.Fatalf("BuildColumnPart err=%v want non-nullable nullable metadata failure", err)
			}
		})
	}
}

func BenchmarkNullableInt64Build(b *testing.B) {
	const rows = 8192
	input := make([]int64, rows)
	nulls := make([]bool, rows)
	defaults := make([]bool, rows)
	for i := 0; i < rows; i++ {
		input[i] = int64(i * 3)
		switch {
		case i%17 == 0:
			nulls[i] = true
		case i%19 == 0:
			defaults[i] = true
		}
	}

	b.Run("non_nullable_delta_baseline", func(b *testing.B) {
		builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
		g, err := builder.BuildInt64(input)
		if err != nil {
			b.Fatalf("warm BuildInt64: %v", err)
		}
		b.SetBytes(int64(g.RawBytes))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			g, err = builder.BuildInt64(input)
			if err != nil {
				b.Fatalf("BuildInt64: %v", err)
			}
		}
		nullableInt64BenchmarkSink = int64(g.RawBytes)
	})

	b.Run("nullable_final", func(b *testing.B) {
		builder := NewGranuleBuilder(Config{Encoding: EncodingNullableInt64, Compression: CompressionNone})
		g, err := builder.BuildNullableInt64(input, nulls, defaults, 0)
		if err != nil {
			b.Fatalf("warm BuildNullableInt64: %v", err)
		}
		b.SetBytes(int64(g.RawBytes))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			g, err = builder.BuildNullableInt64(input, nulls, defaults, 0)
			if err != nil {
				b.Fatalf("BuildNullableInt64: %v", err)
			}
		}
		nullableInt64BenchmarkSink = int64(g.RawBytes)
	})
}

func BenchmarkNullableInt64DecodeInto(b *testing.B) {
	const rows = 8192
	input := make([]int64, rows)
	nulls := make([]bool, rows)
	defaults := make([]bool, rows)
	for i := 0; i < rows; i++ {
		input[i] = int64(i * 3)
		switch {
		case i%17 == 0:
			nulls[i] = true
		case i%19 == 0:
			defaults[i] = true
		}
	}

	b.Run("non_nullable_raw_baseline", func(b *testing.B) {
		builder := NewGranuleBuilder(Config{Encoding: EncodingRawInt64, Compression: CompressionNone})
		g, err := builder.BuildInt64(input)
		if err != nil {
			b.Fatalf("BuildInt64: %v", err)
		}
		var reader GranuleReader
		values := make([]int64, rows)
		values, err = reader.DecodeInt64Into(values[:0], g)
		if err != nil {
			b.Fatalf("warm DecodeInt64Into: %v", err)
		}
		b.SetBytes(int64(g.RawBytes))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			values, err = reader.DecodeInt64Into(values[:0], g)
			if err != nil {
				b.Fatalf("DecodeInt64Into: %v", err)
			}
		}
		nullableInt64BenchmarkSink = values[rows-1]
	})

	b.Run("nullable_final", func(b *testing.B) {
		builder := NewGranuleBuilder(Config{Encoding: EncodingNullableInt64, Compression: CompressionNone})
		g, err := builder.BuildNullableInt64(input, nulls, defaults, 0)
		if err != nil {
			b.Fatalf("BuildNullableInt64: %v", err)
		}
		var reader GranuleReader
		values := make([]int64, rows)
		nullScratch := make([]bool, rows)
		defaultScratch := make([]bool, rows)
		values, nullScratch, defaultScratch, err = reader.DecodeNullableInt64Into(values[:0], nullScratch[:0], defaultScratch[:0], g)
		if err != nil {
			b.Fatalf("warm DecodeNullableInt64Into: %v", err)
		}
		b.SetBytes(int64(g.RawBytes))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			values, nullScratch, defaultScratch, err = reader.DecodeNullableInt64Into(values[:0], nullScratch[:0], defaultScratch[:0], g)
			if err != nil {
				b.Fatalf("DecodeNullableInt64Into: %v", err)
			}
		}
		if nullScratch[0] && defaultScratch[19] {
			nullableInt64BenchmarkSink = values[rows-1]
		}
	})
}
