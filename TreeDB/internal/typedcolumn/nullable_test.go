package typedcolumn

import (
	"encoding/binary"
	"math"
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

func TestColumnPartScannerReadsNullableInt64Encoding(t *testing.T) {
	opts := Options{
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, CompressionSet: true},
			{Name: "value", Type: ColumnTypeInt64, Encoding: EncodingNullableInt64, CompressionSet: true},
			{Name: "flag", Type: ColumnTypeBool, Encoding: EncodingNullableInt64, CompressionSet: true},
			{Name: "code", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Cardinality: 3, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
	}
	part, err := BuildColumnPart(2, opts, Batch{
		Rows: 3,
		Columns: map[string][]int64{
			"id":    {0, 1, 2},
			"value": {10, 20, 30},
			"flag":  {1, 0, 1},
			"code":  {2, 1, 0},
		},
		Nulls: map[string][]bool{
			"value": {false, true, false},
			"flag":  {false, false, true},
			"code":  {false, false, false},
		},
		Defaults: map[string][]bool{
			"value": {false, false, true},
			"flag":  {false, true, false},
			"code":  {false, true, false},
		},
		DefaultValues: map[string]int64{"value": 99, "flag": 0, "code": 1},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	scan, err := part.NewScanner().ScanProjected([]string{"value", "flag", "code"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	if got := scan.Columns["value"]; got[0] != 10 || got[1] != 0 || got[2] != 99 {
		t.Fatalf("value scan=%v", got)
	}
	if got := scan.Columns["flag"]; got[0] != 1 || got[1] != 0 || got[2] != 0 {
		t.Fatalf("flag scan=%v", got)
	}
	if got := scan.Columns["code"]; got[0] != 2 || got[1] != 1 || got[2] != 0 {
		t.Fatalf("code scan=%v", got)
	}
}

func TestBuildColumnPartRejectsInvalidNullableDefaultValue(t *testing.T) {
	cases := []struct {
		name         string
		def          ColumnDefinition
		values       []int64
		defaultValue int64
		want         string
	}{
		{
			name:         "bool",
			def:          ColumnDefinition{Name: "value", Type: ColumnTypeBool, Encoding: EncodingNullableInt64, CompressionSet: true},
			values:       []int64{1, 0},
			defaultValue: 2,
			want:         "bool value 2 outside 0/1",
		},
		{
			name:         "low_cardinality_negative",
			def:          ColumnDefinition{Name: "value", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Cardinality: 3, CompressionSet: true},
			values:       []int64{0, 2},
			defaultValue: -1,
			want:         "code value -1 outside uint32",
		},
		{
			name:         "low_cardinality_overflow",
			def:          ColumnDefinition{Name: "value", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingNullableInt64, Cardinality: 3, CompressionSet: true},
			values:       []int64{0, 2},
			defaultValue: int64(math.MaxUint32) + 1,
			want:         "code value 4294967296 outside uint32",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Columns: []ColumnDefinition{
					{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, CompressionSet: true},
					tc.def,
				},
				LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
			}
			_, err := BuildColumnPart(3, opts, Batch{
				Rows: 2,
				Columns: map[string][]int64{
					"id":    {0, 1},
					"value": tc.values,
				},
				Defaults:      map[string][]bool{"value": {false, true}},
				DefaultValues: map[string]int64{"value": tc.defaultValue},
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("BuildColumnPart err=%v want containing %q", err, tc.want)
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
