package colgranule

import (
	"slices"
	"testing"
)

func TestEncodeDecodeInt64Granule(t *testing.T) {
	values := []int64{-10, -9, -4, 0, 1, 99, 100, 101}
	for _, encoding := range []Encoding{EncodingRawInt64, EncodingDeltaVarint} {
		for _, compression := range []Compression{CompressionNone, CompressionSnappy, CompressionLZ4} {
			g, err := EncodeInt64(nil, values, Config{Encoding: encoding, Compression: compression})
			if err != nil {
				t.Fatalf("EncodeInt64(%s,%s): %v", encoding, compression, err)
			}
			got, err := DecodeInt64(nil, g)
			if err != nil {
				t.Fatalf("DecodeInt64(%s,%s): %v", encoding, compression, err)
			}
			if !slices.Equal(got, values) {
				t.Fatalf("DecodeInt64(%s,%s)=%v want %v", encoding, compression, got, values)
			}
			if g.Min != -10 || g.Max != 101 {
				t.Fatalf("min/max=(%d,%d) want (-10,101)", g.Min, g.Max)
			}
			if g.Rows != len(values) || g.NullCount != 0 || g.DefaultCount != 0 {
				t.Fatalf("metadata rows/null/default=(%d,%d,%d) want (%d,0,0)", g.Rows, g.NullCount, g.DefaultCount, len(values))
			}
			if g.StoredBytes != len(g.Payload) {
				t.Fatalf("stored_bytes=%d want payload len %d", g.StoredBytes, len(g.Payload))
			}
			if g.PayloadRef.Kind != PayloadRefInline || g.PayloadRef.Length != len(g.Payload) {
				t.Fatalf("payload ref=(%s,%d) want (inline,%d)", g.PayloadRef.Kind, g.PayloadRef.Length, len(g.Payload))
			}
		}
	}
}

func TestGranuleBuilderRejectsEmpty(t *testing.T) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingRawInt64, Compression: CompressionNone})
	if _, err := builder.BuildInt64(nil); err == nil {
		t.Fatal("BuildInt64(nil) succeeded, want error")
	}
	if _, err := EncodeInt64(nil, nil, Config{Encoding: EncodingRawInt64, Compression: CompressionNone}); err == nil {
		t.Fatal("EncodeInt64(nil) succeeded, want error")
	}
}

func TestGranuleBuilderMinMaxMetadata(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
		min    int64
		max    int64
	}{
		{name: "negative_positive", values: []int64{-17, -1, 0, 9, 100}, min: -17, max: 100},
		{name: "repeated", values: []int64{42, 42, 42, 42}, min: 42, max: 42},
		{name: "monotonic", values: []int64{-3, -2, -1, 0, 1, 2, 3}, min: -3, max: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, cfg := range []Config{
				{Encoding: EncodingRawInt64, Compression: CompressionNone},
				{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
				{Encoding: EncodingDeltaVarint, Compression: CompressionLZ4},
			} {
				builder := NewGranuleBuilder(cfg)
				g, err := builder.BuildInt64(tt.values)
				if err != nil {
					t.Fatalf("BuildInt64(%s,%s): %v", cfg.Encoding, cfg.Compression, err)
				}
				if g.Rows != len(tt.values) || g.Min != tt.min || g.Max != tt.max {
					t.Fatalf("rows/min/max=(%d,%d,%d) want (%d,%d,%d)", g.Rows, g.Min, g.Max, len(tt.values), tt.min, tt.max)
				}
				if g.RawBytes <= 0 || g.StoredBytes != len(g.Payload) {
					t.Fatalf("raw/stored/payload=(%d,%d,%d), want positive raw and stored payload length", g.RawBytes, g.StoredBytes, len(g.Payload))
				}
				if g.PayloadRef.Kind != PayloadRefInline || g.PayloadRef.Length != g.StoredBytes {
					t.Fatalf("payload ref=(%s,%d) want (inline,%d)", g.PayloadRef.Kind, g.PayloadRef.Length, g.StoredBytes)
				}
			}
		})
	}
}

func TestGranuleBuilderSnappyPayloadDoesNotPrefixScratch(t *testing.T) {
	values := []int64{10, 11, 12, 13, 14, 15, 16, 17}
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy})
	g, err := builder.BuildInt64(values)
	if err != nil {
		t.Fatalf("BuildInt64: %v", err)
	}
	got, err := DecodeInt64(nil, g)
	if err != nil {
		t.Fatalf("DecodeInt64: %v", err)
	}
	if !slices.Equal(got, values) {
		t.Fatalf("DecodeInt64=%v want %v", got, values)
	}
}

func TestGranuleReaderReusesBuffersWithoutStaleValues(t *testing.T) {
	cfg := Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy}
	long, err := EncodeInt64(nil, []int64{1, 2, 3, 4, 5}, cfg)
	if err != nil {
		t.Fatalf("EncodeInt64(long): %v", err)
	}
	shortValues := []int64{100, 101}
	short, err := EncodeInt64(nil, shortValues, cfg)
	if err != nil {
		t.Fatalf("EncodeInt64(short): %v", err)
	}
	var reader GranuleReader
	if _, err := reader.DecodeInt64(long); err != nil {
		t.Fatalf("DecodeInt64(long): %v", err)
	}
	got, err := reader.DecodeInt64(short)
	if err != nil {
		t.Fatalf("DecodeInt64(short): %v", err)
	}
	if !slices.Equal(got, shortValues) {
		t.Fatalf("DecodeInt64(short)=%v want %v", got, shortValues)
	}

	dst := []int64{-1, -1, -1, -1, -1}
	got, err = reader.DecodeInt64Into(dst, short)
	if err != nil {
		t.Fatalf("DecodeInt64Into(short): %v", err)
	}
	if !slices.Equal(got, shortValues) {
		t.Fatalf("DecodeInt64Into(short)=%v want %v", got, shortValues)
	}
}

func TestCorruptPayloadsFailClosed(t *testing.T) {
	values := []int64{-10, -5, 0, 5, 10}
	tests := []struct {
		name string
		cfg  Config
		edit func(EncodedGranule) EncodedGranule
	}{
		{
			name: "raw_truncated",
			cfg:  Config{Encoding: EncodingRawInt64, Compression: CompressionNone},
			edit: func(g EncodedGranule) EncodedGranule {
				g.Payload = g.Payload[:len(g.Payload)-1]
				g.StoredBytes = len(g.Payload)
				g.PayloadRef.Length = len(g.Payload)
				return g
			},
		},
		{
			name: "delta_truncated",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone},
			edit: func(g EncodedGranule) EncodedGranule {
				g.Payload = g.Payload[:len(g.Payload)-1]
				g.StoredBytes = len(g.Payload)
				g.PayloadRef.Length = len(g.Payload)
				return g
			},
		},
		{
			name: "delta_trailing",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone},
			edit: func(g EncodedGranule) EncodedGranule {
				g.Payload = append(append([]byte(nil), g.Payload...), 0)
				g.StoredBytes = len(g.Payload)
				g.PayloadRef.Length = len(g.Payload)
				g.RawBytes = len(g.Payload)
				return g
			},
		},
		{
			name: "delta_huge_rows_tiny_payload",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone},
			edit: func(g EncodedGranule) EncodedGranule {
				g.Rows = maxGranuleDecodeRows + 1
				g.Payload = []byte{0}
				g.StoredBytes = len(g.Payload)
				g.PayloadRef.Length = len(g.Payload)
				g.RawBytes = len(g.Payload)
				return g
			},
		},
		{
			name: "snappy_corrupt",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
			edit: func(g EncodedGranule) EncodedGranule {
				g.Payload = append([]byte(nil), g.Payload...)
				g.Payload = g.Payload[:len(g.Payload)-1]
				g.StoredBytes = len(g.Payload)
				g.PayloadRef.Length = len(g.Payload)
				return g
			},
		},
		{
			name: "metadata_bad_stored_bytes",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
			edit: func(g EncodedGranule) EncodedGranule {
				g.StoredBytes++
				return g
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := EncodeInt64(nil, values, tt.cfg)
			if err != nil {
				t.Fatalf("EncodeInt64: %v", err)
			}
			if _, err := DecodeInt64(nil, tt.edit(g)); err == nil {
				t.Fatal("DecodeInt64(corrupt) succeeded, want error")
			}
		})
	}
}

func TestRangeScanCountUsesMinMaxSkip(t *testing.T) {
	values := []int64{10, 11, 12, 20, 30}
	g, err := EncodeInt64(nil, values, Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy})
	if err != nil {
		t.Fatalf("EncodeInt64: %v", err)
	}
	n, scratch, err := RangeScanCount(g, 100, 200, nil)
	if err != nil {
		t.Fatalf("RangeScanCount(skip): %v", err)
	}
	if n != 0 || len(scratch) != 0 {
		t.Fatalf("skip count=%d scratch=%d want 0,0", n, len(scratch))
	}
	n, _, err = RangeScanCount(g, 11, 20, scratch)
	if err != nil {
		t.Fatalf("RangeScanCount(hit): %v", err)
	}
	if n != 3 {
		t.Fatalf("hit count=%d want 3", n)
	}
}
