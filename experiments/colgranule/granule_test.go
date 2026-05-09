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
		}
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
