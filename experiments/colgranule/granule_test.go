package colgranule

import (
	"slices"
	"testing"
)

func TestEncodeDecodeInt64Granule(t *testing.T) {
	values := []int64{-10, -9, -4, 0, 1, 99, 100, 101}
	for _, encoding := range []Encoding{EncodingRawInt64, EncodingDeltaVarint, EncodingDoubleDeltaVarint} {
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
			if !g.HasMinMax {
				t.Fatalf("HasMinMax=false want true")
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
			if g.CodecReport.Encoding != encoding || g.CodecReport.ActualCompression != g.Compression || g.CodecReport.StoredBytes != g.StoredBytes {
				t.Fatalf("codec report=%+v granule compression=%s stored=%d", g.CodecReport, g.Compression, g.StoredBytes)
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

func TestGranuleBuilderRejectsOversizedRows(t *testing.T) {
	values := make([]int64, maxGranuleDecodeRows+1)
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	if _, err := builder.BuildInt64(values); err == nil {
		t.Fatal("BuildInt64(oversized) succeeded, want error")
	}
	if _, err := EncodeInt64(nil, values, Config{Encoding: EncodingRawInt64, Compression: CompressionNone}); err == nil {
		t.Fatal("EncodeInt64(oversized) succeeded, want error")
	}
	if _, err := builder.BuildNullableInt64(values, nil, nil, 0); err == nil {
		t.Fatal("BuildNullableInt64(oversized) succeeded, want error")
	}
	bools := make([]bool, maxGranuleDecodeRows+1)
	if _, err := builder.BuildBool(bools); err == nil {
		t.Fatal("BuildBool(oversized) succeeded, want error")
	}
	codes := make([]uint32, maxGranuleDecodeRows+1)
	if _, err := builder.BuildUint32Codes(codes, 1); err == nil {
		t.Fatal("BuildUint32Codes(oversized) succeeded, want error")
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
				{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionSnappy},
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

func TestBoolGranuleRoundTripAndCount(t *testing.T) {
	runValues := make([]bool, 192)
	for i := 64; i < 128; i++ {
		runValues[i] = true
	}
	allTrue := make([]bool, 128)
	for i := range allTrue {
		allTrue[i] = true
	}
	tests := []struct {
		name       string
		values     []bool
		wantMode   byte
		wantTrues  int
		wantMinMax [2]int64
	}{
		{name: "alternating", values: []bool{true, false, true, false, true, false, true, false, true}, wantMode: boolPayloadBitpack, wantTrues: 5, wantMinMax: [2]int64{0, 1}},
		{name: "runs", values: runValues, wantMode: boolPayloadRLE, wantTrues: 64, wantMinMax: [2]int64{0, 1}},
		{name: "all_true", values: allTrue, wantMode: boolPayloadRLE, wantTrues: len(allTrue), wantMinMax: [2]int64{1, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewGranuleBuilder(Config{Compression: CompressionNone})
			g, err := builder.BuildBool(tt.values)
			if err != nil {
				t.Fatalf("BuildBool: %v", err)
			}
			if g.Encoding != EncodingBoolBitpackRLE || g.Payload[0] != tt.wantMode {
				t.Fatalf("encoding/mode=(%s,%d) want (%s,%d)", g.Encoding, g.Payload[0], EncodingBoolBitpackRLE, tt.wantMode)
			}
			if g.Min != tt.wantMinMax[0] || g.Max != tt.wantMinMax[1] {
				t.Fatalf("min/max=(%d,%d) want %v", g.Min, g.Max, tt.wantMinMax)
			}
			var reader GranuleReader
			got, err := reader.DecodeBool(g)
			if err != nil {
				t.Fatalf("DecodeBool: %v", err)
			}
			if !slices.Equal(got, tt.values) {
				t.Fatalf("DecodeBool=%v want %v", got, tt.values)
			}
			count, err := reader.CountTrueBool(g)
			if err != nil {
				t.Fatalf("CountTrueBool: %v", err)
			}
			if count != tt.wantTrues {
				t.Fatalf("CountTrueBool=%d want %d", count, tt.wantTrues)
			}
		})
	}
}

func TestBoolCorruptPayloadsFailClosedBeforeDecodeAllocation(t *testing.T) {
	tests := []struct {
		name string
		rows int
		raw  []byte
	}{
		{
			name: "invalid_rle_start",
			rows: 1,
			raw:  []byte{boolPayloadRLE, 2, 1},
		},
		{
			name: "huge_bitpack_rows",
			rows: maxGranuleDecodeRows + 1,
			raw:  []byte{boolPayloadBitpack},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := EncodedGranule{
				Rows:        tt.rows,
				Encoding:    EncodingBoolBitpackRLE,
				Compression: CompressionNone,
				RawBytes:    len(tt.raw),
				StoredBytes: len(tt.raw),
				PayloadRef:  PayloadRef{Kind: PayloadRefInline, Length: len(tt.raw)},
				Payload:     tt.raw,
			}
			var reader GranuleReader
			if _, err := reader.DecodeBool(g); err == nil {
				t.Fatal("DecodeBool(corrupt) succeeded, want error")
			}
			if _, err := reader.CountTrueBool(g); err == nil {
				t.Fatal("CountTrueBool(corrupt) succeeded, want error")
			}
		})
	}
}

func TestBoolAtTreatsShortOptionalBitmapAsFalse(t *testing.T) {
	if !boolAt([]bool{true}, 0) {
		t.Fatal("boolAt existing true row returned false")
	}
	if boolAt([]bool{true}, 1) {
		t.Fatal("boolAt out-of-range row returned true")
	}
	if boolAt(nil, 0) {
		t.Fatal("boolAt nil bitmap returned true")
	}
}

func TestNullableInt64GranuleRoundTrip(t *testing.T) {
	values := []int64{10, 20, 30, 40, 50, 60, 70}
	nulls := []bool{false, true, false, false, false, true, false}
	defaults := []bool{false, false, true, false, true, false, false}
	defaultValue := int64(99)
	builder := NewGranuleBuilder(Config{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionSnappy})
	g, err := builder.BuildNullableInt64(values, nulls, defaults, defaultValue)
	if err != nil {
		t.Fatalf("BuildNullableInt64: %v", err)
	}
	if g.Encoding != EncodingNullableInt64 || g.NullCount != 2 || g.DefaultCount != 2 {
		t.Fatalf("encoding/null/default=(%s,%d,%d) want (%s,2,2)", g.Encoding, g.NullCount, g.DefaultCount, EncodingNullableInt64)
	}
	if !g.HasMinMax || g.Min != 10 || g.Max != 99 {
		t.Fatalf("has/min/max=(%v,%d,%d) want (true,10,99)", g.HasMinMax, g.Min, g.Max)
	}
	var reader GranuleReader
	gotValues, gotNulls, gotDefaults, err := reader.DecodeNullableInt64(g)
	if err != nil {
		t.Fatalf("DecodeNullableInt64: %v", err)
	}
	wantValues := []int64{10, 0, 99, 40, 99, 0, 70}
	if !slices.Equal(gotValues, wantValues) {
		t.Fatalf("values=%v want %v", gotValues, wantValues)
	}
	if !slices.Equal(gotNulls, nulls) {
		t.Fatalf("nulls=%v want %v", gotNulls, nulls)
	}
	if !slices.Equal(gotDefaults, defaults) {
		t.Fatalf("defaults=%v want %v", gotDefaults, defaults)
	}
}

func TestNullableInt64CorruptRowsFailClosedBeforeDecodeAllocation(t *testing.T) {
	payload := make([]byte, nullableInt64HeaderBytes)
	payload[0] = byte(EncodingDeltaVarint)
	g := EncodedGranule{
		Rows:        maxGranuleDecodeRows + 1,
		Encoding:    EncodingNullableInt64,
		Compression: CompressionNone,
		RawBytes:    len(payload),
		StoredBytes: len(payload),
		PayloadRef:  PayloadRef{Kind: PayloadRefInline, Length: len(payload)},
		Payload:     payload,
	}
	var reader GranuleReader
	if _, _, _, err := reader.DecodeNullableInt64(g); err == nil {
		t.Fatal("DecodeNullableInt64(corrupt) succeeded, want error")
	}
}

func TestLowCardinalityUint32RoundTripAndCounts(t *testing.T) {
	codes := []uint32{0, 2, 1, 2, 2, 4, 4, 0, 3, 2}
	builder := NewGranuleBuilder(Config{Compression: CompressionLZ4})
	g, err := builder.BuildUint32Codes(codes, 5)
	if err != nil {
		t.Fatalf("BuildUint32Codes: %v", err)
	}
	if g.Encoding != EncodingLowCardinalityUint32 || g.Min != 0 || g.Max != 4 {
		t.Fatalf("encoding/min/max=(%s,%d,%d) want (%s,0,4)", g.Encoding, g.Min, g.Max, EncodingLowCardinalityUint32)
	}
	var reader GranuleReader
	got, err := reader.DecodeUint32Codes(g)
	if err != nil {
		t.Fatalf("DecodeUint32Codes: %v", err)
	}
	if !slices.Equal(got, codes) {
		t.Fatalf("DecodeUint32Codes=%v want %v", got, codes)
	}
	counts, err := reader.CountUint32Codes(g, nil)
	if err != nil {
		t.Fatalf("CountUint32Codes: %v", err)
	}
	wantCounts := []int{2, 1, 4, 1, 2}
	if !slices.Equal(counts, wantCounts) {
		t.Fatalf("CountUint32Codes=%v want %v", counts, wantCounts)
	}
}

func TestLowCardinalityUint32CorruptRowsFailClosed(t *testing.T) {
	payload := []byte{4, 1}
	g := EncodedGranule{
		Rows:        maxGranuleDecodeRows + 1,
		Encoding:    EncodingLowCardinalityUint32,
		Compression: CompressionNone,
		RawBytes:    len(payload),
		StoredBytes: len(payload),
		PayloadRef:  PayloadRef{Kind: PayloadRefInline, Length: len(payload)},
		Payload:     payload,
	}
	var reader GranuleReader
	if _, err := reader.DecodeUint32Codes(g); err == nil {
		t.Fatal("DecodeUint32Codes(corrupt) succeeded, want error")
	}
	if _, err := reader.CountUint32Codes(g, nil); err == nil {
		t.Fatal("CountUint32Codes(corrupt) succeeded, want error")
	}
}

func TestCompressionAdmissionReportsNoFallbackWhenNoneRequested(t *testing.T) {
	g, err := EncodeInt64(nil, []int64{1, 2, 3}, Config{Encoding: EncodingRawInt64, Compression: CompressionNone})
	if err != nil {
		t.Fatalf("EncodeInt64: %v", err)
	}
	if g.CodecReport.RequestedCompression != CompressionNone || g.CodecReport.ActualCompression != CompressionNone {
		t.Fatalf("codec report compression=(%s,%s), want none/none", g.CodecReport.RequestedCompression, g.CodecReport.ActualCompression)
	}
	if g.CodecReport.CompressionAttempted {
		t.Fatalf("compression none unexpectedly attempted compression: %+v", g.CodecReport)
	}
	if g.CodecReport.CompressionFallbackReason != "" {
		t.Fatalf("compression none fallback reason=%q, want empty", g.CodecReport.CompressionFallbackReason)
	}
}

func TestCompressionAdmissionReportsFallback(t *testing.T) {
	values := makeRandom(8192)
	g, err := EncodeInt64(nil, values, Config{Encoding: EncodingRawInt64, Compression: CompressionSnappy})
	if err != nil {
		t.Fatalf("EncodeInt64: %v", err)
	}
	if g.CodecReport.RequestedCompression != CompressionSnappy || !g.CodecReport.CompressionAttempted {
		t.Fatalf("codec report did not record snappy attempt: %+v", g.CodecReport)
	}
	if g.StoredBytes > g.RawBytes {
		t.Fatalf("stored bytes expanded silently: stored=%d raw=%d report=%+v", g.StoredBytes, g.RawBytes, g.CodecReport)
	}
	if g.Compression == CompressionNone && g.CodecReport.CompressionFallbackReason == "" {
		t.Fatalf("fallback did not report reason: %+v", g.CodecReport)
	}
}

func TestUnsupportedCodecIDsFailClosed(t *testing.T) {
	g, err := EncodeInt64(nil, []int64{1, 2, 3}, Config{Encoding: EncodingRawInt64, Compression: CompressionNone})
	if err != nil {
		t.Fatalf("EncodeInt64: %v", err)
	}
	badEncoding := g
	badEncoding.Encoding = Encoding(255)
	if _, err := DecodeInt64(nil, badEncoding); err == nil {
		t.Fatal("DecodeInt64 with bad encoding succeeded, want error")
	}
	badCompression := g
	badCompression.Compression = Compression(255)
	if _, err := DecodeInt64(nil, badCompression); err == nil {
		t.Fatal("DecodeInt64 with bad compression succeeded, want error")
	}
	if _, err := EncodeInt64(nil, []int64{1, 2, 3}, Config{Encoding: EncodingRawInt64, Compression: CompressionZSTD}); err == nil {
		t.Fatal("EncodeInt64 with unsupported zstd succeeded, want error")
	}
	if _, err := EncodeInt64(nil, []int64{1, 2, 3}, Config{Encoding: EncodingRawInt64, Compression: CompressionZSTDDict}); err == nil {
		t.Fatal("EncodeInt64 with unsupported zstd_dict succeeded, want error")
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

func TestInt64CursorPrefixAndFinish(t *testing.T) {
	values := []int64{1000, 1007, 1016, 1028, 1043, 1061}
	for _, encoding := range []Encoding{EncodingRawInt64, EncodingDeltaVarint, EncodingDoubleDeltaVarint} {
		t.Run(encoding.String(), func(t *testing.T) {
			g, err := EncodeInt64(nil, values, Config{Encoding: encoding, Compression: CompressionNone})
			if err != nil {
				t.Fatalf("EncodeInt64: %v", err)
			}
			var reader GranuleReader
			cursor, err := reader.int64Cursor(g)
			if err != nil {
				t.Fatalf("int64Cursor: %v", err)
			}
			for i, want := range values[:3] {
				got, err := cursor.Next()
				if err != nil {
					t.Fatalf("prefix Next(%d): %v", i, err)
				}
				if got != want {
					t.Fatalf("prefix Next(%d)=%d want %d", i, got, want)
				}
			}
			if cursor.RawBytesRead() <= 0 || cursor.RawBytesRead() >= g.RawBytes {
				t.Fatalf("prefix raw bytes read=%d want between 1 and %d", cursor.RawBytesRead(), g.RawBytes-1)
			}
			if err := cursor.Finish(); err == nil {
				t.Fatal("prefix Finish succeeded, want short-read error")
			}

			cursor, err = reader.int64Cursor(g)
			if err != nil {
				t.Fatalf("int64Cursor(full): %v", err)
			}
			for i, want := range values {
				got, err := cursor.Next()
				if err != nil {
					t.Fatalf("full Next(%d): %v", i, err)
				}
				if got != want {
					t.Fatalf("full Next(%d)=%d want %d", i, got, want)
				}
			}
			if err := cursor.Finish(); err != nil {
				t.Fatalf("full Finish: %v", err)
			}
			if cursor.RawBytesRead() != g.RawBytes {
				t.Fatalf("full raw bytes read=%d want %d", cursor.RawBytesRead(), g.RawBytes)
			}
		})
	}
}

func FuzzDecodeTypedGranuleCorruptPayloads(f *testing.F) {
	boolGranule, err := NewGranuleBuilder(Config{Compression: CompressionNone}).BuildBool([]bool{true, false, true, true, false})
	if err != nil {
		f.Fatalf("BuildBool: %v", err)
	}
	nullableGranule, err := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone}).BuildNullableInt64(
		[]int64{1, 2, 3, 4},
		[]bool{false, true, false, false},
		[]bool{false, false, true, false},
		7,
	)
	if err != nil {
		f.Fatalf("BuildNullableInt64: %v", err)
	}
	codeGranule, err := NewGranuleBuilder(Config{Compression: CompressionNone}).BuildUint32Codes([]uint32{0, 1, 1, 2}, 3)
	if err != nil {
		f.Fatalf("BuildUint32Codes: %v", err)
	}
	f.Add(byte(EncodingBoolBitpackRLE), boolGranule.Payload)
	f.Add(byte(EncodingNullableInt64), nullableGranule.Payload)
	f.Add(byte(EncodingLowCardinalityUint32), codeGranule.Payload)
	f.Fuzz(func(t *testing.T, encoding byte, payload []byte) {
		g := EncodedGranule{
			Rows:        4,
			Encoding:    Encoding(encoding),
			Compression: CompressionNone,
			RawBytes:    len(payload),
			StoredBytes: len(payload),
			PayloadRef:  PayloadRef{Kind: PayloadRefInline, Length: len(payload)},
			Payload:     payload,
		}
		var reader GranuleReader
		switch Encoding(encoding) {
		case EncodingBoolBitpackRLE:
			_, _ = reader.DecodeBool(g)
			_, _ = reader.CountTrueBool(g)
		case EncodingNullableInt64:
			_, _, _, _ = reader.DecodeNullableInt64(g)
		case EncodingLowCardinalityUint32:
			_, _ = reader.DecodeUint32Codes(g)
			_, _ = reader.CountUint32Codes(g, nil)
		}
	})
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
			name: "double_delta_truncated",
			cfg:  Config{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionNone},
			edit: func(g EncodedGranule) EncodedGranule {
				g.Payload = g.Payload[:len(g.Payload)-1]
				g.StoredBytes = len(g.Payload)
				g.PayloadRef.Length = len(g.Payload)
				return g
			},
		},
		{
			name: "double_delta_trailing",
			cfg:  Config{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionNone},
			edit: func(g EncodedGranule) EncodedGranule {
				g.Payload = append(append([]byte(nil), g.Payload...), 0)
				g.StoredBytes = len(g.Payload)
				g.PayloadRef.Length = len(g.Payload)
				g.RawBytes = len(g.Payload)
				return g
			},
		},
		{
			name: "double_delta_huge_rows_tiny_payload",
			cfg:  Config{Encoding: EncodingDoubleDeltaVarint, Compression: CompressionNone},
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
		{
			name: "metadata_bad_payload_ref_kind",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
			edit: func(g EncodedGranule) EncodedGranule {
				g.PayloadRef.Kind = 0
				return g
			},
		},
		{
			name: "metadata_bad_payload_ref_length_zero",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
			edit: func(g EncodedGranule) EncodedGranule {
				g.PayloadRef.Length = 0
				return g
			},
		},
		{
			name: "metadata_bad_payload_ref_offset",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
			edit: func(g EncodedGranule) EncodedGranule {
				g.PayloadRef.Offset = 1
				return g
			},
		},
		{
			name: "metadata_huge_raw_bytes",
			cfg:  Config{Encoding: EncodingDeltaVarint, Compression: CompressionSnappy},
			edit: func(g EncodedGranule) EncodedGranule {
				g.RawBytes = maxGranuleRawPayloadBytes(g.Encoding, g.Rows) + 1
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
