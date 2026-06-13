package typedcolumn

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/compress/zstd"
)

const typedColumnCodecRows = 8192

var typedColumnCodecBenchmarkSink uint64

var typedColumnCodecCompressions = []Compression{CompressionNone, CompressionSnappy, CompressionLZ4, CompressionZSTD}

type typedColumnCodecLayout struct {
	name            string
	rows            int
	encode          func(testing.TB, *GranuleBuilder, Compression) EncodedGranule
	assertRoundTrip func(testing.TB, *GranuleReader, EncodedGranule)
	decodeChecksum  func(testing.TB, *GranuleReader, EncodedGranule) uint64
	assertRaw       func(testing.TB, []byte)
}

func TestTypedColumnCodecRoundTripRepresentativeLayouts(t *testing.T) {
	for _, layout := range typedColumnCodecLayouts() {
		for _, compression := range typedColumnCodecCompressions {
			t.Run(layout.name+"/"+compression.String(), func(t *testing.T) {
				builder := NewGranuleBuilder(Config{})
				granule := layout.encode(t, builder, compression)
				assertTypedColumnCodecAccounting(t, granule, layout.rows, compression)
				raw := mustTypedColumnRawPayload(t, granule)
				if len(raw) != granule.RawBytes {
					t.Fatalf("raw bytes=%d want granule raw=%d", len(raw), granule.RawBytes)
				}
				if layout.assertRaw != nil {
					layout.assertRaw(t, raw)
				}
				var reader GranuleReader
				layout.assertRoundTrip(t, &reader, granule)
			})
		}
	}
}

func TestTypedColumnZstdDecodeCapsDeclaredRawBytes1952(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd encoder: %v", err)
	}
	defer enc.Close()
	stored := enc.EncodeAll([]byte(strings.Repeat("jsonbench-zstd-cap-", 512)), nil)

	_, err = decodeZstdPayload("test", stored, 16, make([]byte, 0, 16))
	if err == nil || !strings.Contains(err.Error(), "zstd decode") {
		t.Fatalf("decodeZstdPayload err=%v want capped zstd decode failure", err)
	}
}

func TestTypedColumnCompressionKeepIfSmallerFallback(t *testing.T) {
	values := []int64{0x1122334455667788}
	for _, compression := range []Compression{CompressionSnappy, CompressionLZ4, CompressionZSTD} {
		t.Run(compression.String(), func(t *testing.T) {
			builder := NewGranuleBuilder(Config{Encoding: EncodingRawInt64, Compression: compression})
			granule, err := builder.BuildInt64(values)
			if err != nil {
				t.Fatalf("BuildInt64: %v", err)
			}
			if granule.CodecReport.RequestedCompression != compression {
				t.Fatalf("requested compression=%s want %s report=%+v", granule.CodecReport.RequestedCompression, compression, granule.CodecReport)
			}
			if granule.Compression != CompressionNone || granule.CodecReport.ActualCompression != CompressionNone {
				t.Fatalf("fallback actual compression=%s report=%+v", granule.Compression, granule.CodecReport)
			}
			if !granule.CodecReport.CompressionAttempted || granule.CodecReport.CompressionKept {
				t.Fatalf("fallback attempt/kept report=%+v", granule.CodecReport)
			}
			if granule.CodecReport.CompressionFallbackReason != "not_smaller" {
				t.Fatalf("fallback reason=%q want not_smaller report=%+v", granule.CodecReport.CompressionFallbackReason, granule.CodecReport)
			}
			if granule.RawBytes != granule.StoredBytes || granule.StoredBytes != len(granule.Payload) {
				t.Fatalf("fallback raw/stored/payload=%d/%d/%d", granule.RawBytes, granule.StoredBytes, len(granule.Payload))
			}
			var reader GranuleReader
			got, err := reader.DecodeInt64(granule)
			if err != nil {
				t.Fatalf("DecodeInt64 fallback: %v", err)
			}
			if !slices.Equal(got, values) {
				t.Fatalf("DecodeInt64 fallback=%v want %v", got, values)
			}
		})
	}
}

func TestTypedColumnCompressionCorruptionFailsClosed(t *testing.T) {
	for _, compression := range []Compression{CompressionSnappy, CompressionLZ4, CompressionZSTD} {
		t.Run(compression.String(), func(t *testing.T) {
			base := mustBuildKeptCompressedDeltaGranule(t, compression)
			cases := []struct {
				name string
				mut  func(EncodedGranule) EncodedGranule
				want string
			}{
				{
					name: "truncated_payload",
					mut: func(g EncodedGranule) EncodedGranule {
						payload := append([]byte(nil), g.Payload[:len(g.Payload)/2]...)
						return typedColumnGranuleWithPayload(g, payload)
					},
					want: compression.String(),
				},
				{
					name: "corrupt_payload",
					mut: func(g EncodedGranule) EncodedGranule {
						payload := make([]byte, len(g.Payload))
						for i := range payload {
							payload[i] = 0xff
						}
						return typedColumnGranuleWithPayload(g, payload)
					},
					want: compression.String(),
				},
				{
					name: "decoded_length_mismatch",
					mut: func(g EncodedGranule) EncodedGranule {
						g.RawBytes++
						g.CodecReport.RawBytes = g.RawBytes
						return g
					},
					want: "decoded length",
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var reader GranuleReader
					_, err := reader.DecodeInt64(tc.mut(cloneTypedColumnGranule(base)))
					requireTypedColumnCodecErrContains(t, err, tc.want)
				})
			}
		})
	}
}

func TestTypedColumnUnsupportedCodecIDsFailClosed(t *testing.T) {
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: CompressionNone})
	base, err := builder.BuildInt64(codecDeltaInt64Values())
	if err != nil {
		t.Fatalf("BuildInt64: %v", err)
	}
	base = cloneTypedColumnGranule(base)

	for _, compression := range []Compression{CompressionZSTDDict, Compression(250)} {
		t.Run("decode_compression_"+compression.String(), func(t *testing.T) {
			bad := cloneTypedColumnGranule(base)
			bad.Compression = compression
			bad.CodecReport.ActualCompression = compression
			var reader GranuleReader
			_, err := reader.DecodeInt64(bad)
			requireTypedColumnCodecErrContains(t, err, "unsupported compression")
			requireTypedColumnCodecErrContains(t, err, compression.String())
		})
		t.Run("encode_compression_"+compression.String(), func(t *testing.T) {
			_, err := EncodeInt64(nil, []int64{1, 1, 1, 1}, Config{Encoding: EncodingDeltaVarint, Compression: compression})
			requireTypedColumnCodecErrContains(t, err, "unsupported compression")
			requireTypedColumnCodecErrContains(t, err, compression.String())
		})
	}

	unknownEncoding := Encoding(250)
	t.Run("decode_encoding_"+unknownEncoding.String(), func(t *testing.T) {
		bad := cloneTypedColumnGranule(base)
		bad.Encoding = unknownEncoding
		var reader GranuleReader
		_, err := reader.DecodeInt64(bad)
		requireTypedColumnCodecErrContains(t, err, "unsupported encoding")
		requireTypedColumnCodecErrContains(t, err, unknownEncoding.String())
	})
	t.Run("encode_encoding_"+unknownEncoding.String(), func(t *testing.T) {
		_, err := EncodeInt64(nil, []int64{1, 2, 3}, Config{Encoding: unknownEncoding, Compression: CompressionNone})
		requireTypedColumnCodecErrContains(t, err, "unsupported encoding")
	})
}

func BenchmarkTypedColumnCodecVariants(b *testing.B) {
	for _, layout := range typedColumnCodecLayouts() {
		for _, compression := range typedColumnCodecCompressions {
			name := layout.name + "/" + compression.String()
			b.Run(name+"/encode", func(b *testing.B) {
				builder := NewGranuleBuilder(Config{})
				granule := layout.encode(b, builder, compression)
				assertTypedColumnCodecAccounting(b, granule, layout.rows, compression)
				b.ReportAllocs()
				b.SetBytes(int64(granule.RawBytes))
				b.ResetTimer()
				var rawStored uint64
				for i := 0; i < b.N; i++ {
					granule = layout.encode(b, builder, compression)
					rawStored += uint64(granule.RawBytes) + uint64(granule.StoredBytes)
				}
				b.ReportMetric(float64(granule.RawBytes), "raw_B/op")
				b.ReportMetric(float64(granule.StoredBytes), "stored_B/op")
				typedColumnCodecBenchmarkSink += rawStored
			})
			b.Run(name+"/decode", func(b *testing.B) {
				builder := NewGranuleBuilder(Config{})
				granule := cloneTypedColumnGranule(layout.encode(b, builder, compression))
				assertTypedColumnCodecAccounting(b, granule, layout.rows, compression)
				var reader GranuleReader
				layout.assertRoundTrip(b, &reader, granule)
				b.ReportAllocs()
				b.SetBytes(int64(granule.RawBytes))
				b.ResetTimer()
				var checksum uint64
				for i := 0; i < b.N; i++ {
					checksum += layout.decodeChecksum(b, &reader, granule)
				}
				b.ReportMetric(float64(granule.RawBytes), "raw_B/op")
				b.ReportMetric(float64(granule.StoredBytes), "stored_B/op")
				typedColumnCodecBenchmarkSink += checksum
			})
		}
	}
}

func typedColumnCodecLayouts() []typedColumnCodecLayout {
	rawValues := codecRawInt64Values()
	deltaValues := codecDeltaInt64Values()
	doubleDeltaValues := codecDoubleDeltaInt64Values()
	nullableValues, nullableNulls, nullableDefaults, nullableWant, nullableDefaultValue := codecNullableInt64Values()
	bitpackBools := codecBoolBitpackValues()
	rleBools := codecBoolRLEValues()
	codes := codecLowCardinalityCodes()

	return []typedColumnCodecLayout{
		{
			name: "raw_int64",
			rows: len(rawValues),
			encode: func(tb testing.TB, builder *GranuleBuilder, compression Compression) EncodedGranule {
				return mustBuildTypedColumnInt64(tb, builder, EncodingRawInt64, compression, rawValues)
			},
			assertRoundTrip: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) {
				assertTypedColumnInt64RoundTrip(tb, reader, g, rawValues)
			},
			decodeChecksum: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
				return checksumTypedColumnInt64(tb, reader, g)
			},
		},
		{
			name: "delta_varint",
			rows: len(deltaValues),
			encode: func(tb testing.TB, builder *GranuleBuilder, compression Compression) EncodedGranule {
				return mustBuildTypedColumnInt64(tb, builder, EncodingDeltaVarint, compression, deltaValues)
			},
			assertRoundTrip: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) {
				assertTypedColumnInt64RoundTrip(tb, reader, g, deltaValues)
			},
			decodeChecksum: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
				return checksumTypedColumnInt64(tb, reader, g)
			},
		},
		{
			name: "double_delta_varint",
			rows: len(doubleDeltaValues),
			encode: func(tb testing.TB, builder *GranuleBuilder, compression Compression) EncodedGranule {
				return mustBuildTypedColumnInt64(tb, builder, EncodingDoubleDeltaVarint, compression, doubleDeltaValues)
			},
			assertRoundTrip: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) {
				assertTypedColumnInt64RoundTrip(tb, reader, g, doubleDeltaValues)
			},
			decodeChecksum: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
				return checksumTypedColumnInt64(tb, reader, g)
			},
		},
		{
			name: "nullable_int64",
			rows: len(nullableValues),
			encode: func(tb testing.TB, builder *GranuleBuilder, compression Compression) EncodedGranule {
				builder.Reset(Config{Encoding: EncodingNullableInt64, Compression: compression})
				granule, err := builder.BuildNullableInt64(nullableValues, nullableNulls, nullableDefaults, nullableDefaultValue)
				if err != nil {
					tb.Fatalf("BuildNullableInt64: %v", err)
				}
				return granule
			},
			assertRoundTrip: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) {
				tb.Helper()
				got, gotNulls, gotDefaults, err := reader.DecodeNullableInt64(g)
				if err != nil {
					tb.Fatalf("DecodeNullableInt64: %v", err)
				}
				if !slices.Equal(got, nullableWant) || !slices.Equal(gotNulls, nullableNulls) || !slices.Equal(gotDefaults, nullableDefaults) {
					tb.Fatalf("DecodeNullableInt64 values/nulls/defaults mismatch")
				}
			},
			decodeChecksum: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
				tb.Helper()
				values, nulls, defaults, err := reader.DecodeNullableInt64(g)
				if err != nil {
					tb.Fatalf("DecodeNullableInt64: %v", err)
				}
				var sum uint64
				for i, value := range values {
					sum += uint64(value) + uint64(i&7)
					if nulls[i] {
						sum += 17
					}
					if defaults[i] {
						sum += 31
					}
				}
				return sum
			},
			assertRaw: func(tb testing.TB, raw []byte) {
				tb.Helper()
				if len(raw) == 0 || raw[0] != byte(EncodingRawInt64) {
					tb.Fatalf("nullable value encoding byte=%v want %d", raw[:min(len(raw), 1)], EncodingRawInt64)
				}
			},
		},
		{
			name: "bool_bitpack",
			rows: len(bitpackBools),
			encode: func(tb testing.TB, builder *GranuleBuilder, compression Compression) EncodedGranule {
				return mustBuildTypedColumnBool(tb, builder, compression, bitpackBools)
			},
			assertRoundTrip: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) {
				assertTypedColumnBoolRoundTrip(tb, reader, g, bitpackBools)
			},
			decodeChecksum: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
				return checksumTypedColumnBool(tb, reader, g)
			},
			assertRaw: func(tb testing.TB, raw []byte) {
				tb.Helper()
				if len(raw) == 0 || raw[0] != boolPayloadBitpack {
					tb.Fatalf("bool payload mode=%v want bitpack", raw[:min(len(raw), 1)])
				}
			},
		},
		{
			name: "bool_rle",
			rows: len(rleBools),
			encode: func(tb testing.TB, builder *GranuleBuilder, compression Compression) EncodedGranule {
				return mustBuildTypedColumnBool(tb, builder, compression, rleBools)
			},
			assertRoundTrip: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) {
				assertTypedColumnBoolRoundTrip(tb, reader, g, rleBools)
			},
			decodeChecksum: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
				return checksumTypedColumnBool(tb, reader, g)
			},
			assertRaw: func(tb testing.TB, raw []byte) {
				tb.Helper()
				if len(raw) == 0 || raw[0] != boolPayloadRLE {
					tb.Fatalf("bool payload mode=%v want rle", raw[:min(len(raw), 1)])
				}
			},
		},
		{
			name: "low_cardinality_uint32",
			rows: len(codes),
			encode: func(tb testing.TB, builder *GranuleBuilder, compression Compression) EncodedGranule {
				builder.Reset(Config{Encoding: EncodingLowCardinalityUint32, Compression: compression})
				granule, err := builder.BuildUint32Codes(codes, 4)
				if err != nil {
					tb.Fatalf("BuildUint32Codes: %v", err)
				}
				return granule
			},
			assertRoundTrip: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) {
				tb.Helper()
				got, err := reader.DecodeUint32Codes(g)
				if err != nil {
					tb.Fatalf("DecodeUint32Codes: %v", err)
				}
				if !slices.Equal(got, codes) {
					tb.Fatalf("DecodeUint32Codes mismatch")
				}
			},
			decodeChecksum: func(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
				tb.Helper()
				got, err := reader.DecodeUint32Codes(g)
				if err != nil {
					tb.Fatalf("DecodeUint32Codes: %v", err)
				}
				var sum uint64
				for _, code := range got {
					sum += uint64(code)
				}
				return sum
			},
			assertRaw: func(tb testing.TB, raw []byte) {
				tb.Helper()
				if len(raw) < 2 || raw[0] != 1 || raw[1] != 4 {
					tb.Fatalf("code header=%v want width=1 cardinality=4", raw[:min(len(raw), 2)])
				}
			},
		},
	}
}

func assertTypedColumnCodecAccounting(tb testing.TB, g EncodedGranule, rows int, requested Compression) {
	tb.Helper()
	if g.Rows != rows {
		tb.Fatalf("rows=%d want %d", g.Rows, rows)
	}
	if g.RawBytes <= 0 || g.StoredBytes <= 0 {
		tb.Fatalf("raw/stored bytes=%d/%d", g.RawBytes, g.StoredBytes)
	}
	if g.StoredBytes != len(g.Payload) || g.PayloadRef.Length != len(g.Payload) || g.PayloadRef.Kind != PayloadRefInline || g.PayloadRef.Offset != 0 {
		tb.Fatalf("payload accounting stored=%d payload=%d ref=%+v", g.StoredBytes, len(g.Payload), g.PayloadRef)
	}
	report := g.CodecReport
	if report.Encoding != g.Encoding || report.RequestedCompression != requested || report.ActualCompression != g.Compression || report.RawBytes != g.RawBytes || report.StoredBytes != g.StoredBytes {
		tb.Fatalf("codec report=%+v granule encoding=%s compression=%s raw/stored=%d/%d requested=%s", report, g.Encoding, g.Compression, g.RawBytes, g.StoredBytes, requested)
	}
	if requested == CompressionNone {
		if g.Compression != CompressionNone || g.RawBytes != g.StoredBytes || report.CompressionAttempted || !report.CompressionKept || report.CompressionFallbackReason != "" {
			tb.Fatalf("unexpected none accounting granule compression=%s report=%+v", g.Compression, report)
		}
		return
	}
	if g.Compression != requested || !report.CompressionAttempted || !report.CompressionKept || report.CompressionFallbackReason != "" || g.StoredBytes >= g.RawBytes {
		tb.Fatalf("compression %s was not kept smaller: granule compression=%s report=%+v", requested, g.Compression, report)
	}
}

func mustTypedColumnRawPayload(tb testing.TB, g EncodedGranule) []byte {
	tb.Helper()
	var reader GranuleReader
	raw, err := reader.decompressPayload(g)
	if err != nil {
		tb.Fatalf("decompressPayload: %v", err)
	}
	return append([]byte(nil), raw...)
}

func mustBuildTypedColumnInt64(tb testing.TB, builder *GranuleBuilder, encoding Encoding, compression Compression, values []int64) EncodedGranule {
	tb.Helper()
	builder.Reset(Config{Encoding: encoding, Compression: compression})
	granule, err := builder.BuildInt64(values)
	if err != nil {
		tb.Fatalf("BuildInt64(%s,%s): %v", encoding, compression, err)
	}
	return granule
}

func mustBuildTypedColumnBool(tb testing.TB, builder *GranuleBuilder, compression Compression, values []bool) EncodedGranule {
	tb.Helper()
	builder.Reset(Config{Encoding: EncodingBoolBitpackRLE, Compression: compression})
	granule, err := builder.BuildBool(values)
	if err != nil {
		tb.Fatalf("BuildBool(%s): %v", compression, err)
	}
	return granule
}

func assertTypedColumnInt64RoundTrip(tb testing.TB, reader *GranuleReader, g EncodedGranule, want []int64) {
	tb.Helper()
	got, err := reader.DecodeInt64(g)
	if err != nil {
		tb.Fatalf("DecodeInt64: %v", err)
	}
	if !slices.Equal(got, want) {
		tb.Fatalf("DecodeInt64 mismatch")
	}
}

func checksumTypedColumnInt64(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
	tb.Helper()
	values, err := reader.DecodeInt64(g)
	if err != nil {
		tb.Fatalf("DecodeInt64: %v", err)
	}
	var sum uint64
	for i, value := range values {
		sum += uint64(value) + uint64(i&15)
	}
	return sum
}

func assertTypedColumnBoolRoundTrip(tb testing.TB, reader *GranuleReader, g EncodedGranule, want []bool) {
	tb.Helper()
	got, err := reader.DecodeBool(g)
	if err != nil {
		tb.Fatalf("DecodeBool: %v", err)
	}
	if !slices.Equal(got, want) {
		tb.Fatalf("DecodeBool mismatch")
	}
}

func checksumTypedColumnBool(tb testing.TB, reader *GranuleReader, g EncodedGranule) uint64 {
	tb.Helper()
	values, err := reader.DecodeBool(g)
	if err != nil {
		tb.Fatalf("DecodeBool: %v", err)
	}
	var sum uint64
	for i, value := range values {
		if value {
			sum += uint64(i + 1)
		}
	}
	return sum
}

func codecRawInt64Values() []int64 {
	values := make([]int64, typedColumnCodecRows)
	for i := range values {
		switch {
		case i%257 == 0:
			values[i] = math.MinInt64
		case i%263 == 0:
			values[i] = math.MaxInt64
		case i%2 == 0:
			values[i] = 42
		default:
			values[i] = -17
		}
	}
	return values
}

func codecDeltaInt64Values() []int64 {
	values := make([]int64, typedColumnCodecRows)
	for i := range values {
		if i%257 == 0 {
			values[i] = 43
			continue
		}
		values[i] = 42
	}
	return values
}

func codecDoubleDeltaInt64Values() []int64 {
	values := make([]int64, typedColumnCodecRows)
	for i := range values {
		values[i] = 1000 + int64(i)*7
	}
	return values
}

func codecNullableInt64Values() (values []int64, nulls []bool, defaults []bool, want []int64, defaultValue int64) {
	values = make([]int64, typedColumnCodecRows)
	nulls = make([]bool, typedColumnCodecRows)
	defaults = make([]bool, typedColumnCodecRows)
	want = make([]int64, typedColumnCodecRows)
	defaultValue = int64(-99)
	for i := range values {
		values[i] = 1234
		switch i % 32 {
		case 0, 16:
			nulls[i] = true
		case 1, 17:
			defaults[i] = true
		}
		switch {
		case nulls[i]:
			want[i] = 0
		case defaults[i]:
			want[i] = defaultValue
		default:
			want[i] = values[i]
		}
	}
	return values, nulls, defaults, want, defaultValue
}

func codecBoolBitpackValues() []bool {
	values := make([]bool, typedColumnCodecRows)
	for i := range values {
		values[i] = i%2 == 0
	}
	return values
}

func codecBoolRLEValues() []bool {
	values := make([]bool, typedColumnCodecRows)
	for i := range values {
		values[i] = (i/16)%2 == 0
	}
	return values
}

func codecLowCardinalityCodes() []uint32 {
	codes := make([]uint32, typedColumnCodecRows)
	for i := range codes {
		codes[i] = uint32(i % 4)
	}
	return codes
}

func mustBuildKeptCompressedDeltaGranule(tb testing.TB, compression Compression) EncodedGranule {
	tb.Helper()
	builder := NewGranuleBuilder(Config{Encoding: EncodingDeltaVarint, Compression: compression})
	granule, err := builder.BuildInt64(codecDeltaInt64Values())
	if err != nil {
		tb.Fatalf("BuildInt64(%s): %v", compression, err)
	}
	if granule.Compression != compression {
		tb.Fatalf("compression %s not kept: granule compression=%s report=%+v", compression, granule.Compression, granule.CodecReport)
	}
	return cloneTypedColumnGranule(granule)
}

func typedColumnGranuleWithPayload(g EncodedGranule, payload []byte) EncodedGranule {
	g.Payload = payload
	g.StoredBytes = len(payload)
	g.PayloadRef.Length = len(payload)
	g.CodecReport.StoredBytes = len(payload)
	return g
}

func cloneTypedColumnGranule(g EncodedGranule) EncodedGranule {
	g.Payload = append([]byte(nil), g.Payload...)
	return g
}

func requireTypedColumnCodecErrContains(tb testing.TB, err error, want string) {
	tb.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		tb.Fatalf("err=%v want substring %q", err, want)
	}
}
