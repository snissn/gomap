package brq

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

var (
	brqEncodedSink EncodedVector
	brqQuerySink   Query
	brqScoreSink   float64
)

func TestConfigCanonicalBytesAndHash2481(t *testing.T) {
	cfg := DefaultConfig()
	want := "codec=brq_1bit\n" +
		"version=1\n" +
		"metric=cosine\n" +
		"normalization=unit_l2\n" +
		"rotation=signed_permutation_fwht_padded_v1\n" +
		"seed=0x6272713162697401\n" +
		"storage_role=packed_codes\n" +
		"storage_logical_type=packed_bit_vector\n" +
		"storage_encoding=raw_packed_bit_vector\n" +
		"bit_order=lsb0\n" +
		"word_order=little_endian_uint64\n" +
		"padding=zero\n" +
		"code_width_bits=1\n" +
		"query_weight_bits=4\n" +
		"query_weight_quantizer=max_abs_uint4_round_half_up\n" +
		"score=brq_1bit_estimated_cosine_q4\n" +
		"data_scale_side_array=quantized_dot_product_inv\n"
	if got := string(cfg.CanonicalBytes()); got != want {
		t.Fatalf("CanonicalBytes mismatch:\ngot:\n%swant:\n%s", got, want)
	}
	if got, want := cfg.Hash64(), uint64(0xf705b6becc1769f9); got != want {
		t.Fatalf("Config.Hash64=%#x want %#x", got, want)
	}
	if got, want := (Config{Seed: 0x0123456789abcdef}).Hash64(), uint64(0xcee1cfbd6c328075); got != want {
		t.Fatalf("custom Config.Hash64=%#x want %#x", got, want)
	}
}

func TestIdentityRotationAndShape2481(t *testing.T) {
	if CodecName != "brq_1bit" || CodecVersion != 1 || DefaultSeed != 0x6272713162697401 {
		t.Fatalf("unexpected identity: codec=%q version=%d seed=%#x", CodecName, CodecVersion, DefaultSeed)
	}
	if RotationName != "signed_permutation_fwht_padded_v1" {
		t.Fatalf("RotationName=%q", RotationName)
	}
	if CodeWidthBits != 1 || QueryWeightBits != 4 {
		t.Fatalf("widths code=%d query=%d", CodeWidthBits, QueryWeightBits)
	}
	metaPlan, err := NewPlan(5, Config{Seed: 0x0123456789abcdef})
	if err != nil {
		t.Fatalf("NewPlan metadata: %v", err)
	}
	wantPerm := []int{6, 2, 4, 1, 0, 3, 7, 5}
	for i, want := range wantPerm {
		if got := metaPlan.perm[i]; got != want {
			t.Fatalf("rotation perm[%d]=%d want %d", i, got, want)
		}
	}
	wantSigns := []float64{-1, 1, 1, -1, -1, -1, 1, 1}
	for i, want := range wantSigns {
		if got := metaPlan.signs[i]; got != want {
			t.Fatalf("rotation signs[%d]=%v want %v", i, got, want)
		}
	}
	if BitOrder != "lsb0" || WordOrder != "little_endian_uint64" || ScoreLabel != "brq_1bit_estimated_cosine_q4" {
		t.Fatalf("unexpected bit/word/score labels: %q %q %q", BitOrder, WordOrder, ScoreLabel)
	}
	if StorageRole != string(quantizedasset.RolePackedCodes) {
		t.Fatalf("StorageRole=%q want quantizedasset.RolePackedCodes", StorageRole)
	}
	if StorageLogicalType != string(typedcolumn.ColumnTypePackedBitVector) {
		t.Fatalf("StorageLogicalType=%q want packed_bit_vector", StorageLogicalType)
	}
	if StorageEncoding != typedcolumn.EncodingRawPackedBitVector.String() {
		t.Fatalf("StorageEncoding=%q want raw_packed_bit_vector", StorageEncoding)
	}

	for _, tc := range []struct {
		dims      int
		codeDims  int
		codeBytes int
	}{
		{dims: 1, codeDims: 1, codeBytes: 1},
		{dims: 3, codeDims: 4, codeBytes: 1},
		{dims: 5, codeDims: 8, codeBytes: 1},
		{dims: 8, codeDims: 8, codeBytes: 1},
		{dims: 9, codeDims: 16, codeBytes: 2},
		{dims: 63, codeDims: 64, codeBytes: 8},
		{dims: 65, codeDims: 128, codeBytes: 16},
		{dims: 129, codeDims: 256, codeBytes: 32},
	} {
		t.Run(fmt.Sprintf("dims_%d", tc.dims), func(t *testing.T) {
			plan, err := NewPlan(tc.dims, DefaultConfig())
			if err != nil {
				t.Fatalf("NewPlan(%d): %v", tc.dims, err)
			}
			if got := plan.Config().Seed; got != DefaultSeed {
				t.Fatalf("plan seed=%#x want %#x", got, DefaultSeed)
			}
			if got := plan.VectorDimensions(); got != tc.dims {
				t.Fatalf("VectorDimensions=%d want %d", got, tc.dims)
			}
			if got := plan.CodeDimensions(); got != tc.codeDims {
				t.Fatalf("CodeDimensions=%d want %d", got, tc.codeDims)
			}
			if got := plan.BytesPerCode(); got != tc.codeBytes {
				t.Fatalf("BytesPerCode=%d want %d", got, tc.codeBytes)
			}
		})
	}
}

func TestEncodeQueryAndScoreGolden2481(t *testing.T) {
	plan, err := NewPlan(5, Config{Seed: 0x0123456789abcdef})
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	var ws Workspace
	encoded, err := plan.Encode(nil, []float32{0.75, -0.5, 0.25, 1.25, -0.75}, &ws)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	query, err := plan.EncodeQuery([]float32{-0.25, 0.5, 1.5, -1, 0.75}, &ws)
	if err != nil {
		t.Fatalf("EncodeQuery: %v", err)
	}
	score, err := plan.ScoreEncoded(query, encoded)
	if err != nil {
		t.Fatalf("ScoreEncoded: %v", err)
	}
	slow, err := plan.ScoreCosineSlow(query, encoded.Code, encoded.CodeCount, encoded.QuantizedDotProductInv)
	if err != nil {
		t.Fatalf("ScoreCosineSlow: %v", err)
	}

	if got, want := encoded.Code, []byte{0xd8}; string(got) != string(want) {
		t.Fatalf("encoded.Code=% x want % x", got, want)
	}
	if got, want := encoded.CodeCount, uint32(4); got != want {
		t.Fatalf("CodeCount=%d want %d", got, want)
	}
	if got, want := math.Float32bits(encoded.QuantizedDotProductInv), uint32(0x3ec0f1c6); got != want {
		t.Fatalf("QuantizedDotProductInv bits=%#x want %#x", got, want)
	}
	if got, want := query.SignBits, []byte{0x75}; string(got) != string(want) {
		t.Fatalf("query.SignBits=% x want % x", got, want)
	}
	wantWeights := []uint8{13, 4, 11, 15, 2, 2, 0, 9}
	if got := query.Weights; string(got) != string(wantWeights) {
		t.Fatalf("query.Weights=%v want %v", got, wantWeights)
	}
	if got, want := math.Float64bits(query.QueryWeightScale), uint64(0x3fa4cbe5efaba9f6); got != want {
		t.Fatalf("QueryWeightScale bits=%#x want %#x", got, want)
	}
	if got, want := query.QueryWeightSumInt, uint32(56); got != want {
		t.Fatalf("QueryWeightSumInt=%d want %d", got, want)
	}
	if got, want := query.NegativeWeightSumInt, uint32(28); got != want {
		t.Fatalf("NegativeWeightSumInt=%d want %d", got, want)
	}
	assertBytes2481(t, "PosQ1", query.PosQ1, []byte{0x05})
	assertBytes2481(t, "PosQ2", query.PosQ2, []byte{0x34})
	assertBytes2481(t, "PosQ4", query.PosQ4, []byte{0x01})
	assertBytes2481(t, "PosQ8", query.PosQ8, []byte{0x05})
	assertBytes2481(t, "NegQ1", query.NegQ1, []byte{0x88})
	assertBytes2481(t, "NegQ2", query.NegQ2, []byte{0x08})
	assertBytes2481(t, "NegQ4", query.NegQ4, []byte{0x0a})
	assertBytes2481(t, "NegQ8", query.NegQ8, []byte{0x88})
	if ScoreLabel != "brq_1bit_estimated_cosine_q4" {
		t.Fatalf("ScoreLabel=%q", ScoreLabel)
	}
	if math.Abs(score-(-0.592816421950027)) > 1e-12 {
		t.Fatalf("score=%.15f want %.15f", score, -0.592816421950027)
	}
	if score != slow {
		t.Fatalf("bit-product score %.17g differs from slow %.17g", score, slow)
	}
}

func TestBitProductFormulaParity2481(t *testing.T) {
	plan, err := NewPlan(10, Config{Seed: 0x2481})
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	var ws Workspace
	encoded, err := plan.Encode(nil, []float32{1, -2, 3, -4, 5, -6, 7, -8, 9, -10}, &ws)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	query, err := plan.EncodeQuery([]float32{-0.5, 1.5, -2.5, 3.5, -4.5, 5.5, -6.5, 7.5, -8.5, 9.5}, &ws)
	if err != nil {
		t.Fatalf("EncodeQuery: %v", err)
	}
	assertBytes2481(t, "code", encoded.Code, []byte{0x41, 0x7b})
	if got, want := encoded.CodeCount, uint32(8); got != want {
		t.Fatalf("CodeCount=%d want %d", got, want)
	}
	if got, want := math.Float32bits(encoded.QuantizedDotProductInv), uint32(0x3e9cf8a9); got != want {
		t.Fatalf("QuantizedDotProductInv bits=%#x want %#x", got, want)
	}
	assertBytes2481(t, "sign", query.SignBits, []byte{0xbf, 0x84})
	wantWeights := []uint8{0, 15, 5, 5, 5, 11, 3, 13, 2, 2, 6, 5, 1, 4, 5, 7}
	if got := query.Weights; string(got) != string(wantWeights) {
		t.Fatalf("query.Weights=%v want %v", got, wantWeights)
	}
	assertBytes2481(t, "PosQ1", query.PosQ1, []byte{0xbe, 0x80})
	assertBytes2481(t, "PosQ2", query.PosQ2, []byte{0x22, 0x84})
	assertBytes2481(t, "PosQ4", query.PosQ4, []byte{0x9e, 0x84})
	assertBytes2481(t, "PosQ8", query.PosQ8, []byte{0xa2, 0x00})
	assertBytes2481(t, "NegQ1", query.NegQ1, []byte{0x40, 0x58})
	assertBytes2481(t, "NegQ2", query.NegQ2, []byte{0x40, 0x03})
	assertBytes2481(t, "NegQ4", query.NegQ4, []byte{0x00, 0x68})
	assertBytes2481(t, "NegQ8", query.NegQ8, []byte{0x00, 0x00})
	posSet, err := plan.BitProduct(encoded.Code, query.PosQ1, query.PosQ2, query.PosQ4, query.PosQ8)
	if err != nil {
		t.Fatalf("BitProduct pos: %v", err)
	}
	negSet, err := plan.BitProduct(encoded.Code, query.NegQ1, query.NegQ2, query.NegQ4, query.NegQ8)
	if err != nil {
		t.Fatalf("BitProduct neg: %v", err)
	}
	matchWeight := posSet + (query.NegativeWeightSumInt - negSet)
	signedWeight := int64(2*matchWeight) - int64(query.QueryWeightSumInt)
	manual := float64(signedWeight) * float64(query.QueryWeightScale) / (float64(encoded.QuantizedDotProductInv) * float64(plan.CodeDimensions()))
	score, err := plan.ScoreEncoded(query, encoded)
	if err != nil {
		t.Fatalf("ScoreEncoded: %v", err)
	}
	slow, err := plan.ScoreCosineSlow(query, encoded.Code, encoded.CodeCount, encoded.QuantizedDotProductInv)
	if err != nil {
		t.Fatalf("ScoreCosineSlow: %v", err)
	}
	if posSet != 0 || negSet != 22 || query.QueryWeightSumInt != 89 || query.NegativeWeightSumInt != 22 {
		t.Fatalf("formula terms pos=%d neg=%d sum=%d negsum=%d", posSet, negSet, query.QueryWeightSumInt, query.NegativeWeightSumInt)
	}
	if score != manual || score != slow {
		t.Fatalf("score parity failed score=%.17g manual=%.17g slow=%.17g", score, manual, slow)
	}
	if math.Abs(score-(-0.663334471054573)) > 1e-12 {
		t.Fatalf("score=%.15f want %.15f", score, -0.663334471054573)
	}
}

func TestPackedCodeBitOrderAndPadding2481(t *testing.T) {
	plan, err := NewPlan(3, Config{Seed: 0x2481}) // CodeDimensions=4, one physical byte with four high padding bits.
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	var ws Workspace
	encoded, err := plan.Encode(nil, []float32{1, -2, 3}, &ws)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded.Code) != 1 {
		t.Fatalf("code bytes=%d want 1", len(encoded.Code))
	}
	if encoded.Code[0]&0xf0 != 0 {
		t.Fatalf("encoded padding bits are non-zero: code=%08b", encoded.Code[0])
	}
	if got := plan.CountCodeBits(encoded.Code); got != encoded.CodeCount {
		t.Fatalf("CountCodeBits=%d want CodeCount=%d", got, encoded.CodeCount)
	}
	corrupt := append([]byte(nil), encoded.Code...)
	corrupt[0] |= 0x80
	if err := plan.ValidateCode(corrupt, encoded.CodeCount); err == nil || !strings.Contains(err.Error(), "padding") {
		t.Fatalf("ValidateCode corrupt padding err=%v want padding failure", err)
	}

	// Explicit LSB0 check for a ten-element logical row: element i maps to bit
	// i%8 of byte i/8, matching typed-column packed_bit_vector rows.
	bits := []bool{true, false, true, true, false, false, true, false, true, true}
	row := make([]byte, 2)
	var count uint32
	for i, bit := range bits {
		if bit {
			row[i>>3] |= byte(1 << uint(i&7))
			count++
		}
	}
	if got, want := row, []byte{0x4d, 0x03}; string(got) != string(want) {
		t.Fatalf("manual packed LSB0 row=% x want % x", got, want)
	}
	if got, want := count, uint32(6); got != want {
		t.Fatalf("manual code_count=%d want %d", got, want)
	}
	packedRows, err := typedcolumn.NewPackedUintRows(1, len(bits), CodeWidthBits, row)
	if err != nil {
		t.Fatalf("typedcolumn.NewPackedUintRows for brq row: %v", err)
	}
	if got, err := packedRows.Element(0, 8); err != nil || got != 1 {
		t.Fatalf("typedcolumn Element(8)=%d err=%v want LSB-first bit 1", got, err)
	}
	badRow := append([]byte(nil), row...)
	badRow[len(badRow)-1] |= 0x80
	if _, err := typedcolumn.NewPackedUintRows(1, len(bits), CodeWidthBits, badRow); err == nil || !strings.Contains(err.Error(), "padding") {
		t.Fatalf("typedcolumn.NewPackedUintRows corrupt padding err=%v want padding failure", err)
	}
}

func TestScoreFailsClosedOnMalformedSideInputs2481(t *testing.T) {
	plan, err := NewPlan(3, Config{Seed: 0x2481}) // CodeDimensions=4, so query/code padding is visible.
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	var ws Workspace
	encoded, err := plan.Encode(nil, []float32{1, -2, 3}, &ws)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	query, err := plan.EncodeQuery([]float32{0.5, 1.5, -0.25}, &ws)
	if err != nil {
		t.Fatalf("EncodeQuery: %v", err)
	}
	if _, err := plan.ScoreEncoded(query, encoded); err != nil {
		t.Fatalf("valid ScoreEncoded: %v", err)
	}

	for _, tc := range []struct {
		name string
		mut  func(*Query, *EncodedVector)
		want error
	}{
		{name: "query_code_dimensions", mut: func(q *Query, _ *EncodedVector) { q.CodeDimensions++ }, want: ErrDimensionMismatch},
		{name: "weight_len", mut: func(q *Query, _ *EncodedVector) { q.Weights = q.Weights[:len(q.Weights)-1] }, want: ErrDimensionMismatch},
		{name: "uint4_weight", mut: func(q *Query, _ *EncodedVector) {
			q.Weights = append([]uint8(nil), q.Weights...)
			q.Weights[0] = 16
		}, want: ErrDimensionMismatch},
		{name: "weight_sum", mut: func(q *Query, _ *EncodedVector) { q.QueryWeightSumInt++ }, want: ErrDimensionMismatch},
		{name: "negative_weight_sum", mut: func(q *Query, _ *EncodedVector) { q.NegativeWeightSumInt++ }, want: ErrDimensionMismatch},
		{name: "zero_scale", mut: func(q *Query, _ *EncodedVector) { q.QueryWeightScale = 0 }, want: ErrDegenerateVector},
		{name: "nan_scale", mut: func(q *Query, _ *EncodedVector) { q.QueryWeightScale = math.NaN() }, want: ErrDegenerateVector},
		{name: "tiny_scale", mut: func(q *Query, _ *EncodedVector) { q.QueryWeightScale = math.SmallestNonzeroFloat64 }, want: ErrDegenerateVector},
		{name: "large_scale", mut: func(q *Query, _ *EncodedVector) { q.QueryWeightScale = 1 }, want: ErrDegenerateVector},
		{name: "sign_padding", mut: func(q *Query, _ *EncodedVector) {
			q.SignBits = append([]byte(nil), q.SignBits...)
			q.SignBits[0] |= 0x80
		}, want: ErrDimensionMismatch},
		{name: "plane_padding", mut: func(q *Query, _ *EncodedVector) {
			q.PosQ1 = append([]byte(nil), q.PosQ1...)
			q.PosQ1[0] |= 0x80
		}, want: ErrDimensionMismatch},
		{name: "plane_mismatch", mut: func(q *Query, _ *EncodedVector) {
			q.NegQ8 = append([]byte(nil), q.NegQ8...)
			q.NegQ8[0] ^= 0x01
		}, want: ErrDimensionMismatch},
		{name: "code_count", mut: func(_ *Query, e *EncodedVector) { e.CodeCount++ }, want: ErrDimensionMismatch},
		{name: "code_padding", mut: func(_ *Query, e *EncodedVector) {
			e.Code = append([]byte(nil), e.Code...)
			e.Code[0] |= 0x80
		}, want: ErrDimensionMismatch},
		{name: "zero_qdp_inv", mut: func(_ *Query, e *EncodedVector) { e.QuantizedDotProductInv = 0 }, want: ErrDegenerateVector},
		{name: "nan_qdp_inv", mut: func(_ *Query, e *EncodedVector) { e.QuantizedDotProductInv = float32(math.NaN()) }, want: ErrDegenerateVector},
		{name: "tiny_qdp_inv", mut: func(_ *Query, e *EncodedVector) { e.QuantizedDotProductInv = math.SmallestNonzeroFloat32 }, want: ErrDegenerateVector},
		{name: "large_qdp_inv", mut: func(_ *Query, e *EncodedVector) { e.QuantizedDotProductInv = 1.25 }, want: ErrDegenerateVector},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badQuery := cloneQuery2481(query)
			badEncoded := EncodedVector{Code: append([]byte(nil), encoded.Code...), CodeCount: encoded.CodeCount, QuantizedDotProductInv: encoded.QuantizedDotProductInv}
			tc.mut(&badQuery, &badEncoded)
			if _, err := plan.ScoreEncoded(badQuery, badEncoded); !errors.Is(err, tc.want) {
				t.Fatalf("ScoreEncoded err=%v want %v", err, tc.want)
			}
		})
	}
}

func TestDegenerateInputsFailClosed2481(t *testing.T) {
	if _, err := NewPlan(0, DefaultConfig()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewPlan(0) err=%v want ErrInvalidConfig", err)
	}
	plan, err := NewPlan(4, DefaultConfig())
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	for _, tc := range []struct {
		name string
		vec  []float32
		want error
	}{
		{name: "short", vec: []float32{1, 2, 3}, want: ErrDimensionMismatch},
		{name: "long", vec: []float32{1, 2, 3, 4, 5}, want: ErrDimensionMismatch},
		{name: "zero", vec: []float32{0, 0, 0, 0}, want: ErrDegenerateVector},
		{name: "nan", vec: []float32{1, float32(math.NaN()), 0, 0}, want: ErrDegenerateVector},
		{name: "inf", vec: []float32{1, 0, float32(math.Inf(1)), 0}, want: ErrDegenerateVector},
	} {
		t.Run(tc.name+"_encode", func(t *testing.T) {
			if _, err := plan.Encode(nil, tc.vec, &Workspace{}); !errors.Is(err, tc.want) {
				t.Fatalf("Encode err=%v want %v", err, tc.want)
			}
		})
		t.Run(tc.name+"_query", func(t *testing.T) {
			if _, err := plan.EncodeQuery(tc.vec, &Workspace{}); !errors.Is(err, tc.want) {
				t.Fatalf("EncodeQuery err=%v want %v", err, tc.want)
			}
		})
	}
}

func TestReferenceKernelsNoAllocAfterWarm2481(t *testing.T) {
	plan, err := NewPlan(16, Config{Seed: 0x2481})
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	vector := []float32{0.30, -0.20, 0.70, 0.10, -0.55, 0.40, 0.05, -0.15, 0.33, -0.44, 0.12, 0.88, -0.25, 0.31, -0.05, 0.18}
	queryVec := []float32{-0.10, 0.35, 0.62, -0.41, 0.22, -0.09, 0.54, 0.13, -0.49, 0.27, 0.16, -0.31, 0.71, -0.06, 0.24, -0.19}
	var ws Workspace
	code := make([]byte, 0, plan.BytesPerCode())
	encoded, err := plan.Encode(code, vector, &ws)
	if err != nil {
		t.Fatalf("warm Encode: %v", err)
	}
	query, err := plan.EncodeQuery(queryVec, &ws)
	if err != nil {
		t.Fatalf("warm EncodeQuery: %v", err)
	}
	if _, err := plan.ScoreEncoded(query, encoded); err != nil {
		t.Fatalf("warm ScoreEncoded: %v", err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		encoded, err := plan.Encode(code, vector, &ws)
		if err != nil {
			panic(err)
		}
		query, err := plan.EncodeQuery(queryVec, &ws)
		if err != nil {
			panic(err)
		}
		score, err := plan.ScoreEncoded(query, encoded)
		if err != nil {
			panic(err)
		}
		brqEncodedSink = encoded
		brqQuerySink = query
		brqScoreSink += score
	})
	if allocs != 0 {
		t.Fatalf("reference encode/query/score allocs/run=%v want 0", allocs)
	}
	if !brqQuerySink.ValidFor(plan) || brqEncodedSink.CodeCount == 0 || brqScoreSink == 0 {
		t.Fatalf("unexpected zero sinks: query=%+v encoded=%+v score=%f", brqQuerySink, brqEncodedSink, brqScoreSink)
	}
}

func assertBytes2481(t *testing.T, name string, got, want []byte) {
	t.Helper()
	if string(got) != string(want) {
		t.Fatalf("%s=% x want % x", name, got, want)
	}
}

func cloneQuery2481(q Query) Query {
	q.SignBits = append([]byte(nil), q.SignBits...)
	q.Weights = append([]uint8(nil), q.Weights...)
	q.PosQ1 = append([]byte(nil), q.PosQ1...)
	q.PosQ2 = append([]byte(nil), q.PosQ2...)
	q.PosQ4 = append([]byte(nil), q.PosQ4...)
	q.PosQ8 = append([]byte(nil), q.PosQ8...)
	q.NegQ1 = append([]byte(nil), q.NegQ1...)
	q.NegQ2 = append([]byte(nil), q.NegQ2...)
	q.NegQ4 = append([]byte(nil), q.NegQ4...)
	q.NegQ8 = append([]byte(nil), q.NegQ8...)
	return q
}
