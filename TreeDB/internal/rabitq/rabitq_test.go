package rabitq

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
	rabitqEncodedSink EncodedVector
	rabitqQuerySink   Query
	rabitqScoreSink   float64
)

func TestContractIdentityAndShape2449(t *testing.T) {
	cfg := Config{Seed: 0x0123456789abcdef}
	identity := string(cfg.CanonicalBytes())
	for _, want := range []string{
		"codec=rabitq_1bit",
		"version=1",
		"metric=cosine",
		"normalization=unit_l2",
		"rotation=signed_permutation_fwht_padded_v1",
		"seed=0x0123456789abcdef",
		"storage_role=packed_codes",
		"storage_logical_type=packed_bit_vector",
		"storage_encoding=raw_packed_bit_vector",
		"bit_order=lsb0",
		"padding=zero",
		"code_width_bits=1",
	} {
		if !strings.Contains(identity, want) {
			t.Fatalf("canonical identity missing %q in:\n%s", want, identity)
		}
	}
	if got, want := cfg.Hash64(), uint64(0xd48ecfa48dc1b711); got != want {
		t.Fatalf("Config.Hash64=%#x want %#x", got, want)
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
		{dims: 128, codeDims: 128, codeBytes: 16},
		{dims: 129, codeDims: 256, codeBytes: 32},
	} {
		t.Run(fmt.Sprintf("dims_%d", tc.dims), func(t *testing.T) {
			plan, err := NewPlan(tc.dims, DefaultConfig())
			if err != nil {
				t.Fatalf("NewPlan(%d): %v", tc.dims, err)
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

func TestEncodeQueryScoreGolden2449(t *testing.T) {
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
	if got, want := math.Float32bits(query.WeightSum), uint32(0x4010d526); got != want {
		t.Fatalf("query.WeightSum bits=%#x want %#x", got, want)
	}
	wantWeights := []uint32{0x3f05b10f, 0x3e32416a, 0x3eded1c4, 0x3f1bf93d, 0x3db2416a, 0x3db2416a, 0x00000000, 0x3eb2416a}
	if len(query.AbsWeights) != len(wantWeights) {
		t.Fatalf("AbsWeights len=%d want %d", len(query.AbsWeights), len(wantWeights))
	}
	for i, want := range wantWeights {
		if got := math.Float32bits(query.AbsWeights[i]); got != want {
			t.Fatalf("AbsWeights[%d] bits=%#x want %#x", i, got, want)
		}
	}
	if math.Abs(score-(-0.577418595825)) > 1e-12 {
		t.Fatalf("score=%.15f want %.15f", score, -0.577418595825)
	}
}

func TestScoreCosineV1ScoringIdentity2560(t *testing.T) {
	plan, err := NewPlan(9, Config{Seed: 0x2560}) // CodeDimensions=16, no packed-byte padding.
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	query := Query{
		SignBits:       []byte{0b1010_0101, 0b0011_1100},
		AbsWeights:     []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		WeightSum:      136,
		CodeDimensions: 16,
	}
	code := []byte{0b1010_0001, 0b0010_1101}
	qdpInv := float32(0.5)

	got, err := plan.ScoreCosine(query, code, plan.CountCodeBits(code), qdpInv)
	if err != nil {
		t.Fatalf("ScoreCosine: %v", err)
	}
	var weightedSignDot float64
	for i, weight32 := range query.AbsWeights {
		mask := byte(1 << uint(i&7))
		if (code[i>>3]&mask != 0) == (query.SignBits[i>>3]&mask != 0) {
			weightedSignDot += float64(weight32)
		} else {
			weightedSignDot -= float64(weight32)
		}
	}
	want := weightedSignDot / (float64(qdpInv) * float64(query.CodeDimensions))
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("ScoreCosine=%0.17g want v1 weighted sign-dot formula %0.17g", got, want)
	}
	if math.Abs(got-10.75) > 1e-12 {
		t.Fatalf("ScoreCosine=%0.17g want golden 10.75", got)
	}

	badQuery := query
	badQuery.AbsWeights = append([]float32(nil), query.AbsWeights...)
	badQuery.AbsWeights[0] *= 2
	if _, err := plan.ScoreCosine(badQuery, code, plan.CountCodeBits(code), qdpInv); !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("ScoreCosine with calibrated weights but stale WeightSum err=%v want %v", err, ErrDimensionMismatch)
	}
}

func TestPackedCodeBitOrderAndPadding2449(t *testing.T) {
	plan, err := NewPlan(3, Config{Seed: 0x2449}) // CodeDimensions=4, one physical byte with four high padding bits.
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
		t.Fatalf("typedcolumn.NewPackedUintRows for rabitq row: %v", err)
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

func TestScoreCosineFailsClosedOnMalformedSideInputs2449(t *testing.T) {
	plan, err := NewPlan(3, Config{Seed: 0x2449}) // CodeDimensions=4, so query/code padding is visible.
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
		{name: "nan_weight", mut: func(q *Query, _ *EncodedVector) {
			q.AbsWeights = append([]float32(nil), q.AbsWeights...)
			q.AbsWeights[0] = float32(math.NaN())
		}, want: ErrDegenerateVector},
		{name: "inf_weight", mut: func(q *Query, _ *EncodedVector) {
			q.AbsWeights = append([]float32(nil), q.AbsWeights...)
			q.AbsWeights[0] = float32(math.Inf(1))
		}, want: ErrDegenerateVector},
		{name: "negative_weight", mut: func(q *Query, _ *EncodedVector) {
			q.AbsWeights = append([]float32(nil), q.AbsWeights...)
			q.AbsWeights[0] = -0.25
		}, want: ErrDegenerateVector},
		{name: "inf_weight_sum", mut: func(q *Query, _ *EncodedVector) { q.WeightSum = float32(math.Inf(1)) }, want: ErrDegenerateVector},
		{name: "mismatched_weight_sum", mut: func(q *Query, _ *EncodedVector) { q.WeightSum *= 2 }, want: ErrDimensionMismatch},
		{name: "query_padding", mut: func(q *Query, _ *EncodedVector) {
			q.SignBits = append([]byte(nil), q.SignBits...)
			q.SignBits[0] |= 0x80
		}, want: ErrDimensionMismatch},
		{name: "code_count", mut: func(_ *Query, e *EncodedVector) { e.CodeCount++ }, want: ErrDimensionMismatch},
		{name: "zero_qdp_inv", mut: func(_ *Query, e *EncodedVector) { e.QuantizedDotProductInv = 0 }, want: ErrDegenerateVector},
		{name: "nan_qdp_inv", mut: func(_ *Query, e *EncodedVector) { e.QuantizedDotProductInv = float32(math.NaN()) }, want: ErrDegenerateVector},
		{name: "tiny_qdp_inv", mut: func(_ *Query, e *EncodedVector) { e.QuantizedDotProductInv = math.SmallestNonzeroFloat32 }, want: ErrDegenerateVector},
		{name: "large_qdp_inv", mut: func(_ *Query, e *EncodedVector) { e.QuantizedDotProductInv = 1.25 }, want: ErrDegenerateVector},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badQuery := query
			badEncoded := encoded
			tc.mut(&badQuery, &badEncoded)
			if _, err := plan.ScoreEncoded(badQuery, badEncoded); !errors.Is(err, tc.want) {
				t.Fatalf("ScoreEncoded err=%v want %v", err, tc.want)
			}
		})
	}
}

func TestDegenerateInputsFailClosed2449(t *testing.T) {
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

func TestReferenceScorerSanityVersusExactCosine2449(t *testing.T) {
	plan, err := NewPlan(8, Config{Seed: 0x2449})
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	queryVec := []float32{0.30, -0.20, 0.70, 0.10, -0.55, 0.40, 0.05, -0.15}
	candidates := [][]float32{
		{0.30, -0.20, 0.70, 0.10, -0.55, 0.40, 0.05, -0.15},
		{-0.20, 0.15, -0.72, -0.05, 0.50, -0.35, -0.10, 0.18},
		{0.70, 0.60, -0.10, 0.05, 0.10, -0.05, 0.25, 0.20},
	}
	var ws Workspace
	query, err := plan.EncodeQuery(queryVec, &ws)
	if err != nil {
		t.Fatalf("EncodeQuery: %v", err)
	}
	var bestExact, bestApprox int
	bestExactScore := math.Inf(-1)
	bestApproxScore := math.Inf(-1)
	for i, candidate := range candidates {
		encoded, err := plan.Encode(nil, candidate, &ws)
		if err != nil {
			t.Fatalf("Encode candidate %d: %v", i, err)
		}
		approx, err := plan.ScoreEncoded(query, encoded)
		if err != nil {
			t.Fatalf("Score candidate %d: %v", i, err)
		}
		exact := exactCosineForTest2449(queryVec, candidate)
		if exact > bestExactScore {
			bestExactScore = exact
			bestExact = i
		}
		if approx > bestApproxScore {
			bestApproxScore = approx
			bestApprox = i
		}
		if math.Abs(exact) >= 0.20 && math.Signbit(exact) != math.Signbit(approx) {
			t.Fatalf("candidate %d exact=%f approx=%f sign mismatch", i, exact, approx)
		}
	}
	if bestExact != 0 || bestApprox != 0 {
		t.Fatalf("best exact=%d approx=%d want identical vector to win", bestExact, bestApprox)
	}
	if bestApproxScore <= 0.45 {
		t.Fatalf("best approximate score=%f want strong positive self score", bestApproxScore)
	}
}

func TestReferenceKernelsNoAllocAfterWarm2449(t *testing.T) {
	plan, err := NewPlan(16, Config{Seed: 0x2449})
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
		rabitqEncodedSink = encoded
		rabitqQuerySink = query
		rabitqScoreSink += score
	})
	if allocs != 0 {
		t.Fatalf("reference encode/query/score allocs/run=%v want 0", allocs)
	}
	if !rabitqQuerySink.ValidFor(plan) || rabitqEncodedSink.CodeCount == 0 || rabitqScoreSink == 0 {
		t.Fatalf("unexpected zero sinks: query=%+v encoded=%+v score=%f", rabitqQuerySink, rabitqEncodedSink, rabitqScoreSink)
	}
}

func TestPrepareQueryByteMismatchWeightsCacheRefreshes2519(t *testing.T) {
	plan, err := NewPlan(1536, DefaultConfig())
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	queryVec := rabitqVectorForTest2519(plan.VectorDimensions(), 17)
	changedQueryVec := rabitqVectorForTest2519(plan.VectorDimensions(), 31)

	var ws Workspace
	query, err := plan.EncodeQuery(queryVec, &ws)
	if err != nil {
		t.Fatalf("EncodeQuery: %v", err)
	}
	tables, weightSum, ok := PrepareQueryByteMismatchWeights(query, &ws)
	if !ok {
		t.Fatal("PrepareQueryByteMismatchWeights initial failed")
	}
	if !ws.queryByteMismatchCacheValid || ws.queryByteMismatchCodeDims != plan.CodeDimensions() || len(ws.queryByteMismatchWeights) != len(tables) || weightSum <= 0 {
		t.Fatalf("cache not populated: valid=%v codeDims=%d table=%d weightSum=%v", ws.queryByteMismatchCacheValid, ws.queryByteMismatchCodeDims, len(ws.queryByteMismatchWeights), weightSum)
	}
	firstTable := tables
	firstTableCopy := append([]float64(nil), tables...)
	firstWeightSum := weightSum

	queryAgain, err := plan.EncodeQuery(queryVec, &ws)
	if err != nil {
		t.Fatalf("EncodeQuery again: %v", err)
	}
	tablesAgain, weightSumAgain, ok := PrepareQueryByteMismatchWeights(queryAgain, &ws)
	if !ok {
		t.Fatal("PrepareQueryByteMismatchWeights cached failed")
	}
	if len(firstTable) == 0 || len(tablesAgain) == 0 || &firstTable[0] != &tablesAgain[0] || weightSumAgain != firstWeightSum {
		t.Fatalf("cache miss for repeated query: first_ptr=%p again_ptr=%p first_sum=%v again_sum=%v", &firstTable[0], &tablesAgain[0], firstWeightSum, weightSumAgain)
	}

	badQuery := queryAgain
	badQuery.AbsWeights = append([]float32(nil), queryAgain.AbsWeights...)
	badQuery.AbsWeights[9] = -1
	if _, _, ok := PrepareQueryByteMismatchWeights(badQuery, &ws); ok {
		t.Fatal("PrepareQueryByteMismatchWeights bad query unexpectedly succeeded")
	}
	if ws.queryByteMismatchCacheValid {
		t.Fatal("failed prepare left byte-table cache marked valid")
	}
	queryAfterFailure, err := plan.EncodeQuery(queryVec, &ws)
	if err != nil {
		t.Fatalf("EncodeQuery after failure: %v", err)
	}
	tablesAfterFailure, weightSumAfterFailure, ok := PrepareQueryByteMismatchWeights(queryAfterFailure, &ws)
	if !ok {
		t.Fatal("PrepareQueryByteMismatchWeights after failed prepare failed")
	}
	if weightSumAfterFailure != firstWeightSum || !equalFloat64Slice(tablesAfterFailure, firstTableCopy) {
		t.Fatalf("cache rebuild after failed prepare mismatch: got_sum=%v want_sum=%v", weightSumAfterFailure, firstWeightSum)
	}

	changedQuery, err := plan.EncodeQuery(changedQueryVec, &ws)
	if err != nil {
		t.Fatalf("EncodeQuery changed: %v", err)
	}
	changedTables, changedWeightSum, ok := PrepareQueryByteMismatchWeights(changedQuery, &ws)
	if !ok {
		t.Fatal("PrepareQueryByteMismatchWeights changed failed")
	}
	var independentWS Workspace
	independentQuery, err := plan.EncodeQuery(changedQueryVec, &independentWS)
	if err != nil {
		t.Fatalf("EncodeQuery independent changed: %v", err)
	}
	wantTables, wantWeightSum, ok := PrepareQueryByteMismatchWeights(independentQuery, &independentWS)
	if !ok {
		t.Fatal("PrepareQueryByteMismatchWeights independent changed failed")
	}
	if changedWeightSum != wantWeightSum || !equalFloat64Slice(changedTables, wantTables) {
		t.Fatalf("changed query cache refresh mismatch: got_sum=%v want_sum=%v", changedWeightSum, wantWeightSum)
	}
}

func BenchmarkReferenceScoreCosine2449(b *testing.B) {
	plan, err := NewPlan(128, DefaultConfig())
	if err != nil {
		b.Fatalf("NewPlan: %v", err)
	}
	vector := make([]float32, plan.VectorDimensions())
	queryVec := make([]float32, plan.VectorDimensions())
	for i := range vector {
		vector[i] = float32((i%17)-8) / 17
		queryVec[i] = float32((i%23)-11) / 23
	}
	var ws Workspace
	encoded, err := plan.Encode(nil, vector, &ws)
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}
	query, err := plan.EncodeQuery(queryVec, &ws)
	if err != nil {
		b.Fatalf("EncodeQuery: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(plan.BytesPerCode()))
	b.ResetTimer()
	var score float64
	for i := 0; i < b.N; i++ {
		s, err := plan.ScoreEncoded(query, encoded)
		if err != nil {
			b.Fatalf("ScoreEncoded: %v", err)
		}
		score += s
	}
	rabitqScoreSink += score
}

func rabitqVectorForTest2519(dims int, seed uint64) []float32 {
	v := make([]float32, dims)
	for i := range v {
		x := math.Sin(float64((i+1)*int(seed+3))*0.731) + 0.5*math.Cos(float64((i+5)*int(seed+11))*0.173)
		if x == 0 {
			x = float64(i+1) * 0.001
		}
		v[i] = float32(x)
	}
	return v
}

func equalFloat64Slice(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func exactCosineForTest2449(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		af := float64(a[i])
		bf := float64(b[i])
		dot += af * bf
		na += af * af
		nb += bf * bf
	}
	return dot / math.Sqrt(na*nb)
}
