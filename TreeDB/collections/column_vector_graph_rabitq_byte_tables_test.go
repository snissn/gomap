package collections

import (
	"math"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rabitq"
)

func TestRabitQByteMismatchWeightsLSBAndPadding2477(t *testing.T) {
	query := rabitq.Query{
		SignBits:       []byte{0b1010_0101, 0b0000_0010},
		AbsWeights:     []float32{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
		WeightSum:      2047,
		CodeDimensions: 11,
	}
	var tableWS rabitq.Workspace
	tables, weightSum, ok := rabitq.PrepareQueryByteMismatchWeights(query, &tableWS)
	if !ok {
		t.Fatal("prepare query byte mismatch weights failed")
	}
	if weightSum != 2047 {
		t.Fatalf("weightSum=%v want 2047", weightSum)
	}
	if got := tables[0b0000_0001]; got != 1 {
		t.Fatalf("byte0 bit0 mismatch weight=%v want 1", got)
	}
	if got := tables[0b1000_0000]; got != 128 {
		t.Fatalf("byte0 bit7 mismatch weight=%v want 128", got)
	}
	if got := tables[0b1000_0101]; got != 1+4+128 {
		t.Fatalf("byte0 combined LSB-first mismatch weight=%v want %v", got, 1+4+128)
	}
	base := rabitq.ByteMismatchTableEntries
	if got := tables[base+0b0000_0001]; got != 256 {
		t.Fatalf("partial byte bit0 mismatch weight=%v want 256", got)
	}
	if got := tables[base+0b0000_0010]; got != 512 {
		t.Fatalf("partial byte bit1 mismatch weight=%v want 512", got)
	}
	if got := tables[base+0b0000_0100]; got != 1024 {
		t.Fatalf("partial byte bit2 mismatch weight=%v want 1024", got)
	}
	if got := tables[base+0b1111_1000]; got != 0 {
		t.Fatalf("partial byte high padding mismatch weight=%v want 0", got)
	}
	if got := tables[base+0b1111_1111]; got != 256+512+1024 {
		t.Fatalf("partial byte high padding combined mismatch weight=%v want %v", got, 256+512+1024)
	}

	paddedHighBitsOnly := []byte{query.SignBits[0], query.SignBits[1] | 0b1111_1000}
	got, ok := rabitqQuantizedCosineScoreWithByteTables(query, paddedHighBitsOnly, 1, tables, weightSum)
	if !ok {
		t.Fatal("byte-table score rejected code with only high padding-bit differences")
	}
	want, ok := rabitqQuantizedCosineScore(query, paddedHighBitsOnly, 1)
	if !ok {
		t.Fatal("current score rejected code with only high padding-bit differences")
	}
	if got != want {
		t.Fatalf("high padding score=%v want current=%v", got, want)
	}

	finalLogicalBitMismatch := []byte{query.SignBits[0], query.SignBits[1] ^ 0b0000_0100}
	got, ok = rabitqQuantizedCosineScoreWithByteTables(query, finalLogicalBitMismatch, 1, tables, weightSum)
	if !ok {
		t.Fatal("byte-table score rejected final logical bit mismatch")
	}
	want, ok = rabitqQuantizedCosineScore(query, finalLogicalBitMismatch, 1)
	if !ok {
		t.Fatal("current score rejected final logical bit mismatch")
	}
	if got != want || got != -1.0/11.0 {
		t.Fatalf("final logical mismatch score=%v want current=%v and -1/11", got, want)
	}
}

func TestRabitQByteTableScorerMatchesOracleVariedDimensions2477(t *testing.T) {
	for _, dims := range []int{1, 3, 7, 8, 9, 15, 16, 17, 31, 32, 64, 128} {
		t.Run("dims_"+strconv.Itoa(dims), func(t *testing.T) {
			plan, err := rabitq.NewPlan(dims, rabitq.DefaultConfig())
			if err != nil {
				t.Fatalf("NewPlan: %v", err)
			}
			var queryWS rabitq.Workspace
			query, err := plan.EncodeQuery(rabitqTestVector2477(dims, 17), &queryWS)
			if err != nil {
				t.Fatalf("EncodeQuery: %v", err)
			}
			tables, weightSum, ok := rabitq.PrepareQueryByteMismatchWeights(query, &queryWS)
			if !ok {
				t.Fatal("prepare query byte mismatch weights failed")
			}
			var encodeWS rabitq.Workspace
			for candidate := 0; candidate < 9; candidate++ {
				encoded, err := plan.Encode(nil, rabitqTestVector2477(dims, uint64(candidate+31)), &encodeWS)
				if err != nil {
					t.Fatalf("Encode candidate %d: %v", candidate, err)
				}
				got, ok := rabitqQuantizedCosineScoreWithByteTables(query, encoded.Code, encoded.QuantizedDotProductInv, tables, weightSum)
				if !ok {
					t.Fatalf("byte-table score rejected dims=%d candidate=%d", dims, candidate)
				}
				want, err := plan.ScoreEncoded(query, encoded)
				if err != nil {
					t.Fatalf("ScoreEncoded candidate %d: %v", candidate, err)
				}
				current, ok := rabitqQuantizedCosineScore(query, encoded.Code, encoded.QuantizedDotProductInv)
				if !ok {
					t.Fatalf("current score rejected dims=%d candidate=%d", dims, candidate)
				}
				if math.Abs(got-want) > 1e-9 || math.Abs(got-current) > 1e-9 {
					t.Fatalf("dims=%d candidate=%d byte-table score=%0.17g oracle=%0.17g current=%0.17g", dims, candidate, got, want, current)
				}
			}
		})
	}
}

func TestRabitQByteTableScorerFailClosed2477(t *testing.T) {
	valid := rabitq.Query{
		SignBits:       []byte{0b0101_1010, 0b0000_0011},
		AbsWeights:     []float32{1, 2, 3, 4, 5, 6, 7, 8, 9},
		WeightSum:      45,
		CodeDimensions: 9,
	}
	var tableWS rabitq.Workspace
	tables, weightSum, ok := rabitq.PrepareQueryByteMismatchWeights(valid, &tableWS)
	if !ok {
		t.Fatal("prepare valid query failed")
	}

	badQueries := []struct {
		name  string
		query rabitq.Query
	}{
		{name: "zero dimensions", query: rabitq.Query{SignBits: []byte{0}, AbsWeights: []float32{1}}},
		{name: "short sign bits", query: rabitq.Query{SignBits: []byte{0}, AbsWeights: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9}, CodeDimensions: 9}},
		{name: "short weights", query: rabitq.Query{SignBits: []byte{0, 0}, AbsWeights: []float32{1}, CodeDimensions: 9}},
		{name: "nan weight", query: rabitq.Query{SignBits: []byte{0, 0}, AbsWeights: []float32{1, 2, float32(math.NaN()), 4, 5, 6, 7, 8, 9}, CodeDimensions: 9}},
		{name: "inf weight", query: rabitq.Query{SignBits: []byte{0, 0}, AbsWeights: []float32{1, 2, float32(math.Inf(1)), 4, 5, 6, 7, 8, 9}, CodeDimensions: 9}},
		{name: "negative weight", query: rabitq.Query{SignBits: []byte{0, 0}, AbsWeights: []float32{1, 2, -3, 4, 5, 6, 7, 8, 9}, CodeDimensions: 9}},
		{name: "zero weight sum", query: rabitq.Query{SignBits: []byte{0, 0}, AbsWeights: make([]float32, 9), CodeDimensions: 9}},
	}
	for _, tc := range badQueries {
		if _, _, ok := rabitq.PrepareQueryByteMismatchWeights(tc.query, &tableWS); ok {
			t.Fatalf("prepare bad query %q succeeded", tc.name)
		}
	}

	badScores := []struct {
		name      string
		query     rabitq.Query
		code      []byte
		qdpInv    float32
		tables    []float64
		weightSum float64
	}{
		{name: "short code", query: valid, code: []byte{0}, qdpInv: 1, tables: tables, weightSum: weightSum},
		{name: "zero qdp", query: valid, code: []byte{0, 0}, qdpInv: 0, tables: tables, weightSum: weightSum},
		{name: "nan qdp", query: valid, code: []byte{0, 0}, qdpInv: float32(math.NaN()), tables: tables, weightSum: weightSum},
		{name: "inf qdp", query: valid, code: []byte{0, 0}, qdpInv: float32(math.Inf(1)), tables: tables, weightSum: weightSum},
		{name: "short tables", query: valid, code: []byte{0, 0}, qdpInv: 1, tables: tables[:len(tables)-1], weightSum: weightSum},
		{name: "nan weight sum", query: valid, code: []byte{0, 0}, qdpInv: 1, tables: tables, weightSum: math.NaN()},
	}
	for _, tc := range badScores {
		if _, ok := rabitqQuantizedCosineScoreWithByteTables(tc.query, tc.code, tc.qdpInv, tc.tables, tc.weightSum); ok {
			t.Fatalf("bad score %q succeeded", tc.name)
		}
	}
}

func TestRabitQByteMismatchWeightsScratchReuseNoAlloc2477(t *testing.T) {
	query := rabitq.Query{
		SignBits:       []byte{0b0101_1010, 0b0000_0011},
		AbsWeights:     []float32{1, 2, 3, 4, 5, 6, 7, 8, 9},
		WeightSum:      45,
		CodeDimensions: 9,
	}
	var tableWS rabitq.Workspace
	if _, _, ok := rabitq.PrepareQueryByteMismatchWeights(query, &tableWS); !ok {
		t.Fatal("prepare query byte mismatch weights failed")
	}
	allocs := testing.AllocsPerRun(1000, func() {
		_, weightSum, ok := rabitq.PrepareQueryByteMismatchWeights(query, &tableWS)
		if !ok || weightSum != 45 {
			panic("prepare query byte mismatch weights failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("prepared query byte mismatch weights allocs/run=%v want 0", allocs)
	}
}

func rabitqTestVector2477(dims int, seed uint64) []float32 {
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
