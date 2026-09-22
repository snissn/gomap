package vectorops

import "testing"

var (
	scalarU8CenteredQuerySink ScalarU8CenteredQuery
	scalarU8CenteredDotSink   int64
)

func TestScalarU8CenteredQueryParity2258(t *testing.T) {
	codes := []byte{0, 1, 127, 128, 254, 255}
	row := []byte{255, 254, 128, 127, 1, 0}
	dst := make([]ScalarU8CenteredCode, 0, len(codes))

	query, scratch, ok := PrepareScalarU8CenteredQuery(dst, codes, len(codes))
	if !ok || !query.ValidForDims(len(codes)) || len(scratch) != len(codes) {
		t.Fatalf("PrepareScalarU8CenteredQuery ok=%v query=%+v scratch_len=%d", ok, query, len(scratch))
	}
	wantCentered := []ScalarU8CenteredCode{-255, -253, -1, 1, 253, 255}
	var wantSum int64
	for i, want := range wantCentered {
		wantSum += int64(want)
		got, ok := query.Value(i)
		if !ok || got != want {
			t.Fatalf("centered[%d]=%d ok=%v want %d", i, got, ok, want)
		}
		if got := ScalarU8CenteredValue(codes[i]); got != want {
			t.Fatalf("ScalarU8CenteredValue(%d)=%d want %d", codes[i], got, want)
		}
	}
	if got := query.CenteredSum(); got != wantSum {
		t.Fatalf("query.CenteredSum()=%d want %d", got, wantSum)
	}
	if got := (ScalarU8CenteredQuery{values: wantCentered}).CenteredSum(); got != wantSum {
		t.Fatalf("manual query CenteredSum()=%d want %d", got, wantSum)
	}

	resliced := query
	resliced.values = resliced.values[:3]
	var wantReslicedSum int64
	for _, v := range wantCentered[:3] {
		wantReslicedSum += int64(v)
	}
	if got := resliced.CenteredSum(); got != wantReslicedSum {
		t.Fatalf("resliced query CenteredSum()=%d want %d", got, wantReslicedSum)
	}

	gotDot, ok := ScalarU8CenteredDot(query, row)
	if !ok {
		t.Fatal("ScalarU8CenteredDot rejected valid query/row")
	}
	var wantDot int64
	for i, qc := range codes {
		q := int64(2*int(qc) - 255)
		c := int64(2*int(row[i]) - 255)
		wantDot += q * c
	}
	if gotDot != wantDot {
		t.Fatalf("centered dot=%d want legacy dot=%d", gotDot, wantDot)
	}
}

func TestScalarU8CenteredQueryFromCentered2258(t *testing.T) {
	centered := []ScalarU8CenteredCode{-255, -1, 255}
	var sum int64
	for _, value := range centered {
		sum += int64(value)
	}
	query, scratch, ok := PrepareScalarU8CenteredQueryFromCentered(centered, len(centered), sum)
	if !ok || !query.ValidForDims(len(centered)) || len(scratch) != len(centered) {
		t.Fatalf("PrepareScalarU8CenteredQueryFromCentered ok=%v query=%+v scratch_len=%d", ok, query, len(scratch))
	}
	if got := query.CenteredSum(); got != sum {
		t.Fatalf("query.CenteredSum()=%d want cached sum %d", got, sum)
	}
	for i, want := range centered {
		got, ok := query.Value(i)
		if !ok || got != want {
			t.Fatalf("query.Value(%d)=%d ok=%v want %d", i, got, ok, want)
		}
	}
}

func TestScalarU8CenteredQueryFromCenteredWithHalf(t *testing.T) {
	centered := []ScalarU8CenteredCode{-255, -1, 1, 255}
	half := []int8{-128, -1, 0, 127}
	query, scratch, ok := PrepareScalarU8CenteredQueryFromCenteredWithHalf(centered, half, len(centered), 0)
	if !ok || !query.ValidForDims(len(centered)) || len(scratch) != len(centered) {
		t.Fatalf("prepared-byte query ok=%v query=%+v scratch_len=%d", ok, query, len(scratch))
	}
	if len(query.halfValues) != len(half) {
		t.Fatalf("half values=%d want %d", len(query.halfValues), len(half))
	}
	if _, scratch, ok := PrepareScalarU8CenteredQueryFromCenteredWithHalf(centered, half[:3], len(centered), 0); ok || len(scratch) != 0 {
		t.Fatalf("short half metadata accepted")
	}
}

func TestScalarU8CenteredQueryRejectsInvalidShapes2258(t *testing.T) {
	codes := []byte{1, 2, 3}
	validScratch := make([]ScalarU8CenteredCode, 0, len(codes))
	cases := []struct {
		name  string
		dst   []ScalarU8CenteredCode
		codes []byte
		dims  int
	}{
		{name: "zero_dims", dst: validScratch, codes: codes, dims: 0},
		{name: "negative_dims", dst: validScratch, codes: codes, dims: -1},
		{name: "short_codes", dst: validScratch, codes: codes[:2], dims: 3},
		{name: "long_codes", dst: validScratch, codes: append(append([]byte(nil), codes...), 4), dims: 3},
		{name: "nil_scratch", dst: nil, codes: codes, dims: 3},
		{name: "short_scratch", dst: make([]ScalarU8CenteredCode, 0, 2), codes: codes, dims: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query, scratch, ok := PrepareScalarU8CenteredQuery(tc.dst, tc.codes, tc.dims)
			if ok || query.Valid() || len(scratch) != 0 {
				t.Fatalf("PrepareScalarU8CenteredQuery ok=%v query=%+v scratch_len=%d want invalid", ok, query, len(scratch))
			}
		})
	}
	for _, tc := range []struct {
		name   string
		values []ScalarU8CenteredCode
		dims   int
	}{
		{name: "zero_dims", values: make([]ScalarU8CenteredCode, 0), dims: 0},
		{name: "negative_dims", values: make([]ScalarU8CenteredCode, 0), dims: -1},
		{name: "short_values", values: make([]ScalarU8CenteredCode, 2), dims: 3},
		{name: "long_values", values: make([]ScalarU8CenteredCode, 4), dims: 3},
	} {
		t.Run("from_centered/"+tc.name, func(t *testing.T) {
			query, scratch, ok := PrepareScalarU8CenteredQueryFromCentered(tc.values, tc.dims, 0)
			if ok || query.Valid() || len(scratch) != 0 {
				t.Fatalf("PrepareScalarU8CenteredQueryFromCentered ok=%v query=%+v scratch_len=%d want invalid", ok, query, len(scratch))
			}
		})
	}

	query, _, ok := PrepareScalarU8CenteredQuery(validScratch, codes, len(codes))
	if !ok {
		t.Fatal("valid centered query rejected")
	}
	if dot, ok := ScalarU8CenteredDot(ScalarU8CenteredQuery{}, codes); ok || dot != 0 {
		t.Fatalf("zero query dot=%d ok=%v want invalid", dot, ok)
	}
	if dot, ok := ScalarU8CenteredDot(query, codes[:2]); ok || dot != 0 {
		t.Fatalf("short row dot=%d ok=%v want invalid", dot, ok)
	}
}

func TestScalarU8CenteredQueryZeroAllocs2258(t *testing.T) {
	codes := make([]byte, 128)
	row := make([]byte, 128)
	for i := range codes {
		codes[i] = byte((i*17 + 3) & 0xff)
		row[i] = byte((255 - i*11) & 0xff)
	}
	dst := make([]ScalarU8CenteredCode, 0, len(codes))

	allocs := testing.AllocsPerRun(1000, func() {
		query, scratch, ok := PrepareScalarU8CenteredQuery(dst, codes, len(codes))
		if !ok {
			panic("valid centered query rejected")
		}
		dot, ok := ScalarU8CenteredDot(query, row)
		if !ok {
			panic("valid centered dot rejected")
		}
		scalarU8CenteredQuerySink = query
		scalarU8CenteredDotSink += dot + int64(len(scratch))
	})
	if allocs != 0 {
		t.Fatalf("centered scalar_u8 query allocs/run=%v want 0", allocs)
	}
	if !scalarU8CenteredQuerySink.ValidForDims(len(codes)) || scalarU8CenteredDotSink == 0 {
		t.Fatalf("unexpected zero sinks: query=%+v dot=%d", scalarU8CenteredQuerySink, scalarU8CenteredDotSink)
	}
}
