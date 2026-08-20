package vectorops

import (
	"fmt"
	"testing"
)

var (
	scalarU8DotBatchStatusSink ScalarU8DotBatchStatus
	scalarU8DotBatchIntSink    int64
)

func TestDotScalarU8CenteredIndexedParity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dims   int
		rowIDs []uint32
	}{
		{name: "zero_rows", dims: 32, rowIDs: nil},
		{name: "short_dims_single", dims: 1, rowIDs: []uint32{0}},
		{name: "short_dims_tail", dims: 7, rowIDs: []uint32{0, 1, 2, 3, 4}},
		{name: "below_simd_width", dims: 15, rowIDs: []uint32{0, 2, 1}},
		{name: "exact_simd_width", dims: 16, rowIDs: []uint32{0, 1, 2, 3}},
		{name: "tail17", dims: 17, rowIDs: []uint32{0, 3, 1, 2}},
		{name: "tail31", dims: 31, rowIDs: []uint32{5, 1, 5, 0, 3}},
		{name: "dims32_shuffled_repeated", dims: 32, rowIDs: []uint32{7, 0, 3, 7, 1, 6, 2, 5, 4, 0}},
		{name: "tail33", dims: 33, rowIDs: []uint32{2, 9, 0, 7, 3, 11, 1, 6, 10, 4, 8, 5}},
		{name: "dims64", dims: 64, rowIDs: []uint32{12, 2, 9, 0, 7, 3, 15, 1, 6, 10, 4, 14, 5}},
		{name: "tail65", dims: 65, rowIDs: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{name: "dims128_larger", dims: 128, rowIDs: scalarU8DotBatchTestRowIDs(64, 97)},
		{name: "avx512_tail159", dims: 159, rowIDs: scalarU8DotBatchTestRowIDs(17, 31)},
		{name: "avx512_tail191", dims: 191, rowIDs: scalarU8DotBatchTestRowIDs(17, 37)},
		{name: "dims256", dims: 256, rowIDs: scalarU8DotBatchTestRowIDs(16, 41)},
		{name: "dims768_odd_rows", dims: 768, rowIDs: scalarU8DotBatchTestRowIDs(17, 47)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseRows := maxScalarU8RowID(tc.rowIDs) + 1
			codes := scalarU8DotBatchTestCodes(baseRows, tc.dims)
			query := scalarU8DotBatchTestQuery(t, tc.dims, 17)
			got := make([]int64, len(tc.rowIDs))

			status := DotScalarU8CenteredIndexed(got, codes, query, tc.rowIDs, tc.dims)
			if status.Invalid {
				t.Fatalf("DotScalarU8CenteredIndexed rejected valid shape: %+v", status)
			}
			if status.Rows != len(tc.rowIDs) {
				t.Fatalf("status.Rows=%d want %d", status.Rows, len(tc.rowIDs))
			}
			if status.Rows > 0 && status.Optimized == status.Fallback {
				t.Fatalf("optimized/fallback status mismatch: %+v", status)
			}
			if status.Optimized && !ScalarU8DotBatchOptimizedAvailable() {
				t.Fatalf("optimized status on backend without optimized batch support: %+v impl=%s", status, ScalarU8DotBatchImplementation())
			}
			if DotScalarU8CenteredIndexedOptimizedEligible(status.Rows, tc.dims) && status.Rows > 0 && !status.Optimized {
				t.Fatalf("status=%+v want optimized eligible backend impl=%s", status, ScalarU8DotBatchImplementation())
			}

			want := make([]int64, len(tc.rowIDs))
			dotScalarU8CenteredIndexedScalar(want, codes, query, tc.rowIDs, tc.dims, len(tc.rowIDs))
			assertInt64SliceExact(t, got, want)
		})
	}
}

func TestDotScalarU8CenteredIndexedPrevalidatedParity(t *testing.T) {
	t.Parallel()

	const dims = 128
	rowIDs := []uint32{12, 2, 9, 0, 7, 3, 15, 1, 6, 10, 4, 14, 5}
	codes := scalarU8DotBatchTestCodes(16, dims)
	query := scalarU8DotBatchTestQuery(t, dims, 19)
	got := make([]int64, len(rowIDs))
	status := DotScalarU8CenteredIndexedPrevalidated(got, codes, query, rowIDs, dims)
	if status.Invalid || status.Rows != len(rowIDs) {
		t.Fatalf("prevalidated status=%+v want rows=%d", status, len(rowIDs))
	}
	want := make([]int64, len(rowIDs))
	dotScalarU8CenteredIndexedScalar(want, codes, query, rowIDs, dims, len(rowIDs))
	assertInt64SliceExact(t, got, want)

	badQuery := ScalarU8CenteredQuery{}
	before := append([]int64(nil), got...)
	badStatus := DotScalarU8CenteredIndexedPrevalidated(got, codes, badQuery, rowIDs, dims)
	if !badStatus.Invalid || badStatus.Rows != 0 {
		t.Fatalf("prevalidated invalid status=%+v want invalid", badStatus)
	}
	assertInt64SliceExact(t, got, before)
}

func TestDotScalarU8CenteredIndexedOptimizedStatusAndFallback(t *testing.T) {
	t.Parallel()

	codes := scalarU8DotBatchTestCodes(4, 64)
	query := scalarU8DotBatchTestQuery(t, 64, 23)
	rowIDs := []uint32{0, 1, 2, 3}
	dst := make([]int64, len(rowIDs))
	status := DotScalarU8CenteredIndexed(dst, codes, query, rowIDs, 64)
	if status.Invalid || status.Rows != len(rowIDs) {
		t.Fatalf("status=%+v want valid rows=%d", status, len(rowIDs))
	}
	if DotScalarU8CenteredIndexedOptimizedEligible(len(rowIDs), 64) {
		if !status.Optimized || status.Fallback {
			t.Fatalf("status=%+v want optimized indexed backend", status)
		}
	} else if status.Optimized || !status.Fallback {
		t.Fatalf("status=%+v want platform fallback", status)
	}
	want := make([]int64, len(rowIDs))
	dotScalarU8CenteredIndexedScalar(want, codes, query, rowIDs, 64, len(rowIDs))
	assertInt64SliceExact(t, dst, want)

	tinyDst := make([]int64, len(rowIDs))
	tinyStatus := DotScalarU8CenteredIndexed(tinyDst, codes, ScalarU8CenteredQuery{values: query.values[:15]}, rowIDs, 15)
	if tinyStatus.Invalid || tinyStatus.Rows != len(rowIDs) || tinyStatus.Optimized || !tinyStatus.Fallback {
		t.Fatalf("tiny status=%+v want scalar fallback", tinyStatus)
	}
	tinyWant := make([]int64, len(rowIDs))
	dotScalarU8CenteredIndexedScalar(tinyWant, codes, ScalarU8CenteredQuery{values: query.values[:15]}, rowIDs, 15, len(rowIDs))
	assertInt64SliceExact(t, tinyDst, tinyWant)

	singleDst := make([]int64, 1)
	singleStatus := DotScalarU8CenteredIndexed(singleDst, codes, query, rowIDs[:1], 64)
	if singleStatus.Invalid || singleStatus.Rows != 1 {
		t.Fatalf("single-row status=%+v want valid row", singleStatus)
	}
	if DotScalarU8CenteredIndexedOptimizedEligible(1, 64) {
		if !singleStatus.Optimized || singleStatus.Fallback {
			t.Fatalf("single-row status=%+v want optimized indexed backend", singleStatus)
		}
	} else if singleStatus.Optimized || !singleStatus.Fallback {
		t.Fatalf("single-row status=%+v want scalar fallback", singleStatus)
	}
	singleWant := make([]int64, 1)
	dotScalarU8CenteredIndexedScalar(singleWant, codes, query, rowIDs[:1], 64, 1)
	assertInt64SliceExact(t, singleDst, singleWant)
}

func TestDotScalarU8CenteredIndexedMinRows(t *testing.T) {
	t.Parallel()

	codes := scalarU8DotBatchTestCodes(2, 4)
	query := scalarU8DotBatchTestQuery(t, 4, 29)
	dst := []int64{-1, -1}
	status := DotScalarU8CenteredIndexed(dst, codes, query, []uint32{0, 1, 99}, 4)
	if status.Invalid {
		t.Fatalf("row IDs beyond min(dst,rowIDs) should be ignored: %+v", status)
	}
	if status.Rows != len(dst) {
		t.Fatalf("rows=%d want %d", status.Rows, len(dst))
	}
	want := make([]int64, len(dst))
	dotScalarU8CenteredIndexedScalar(want, codes, query, []uint32{0, 1}, 4, len(dst))
	assertInt64SliceExact(t, dst, want)
}

func TestDotScalarU8CenteredIndexedInvalidShapesLeaveDestinationUnchanged(t *testing.T) {
	t.Parallel()

	codes := scalarU8DotBatchTestCodes(2, 4)
	query := scalarU8DotBatchTestQuery(t, 4, 31)
	wrongDimsQuery := scalarU8DotBatchTestQuery(t, 3, 37)

	cases := []struct {
		name   string
		codes  []byte
		query  ScalarU8CenteredQuery
		rowIDs []uint32
		dims   int
	}{
		{name: "zero_dims", codes: codes, query: query, rowIDs: []uint32{0, 1}, dims: 0},
		{name: "invalid_query", codes: codes, query: ScalarU8CenteredQuery{}, rowIDs: []uint32{0}, dims: 4},
		{name: "wrong_query_dims", codes: codes, query: wrongDimsQuery, rowIDs: []uint32{0}, dims: 4},
		{name: "short_codes", codes: codes[:3], query: query, rowIDs: []uint32{0}, dims: 4},
		{name: "row_out_of_range", codes: codes, query: query, rowIDs: []uint32{0, 2}, dims: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dst := []int64{-11, -12, -13}
			before := append([]int64(nil), dst...)
			status := DotScalarU8CenteredIndexed(dst, tc.codes, tc.query, tc.rowIDs, tc.dims)
			if !status.Invalid || status.Rows != 0 || status.Optimized || status.Fallback {
				t.Fatalf("status=%+v want invalid without writes", status)
			}
			assertInt64SliceExact(t, dst, before)
		})
	}
}

func TestDotScalarU8CenteredIndexedUsesReslicedQuerySum(t *testing.T) {
	t.Parallel()

	const (
		preparedDims = 32
		dims         = 16
	)
	queryCodes := make([]byte, preparedDims)
	for i := 0; i < dims; i++ {
		queryCodes[i] = 255
	}
	scratch := make([]ScalarU8CenteredCode, 0, preparedDims)
	query, _, ok := PrepareScalarU8CenteredQuery(scratch, queryCodes, preparedDims)
	if !ok {
		t.Fatal("PrepareScalarU8CenteredQuery rejected resliced query")
	}
	query.values = query.values[:dims]

	codes := make([]byte, 2*dims)
	for i := 0; i < dims; i++ {
		codes[i] = 255
		codes[dims+i] = 0
	}
	dst := make([]int64, 2)
	status := DotScalarU8CenteredIndexed(dst, codes, query, []uint32{0, 1}, dims)
	if status.Invalid || status.Rows != 2 {
		t.Fatalf("status=%+v want valid resliced query rows", status)
	}
	want := []int64{int64(65025 * dims), -int64(65025 * dims)}
	assertInt64SliceExact(t, dst, want)
}

func TestDotScalarU8CenteredIndexedZeroAllocs(t *testing.T) {
	codes := scalarU8DotBatchTestCodes(32, 128)
	query := scalarU8DotBatchTestQuery(t, 128, 41)
	rowIDs := []uint32{12, 2, 9, 0, 7, 3, 15, 1, 6, 10, 4, 14, 5}
	dst := make([]int64, len(rowIDs))

	allocs := testing.AllocsPerRun(1000, func() {
		status := DotScalarU8CenteredIndexed(dst, codes, query, rowIDs, 128)
		scalarU8DotBatchStatusSink = status
		scalarU8DotBatchIntSink += dst[0]
	})
	if allocs != 0 {
		t.Fatalf("DotScalarU8CenteredIndexed allocs/run=%v want 0", allocs)
	}
	if scalarU8DotBatchIntSink == 0 && scalarU8DotBatchStatusSink.Rows == 0 {
		t.Fatalf("unexpected zero sinks")
	}
}

func TestDotScalarU8CenteredIndexedOverflowSensitiveDims(t *testing.T) {
	t.Parallel()

	const dims = 40000
	queryCodes := make([]byte, dims)
	for i := range queryCodes {
		queryCodes[i] = 255
	}
	scratch := make([]ScalarU8CenteredCode, 0, dims)
	query, _, ok := PrepareScalarU8CenteredQuery(scratch, queryCodes, dims)
	if !ok {
		t.Fatal("PrepareScalarU8CenteredQuery rejected overflow-sensitive query")
	}
	codes := make([]byte, 2*dims)
	for i := 0; i < dims; i++ {
		codes[i] = 255
		codes[dims+i] = 0
	}
	dst := make([]int64, 2)
	status := DotScalarU8CenteredIndexed(dst, codes, query, []uint32{0, 1}, dims)
	if status.Invalid || status.Rows != 2 {
		t.Fatalf("status=%+v want valid overflow-sensitive rows", status)
	}
	// Scalar_u8 centering maps both all-255 query codes and all-255 row codes to
	// +255, and all-0 row codes to -255, so each dimension contributes +/-65025.
	want := []int64{int64(65025 * dims), -int64(65025 * dims)}
	assertInt64SliceExact(t, dst, want)
}

func BenchmarkDotScalarU8CenteredIndexed(b *testing.B) {
	benchmarkDotScalarU8CenteredIndexedMatrix(b, "optimized_or_fallback", func(dst []int64, codes []byte, query ScalarU8CenteredQuery, rowIDs []uint32, dims, rows int) ScalarU8DotBatchStatus {
		return DotScalarU8CenteredIndexed(dst, codes, query, rowIDs, dims)
	})
}

func BenchmarkDotScalarU8CenteredIndexedScalar(b *testing.B) {
	benchmarkDotScalarU8CenteredIndexedMatrix(b, "scalar_reference", func(dst []int64, codes []byte, query ScalarU8CenteredQuery, rowIDs []uint32, dims, rows int) ScalarU8DotBatchStatus {
		dotScalarU8CenteredIndexedScalar(dst, codes, query, rowIDs, dims, rows)
		return scalarU8DotBatchStatus(rows, false)
	})
}

func benchmarkDotScalarU8CenteredIndexedMatrix(b *testing.B, impl string, call func(dst []int64, codes []byte, query ScalarU8CenteredQuery, rowIDs []uint32, dims, rows int) ScalarU8DotBatchStatus) {
	b.Helper()
	dimsCases := []int{32, 64, 128, 256, 768, 1536}
	rowCases := []int{1, 2, 8, 16, 64}
	for _, dims := range dimsCases {
		for _, rows := range rowCases {
			name := fmt.Sprintf("impl=%s/dims=%d/rows=%d/indexed", impl, dims, rows)
			b.Run(name, func(b *testing.B) {
				baseRows := rows*3 + 17
				codes := scalarU8DotBatchTestCodes(baseRows, dims)
				query := scalarU8DotBatchTestQuery(b, dims, 43)
				rowIDs := scalarU8DotBatchTestRowIDs(rows, baseRows)
				dst := make([]int64, rows)
				benchmarkDotScalarU8CenteredIndexedCall(b, dims, rows, func() ScalarU8DotBatchStatus {
					return call(dst, codes, query, rowIDs, dims, rows)
				}, dst)
			})
		}
	}
}

func benchmarkDotScalarU8CenteredIndexedCall(b *testing.B, dims, rows int, call func() ScalarU8DotBatchStatus, dst []int64) {
	b.Helper()
	b.ReportAllocs()

	var optimizedCalls, fallbackCalls, invalidCalls int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status := call()
		if status.Optimized {
			optimizedCalls++
		}
		if status.Fallback {
			fallbackCalls++
		}
		if status.Invalid {
			invalidCalls++
		}
		scalarU8DotBatchStatusSink = status
		scalarU8DotBatchIntSink += dst[0]
	}
	elapsed := b.Elapsed().Seconds()
	b.StopTimer()

	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(rows), "rows/tile")
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
		b.ReportMetric(float64(b.N*rows)/elapsed, "scores/sec")
	}
	if b.N > 0 {
		b.ReportMetric(float64(optimizedCalls)/float64(b.N), "optimized/call")
		b.ReportMetric(float64(fallbackCalls)/float64(b.N), "fallback/call")
		b.ReportMetric(float64(invalidCalls)/float64(b.N), "invalid/call")
	}
}

func scalarU8DotBatchTestQuery(tb testing.TB, dims int, salt int) ScalarU8CenteredQuery {
	tb.Helper()
	codes := make([]byte, dims)
	for i := range codes {
		codes[i] = byte((i*salt + 5) & 0xff)
	}
	scratch := make([]ScalarU8CenteredCode, 0, dims)
	query, _, ok := PrepareScalarU8CenteredQuery(scratch, codes, dims)
	if !ok {
		tb.Fatalf("PrepareScalarU8CenteredQuery dims=%d failed", dims)
	}
	return query
}

func scalarU8DotBatchTestCodes(rows, dims int) []byte {
	if rows <= 0 || dims <= 0 {
		return nil
	}
	codes := make([]byte, rows*dims)
	for row := 0; row < rows; row++ {
		start := row * dims
		for col := 0; col < dims; col++ {
			codes[start+col] = byte((row*37 + col*17 + row*col*3 + 11) & 0xff)
		}
	}
	return codes
}

func scalarU8DotBatchTestRowIDs(rows, baseRows int) []uint32 {
	rowIDs := make([]uint32, rows)
	for i := range rowIDs {
		rowIDs[i] = uint32((i*7 + 3) % baseRows)
	}
	return rowIDs
}

func maxScalarU8RowID(rowIDs []uint32) int {
	maxID := 0
	for _, rowID := range rowIDs {
		if int(rowID) > maxID {
			maxID = int(rowID)
		}
	}
	return maxID
}

func assertInt64SliceExact(t *testing.T, got, want []int64) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}
