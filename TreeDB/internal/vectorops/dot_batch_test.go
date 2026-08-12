package vectorops

import (
	"fmt"
	"math"
	"testing"
)

var (
	dotBatchStatusSink DotFloat32BatchStatus
	dotBatchFloatSink  float32
)

func TestDotFloat32IndexedParity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dims   int
		rowIDs []uint32
	}{
		{name: "short_dims_single", dims: 1, rowIDs: []uint32{0}},
		{name: "short_dims_tail", dims: 7, rowIDs: []uint32{0, 1, 2, 3, 4}},
		{name: "contiguous_batch", dims: 64, rowIDs: []uint32{0, 1, 2, 3}},
		{name: "contiguous_tail", dims: 65, rowIDs: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
		{name: "scattered_batch", dims: 128, rowIDs: []uint32{12, 2, 9, 0, 7, 3, 15, 1, 6, 10, 4, 14, 5}},
		{name: "duplicates", dims: 63, rowIDs: []uint32{3, 3, 1, 7, 1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseRows := maxRowID(tc.rowIDs) + 1
			base := dotBatchTestBase(baseRows, tc.dims, tc.dims)
			query := dotBatchTestVector(tc.dims, 17)
			got := make([]float32, len(tc.rowIDs))

			status := DotFloat32Indexed(got, base, query, tc.rowIDs, tc.dims)
			if status.Invalid {
				t.Fatalf("DotFloat32Indexed rejected valid shape: %+v", status)
			}
			if status.Rows != len(tc.rowIDs) {
				t.Fatalf("status.Rows=%d want %d", status.Rows, len(tc.rowIDs))
			}
			if status.Rows > 0 && status.Optimized == status.Fallback {
				t.Fatalf("optimized/fallback status mismatch: %+v", status)
			}
			if status.Optimized && !DotFloat32BatchOptimizedAvailable() {
				t.Fatalf("optimized status on backend without optimized batch support: %+v impl=%s", status, DotFloat32BatchImplementation())
			}

			want := make([]float32, len(tc.rowIDs))
			dotFloat32IndexedScalar(want, base, query, tc.rowIDs, tc.dims, len(tc.rowIDs))
			assertFloat32SliceNear(t, got, want, tc.dims)
		})
	}
}

func TestDotFloat32IndexedPrevalidatedParity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dims   int
		rowIDs []uint32
	}{
		{name: "short_dims_tail", dims: 7, rowIDs: []uint32{0, 1, 2, 3, 4}},
		{name: "contiguous_batch", dims: 64, rowIDs: []uint32{0, 1, 2, 3}},
		{name: "scattered_batch", dims: 128, rowIDs: []uint32{12, 2, 9, 0, 7, 3, 15, 1, 6, 10, 4, 14, 5}},
		{name: "duplicates", dims: 63, rowIDs: []uint32{3, 3, 1, 7, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseRows := maxRowID(tc.rowIDs) + 1
			base := dotBatchTestBase(baseRows, tc.dims, tc.dims)
			query := dotBatchTestVector(tc.dims, 47)
			got := make([]float32, len(tc.rowIDs))

			status := DotFloat32IndexedPrevalidated(got, base, query, tc.rowIDs, tc.dims)
			if status.Invalid {
				t.Fatalf("DotFloat32IndexedPrevalidated rejected valid prevalidated shape: %+v", status)
			}
			if status.Rows != len(tc.rowIDs) {
				t.Fatalf("status.Rows=%d want %d", status.Rows, len(tc.rowIDs))
			}
			if status.Rows > 0 && status.Optimized == status.Fallback {
				t.Fatalf("optimized/fallback status mismatch: %+v", status)
			}

			want := make([]float32, len(tc.rowIDs))
			dotFloat32IndexedScalar(want, base, query, tc.rowIDs, tc.dims, len(tc.rowIDs))
			assertFloat32SliceNear(t, got, want, tc.dims)
		})
	}
}

func TestDotFloat32IndexedPrevalidatedInvalidShapesLeaveDestinationUnchanged(t *testing.T) {
	t.Parallel()

	base := dotBatchTestBase(2, 4, 4)
	query := dotBatchTestVector(4, 53)
	cases := []struct {
		name  string
		base  []float32
		query []float32
		dims  int
	}{
		{name: "zero_dims", base: base, query: query, dims: 0},
		{name: "short_query", base: base, query: query[:3], dims: 4},
		{name: "short_base", base: base[:3], query: query, dims: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dst := []float32{-31, -32, -33}
			before := append([]float32(nil), dst...)
			status := DotFloat32IndexedPrevalidated(dst, tc.base, tc.query, []uint32{0, 1}, tc.dims)
			if !status.Invalid || status.Rows != 0 || status.Optimized || status.Fallback {
				t.Fatalf("status=%+v want invalid without writes", status)
			}
			assertFloat32SliceExact(t, dst, before)
		})
	}
}

func TestDotFloat32StridedParity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dims   int
		rows   int
		stride int
	}{
		{name: "short_dims_single", dims: 1, rows: 1, stride: 1},
		{name: "short_dims_tail", dims: 7, rows: 5, stride: 10},
		{name: "contiguous_batch", dims: 64, rows: 4, stride: 64},
		{name: "contiguous_tail", dims: 65, rows: 13, stride: 65},
		{name: "padded_batch", dims: 128, rows: 8, stride: 133},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := dotBatchTestBase(tc.rows, tc.dims, tc.stride)
			query := dotBatchTestVector(tc.dims, 19)
			got := make([]float32, tc.rows)

			status := DotFloat32Strided(got, base, query, tc.rows, tc.dims, tc.stride)
			if status.Invalid {
				t.Fatalf("DotFloat32Strided rejected valid shape: %+v", status)
			}
			if status.Rows != tc.rows {
				t.Fatalf("status.Rows=%d want %d", status.Rows, tc.rows)
			}
			if status.Rows > 0 && status.Optimized == status.Fallback {
				t.Fatalf("optimized/fallback status mismatch: %+v", status)
			}
			if status.Optimized && !DotFloat32BatchOptimizedAvailable() {
				t.Fatalf("optimized status on backend without optimized batch support: %+v impl=%s", status, DotFloat32BatchImplementation())
			}

			want := make([]float32, tc.rows)
			dotFloat32StridedScalar(want, base, query, tc.rows, tc.dims, tc.stride)
			assertFloat32SliceNear(t, got, want, tc.dims)
		})
	}
}

func TestDotFloat32IndexedOptimizedStatusAndTinyFallback(t *testing.T) {
	t.Parallel()

	base := dotBatchTestBase(4, 64, 64)
	query := dotBatchTestVector(64, 43)
	rowIDs := []uint32{0, 1, 2, 3}
	dst := make([]float32, len(rowIDs))
	status := DotFloat32Indexed(dst, base, query, rowIDs, 64)
	if status.Invalid || status.Rows != len(rowIDs) {
		t.Fatalf("status=%+v want valid rows=%d", status, len(rowIDs))
	}
	if DotFloat32IndexedOptimizedEligible(len(rowIDs), 64) {
		if !status.Optimized || status.Fallback {
			t.Fatalf("status=%+v want optimized indexed backend", status)
		}
	} else if status.Optimized || !status.Fallback {
		t.Fatalf("status=%+v want platform fallback", status)
	}
	want := make([]float32, len(rowIDs))
	dotFloat32IndexedScalar(want, base, query, rowIDs, 64, len(rowIDs))
	assertFloat32SliceNear(t, dst, want, 64)

	tinyDst := make([]float32, len(rowIDs))
	tinyStatus := DotFloat32Indexed(tinyDst, base, query[:16], rowIDs, 16)
	if tinyStatus.Invalid || tinyStatus.Rows != len(rowIDs) || tinyStatus.Optimized || !tinyStatus.Fallback {
		t.Fatalf("tiny status=%+v want scalar fallback", tinyStatus)
	}
	tinyWant := make([]float32, len(rowIDs))
	dotFloat32IndexedScalar(tinyWant, base, query[:16], rowIDs, 16, len(rowIDs))
	assertFloat32SliceNear(t, tinyDst, tinyWant, 16)

	singleDst := make([]float32, 1)
	singleStatus := DotFloat32Indexed(singleDst, base, query, rowIDs[:1], 64)
	if singleStatus.Invalid || singleStatus.Rows != 1 || singleStatus.Optimized || !singleStatus.Fallback {
		t.Fatalf("single-row status=%+v want scalar fallback", singleStatus)
	}
	singleWant := make([]float32, 1)
	dotFloat32IndexedScalar(singleWant, base, query, rowIDs[:1], 64, 1)
	assertFloat32SliceNear(t, singleDst, singleWant, 64)
}

func TestDotFloat32BatchMinRows(t *testing.T) {
	t.Parallel()

	base := dotBatchTestBase(4, 3, 3)
	query := dotBatchTestVector(3, 23)
	indexedDst := []float32{-1, -1}
	indexedStatus := DotFloat32Indexed(indexedDst, base, query, []uint32{0, 1, 99}, 3)
	if indexedStatus.Invalid {
		t.Fatalf("row IDs beyond min(dst,rowIDs) should be ignored: %+v", indexedStatus)
	}
	if indexedStatus.Rows != len(indexedDst) {
		t.Fatalf("indexed rows=%d want %d", indexedStatus.Rows, len(indexedDst))
	}
	stridedDst := []float32{-1, -1}
	stridedStatus := DotFloat32Strided(stridedDst, base, query, 4, 3, 3)
	if stridedStatus.Invalid {
		t.Fatalf("rows beyond len(dst) should be ignored: %+v", stridedStatus)
	}
	if stridedStatus.Rows != len(stridedDst) {
		t.Fatalf("strided rows=%d want %d", stridedStatus.Rows, len(stridedDst))
	}
}

func TestDotFloat32BatchInvalidShapesLeaveDestinationUnchanged(t *testing.T) {
	t.Parallel()

	base := dotBatchTestBase(2, 4, 4)
	query := dotBatchTestVector(4, 29)

	indexedCases := []struct {
		name   string
		base   []float32
		query  []float32
		rowIDs []uint32
		dims   int
	}{
		{name: "zero_dims", base: base, query: query, rowIDs: []uint32{0, 1}, dims: 0},
		{name: "short_query", base: base, query: query[:3], rowIDs: []uint32{0, 1}, dims: 4},
		{name: "short_base", base: base[:3], query: query, rowIDs: []uint32{0}, dims: 4},
		{name: "row_out_of_range", base: base, query: query, rowIDs: []uint32{0, 2}, dims: 4},
	}
	for _, tc := range indexedCases {
		t.Run("indexed_"+tc.name, func(t *testing.T) {
			dst := []float32{-11, -12, -13}
			before := append([]float32(nil), dst...)
			status := DotFloat32Indexed(dst, tc.base, tc.query, tc.rowIDs, tc.dims)
			if !status.Invalid || status.Rows != 0 || status.Optimized || status.Fallback {
				t.Fatalf("status=%+v want invalid without writes", status)
			}
			assertFloat32SliceExact(t, dst, before)
		})
	}

	stridedCases := []struct {
		name     string
		base     []float32
		query    []float32
		rowCount int
		dims     int
		stride   int
	}{
		{name: "zero_dims", base: base, query: query, rowCount: 2, dims: 0, stride: 4},
		{name: "short_query", base: base, query: query[:3], rowCount: 2, dims: 4, stride: 4},
		{name: "short_base", base: base[:3], query: query, rowCount: 1, dims: 4, stride: 4},
		{name: "overlap_stride", base: base, query: query, rowCount: 2, dims: 4, stride: 3},
		{name: "row_out_of_range", base: base, query: query, rowCount: 3, dims: 4, stride: 4},
	}
	for _, tc := range stridedCases {
		t.Run("strided_"+tc.name, func(t *testing.T) {
			dst := []float32{-21, -22, -23}
			before := append([]float32(nil), dst...)
			status := DotFloat32Strided(dst, tc.base, tc.query, tc.rowCount, tc.dims, tc.stride)
			if !status.Invalid || status.Rows != 0 || status.Optimized || status.Fallback {
				t.Fatalf("status=%+v want invalid without writes", status)
			}
			assertFloat32SliceExact(t, dst, before)
		})
	}
}

func TestDotFloat32BatchZeroAllocs(t *testing.T) {
	base := dotBatchTestBase(16, 128, 131)
	query := dotBatchTestVector(128, 31)
	rowIDs := []uint32{12, 2, 9, 0, 7, 3, 15, 1, 6, 10, 4, 14, 5}
	indexedDst := make([]float32, len(rowIDs))
	stridedDst := make([]float32, 13)

	indexedAllocs := testing.AllocsPerRun(1000, func() {
		status := DotFloat32Indexed(indexedDst, base, query, rowIDs, 128)
		dotBatchStatusSink = status
		dotBatchFloatSink += indexedDst[0]
	})
	if indexedAllocs != 0 {
		t.Fatalf("DotFloat32Indexed allocs/run=%v want 0", indexedAllocs)
	}

	stridedAllocs := testing.AllocsPerRun(1000, func() {
		status := DotFloat32Strided(stridedDst, base, query, len(stridedDst), 128, 131)
		dotBatchStatusSink = status
		dotBatchFloatSink += stridedDst[0]
	})
	if stridedAllocs != 0 {
		t.Fatalf("DotFloat32Strided allocs/run=%v want 0", stridedAllocs)
	}
	if dotBatchFloatSink == 0 && dotBatchStatusSink.Rows == 0 {
		t.Fatalf("unexpected zero sinks")
	}
}

func BenchmarkDotFloat32IndexedWrapper(b *testing.B) {
	cases := dotFloat32IndexedBenchmarkCases()
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			baseRows := tc.rows
			if tc.scattered {
				baseRows = tc.rows*4 + 17
			}
			base := dotBatchTestBase(baseRows, tc.dims, tc.dims)
			query := dotBatchTestVector(tc.dims, 37)
			rowIDs := dotBatchTestRowIDs(tc.rows, baseRows, tc.scattered)
			dst := make([]float32, tc.rows)
			benchmarkDotFloat32BatchCall(b, tc.dims, tc.rows, func() DotFloat32BatchStatus {
				return DotFloat32Indexed(dst, base, query, rowIDs, tc.dims)
			}, dst)
		})
	}
}

func BenchmarkDotFloat32IndexedPrevalidatedWrapper(b *testing.B) {
	cases := dotFloat32IndexedBenchmarkCases()
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			baseRows := tc.rows
			if tc.scattered {
				baseRows = tc.rows*4 + 17
			}
			base := dotBatchTestBase(baseRows, tc.dims, tc.dims)
			query := dotBatchTestVector(tc.dims, 39)
			rowIDs := dotBatchTestRowIDs(tc.rows, baseRows, tc.scattered)
			dst := make([]float32, tc.rows)
			benchmarkDotFloat32BatchCall(b, tc.dims, tc.rows, func() DotFloat32BatchStatus {
				return DotFloat32IndexedPrevalidated(dst, base, query, rowIDs, tc.dims)
			}, dst)
		})
	}
}

type dotFloat32IndexedBenchmarkCase struct {
	name      string
	dims      int
	rows      int
	scattered bool
}

func dotFloat32IndexedBenchmarkCases() []dotFloat32IndexedBenchmarkCase {
	dimensions := []int{16, 64, 128, 256, 768, 1536}
	rowTiles := []int{2, 4, 8, 16, 32}
	cases := make([]dotFloat32IndexedBenchmarkCase, 0, len(dimensions)*len(rowTiles)*2)
	for _, dims := range dimensions {
		for _, rows := range rowTiles {
			for _, scattered := range []bool{false, true} {
				layout := "contiguous"
				if scattered {
					layout = "scattered"
				}
				cases = append(cases, dotFloat32IndexedBenchmarkCase{
					name:      fmt.Sprintf("dims%d_rows%d_%s", dims, rows, layout),
					dims:      dims,
					rows:      rows,
					scattered: scattered,
				})
			}
		}
	}
	return cases
}

func BenchmarkDotFloat32StridedWrapper(b *testing.B) {
	cases := []struct {
		name   string
		dims   int
		rows   int
		stride int
	}{
		{name: "dims16_rows4_contiguous_fallback", dims: 16, rows: 4, stride: 16},
		{name: "dims64_rows4_contiguous", dims: 64, rows: 4, stride: 64},
		{name: "dims64_rows13_padded", dims: 64, rows: 13, stride: 67},
		{name: "dims128_rows32_padded", dims: 128, rows: 32, stride: 131},
		{name: "dims768_rows13_padded", dims: 768, rows: 13, stride: 771},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			base := dotBatchTestBase(tc.rows, tc.dims, tc.stride)
			query := dotBatchTestVector(tc.dims, 41)
			dst := make([]float32, tc.rows)
			benchmarkDotFloat32BatchCall(b, tc.dims, tc.rows, func() DotFloat32BatchStatus {
				return DotFloat32Strided(dst, base, query, tc.rows, tc.dims, tc.stride)
			}, dst)
		})
	}
}

func benchmarkDotFloat32BatchCall(b *testing.B, dims, rows int, call func() DotFloat32BatchStatus, dst []float32) {
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
		dotBatchStatusSink = status
		dotBatchFloatSink += dst[0]
	}
	elapsed := b.Elapsed().Seconds()
	b.StopTimer()

	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(rows), "rows/tile")
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
		b.ReportMetric(float64(b.N*rows)/elapsed, "dots/sec")
	}
	if b.N > 0 {
		b.ReportMetric(float64(optimizedCalls)/float64(b.N), "optimized/call")
		b.ReportMetric(float64(fallbackCalls)/float64(b.N), "fallback/call")
		b.ReportMetric(float64(invalidCalls)/float64(b.N), "invalid/call")
	}
}

func dotBatchTestVector(n int, salt int) []float32 {
	values := make([]float32, n)
	for i := range values {
		values[i] = float32(((i*salt + 3) % 17) - 8)
	}
	return values
}

func dotBatchTestBase(rows, dims, stride int) []float32 {
	if rows <= 0 {
		return nil
	}
	base := make([]float32, (rows-1)*stride+dims)
	for row := 0; row < rows; row++ {
		start := row * stride
		for col := 0; col < dims; col++ {
			base[start+col] = float32(((row+1)*(col+5)+col*3)%19 - 9)
		}
		for col := dims; col < stride && start+col < len(base); col++ {
			base[start+col] = float32(1000 + row + col)
		}
	}
	return base
}

func dotBatchTestRowIDs(rows, baseRows int, scattered bool) []uint32 {
	rowIDs := make([]uint32, rows)
	if !scattered {
		for i := range rowIDs {
			rowIDs[i] = uint32(i)
		}
		return rowIDs
	}
	for i := range rowIDs {
		rowIDs[i] = uint32((i*7 + 3) % baseRows)
	}
	return rowIDs
}

func maxRowID(rowIDs []uint32) int {
	maxID := 0
	for _, rowID := range rowIDs {
		if int(rowID) > maxID {
			maxID = int(rowID)
		}
	}
	return maxID
}

func assertFloat32SliceNear(t *testing.T, got, want []float32, dims int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d want %d", len(got), len(want))
	}
	tol := float64(dims) * 1e-5
	if tol < 1e-5 {
		tol = 1e-5
	}
	for i := range got {
		if math.IsNaN(float64(got[i])) || math.IsNaN(float64(want[i])) || math.Abs(float64(got[i]-want[i])) > tol {
			t.Fatalf("[%d] got=%v want=%v diff=%v tol=%v", i, got[i], want[i], got[i]-want[i], tol)
		}
	}
}

func assertFloat32SliceExact(t *testing.T, got, want []float32) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}
