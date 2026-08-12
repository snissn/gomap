//go:build amd64 && !purego

package vectorops

import (
	"fmt"
	"math"
	"testing"

	simdcpu "github.com/tphakala/simd/cpu"
)

func TestDotFloat32IndexedAMD64RepresentativeOptimized4137(t *testing.T) {
	const (
		dims = 128
		rows = 32
	)
	base := dotBatchTestBase(rows*4+17, dims, dims)
	query := dotBatchTestVector(dims, 61)
	rowIDs := dotBatchTestRowIDs(rows, rows*4+17, true)
	dst := make([]float32, rows)

	status := DotFloat32IndexedPrevalidated(dst, base, query, rowIDs, dims)
	if !status.Optimized || status.Fallback || status.Invalid || status.Rows != rows {
		t.Fatalf("status=%+v implementation=%q want optimized rows=%d", status, DotFloat32BatchImplementation(), rows)
	}
}

func TestDotFloat32IndexedAMD64Dispatch4137(t *testing.T) {
	wantVariant := dotFloat32IndexedAMD64Scalar
	wantImplementation := "per_row_" + DotFloat32Implementation()
	switch {
	case simdcpu.X86.AVX512F && simdcpu.X86.AVX512VL:
		wantVariant = dotFloat32IndexedAMD64AVX512
		wantImplementation = "indexed_amd64_avx512"
	case simdcpu.X86.AVX && simdcpu.X86.FMA:
		wantVariant = dotFloat32IndexedAMD64AVXFMA
		wantImplementation = "indexed_amd64_avx_fma"
	case simdcpu.X86.SSE2:
		wantVariant = dotFloat32IndexedAMD64SSE2
		wantImplementation = "indexed_amd64_sse2"
	}
	if dotFloat32IndexedAMD64ActiveVariant != wantVariant || DotFloat32BatchImplementation() != wantImplementation {
		t.Fatalf("variant=%d implementation=%q want variant=%d implementation=%q", dotFloat32IndexedAMD64ActiveVariant, DotFloat32BatchImplementation(), wantVariant, wantImplementation)
	}
}

func TestDotFloat32IndexedAMD64ExactBits4137(t *testing.T) {
	dimensions := []int{16, 64, 65, 128, 129, 256, 257, 768, 769, 1536, 1537}
	rowTiles := []int{0, 1, 2, 4, 8, 16, 32}
	for _, dims := range dimensions {
		for _, rows := range rowTiles {
			for _, scattered := range []bool{false, true} {
				for _, offset := range []int{0, 1} {
					name := fmt.Sprintf("dims=%d/rows=%d/scattered=%t/offset=%d", dims, rows, scattered, offset)
					t.Run(name, func(t *testing.T) {
						baseRows := rows*4 + 17
						baseValues := make([]float32, baseRows*dims)
						queryValues := make([]float32, dims)
						for i := range queryValues {
							queryValues[i] = dotFloat32ExactTestValue4137(uint32(i + 1))
						}
						for i := range baseValues {
							baseValues[i] = dotFloat32ExactTestValue4137(uint32(i + dims + 29))
						}
						baseBacking := make([]float32, len(baseValues)+offset)
						queryBacking := make([]float32, len(queryValues)+offset)
						base := baseBacking[offset:]
						query := queryBacking[offset:]
						copy(base, baseValues)
						copy(query, queryValues)
						rowIDs := dotBatchTestRowIDs(rows, baseRows, scattered)
						want := make([]float32, rows)
						for i, rowID := range rowIDs {
							start := int(rowID) * dims
							want[i] = DotFloat32(base[start:start+dims], query)
						}

						checked := make([]float32, rows)
						checkedStatus := DotFloat32Indexed(checked, base, query, rowIDs, dims)
						assertDotFloat32IndexedStatus4137(t, checkedStatus, rows, dims)
						assertFloat32Bits4137(t, checked, want)

						prevalidated := make([]float32, rows)
						prevalidatedStatus := DotFloat32IndexedPrevalidated(prevalidated, base, query, rowIDs, dims)
						assertDotFloat32IndexedStatus4137(t, prevalidatedStatus, rows, dims)
						assertFloat32Bits4137(t, prevalidated, want)
					})
				}
			}
		}
	}
}

func TestDotFloat32IndexedAMD64ExactSpecialValues4137(t *testing.T) {
	const dims = 129
	cases := []struct {
		name  string
		setup func(base, query []float32)
	}{
		{name: "signed_zero", setup: func(base, query []float32) {
			for i := range base {
				base[i] = math.Float32frombits(0x80000000)
			}
		}},
		{name: "subnormal", setup: func(base, query []float32) {
			for i := range base {
				base[i] = math.Float32frombits(uint32(i%31 + 1))
			}
			for i := range query {
				query[i] = math.Float32frombits(uint32(i%17 + 1))
			}
		}},
		{name: "nan_vector", setup: func(base, _ []float32) {
			base[dims+64] = math.Float32frombits(0x7fc12345)
		}},
		{name: "nan_scalar_tail", setup: func(base, _ []float32) {
			base[2*dims-1] = math.Float32frombits(0x7fc54321)
		}},
		{name: "infinity", setup: func(base, _ []float32) {
			base[3*dims+17] = float32(math.Inf(1))
		}},
		{name: "opposing_infinities", setup: func(base, _ []float32) {
			base[3*dims+17] = float32(math.Inf(1))
			base[3*dims+18] = float32(math.Inf(-1))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := make([]float32, 4*dims)
			query := make([]float32, dims)
			for i := range base {
				base[i] = dotFloat32ExactTestValue4137(uint32(i + 73))
			}
			for i := range query {
				query[i] = dotFloat32ExactTestValue4137(uint32(i + 101))
			}
			tc.setup(base, query)
			rowIDs := []uint32{3, 1}
			want := []float32{
				DotFloat32(base[3*dims:4*dims], query),
				DotFloat32(base[dims:2*dims], query),
			}
			got := make([]float32, len(rowIDs))
			status := DotFloat32Indexed(got, base, query, rowIDs, dims)
			assertDotFloat32IndexedStatus4137(t, status, len(rowIDs), dims)
			assertFloat32Bits4137(t, got, want)
		})
	}
}

func TestDotFloat32IndexedAMD64CheckedAndPrevalidatedZeroAllocs4137(t *testing.T) {
	const (
		dims = 128
		rows = 32
	)
	base := dotBatchTestBase(rows*4+17, dims, dims)
	query := dotBatchTestVector(dims, 67)
	rowIDs := dotBatchTestRowIDs(rows, rows*4+17, true)
	dst := make([]float32, rows)
	checked := testing.AllocsPerRun(1000, func() {
		dotBatchStatusSink = DotFloat32Indexed(dst, base, query, rowIDs, dims)
	})
	prevalidated := testing.AllocsPerRun(1000, func() {
		dotBatchStatusSink = DotFloat32IndexedPrevalidated(dst, base, query, rowIDs, dims)
	})
	if checked != 0 || prevalidated != 0 {
		t.Fatalf("allocations checked=%v prevalidated=%v want 0", checked, prevalidated)
	}
}

func dotFloat32ExactTestValue4137(seed uint32) float32 {
	bits := uint32(0x3f000000) | ((seed * 2654435761) & 0x007fffff)
	if seed&1 != 0 {
		bits |= 0x80000000
	}
	return math.Float32frombits(bits)
}

func assertDotFloat32IndexedStatus4137(t *testing.T, status DotFloat32BatchStatus, rows, dims int) {
	t.Helper()
	if status.Invalid || status.Rows != rows {
		t.Fatalf("status=%+v want valid rows=%d", status, rows)
	}
	optimized := DotFloat32IndexedOptimizedEligible(rows, dims)
	if status.Optimized != optimized || status.Fallback != (rows > 0 && !optimized) {
		t.Fatalf("status=%+v eligible=%t", status, optimized)
	}
}

func assertFloat32Bits4137(t *testing.T, got, want []float32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d want=%d", len(got), len(want))
	}
	for i := range got {
		gotBits := math.Float32bits(got[i])
		wantBits := math.Float32bits(want[i])
		if gotBits != wantBits {
			t.Fatalf("score[%d]=%v bits=%08x want=%v bits=%08x", i, got[i], gotBits, want[i], wantBits)
		}
	}
}
