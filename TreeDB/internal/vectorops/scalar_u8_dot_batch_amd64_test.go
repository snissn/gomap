//go:build amd64 && !purego

package vectorops

import (
	"fmt"
	"testing"

	"golang.org/x/sys/cpu"
)

func TestScalarU8DotBatchAMD64Dispatch2702(t *testing.T) {
	want := "indexed_amd64_sse2"
	switch {
	case cpu.X86.HasAVX2 && cpu.X86.HasAVX512F && cpu.X86.HasAVX512BW && cpu.X86.HasAVX512DQ && cpu.X86.HasAVX512VL && cpu.X86.HasAVX512VNNI:
		want = "indexed_amd64_avx512_vnni"
	case cpu.X86.HasAVX2:
		want = "indexed_amd64_avx2"
	}
	if got := ScalarU8DotBatchImplementation(); got != want {
		t.Fatalf("ScalarU8DotBatchImplementation()=%q want %q", got, want)
	}
}

func TestDotScalarU8CenteredIndexedAMD64AVX512VNNIParity4225(t *testing.T) {
	if !dotScalarU8CenteredIndexedAMD64AVX512VNNIAvailable {
		t.Skip("AVX-512 VNNI unavailable")
	}

	for _, dims := range []int{128, 129, 256, 257, 768, 769, 1536, 1537} {
		for _, rows := range []int{1, 2, 3, 8, 17} {
			name := fmt.Sprintf("dims=%d/rows=%d", dims, rows)
			t.Run(name, func(t *testing.T) {
				const baseRows = 53
				codes := scalarU8DotBatchTestCodes(baseRows, dims)
				query := scalarU8DotBatchTestQuery(t, dims, 59)
				rowIDs := scalarU8DotBatchTestRowIDs(rows, baseRows)
				got := make([]int64, rows)
				want := make([]int64, rows)

				dotScalarU8CenteredIndexedAMD64AVX512VNNI(got, codes, query.values, rowIDs, dims, rows, query.CenteredSum())
				dotScalarU8CenteredIndexedScalar(want, codes, query, rowIDs, dims, rows)
				assertInt64SliceExact(t, got, want)
			})
		}
	}
}

func TestDotScalarU8CenteredIndexedAMD64AVX512VNNIMaxSIMDDimsFourRowBoundary4225(t *testing.T) {
	if !dotScalarU8CenteredIndexedAMD64AVX512VNNIAvailable {
		t.Skip("AVX-512 VNNI unavailable")
	}

	const dims = dotScalarU8CenteredIndexedAMD64MaxSIMDDims
	queryCodes := make([]byte, dims)
	for i := range queryCodes {
		queryCodes[i] = 255
	}
	query, _, ok := PrepareScalarU8CenteredQuery(make([]ScalarU8CenteredCode, 0, dims), queryCodes, dims)
	if !ok {
		t.Fatal("PrepareScalarU8CenteredQuery rejected max SIMD dimensions")
	}

	for _, rows := range []int{4, 5} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			codes := make([]byte, rows*dims)
			for row := 0; row < rows; row += 2 {
				for i := row * dims; i < (row+1)*dims; i++ {
					codes[i] = 255
				}
			}
			rowIDs := scalarU8DotBatchTestRowIDs(rows, rows)
			got := make([]int64, rows)
			want := make([]int64, rows)
			dotScalarU8CenteredIndexedAMD64AVX512VNNI(got, codes, query.values, rowIDs, dims, rows, query.CenteredSum())
			dotScalarU8CenteredIndexedScalar(want, codes, query, rowIDs, dims, rows)
			assertInt64SliceExact(t, got, want)
		})
	}
}

func TestDotScalarU8CenteredIndexedAMD64AVX512VNNIZeroAllocs4225(t *testing.T) {
	if !dotScalarU8CenteredIndexedAMD64AVX512VNNIAvailable {
		t.Skip("AVX-512 VNNI unavailable")
	}

	const (
		dims     = 768
		rows     = 17
		baseRows = 53
	)
	codes := scalarU8DotBatchTestCodes(baseRows, dims)
	query := scalarU8DotBatchTestQuery(t, dims, 61)
	rowIDs := scalarU8DotBatchTestRowIDs(rows, baseRows)
	dst := make([]int64, rows)
	allocs := testing.AllocsPerRun(1000, func() {
		dotScalarU8CenteredIndexedAMD64AVX512VNNI(dst, codes, query.values, rowIDs, dims, rows, query.CenteredSum())
		scalarU8DotBatchIntSink += dst[0]
	})
	if allocs != 0 {
		t.Fatalf("AVX-512 VNNI allocs/run=%v want 0", allocs)
	}
}

func BenchmarkDotScalarU8CenteredIndexedAMD64AVX512VNNIRandomWorkingSet4225(b *testing.B) {
	if !dotScalarU8CenteredIndexedAMD64AVX512VNNIAvailable {
		b.Skip("AVX-512 VNNI unavailable")
	}

	const (
		dims      = 768
		baseRows  = 1 << 16 // 48 MiB, four times this host's last-level cache.
		tileCount = 1 << 12
	)
	codes := scalarU8DotBatchTestCodes(baseRows, dims)
	query := scalarU8DotBatchTestQuery(b, dims, 67)
	for _, rows := range []int{4, 8, 16, 32} {
		b.Run(fmt.Sprintf("dims=%d/rows=%d/working_set=48MiB", dims, rows), func(b *testing.B) {
			rowIDs := make([]uint32, tileCount*rows)
			state := uint32(0x6d2b79f5)
			for i := range rowIDs {
				state = state*1664525 + 1013904223
				rowIDs[i] = state & (baseRows - 1)
			}
			dst := make([]int64, rows)
			b.ReportAllocs()
			b.SetBytes(int64(rows * dims))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := (i & (tileCount - 1)) * rows
				tile := rowIDs[start : start+rows]
				dotScalarU8CenteredIndexedAMD64AVX512VNNI(dst, codes, query.values, tile, dims, rows, query.CenteredSum())
				scalarU8DotBatchIntSink += dst[0]
			}
		})
	}
}
