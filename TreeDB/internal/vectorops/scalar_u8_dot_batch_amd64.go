//go:build amd64 && !purego

package vectorops

import "golang.org/x/sys/cpu"

var (
	dotScalarU8CenteredIndexedAMD64AVX512VNNIAvailable = cpu.X86.HasAVX512F && cpu.X86.HasAVX512BW && cpu.X86.HasAVX512DQ && cpu.X86.HasAVX512VL && cpu.X86.HasAVX512VNNI
	dotScalarU8CenteredIndexedAMD64AVX2Available       = cpu.X86.HasAVX2
)

var scalarU8DotBatchImplementation = scalarU8DotBatchImplementationAMD64()

const scalarU8DotBatchOptimizedAvailable = true

func scalarU8DotBatchImplementationAMD64() string {
	if dotScalarU8CenteredIndexedAMD64AVX512VNNIAvailable {
		return "indexed_amd64_avx512_vnni"
	}
	if dotScalarU8CenteredIndexedAMD64AVX2Available {
		return "indexed_amd64_avx2"
	}
	return "indexed_amd64_sse2"
}

const (
	dotScalarU8CenteredIndexedAMD64MinDims           = 16
	dotScalarU8CenteredIndexedAMD64MinRows           = 1
	dotScalarU8CenteredIndexedAMD64AVX512VNNIMinDims = 128
	// The amd64 SIMD kernels accumulate into signed 32-bit vector lanes and reduce
	// once per row. At 32,768 dimensions, the worst-case full-row scalar_u8 dot
	// product is 65,025*32,768 = 2,130,739,200, which still fits int32. Larger
	// dimensions use the portable int64 scalar fallback to keep accumulation
	// overflow-safe.
	dotScalarU8CenteredIndexedAMD64MaxSIMDDims = 32768
)

func dotScalarU8CenteredIndexedOptimizedEligible(rows, dims int) bool {
	return rows >= dotScalarU8CenteredIndexedAMD64MinRows && dims >= dotScalarU8CenteredIndexedAMD64MinDims && dims <= dotScalarU8CenteredIndexedAMD64MaxSIMDDims
}

// DotScalarU8CenteredIndexed writes integer dot products for row-major scalar_u8
// code rows selected by rowIDs against a pre-centered scalar_u8 query. Supported
// amd64 builds use an AVX-512 VNNI indexed-row kernel when available, then AVX2
// or SSE2, for sufficiently large overflow-safe tiles. Tiny/short or very
// high-dimensional shapes use the portable scalar fallback. Invalid shapes leave
// dst unchanged and return Invalid=true.
func DotScalarU8CenteredIndexed(dst []int64, codes []byte, query ScalarU8CenteredQuery, rowIDs []uint32, dims int) ScalarU8DotBatchStatus {
	rows := dotScalarU8CenteredIndexedRows(dst, rowIDs)
	if rows == 0 {
		return ScalarU8DotBatchStatus{}
	}
	if !dotScalarU8CenteredIndexedShapeOK(codes, query, rowIDs, dims, rows) {
		return scalarU8DotBatchInvalidStatus()
	}
	if !dotScalarU8CenteredIndexedOptimizedEligible(rows, dims) {
		dotScalarU8CenteredIndexedScalar(dst, codes, query, rowIDs, dims, rows)
		return scalarU8DotBatchStatus(rows, false)
	}
	queryValues := query.values[:dims]
	querySum := query.CenteredSum()
	if dotScalarU8CenteredIndexedAMD64AVX512VNNIAvailable && dims >= dotScalarU8CenteredIndexedAMD64AVX512VNNIMinDims {
		dotScalarU8CenteredIndexedAMD64AVX512VNNI(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	} else if dotScalarU8CenteredIndexedAMD64AVX2Available {
		dotScalarU8CenteredIndexedAMD64AVX2(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	} else {
		dotScalarU8CenteredIndexedAMD64SSE2(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	}
	return scalarU8DotBatchStatus(rows, true)
}

// DotScalarU8CenteredIndexedPrevalidated is the trusted-call variant of
// DotScalarU8CenteredIndexed for hot paths that already proved every row ID is
// within the row-major code payload. It still validates the basic slice/query
// shape but deliberately skips the per-call rowID bounds scan.
func DotScalarU8CenteredIndexedPrevalidated(dst []int64, codes []byte, query ScalarU8CenteredQuery, rowIDs []uint32, dims int) ScalarU8DotBatchStatus {
	rows := dotScalarU8CenteredIndexedRows(dst, rowIDs)
	if rows == 0 {
		return ScalarU8DotBatchStatus{}
	}
	if !dotScalarU8CenteredIndexedBasicShapeOK(codes, query, dims, rows) {
		return scalarU8DotBatchInvalidStatus()
	}
	if !dotScalarU8CenteredIndexedOptimizedEligible(rows, dims) {
		dotScalarU8CenteredIndexedScalar(dst, codes, query, rowIDs, dims, rows)
		return scalarU8DotBatchStatus(rows, false)
	}
	queryValues := query.values[:dims]
	querySum := query.CenteredSum()
	if dotScalarU8CenteredIndexedAMD64AVX512VNNIAvailable && dims >= dotScalarU8CenteredIndexedAMD64AVX512VNNIMinDims {
		dotScalarU8CenteredIndexedAMD64AVX512VNNI(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	} else if dotScalarU8CenteredIndexedAMD64AVX2Available {
		dotScalarU8CenteredIndexedAMD64AVX2(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	} else {
		dotScalarU8CenteredIndexedAMD64SSE2(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	}
	return scalarU8DotBatchStatus(rows, true)
}

// dotScalarU8CenteredIndexedAMD64SSE2 computes rows indexed scalar_u8 centered
// dot products. The Go wrapper must validate all slice lengths, row IDs, dims,
// and rows before calling. querySum is sum(query[:dims]); the kernel computes
// sum(q*(2*row-255)) as 2*sum(q*row)-255*querySum.
func dotScalarU8CenteredIndexedAMD64SSE2(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)

// dotScalarU8CenteredIndexedAMD64AVX2 computes rows indexed scalar_u8 centered
// dot products with an AVX2 kernel. The Go wrapper must validate all slice
// lengths, row IDs, dims, rows, CPU feature availability, and the int32 SIMD
// accumulation bound before calling.
func dotScalarU8CenteredIndexedAMD64AVX2(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)

// dotScalarU8CenteredIndexedAMD64AVX512VNNI computes rows indexed scalar_u8
// centered dot products with AVX-512 VNNI. The Go wrapper validates shapes,
// CPU features, and the int32 SIMD accumulation bound before calling.
func dotScalarU8CenteredIndexedAMD64AVX512VNNI(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
