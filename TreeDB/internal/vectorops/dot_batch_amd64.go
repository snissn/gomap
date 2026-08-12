//go:build amd64 && !purego

package vectorops

import simdcpu "github.com/tphakala/simd/cpu"

type dotFloat32IndexedAMD64Variant uint8

const (
	dotFloat32IndexedAMD64Scalar dotFloat32IndexedAMD64Variant = iota
	dotFloat32IndexedAMD64SSE2
	dotFloat32IndexedAMD64AVXFMA
	dotFloat32IndexedAMD64AVX512
)

const (
	dotFloat32IndexedAMD64MinDims = 64
	dotFloat32IndexedAMD64MinRows = 2
)

var (
	dotFloat32IndexedAMD64ActiveVariant = selectDotFloat32IndexedAMD64Variant()
	dotFloat32BatchImplementation       = dotFloat32BatchImplementationAMD64()
	dotFloat32BatchOptimizedAvailable   = dotFloat32IndexedAMD64ActiveVariant != dotFloat32IndexedAMD64Scalar
)

func selectDotFloat32IndexedAMD64Variant() dotFloat32IndexedAMD64Variant {
	switch {
	case simdcpu.X86.AVX512F && simdcpu.X86.AVX512VL:
		return dotFloat32IndexedAMD64AVX512
	case simdcpu.X86.AVX && simdcpu.X86.FMA:
		return dotFloat32IndexedAMD64AVXFMA
	case simdcpu.X86.SSE2:
		return dotFloat32IndexedAMD64SSE2
	default:
		return dotFloat32IndexedAMD64Scalar
	}
}

func dotFloat32BatchImplementationAMD64() string {
	switch dotFloat32IndexedAMD64ActiveVariant {
	case dotFloat32IndexedAMD64AVX512:
		return "indexed_amd64_avx512"
	case dotFloat32IndexedAMD64AVXFMA:
		return "indexed_amd64_avx_fma"
	case dotFloat32IndexedAMD64SSE2:
		return "indexed_amd64_sse2"
	default:
		return "per_row_" + DotFloat32Implementation()
	}
}

func dotFloat32IndexedOptimizedEligible(rows, dims int) bool {
	return dotFloat32BatchOptimizedAvailable && rows >= dotFloat32IndexedAMD64MinRows && dims >= dotFloat32IndexedAMD64MinDims
}

// DotFloat32Indexed writes dot products for row-major base rows selected by
// rowIDs. Eligible amd64 shapes use one runtime-selected indexed SIMD kernel;
// tiny/unsupported shapes retain the exact per-row DotFloat32 fallback.
func DotFloat32Indexed(dst []float32, base []float32, query []float32, rowIDs []uint32, dims int) DotFloat32BatchStatus {
	rows := dotFloat32IndexedRows(dst, rowIDs)
	if rows == 0 {
		return DotFloat32BatchStatus{}
	}
	if !dotFloat32IndexedShapeOK(base, query, rowIDs, dims, rows) {
		return dotFloat32BatchInvalidStatus()
	}
	return dotFloat32IndexedPrevalidated(dst, base, query, rowIDs, dims, rows)
}

// DotFloat32IndexedPrevalidated is the trusted-call variant for callers that
// already checked every row ID. It still validates the basic slice shape.
func DotFloat32IndexedPrevalidated(dst []float32, base []float32, query []float32, rowIDs []uint32, dims int) DotFloat32BatchStatus {
	rows := dotFloat32IndexedRows(dst, rowIDs)
	if rows == 0 {
		return DotFloat32BatchStatus{}
	}
	if !dotFloat32IndexedPrevalidatedShapeOK(base, query, dims, rows) {
		return dotFloat32BatchInvalidStatus()
	}
	return dotFloat32IndexedPrevalidated(dst, base, query, rowIDs, dims, rows)
}

func dotFloat32IndexedPrevalidated(dst []float32, base []float32, query []float32, rowIDs []uint32, dims, rows int) DotFloat32BatchStatus {
	if !dotFloat32IndexedOptimizedEligible(rows, dims) {
		dotFloat32IndexedScalar(dst, base, query, rowIDs, dims, rows)
		return dotFloat32BatchStatus(rows, false)
	}
	switch dotFloat32IndexedAMD64ActiveVariant {
	case dotFloat32IndexedAMD64AVX512:
		dotFloat32IndexedAMD64AVX512Kernel(dst, base, query, rowIDs, dims, rows)
	case dotFloat32IndexedAMD64AVXFMA:
		dotFloat32IndexedAMD64AVXFMAKernel(dst, base, query, rowIDs, dims, rows)
	case dotFloat32IndexedAMD64SSE2:
		dotFloat32IndexedAMD64SSE2Kernel(dst, base, query, rowIDs, dims, rows)
	default:
		dotFloat32IndexedScalar(dst, base, query, rowIDs, dims, rows)
		return dotFloat32BatchStatus(rows, false)
	}
	return dotFloat32BatchStatus(rows, true)
}

// DotFloat32Strided retains the per-row implementation on amd64. Issue #4137
// is limited to the shared indexed-row surface used by TreeDB search.
func DotFloat32Strided(dst []float32, base []float32, query []float32, rowCount, dims, stride int) DotFloat32BatchStatus {
	rows := dotFloat32StridedRows(dst, rowCount)
	if rows == 0 {
		return DotFloat32BatchStatus{}
	}
	if !dotFloat32StridedShapeOK(base, query, rows, dims, stride) {
		return dotFloat32BatchInvalidStatus()
	}
	dotFloat32StridedScalar(dst, base, query, rows, dims, stride)
	return dotFloat32BatchStatus(rows, false)
}

// The Go wrapper validates all lengths, indices, dimensions, rows, and CPU
// features before entering these kernels.
func dotFloat32IndexedAMD64AVX512Kernel(dst []float32, base []float32, query []float32, rowIDs []uint32, dims, rows int)
func dotFloat32IndexedAMD64AVXFMAKernel(dst []float32, base []float32, query []float32, rowIDs []uint32, dims, rows int)
func dotFloat32IndexedAMD64SSE2Kernel(dst []float32, base []float32, query []float32, rowIDs []uint32, dims, rows int)
