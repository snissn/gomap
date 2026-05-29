//go:build amd64 && !purego

package vectorops

import simdf32 "github.com/tphakala/simd/f32"

const (
	dotFloat32BatchImplementation     = "simd_snissn_f32"
	dotFloat32BatchOptimizedAvailable = true
)

// DotFloat32Indexed writes dot products for row-major base rows selected by
// rowIDs. It writes min(len(dst), len(rowIDs)) scores for valid full-row shapes.
// Invalid shapes leave dst unchanged and return Invalid=true. Unsupported CPU
// features or backend thresholds compute correct fallback results and return
// Optimized=false.
func DotFloat32Indexed(dst []float32, base []float32, query []float32, rowIDs []uint32, dims int) DotFloat32BatchStatus {
	rows := dotFloat32IndexedRows(dst, rowIDs)
	if rows == 0 {
		return DotFloat32BatchStatus{}
	}
	if !dotFloat32IndexedShapeOK(base, query, rowIDs, dims, rows) {
		return dotFloat32BatchInvalidStatus()
	}
	optimized := simdf32.DotProductIndexed(dst[:rows], base, query[:dims], rowIDs[:rows], dims)
	return dotFloat32BatchStatus(rows, optimized)
}

// DotFloat32Strided writes dot products for row-major base rows at i*stride.
// It writes min(len(dst), rowCount) scores for valid full-row shapes. Invalid
// shapes leave dst unchanged and return Invalid=true. stride and dims are in
// float32 elements; stride must be >= dims.
func DotFloat32Strided(dst []float32, base []float32, query []float32, rowCount, dims, stride int) DotFloat32BatchStatus {
	rows := dotFloat32StridedRows(dst, rowCount)
	if rows == 0 {
		return DotFloat32BatchStatus{}
	}
	if !dotFloat32StridedShapeOK(base, query, rows, dims, stride) {
		return dotFloat32BatchInvalidStatus()
	}
	optimized := simdf32.DotProductStrided(dst[:rows], base, query[:dims], rows, dims, stride)
	return dotFloat32BatchStatus(rows, optimized)
}
