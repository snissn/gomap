//go:build purego || !gomap_simd_batch || (!amd64 && !arm64)

package vectorops

var dotFloat32BatchImplementation = "per_row_" + DotFloat32Implementation()

const dotFloat32BatchOptimizedAvailable = false

// DotFloat32Indexed writes dot products for row-major base rows selected by
// rowIDs using the per-row DotFloat32 fallback. It writes min(len(dst), len(rowIDs))
// scores for valid full-row shapes. Invalid shapes leave dst unchanged and
// return Invalid=true.
func DotFloat32Indexed(dst []float32, base []float32, query []float32, rowIDs []uint32, dims int) DotFloat32BatchStatus {
	rows := dotFloat32IndexedRows(dst, rowIDs)
	if rows == 0 {
		return DotFloat32BatchStatus{}
	}
	if !dotFloat32IndexedShapeOK(base, query, rowIDs, dims, rows) {
		return dotFloat32BatchInvalidStatus()
	}
	dotFloat32IndexedScalar(dst, base, query, rowIDs, dims, rows)
	return dotFloat32BatchStatus(rows, false)
}

// DotFloat32Strided writes dot products for row-major base rows at i*stride
// using the per-row DotFloat32 fallback. It writes min(len(dst), rowCount) scores
// for valid full-row shapes. Invalid shapes leave dst unchanged and return
// Invalid=true. stride and dims are in float32 elements; stride must be >= dims.
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
