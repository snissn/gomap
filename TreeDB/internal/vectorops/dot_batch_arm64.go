//go:build arm64 && !purego

package vectorops

var dotFloat32BatchImplementation = "indexed_arm64_neon_per_row_" + DotFloat32Implementation()

const dotFloat32BatchOptimizedAvailable = true

const (
	dotFloat32IndexedARM64MinDims = 32
	dotFloat32IndexedARM64MinRows = 2
)

func dotFloat32IndexedOptimizedEligible(rows, dims int) bool {
	return rows >= dotFloat32IndexedARM64MinRows && dims >= dotFloat32IndexedARM64MinDims
}

// DotFloat32Indexed writes dot products for row-major base rows selected by
// rowIDs. Supported arm64 builds use a single NEON-backed indexed-row kernel for
// sufficiently large tiles, avoiding per-row Go wrapper and slice setup while
// preserving the same row application order. Tiny/short shapes use the per-row
// DotFloat32 fallback. Invalid shapes leave dst unchanged and return Invalid=true.
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

// DotFloat32IndexedPrevalidated writes dot products for row-major base rows
// selected by rowIDs whose bounds were already checked by the caller. It still
// validates the non-indexed shape (dst/row count, dims, base/query minimum
// length) but intentionally skips scanning rowIDs so hot graph-search paths that
// already validated adjacency ordinals do not pay that check twice. Passing an
// out-of-range row ID violates this function's contract.
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
	dotFloat32IndexedARM64(dst, base, query, rowIDs, dims, rows)
	return dotFloat32BatchStatus(rows, true)
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

// dotFloat32IndexedARM64 computes rows indexed dot products. The Go wrapper must
// validate all slice lengths, row IDs, dims, and rows before calling.
func dotFloat32IndexedARM64(dst []float32, base []float32, query []float32, rowIDs []uint32, dims int, rows int)
