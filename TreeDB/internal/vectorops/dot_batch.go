package vectorops

// DotFloat32BatchStatus reports how a row-major FP32 batch dot wrapper handled
// a call. Rows is the number of dst entries written. Optimized is true only
// when the active platform backend reports that a batch SIMD kernel handled at
// least one row batch. Fallback is true when valid rows were written and the
// call was completed without a batch SIMD kernel. Invalid is true when the
// wrapper rejected the shape and left dst unchanged.
//
// Status is call-level: Optimized and Fallback are mutually exclusive. A call
// can be valid and non-optimized: unsupported platforms, purego builds, short
// dims, tiny row counts, or backend-specific thresholds all use the non-batch
// fallback and return Optimized=false, Fallback=true. The purego
// and unsupported-platform fallback is the portable scalar implementation.
type DotFloat32BatchStatus struct {
	Rows      int
	Optimized bool
	Fallback  bool
	Invalid   bool
}

// DotFloat32BatchImplementation identifies the active row-major batch dot
// backend. The per-call status is still authoritative because optimized backends
// may fall back for unsupported runtime CPU features or individual shapes.
func DotFloat32BatchImplementation() string { return dotFloat32BatchImplementation }

// DotFloat32BatchOptimizedAvailable reports whether this build includes a
// platform batch SIMD backend. A true value does not guarantee that a given call
// uses SIMD; callers must inspect DotFloat32BatchStatus.Optimized.
func DotFloat32BatchOptimizedAvailable() bool { return dotFloat32BatchOptimizedAvailable }

// DotFloat32IndexedOptimizedEligible reports whether this build's indexed-row
// backend is expected to use optimized execution for a valid rows/dims shape.
// Callers must still inspect DotFloat32BatchStatus because invalid shapes are
// rejected before backend selection.
func DotFloat32IndexedOptimizedEligible(rows, dims int) bool {
	return dotFloat32IndexedOptimizedEligible(rows, dims)
}

func dotFloat32BatchStatus(rows int, optimized bool) DotFloat32BatchStatus {
	return DotFloat32BatchStatus{
		Rows:      rows,
		Optimized: optimized,
		Fallback:  rows > 0 && !optimized,
	}
}

func dotFloat32BatchInvalidStatus() DotFloat32BatchStatus {
	return DotFloat32BatchStatus{Invalid: true}
}

func dotFloat32IndexedRows(dst []float32, rowIDs []uint32) int {
	rows := len(rowIDs)
	if len(dst) < rows {
		rows = len(dst)
	}
	return rows
}

func dotFloat32StridedRows(dst []float32, rowCount int) int {
	if rowCount <= 0 || len(dst) == 0 {
		return 0
	}
	if len(dst) < rowCount {
		return len(dst)
	}
	return rowCount
}

func dotFloat32IndexedShapeOK(base []float32, query []float32, rowIDs []uint32, dims, rows int) bool {
	if !dotFloat32IndexedPrevalidatedShapeOK(base, query, dims, rows) {
		return false
	}
	if rows == 0 {
		return true
	}
	maxRow := uint64((len(base) - dims) / dims)
	for i := 0; i < rows; i++ {
		if uint64(rowIDs[i]) > maxRow {
			return false
		}
	}
	return true
}

func dotFloat32IndexedPrevalidatedShapeOK(base []float32, query []float32, dims, rows int) bool {
	if rows == 0 {
		return true
	}
	return dims > 0 && len(query) >= dims && len(base) >= dims
}

func dotFloat32StridedShapeOK(base []float32, query []float32, rows, dims, stride int) bool {
	if rows == 0 {
		return true
	}
	if dims <= 0 || stride < dims || len(query) < dims || len(base) < dims {
		return false
	}
	maxRow := uint64((len(base) - dims) / stride)
	return uint64(rows-1) <= maxRow
}

func dotFloat32IndexedScalar(dst []float32, base []float32, query []float32, rowIDs []uint32, dims, rows int) {
	query = query[:dims]
	for i := 0; i < rows; i++ {
		start := int(rowIDs[i]) * dims
		dst[i] = DotFloat32(base[start:start+dims], query)
	}
}

func dotFloat32StridedScalar(dst []float32, base []float32, query []float32, rows, dims, stride int) {
	query = query[:dims]
	for i := 0; i < rows; i++ {
		start := i * stride
		dst[i] = DotFloat32(base[start:start+dims], query)
	}
}
