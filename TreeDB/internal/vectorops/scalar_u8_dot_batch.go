package vectorops

// ScalarU8DotBatchStatus reports how a row-major scalar_u8 centered dot batch
// wrapper handled a call. Rows is the number of dst entries written. Optimized
// is true only when the active platform backend reports that a batch SIMD
// kernel handled the call. Fallback is true when valid rows were written without
// a batch SIMD kernel. Invalid is true when the wrapper rejected the shape and
// left dst unchanged.
//
// Status is call-level: Optimized and Fallback are mutually exclusive. A call
// can be valid and non-optimized on unsupported platforms, purego builds, tiny
// row counts, short dimensions, or backend-specific overflow guard thresholds.
type ScalarU8DotBatchStatus struct {
	Rows      int
	Optimized bool
	Fallback  bool
	Invalid   bool
}

// ScalarU8DotBatchImplementation identifies the active scalar_u8 centered
// indexed batch dot backend. Per-call status is still authoritative because
// optimized backends may fall back for individual shapes.
func ScalarU8DotBatchImplementation() string { return scalarU8DotBatchImplementation }

// ScalarU8DotBatchOptimizedAvailable reports whether this build includes a
// platform scalar_u8 batch SIMD backend. A true value does not guarantee that a
// given call uses SIMD; callers must inspect ScalarU8DotBatchStatus.Optimized.
func ScalarU8DotBatchOptimizedAvailable() bool { return scalarU8DotBatchOptimizedAvailable }

// DotScalarU8CenteredIndexedOptimizedEligible reports whether this build's
// indexed-row scalar_u8 backend is expected to use optimized execution for a
// valid rows/dims shape. Callers must still inspect ScalarU8DotBatchStatus
// because invalid shapes are rejected before backend selection.
func DotScalarU8CenteredIndexedOptimizedEligible(rows, dims int) bool {
	return dotScalarU8CenteredIndexedOptimizedEligible(rows, dims)
}

func scalarU8DotBatchStatus(rows int, optimized bool) ScalarU8DotBatchStatus {
	return ScalarU8DotBatchStatus{
		Rows:      rows,
		Optimized: optimized,
		Fallback:  rows > 0 && !optimized,
	}
}

func scalarU8DotBatchInvalidStatus() ScalarU8DotBatchStatus {
	return ScalarU8DotBatchStatus{Invalid: true}
}

func dotScalarU8CenteredIndexedRows(dst []int64, rowIDs []uint32) int {
	rows := len(rowIDs)
	if len(dst) < rows {
		rows = len(dst)
	}
	return rows
}

func dotScalarU8CenteredIndexedShapeOK(codes []byte, query ScalarU8CenteredQuery, rowIDs []uint32, dims, rows int) bool {
	if rows == 0 {
		return true
	}
	if dims <= 0 || !query.ValidForDims(dims) || len(codes) < dims {
		return false
	}
	maxRow := uint64((len(codes) - dims) / dims)
	for i := 0; i < rows; i++ {
		if uint64(rowIDs[i]) > maxRow {
			return false
		}
	}
	return true
}

func scalarU8CenteredQuerySum(values []ScalarU8CenteredCode) int64 {
	var sum int64
	for _, v := range values {
		sum += int64(v)
	}
	return sum
}

func dotScalarU8CenteredIndexedScalar(dst []int64, codes []byte, query ScalarU8CenteredQuery, rowIDs []uint32, dims, rows int) {
	queryValues := query.Values[:dims]
	for i := 0; i < rows; i++ {
		start := int(rowIDs[i]) * dims
		row := codes[start : start+dims]
		var dot int64
		for j, q := range queryValues {
			dot += int64(q) * int64(ScalarU8CenteredValue(row[j]))
		}
		dst[i] = dot
	}
}
