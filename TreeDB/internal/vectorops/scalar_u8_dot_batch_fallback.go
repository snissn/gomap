//go:build purego || (!amd64 && !arm64)

package vectorops

const scalarU8DotBatchImplementation = "scalar_portable"

const scalarU8DotBatchOptimizedAvailable = false

func dotScalarU8CenteredIndexedOptimizedEligible(rows, dims int) bool { return false }

// DotScalarU8CenteredIndexed writes integer dot products for row-major scalar_u8
// code rows selected by rowIDs against a pre-centered scalar_u8 query. The
// portable fallback writes min(len(dst), len(rowIDs)) scores for valid full-row
// shapes. Invalid shapes leave dst unchanged and return Invalid=true.
func DotScalarU8CenteredIndexed(dst []int64, codes []byte, query ScalarU8CenteredQuery, rowIDs []uint32, dims int) ScalarU8DotBatchStatus {
	rows := dotScalarU8CenteredIndexedRows(dst, rowIDs)
	if rows == 0 {
		return ScalarU8DotBatchStatus{}
	}
	if !dotScalarU8CenteredIndexedShapeOK(codes, query, rowIDs, dims, rows) {
		return scalarU8DotBatchInvalidStatus()
	}
	dotScalarU8CenteredIndexedScalar(dst, codes, query, rowIDs, dims, rows)
	return scalarU8DotBatchStatus(rows, false)
}
