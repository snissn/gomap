//go:build amd64 && !purego

package vectorops

const scalarU8DotBatchImplementation = "indexed_amd64_sse2"

const scalarU8DotBatchOptimizedAvailable = true

const (
	dotScalarU8CenteredIndexedAMD64MinDims = 16
	dotScalarU8CenteredIndexedAMD64MinRows = 2
	// The SSE2 kernel accumulates into signed 32-bit vector lanes and reduces once
	// per row. At 32,768 dimensions, the worst-case full-row scalar_u8 dot product
	// is 65,025*32,768 = 2,130,739,200, which still fits int32. Larger dimensions
	// use the portable int64 scalar fallback to keep accumulation overflow-safe.
	dotScalarU8CenteredIndexedAMD64MaxSIMDDims = 32768
)

func dotScalarU8CenteredIndexedOptimizedEligible(rows, dims int) bool {
	return rows >= dotScalarU8CenteredIndexedAMD64MinRows && dims >= dotScalarU8CenteredIndexedAMD64MinDims && dims <= dotScalarU8CenteredIndexedAMD64MaxSIMDDims
}

// DotScalarU8CenteredIndexed writes integer dot products for row-major scalar_u8
// code rows selected by rowIDs against a pre-centered scalar_u8 query. Supported
// amd64 builds use an SSE2-backed indexed-row kernel for sufficiently large
// overflow-safe tiles. Tiny/short or very high-dimensional shapes use the
// portable scalar fallback. Invalid shapes leave dst unchanged and return
// Invalid=true.
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
	dotScalarU8CenteredIndexedAMD64(dst, codes, queryValues, rowIDs, dims, rows, query.CenteredSum())
	return scalarU8DotBatchStatus(rows, true)
}

// dotScalarU8CenteredIndexedAMD64 computes rows indexed scalar_u8 centered dot
// products. The Go wrapper must validate all slice lengths, row IDs, dims, and
// rows before calling. querySum is sum(query[:dims]); the kernel computes
// sum(q*(2*row-255)) as 2*sum(q*row)-255*querySum.
func dotScalarU8CenteredIndexedAMD64(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
