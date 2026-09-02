//go:build arm64 && !purego

package vectorops

const scalarU8DotBatchImplementation = "indexed_arm64_neon"

const scalarU8DotBatchOptimizedAvailable = true

const (
	dotScalarU8CenteredIndexedARM64MinDims = 16
	dotScalarU8CenteredIndexedARM64MinRows = 1
	// The bounded int32-accumulation kernel sums at most ceil(dims/32) products
	// per vector lane. Each q*row product is bounded by 255*255, so this conservative
	// limit keeps per-lane raw sums below int32 max while covering TreeDB's common
	// 128/256/768-dimensional vector-search shapes.
	dotScalarU8CenteredIndexedARM64Int32MaxDims = 32768
)

func dotScalarU8CenteredIndexedOptimizedEligible(rows, dims int) bool {
	return rows >= dotScalarU8CenteredIndexedARM64MinRows && dims >= dotScalarU8CenteredIndexedARM64MinDims
}

func dotScalarU8CenteredIndexedPreparedByteEligible(dims int) bool { return false }

func dotScalarU8CenteredIndexedPreparedByte(dst []int64, codes []byte, queryHalf []int8, rowByteSums []uint32, rowIDs []uint32, dims int, rows int, querySum int64) {
}

// DotScalarU8CenteredIndexed writes integer dot products for row-major scalar_u8
// code rows selected by rowIDs against a pre-centered scalar_u8 query. Supported
// arm64 builds use a NEON-backed indexed-row kernel for sufficiently large
// tiles. Tiny/short shapes use the portable scalar fallback. Invalid shapes
// leave dst unchanged and return Invalid=true.
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
	if dims <= dotScalarU8CenteredIndexedARM64Int32MaxDims {
		dotScalarU8CenteredIndexedARM64Int32(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	} else {
		dotScalarU8CenteredIndexedARM64(dst, codes, queryValues, rowIDs, dims, rows, querySum)
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
	if dims <= dotScalarU8CenteredIndexedARM64Int32MaxDims {
		dotScalarU8CenteredIndexedARM64Int32(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	} else {
		dotScalarU8CenteredIndexedARM64(dst, codes, queryValues, rowIDs, dims, rows, querySum)
	}
	return scalarU8DotBatchStatus(rows, true)
}

// dotScalarU8CenteredIndexedARM64 computes rows indexed scalar_u8 centered dot
// products. The Go wrapper must validate all slice lengths, row IDs, dims, and
// rows before calling. querySum is sum(query[:dims]); the kernel computes
// sum(q*(2*row-255)) as 2*sum(q*row)-255*querySum.
func dotScalarU8CenteredIndexedARM64(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)

// dotScalarU8CenteredIndexedARM64Int32 computes rows indexed scalar_u8 centered
// dot products for bounded dimensions whose per-lane int32 partial sums cannot
// overflow. The Go wrapper must validate all slice lengths, row IDs, dims, and
// rows before calling.
func dotScalarU8CenteredIndexedARM64Int32(dst []int64, codes []byte, query []ScalarU8CenteredCode, rowIDs []uint32, dims int, rows int, querySum int64)
