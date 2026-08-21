package vectorops

const scalarU8CenterOffset = 255

// ScalarU8CenteredCode is the stable element type for centered scalar_u8 query
// codes. Each value is 2*code-255, so the valid range is [-255, 255]. The
// slice layout is contiguous native-endian int16 values with ordinary Go slice
// alignment; SIMD backends that consume this layout must tolerate unaligned
// loads.
type ScalarU8CenteredCode = int16

// ScalarU8CenteredQuery is a validated, allocation-free view over centered
// scalar_u8 query codes. Centered values alias caller-owned scratch, must be
// treated as immutable while the query is in use, and remain valid until that
// scratch is reused. The values slice is intentionally unexported so callers
// cannot mutate cached query metadata through the query handle.
type ScalarU8CenteredQuery struct {
	values     []ScalarU8CenteredCode
	halfValues []int8

	sum      int64
	sumDims  int
	sumValid bool
}

// Dims returns the number of centered query dimensions.
func (q ScalarU8CenteredQuery) Dims() int { return len(q.values) }

// Valid reports whether q has a positive-dimension centered-code layout.
func (q ScalarU8CenteredQuery) Valid() bool { return len(q.values) > 0 }

// ValidForDims reports whether q is valid for exactly dims dimensions.
func (q ScalarU8CenteredQuery) ValidForDims(dims int) bool {
	return dims > 0 && len(q.values) == dims
}

// Value returns the centered value at i for tests and diagnostics without
// exposing the mutable backing slice.
func (q ScalarU8CenteredQuery) Value(i int) (ScalarU8CenteredCode, bool) {
	if i < 0 || i >= len(q.values) {
		return 0, false
	}
	return q.values[i], true
}

// CenteredSum returns sum(q.values). Queries returned by
// PrepareScalarU8CenteredQuery carry this value so batch kernels can reuse it
// without rescanning the query per call. Manually constructed or resliced
// queries still return the correct sum by scanning values.
func (q ScalarU8CenteredQuery) CenteredSum() int64 {
	if q.sumValid && q.sumDims == len(q.values) {
		return q.sum
	}
	return scalarU8CenteredQuerySum(q.values)
}

// ScalarU8CenteredValue returns the centered scalar_u8 value 2*code-255.
func ScalarU8CenteredValue(code byte) ScalarU8CenteredCode {
	return ScalarU8CenteredCode(2*int(code) - scalarU8CenterOffset)
}

// PrepareScalarU8CenteredQuery fills dst with centered values for codes and
// returns a validated query view. The function never allocates: callers must
// provide scratch capacity for dims values. Invalid dimensions or insufficient
// scratch return ok=false and leave dst contents unspecified.
func PrepareScalarU8CenteredQuery(dst []ScalarU8CenteredCode, codes []byte, dims int) (query ScalarU8CenteredQuery, scratch []ScalarU8CenteredCode, ok bool) {
	if dims <= 0 || len(codes) != dims || cap(dst) < dims {
		return ScalarU8CenteredQuery{}, dst[:0], false
	}
	dst = dst[:dims]
	var sum int64
	for i, code := range codes {
		centered := ScalarU8CenteredValue(code)
		dst[i] = centered
		sum += int64(centered)
	}
	return PrepareScalarU8CenteredQueryFromCentered(dst, dims, sum)
}

// PrepareScalarU8CenteredQueryFromCentered returns a validated query view over
// caller-filled centered values. The caller must provide the exact sum of
// values[:dims]; SIMD batch kernels use it to preserve the scalar_u8 centered
// scoring identity without rescanning the query on each score batch.
func PrepareScalarU8CenteredQueryFromCentered(values []ScalarU8CenteredCode, dims int, sum int64) (query ScalarU8CenteredQuery, scratch []ScalarU8CenteredCode, ok bool) {
	if dims <= 0 || len(values) != dims {
		return ScalarU8CenteredQuery{}, values[:0], false
	}
	return ScalarU8CenteredQuery{values: values, sum: sum, sumDims: dims, sumValid: true}, values, true
}

// PrepareScalarU8CenteredQueryFromCenteredWithHalf is the prepared-byte VNNI
// variant of PrepareScalarU8CenteredQueryFromCentered. The caller must provide
// halfValues[i] == values[i]>>1, just as it must provide the exact sum metadata;
// scalar_u8 centered codes are odd, so byte-dot kernels can recover the exact
// centered score using one precomputed raw-code sum per row.
func PrepareScalarU8CenteredQueryFromCenteredWithHalf(values []ScalarU8CenteredCode, halfValues []int8, dims int, sum int64) (query ScalarU8CenteredQuery, scratch []ScalarU8CenteredCode, ok bool) {
	if dims <= 0 || len(values) != dims || len(halfValues) != dims {
		return ScalarU8CenteredQuery{}, values[:0], false
	}
	return ScalarU8CenteredQuery{values: values, halfValues: halfValues, sum: sum, sumDims: dims, sumValid: true}, values, true
}

// ScalarU8CenteredDot computes the integer dot product between a centered query
// and a raw scalar_u8 row. The row is centered as 2*row_code-255 while the query
// is consumed from its pre-centered layout.
func ScalarU8CenteredDot(query ScalarU8CenteredQuery, row []byte) (int64, bool) {
	if !query.Valid() || len(row) != len(query.values) {
		return 0, false
	}
	var dot int64
	for i, q := range query.values {
		c := ScalarU8CenteredValue(row[i])
		dot += int64(q) * int64(c)
	}
	return dot, true
}
