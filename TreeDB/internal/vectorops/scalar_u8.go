package vectorops

const scalarU8CenterOffset = 255

// ScalarU8CenteredCode is the stable element type for centered scalar_u8 query
// codes. Each value is 2*code-255, so the valid range is [-255, 255]. The
// slice layout is contiguous native-endian int16 values with ordinary Go slice
// alignment; SIMD backends that consume this layout must tolerate unaligned
// loads.
type ScalarU8CenteredCode = int16

// ScalarU8CenteredQuery is a validated, allocation-free view over centered
// scalar_u8 query codes. Values aliases caller-owned scratch and remains valid
// until that scratch is reused.
type ScalarU8CenteredQuery struct {
	Values []ScalarU8CenteredCode
}

// Dims returns the number of centered query dimensions.
func (q ScalarU8CenteredQuery) Dims() int { return len(q.Values) }

// Valid reports whether q has a positive-dimension centered-code layout.
func (q ScalarU8CenteredQuery) Valid() bool { return len(q.Values) > 0 }

// ValidForDims reports whether q is valid for exactly dims dimensions.
func (q ScalarU8CenteredQuery) ValidForDims(dims int) bool {
	return dims > 0 && len(q.Values) == dims
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
	for i, code := range codes {
		dst[i] = ScalarU8CenteredValue(code)
	}
	return ScalarU8CenteredQuery{Values: dst}, dst, true
}

// ScalarU8CenteredDot computes the integer dot product between a centered query
// and a raw scalar_u8 row. The row is centered as 2*row_code-255 while the query
// is consumed from its pre-centered layout.
func ScalarU8CenteredDot(query ScalarU8CenteredQuery, row []byte) (int64, bool) {
	if !query.Valid() || len(row) != len(query.Values) {
		return 0, false
	}
	var dot int64
	for i, q := range query.Values {
		c := ScalarU8CenteredValue(row[i])
		dot += int64(q) * int64(c)
	}
	return dot, true
}
