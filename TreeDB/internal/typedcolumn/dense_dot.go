package typedcolumn

import "github.com/snissn/gomap/TreeDB/internal/vectorops"

// DenseFloat32Dot returns the float32 dot product for dense typed-column vector
// rows. The hot kernel only accepts already validated []float32 values; callers
// that start from column-section bytes should first use mappedresource.Float32View
// (or another checked decode path) to validate length, alignment, and endian
// preconditions before slicing rows.
func DenseFloat32Dot(left, right []float32) float32 {
	return vectorops.DotFloat32(left, right)
}

// DenseFloat32DotScalar is the portable scalar baseline for dense typed-column
// vector rows.
func DenseFloat32DotScalar(left, right []float32) float32 {
	return vectorops.DotFloat32Scalar(left, right)
}

// DenseFloat32DotImplementation identifies the active optimized dot kernel.
func DenseFloat32DotImplementation() string {
	return vectorops.DotFloat32Implementation()
}
