//go:build arm64

package colgranule

import axiomsimd "github.com/axiomhq/simd-go"

// columnVectorGraphDotProductFloat32 returns the float32 dot product.
// Precondition: left and right must have equal length; callers enforce graph
// query and row-vector dimensionality before reaching this hot path.
func columnVectorGraphDotProductFloat32(left, right []float32) float32 {
	return axiomsimd.DotProductFloat32(left, right)
}
