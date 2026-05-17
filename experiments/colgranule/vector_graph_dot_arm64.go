//go:build arm64

package colgranule

import axiomsimd "github.com/axiomhq/simd-go"

func columnVectorGraphDotProductFloat32(left, right []float32) float32 {
	return axiomsimd.DotProductFloat32(left, right)
}
