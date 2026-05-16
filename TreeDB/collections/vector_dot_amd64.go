//go:build amd64

package collections

import simdf32 "github.com/tphakala/simd/f32"

func vectorDotProductFloat32(left, right []float32) float32 {
	return simdf32.DotProduct(left, right)
}
