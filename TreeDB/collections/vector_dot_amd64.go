//go:build amd64

package collections

import simdf32 "github.com/tphakala/simd/f32"

func vectorDotProductFloat32(left, right []float32) float32 {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	if n == 0 {
		return 0
	}
	return vectorDotProductFloat32SameLen(left[:n], right[:n])
}

func vectorDotProductFloat32SameLen(left, right []float32) float32 {
	if len(left) == 0 {
		return 0
	}
	return simdf32.DotProduct(left, right)
}
