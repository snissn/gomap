//go:build arm64

package collections

import (
	_ "github.com/axiomhq/simd-go"
	_ "unsafe"
)

//go:linkname axiomsimdDotProductFloat32Impl github.com/axiomhq/simd-go.dotProductFloat32Impl
func axiomsimdDotProductFloat32Impl(left, right []float32) float32

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
	return axiomsimdDotProductFloat32Impl(left, right)
}
