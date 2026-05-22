//go:build arm64

package collections

import axiomsimd "github.com/axiomhq/simd-go"

func vectorDotProductFloat32(left, right []float32) float32 {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	if n == 0 {
		return 0
	}
	return axiomsimd.DotProductFloat32(left[:n], right[:n])
}
