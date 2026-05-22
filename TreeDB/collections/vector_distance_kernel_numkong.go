//go:build numkong && cgo

package collections

import nk "github.com/ashvardanian/NumKong/golang"

// dotProductFloat32 requires equal-length vectors and returns a float64
// accumulator-compatible result, matching the pure-Go fallback contract.
func dotProductFloat32(left, right []float32) float64 {
	if len(left) != len(right) {
		panic("collections: dotProductFloat32 requires equal-length vectors")
	}
	return nk.DotF32(left, right)
}
