//go:build cgo

package collections

import nk "github.com/ashvardanian/NumKong/golang"

func dotProductFloat32(left, right []float32) float64 {
	return nk.DotF32(left, right)
}
