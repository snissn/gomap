package collections

import "github.com/snissn/gomap/TreeDB/internal/vectorops"

func vectorDotProductFloat32(left, right []float32) float32 {
	return vectorops.DotFloat32(left, right)
}
