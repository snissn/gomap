//go:build !arm64

package colgranule

func columnVectorGraphDotProductFloat32(left, right []float32) float32 {
	var dot float32
	for i := range left {
		dot += left[i] * right[i]
	}
	return dot
}
