//go:build !cgo

package collections

func dotProductFloat32(left, right []float32) float64 {
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	return dot
}
