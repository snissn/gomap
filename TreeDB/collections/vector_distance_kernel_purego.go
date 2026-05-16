//go:build !cgo

package collections

// dotProductFloat32 requires equal-length vectors and accumulates in float64 so
// cosine scoring stays stable across pure-Go and native-kernel builds.
func dotProductFloat32(left, right []float32) float64 {
	if len(left) != len(right) {
		panic("collections: dotProductFloat32 requires equal-length vectors")
	}
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	return dot
}
