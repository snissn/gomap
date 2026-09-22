//go:build arm64 && !purego

package vectorops

func cosineDistanceFloat32Normalized(left, right []float32, leftInvNorm, rightInvNorm float64) float32 {
	if len(left) >= 16 {
		return float32(0.5 * cosineSquaredDifferenceFloat32ARM64(&left[0], &right[0], len(left), leftInvNorm, rightInvNorm))
	}
	return cosineDistanceFloat32NormalizedScalar(left, right, leftInvNorm, rightInvNorm)
}

//go:noescape
func cosineSquaredDifferenceFloat32ARM64(left, right *float32, n int, leftInvNorm, rightInvNorm float64) float64
