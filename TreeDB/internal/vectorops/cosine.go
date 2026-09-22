package vectorops

// CosineDistanceFloat32Normalized computes half the squared distance between
// normalized, equally sized FP32 vectors. Callers supply finite, nonzero vectors
// and their inverse norms calculated in FP64. Promoting before multiplication
// avoids FP32 overflow/underflow; normalized differences preserve close angles.
func CosineDistanceFloat32Normalized(left, right []float32, leftInvNorm, rightInvNorm float64) float32 {
	if len(left) != len(right) {
		panic("vectorops: cosine vector dimensions differ")
	}
	return cosineDistanceFloat32Normalized(left, right, leftInvNorm, rightInvNorm)
}

func cosineDistanceFloat32NormalizedScalar(left, right []float32, leftInvNorm, rightInvNorm float64) float32 {
	var squared float64
	for i, value := range left {
		// Explicit rounding prevents asymmetric fused multiply-subtract on ARM64.
		leftNormalized := float64(float64(value) * leftInvNorm)
		rightNormalized := float64(float64(right[i]) * rightInvNorm)
		difference := leftNormalized - rightNormalized
		squared += difference * difference
	}
	return float32(0.5 * squared)
}
