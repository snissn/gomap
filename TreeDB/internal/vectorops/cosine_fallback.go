//go:build purego || (!amd64 && !arm64)

package vectorops

func cosineDistanceFloat32Normalized(left, right []float32, leftInvNorm, rightInvNorm float64) float32 {
	return cosineDistanceFloat32NormalizedScalar(left, right, leftInvNorm, rightInvNorm)
}
