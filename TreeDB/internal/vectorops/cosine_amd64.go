//go:build amd64 && !purego

package vectorops

import simdcpu "github.com/tphakala/simd/cpu"

func cosineDistanceFloat32Normalized(left, right []float32, leftInvNorm, rightInvNorm float64) float32 {
	if len(left) >= 32 && simdcpu.X86.AVX512F && simdcpu.X86.AVX512VL {
		return float32(0.5 * cosineSquaredDifferenceFloat32AVX512(&left[0], &right[0], len(left), leftInvNorm, rightInvNorm))
	}
	if len(left) >= 16 && simdcpu.X86.AVX {
		return float32(0.5 * cosineSquaredDifferenceFloat32AVX(&left[0], &right[0], len(left), leftInvNorm, rightInvNorm))
	}
	return cosineDistanceFloat32NormalizedScalar(left, right, leftInvNorm, rightInvNorm)
}

//go:noescape
func cosineSquaredDifferenceFloat32AVX(left, right *float32, n int, leftInvNorm, rightInvNorm float64) float64

//go:noescape
func cosineSquaredDifferenceFloat32AVX512(left, right *float32, n int, leftInvNorm, rightInvNorm float64) float64
