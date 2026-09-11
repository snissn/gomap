//go:build amd64 && !purego

package vectorops

import (
	"testing"

	simdcpu "github.com/tphakala/simd/cpu"
)

func TestCosineDistanceFloat32NormalizedAMD64Kernels(t *testing.T) {
	for _, kernel := range []struct {
		name      string
		available bool
		run       func(*float32, *float32, int, float64, float64) float64
	}{
		{"avx", simdcpu.X86.AVX, cosineSquaredDifferenceFloat32AVX},
		{"avx512", simdcpu.X86.AVX512F && simdcpu.X86.AVX512VL, cosineSquaredDifferenceFloat32AVX512},
	} {
		t.Run(kernel.name, func(t *testing.T) {
			if !kernel.available {
				t.Skip("CPU does not support this kernel")
			}
			testCosineDistanceFloat32Normalized(t, func(left, right []float32, li, ri float64) float32 {
				if len(left) == 0 {
					return 0
				}
				return float32(0.5 * kernel.run(&left[0], &right[0], len(left), li, ri))
			})
		})
	}
}
