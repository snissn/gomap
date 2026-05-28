//go:build amd64 && !purego

package vectorops

import (
	simdcpu "github.com/tphakala/simd/cpu"
	simdf32 "github.com/tphakala/simd/f32"
)

const dotFloat32BatchImplementation = "simd_tphakala_f32_batch"

// DotFloat32Batch computes one dot product for each row against vec and stores
// the results in dst. It processes min(len(dst), len(rows)) rows and returns
// true when the optimized batch kernel handled the work.
func DotFloat32Batch(dst []float32, rows [][]float32, vec []float32) bool {
	n := len(dst)
	if len(rows) < n {
		n = len(rows)
	}
	if n == 0 {
		return true
	}
	if len(vec) == 0 {
		clear(dst[:n])
		return true
	}
	optimized := DotFloat32BatchOptimized() && n >= 4 && len(vec) >= 16
	simdf32.DotProductBatch(dst[:n], rows[:n], vec)
	return optimized
}

// DotFloat32BatchOptimized reports whether DotFloat32Batch is backed by a
// platform batch kernel instead of the portable scalar fallback.
func DotFloat32BatchOptimized() bool { return simdcpu.X86.AVX512F && simdcpu.X86.AVX512VL }

// DotFloat32BatchImplementation identifies the active batch implementation.
func DotFloat32BatchImplementation() string { return dotFloat32BatchImplementation }
