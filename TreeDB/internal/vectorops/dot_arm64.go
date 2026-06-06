//go:build arm64 && !purego

package vectorops

import axiomsimd "github.com/axiomhq/simd-go"

const dotFloat32Implementation = "simd_axiomhq_simdgo"

const dotFloat32OptimizedAvailable = true

// DotFloat32 returns the float32 dot product over the shared prefix of left and
// right using the arm64 SIMD implementation. Callers must pass validated
// []float32 slices; this kernel does not reinterpret raw bytes.
func DotFloat32(left, right []float32) float32 {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	if n == 0 {
		return 0
	}
	return axiomsimd.DotProductFloat32(left[:n], right[:n])
}

// DotFloat32Implementation identifies the active DotFloat32 implementation.
func DotFloat32Implementation() string { return dotFloat32Implementation }

// DotFloat32OptimizedAvailable reports whether this build's single-vector dot
// product uses a platform-optimized backend rather than the portable scalar loop.
func DotFloat32OptimizedAvailable() bool { return dotFloat32OptimizedAvailable }
