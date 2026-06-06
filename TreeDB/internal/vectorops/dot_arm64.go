//go:build arm64 && !purego

package vectorops

import axiomsimd "github.com/axiomhq/simd-go"

const dotFloat32Implementation = "simd_axiomhq_simdgo"

const dotFloat32OptimizedAvailable = true

// Keep this threshold aligned with axiomhq/simd-go's Apple/NEON
// DotProductFloat32 dispatch threshold. Below it DotProductFloat32 may execute
// the scalar fallback even though this build includes a SIMD backend.
const dotFloat32OptimizedMinLength = 32

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

// DotFloat32OptimizedAvailable reports whether this build includes a
// platform-optimized single-vector dot backend. It does not guarantee that every
// call uses SIMD; use DotFloat32OptimizedEligible for per-length accounting.
func DotFloat32OptimizedAvailable() bool { return dotFloat32OptimizedAvailable }

// DotFloat32OptimizedEligible reports whether DotFloat32 is expected to use the
// platform-optimized backend for a dot product of n float32 values. arm64
// axiomhq/simd-go dispatches small dots to scalar below its threshold.
func DotFloat32OptimizedEligible(n int) bool { return n >= dotFloat32OptimizedMinLength }
