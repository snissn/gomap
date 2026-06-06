//go:build amd64 && !purego

package vectorops

import simdf32 "github.com/tphakala/simd/f32"

const dotFloat32Implementation = "simd_tphakala_f32"

const dotFloat32OptimizedAvailable = true

// Conservative minimum that guarantees the selected amd64 SIMD backend executes
// at least one packed-vector dot chunk (AVX-512: 16 lanes, AVX: 8, SSE: 4).
const dotFloat32OptimizedMinLength = 16

// DotFloat32 returns the float32 dot product over the shared prefix of left and
// right using the amd64 SIMD implementation. Callers must pass validated
// []float32 slices; this kernel does not reinterpret raw bytes.
func DotFloat32(left, right []float32) float32 {
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	if n == 0 {
		return 0
	}
	return simdf32.DotProduct(left[:n], right[:n])
}

// DotFloat32Implementation identifies the active DotFloat32 implementation.
func DotFloat32Implementation() string { return dotFloat32Implementation }

// DotFloat32OptimizedAvailable reports whether this build includes a
// platform-optimized single-vector dot backend. It does not guarantee that every
// call uses SIMD; use DotFloat32OptimizedEligible for per-length accounting.
func DotFloat32OptimizedAvailable() bool { return dotFloat32OptimizedAvailable }

// DotFloat32OptimizedEligible reports whether DotFloat32 is expected to use the
// platform-optimized backend for a dot product of n float32 values.
func DotFloat32OptimizedEligible(n int) bool { return n >= dotFloat32OptimizedMinLength }
