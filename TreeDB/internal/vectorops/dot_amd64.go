//go:build amd64 && !purego

package vectorops

import (
	simdcpu "github.com/tphakala/simd/cpu"
	simdf32 "github.com/tphakala/simd/f32"
)

const dotFloat32Implementation = "simd_tphakala_f32"

// Minimum that guarantees the runtime-selected amd64 SIMD backend executes at
// least one packed-vector dot chunk (AVX-512: 16 lanes, AVX: 8, SSE: 4).
// Keep this aligned with github.com/tphakala/simd/f32 init dispatch.
var dotFloat32OptimizedMinLength = amd64DotFloat32OptimizedMinLength()

func amd64DotFloat32OptimizedMinLength() int {
	switch {
	case simdcpu.X86.AVX512F && simdcpu.X86.AVX512VL:
		return 16
	case simdcpu.X86.AVX && simdcpu.X86.FMA:
		return 8
	case simdcpu.X86.SSE2:
		return 4
	default:
		return 0
	}
}

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
func DotFloat32OptimizedAvailable() bool { return dotFloat32OptimizedMinLength > 0 }

// DotFloat32OptimizedEligible reports whether DotFloat32 is expected to use the
// platform-optimized backend for a dot product of n float32 values.
func DotFloat32OptimizedEligible(n int) bool {
	return dotFloat32OptimizedMinLength > 0 && n >= dotFloat32OptimizedMinLength
}
