//go:build purego || (!amd64 && !arm64)

package vectorops

const dotFloat32Implementation = "scalar"

const dotFloat32OptimizedAvailable = false

// DotFloat32 returns the float32 dot product over the shared prefix of left and
// right using the portable scalar implementation. Callers must pass validated
// []float32 slices; this kernel does not reinterpret raw bytes.
func DotFloat32(left, right []float32) float32 {
	return DotFloat32Scalar(left, right)
}

// DotFloat32Implementation identifies the active DotFloat32 implementation.
func DotFloat32Implementation() string { return dotFloat32Implementation }

// DotFloat32OptimizedAvailable reports whether this build includes a
// platform-optimized single-vector dot backend. It does not guarantee that every
// call uses SIMD; use DotFloat32OptimizedEligible for per-length accounting.
func DotFloat32OptimizedAvailable() bool { return dotFloat32OptimizedAvailable }

// DotFloat32OptimizedEligible reports whether DotFloat32 is expected to use the
// platform-optimized backend for a dot product of n float32 values.
func DotFloat32OptimizedEligible(n int) bool { return false }
