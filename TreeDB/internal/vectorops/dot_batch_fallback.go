//go:build purego || !amd64

package vectorops

const dotFloat32BatchImplementation = "scalar_batch_fallback"

// DotFloat32Batch computes one dot product for each row against vec and stores
// the results in dst. It processes min(len(dst), len(rows)) rows. On platforms
// without an optimized batch kernel it uses the portable scalar dot product and
// returns false.
func DotFloat32Batch(dst []float32, rows [][]float32, vec []float32) bool {
	n := len(dst)
	if len(rows) < n {
		n = len(rows)
	}
	for i := 0; i < n; i++ {
		dst[i] = DotFloat32Scalar(rows[i], vec)
	}
	return false
}

// DotFloat32BatchOptimized reports whether DotFloat32Batch is backed by a
// platform batch kernel instead of the portable scalar fallback.
func DotFloat32BatchOptimized() bool { return false }

// DotFloat32BatchImplementation identifies the active batch implementation.
func DotFloat32BatchImplementation() string { return dotFloat32BatchImplementation }
