//go:build !arm64

package collections

import "gonum.org/v1/gonum/blas/blas32"

func vectorDotProductFloat32(left, right []float32) float32 {
	// Axiom's non-arm64 implementation is scalar; Gonum gives a faster BLAS32
	// dot path on linux/amd64 in the TreeDB HNSW search profile.
	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	if n == 0 {
		return 0
	}
	return blas32.Dot(
		blas32.Vector{N: n, Inc: 1, Data: left[:n]},
		blas32.Vector{N: n, Inc: 1, Data: right[:n]},
	)
}
