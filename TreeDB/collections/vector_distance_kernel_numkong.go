//go:build numkong && cgo

package collections

import (
	"runtime"

	nk "github.com/ashvardanian/NumKong/golang"
)

// dotProductFloat32 requires equal-length vectors and returns a float64
// accumulator-compatible result, matching the pure-Go fallback contract.
func dotProductFloat32(left, right []float32) float64 {
	if len(left) != len(right) {
		panic("collections: dotProductFloat32 requires equal-length vectors")
	}
	return nk.DotF32(left, right)
}

func angularDistancesFloat32Batch(queries, documents []float32, queryCount, documentCount, dims int, distances []float64) {
	packedDocs := nk.NewPackedMatrixF32(documents, documentCount, dims)
	workerCount := minInt(queryCount, runtime.GOMAXPROCS(0))
	pool := nk.NewWorkerPool(workerCount)
	defer pool.Close()
	packedDocs.AngularsF32WithPool(queries, distances, queryCount, pool)
}
