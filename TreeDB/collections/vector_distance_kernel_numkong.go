//go:build cgo

package collections

import (
	"runtime"

	nk "github.com/ashvardanian/NumKong/golang"
)

func dotProductFloat32(left, right []float32) float64 {
	return nk.DotF32(left, right)
}

func angularDistancesFloat32Batch(queries, documents []float32, queryCount, documentCount, dims int, distances []float64) {
	packedDocs := nk.NewPackedMatrixF32(documents, documentCount, dims)
	workerCount := minInt(queryCount, runtime.GOMAXPROCS(0))
	pool := nk.NewWorkerPool(workerCount)
	defer pool.Close()
	packedDocs.AngularsF32WithPool(queries, distances, queryCount, pool)
}
