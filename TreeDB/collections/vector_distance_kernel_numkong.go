//go:build numkong && cgo

package collections

import (
	"math"

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

func angularDistancesFloat32Batch(queries, documents []float32, documentNorms []float64, queryCount, documentCount, dims int, distances []float64) {
	// CheckRecall compares against SearchVectorsExact, so its batch baseline must
	// use the same float64 accumulation instead of NumKong's F32 angular kernel.
	for queryIndex := 0; queryIndex < queryCount; queryIndex++ {
		query := queries[queryIndex*dims : (queryIndex+1)*dims]
		queryNorm := vectorNormSquared(query)
		row := distances[queryIndex*documentCount : (queryIndex+1)*documentCount]
		for docIndex := 0; docIndex < documentCount; docIndex++ {
			document := documents[docIndex*dims : (docIndex+1)*dims]
			dot := dotProductFloat32Wide(query, document)
			row[docIndex] = 1 - dot/(math.Sqrt(queryNorm)*math.Sqrt(documentNorms[docIndex]))
		}
	}
}
