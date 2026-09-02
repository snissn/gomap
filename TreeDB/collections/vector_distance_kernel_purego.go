//go:build !numkong || !cgo

package collections

import "math"

// dotProductFloat32 requires equal-length vectors and accumulates in float64 so
// cosine scoring stays stable across pure-Go and native-kernel builds.
func dotProductFloat32(left, right []float32) float64 {
	if len(left) != len(right) {
		panic("collections: dotProductFloat32 requires equal-length vectors")
	}
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	return dot
}

func angularDistancesFloat32Batch(queries, documents []float32, documentNorms []float64, queryCount, documentCount, dims int, distances []float64) {
	for queryIndex := 0; queryIndex < queryCount; queryIndex++ {
		query := queries[queryIndex*dims : (queryIndex+1)*dims]
		queryNorm := vectorNormSquared(query)
		row := distances[queryIndex*documentCount : (queryIndex+1)*documentCount]
		for docIndex := 0; docIndex < documentCount; docIndex++ {
			document := documents[docIndex*dims : (docIndex+1)*dims]
			dot := dotProductFloat32(query, document)
			row[docIndex] = 1 - dot/(math.Sqrt(queryNorm)*math.Sqrt(documentNorms[docIndex]))
		}
	}
}
