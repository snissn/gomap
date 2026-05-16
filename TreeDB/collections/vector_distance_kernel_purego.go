//go:build !cgo

package collections

import "math"

func dotProductFloat32(left, right []float32) float64 {
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	return dot
}

func angularDistancesFloat32Batch(queries, documents []float32, queryCount, documentCount, dims int, distances []float64) {
	for queryIndex := 0; queryIndex < queryCount; queryIndex++ {
		query := queries[queryIndex*dims : (queryIndex+1)*dims]
		queryNorm := vectorNormSquared(query)
		queryScale := 1 / math.Sqrt(queryNorm)
		row := distances[queryIndex*documentCount : (queryIndex+1)*documentCount]
		for docIndex := 0; docIndex < documentCount; docIndex++ {
			document := documents[docIndex*dims : (docIndex+1)*dims]
			documentNorm := vectorNormSquared(document)
			dot := dotProductFloat32(query, document)
			row[docIndex] = 1 - dot*queryScale/math.Sqrt(documentNorm)
		}
	}
}
