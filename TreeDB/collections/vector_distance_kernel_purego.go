//go:build !numkong || !cgo

package collections

import (
	"math"

	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

func angularDistancesFloat32Batch(queries, documents []float32, documentNorms []float64, queryCount, documentCount, dims int, distances []float64) {
	for queryIndex := 0; queryIndex < queryCount; queryIndex++ {
		query := queries[queryIndex*dims : (queryIndex+1)*dims]
		queryNorm := vectorNormSquared(query)
		queryInvNorm := 1 / math.Sqrt(queryNorm)
		row := distances[queryIndex*documentCount : (queryIndex+1)*documentCount]
		for docIndex := 0; docIndex < documentCount; docIndex++ {
			document := documents[docIndex*dims : (docIndex+1)*dims]
			row[docIndex] = float64(vectorops.CosineDistanceFloat32Normalized(query, document, queryInvNorm, 1/math.Sqrt(documentNorms[docIndex])))
		}
	}
}
