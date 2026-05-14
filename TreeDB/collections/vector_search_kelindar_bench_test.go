//go:build kelindarbench

package collections

import (
	"testing"

	kelindarsearch "github.com/kelindar/search"
)

func BenchmarkKelindarVectorSearchExact(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	index := kelindarsearch.NewIndex[int]()
	for i := 0; i < docs; i++ {
		vector := vectorBenchmarkEmbedding(i, dims)
		index.Add(vector, i)
	}
	query := vectorBenchmarkEmbedding(docs/3, dims)

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results := index.Search(query, vectorBenchmarkTopK)
		if len(results) != vectorBenchmarkTopK {
			b.Fatalf("kelindar result count=%d want %d", len(results), vectorBenchmarkTopK)
		}
	}
}
