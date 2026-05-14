//go:build usearch_bench && cgo

package collections

import (
	"os"
	"runtime"
	"testing"

	usearch "github.com/unum-cloud/usearch/golang"
)

func BenchmarkCollectionVectorUSearchBuild(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	vectors := make([][]float32, docs)
	for i := 0; i < docs; i++ {
		vectors[i] = vectorBenchmarkEmbedding(i, dims)
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := newVectorUSearchIndex(b, dims, docs, 16, 128, 128)
		for j, vector := range vectors {
			if err := index.Add(usearch.Key(j), vector); err != nil {
				_ = index.Destroy()
				b.Fatalf("add usearch vector %d: %v", j, err)
			}
		}
		memory, _ := index.MemoryUsage()
		b.ReportMetric(float64(memory), "index_bytes")
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch index: %v", err)
		}
	}
}

func BenchmarkCollectionVectorUSearchBaseline(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	index := openVectorUSearchBenchmarkIndex(b, docs, dims, 16, 128, 128)
	defer func() {
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch index: %v", err)
		}
	}()
	query := vectorBenchmarkEmbedding(docs/3, dims)

	keys, _, err := index.Search(query, uint(vectorBenchmarkTopK))
	if err != nil {
		b.Fatalf("warm usearch search: %v", err)
	}
	if len(keys) == 0 {
		b.Fatal("warm usearch search returned no results")
	}
	memory, _ := index.MemoryUsage()
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(memory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys, _, err := index.Search(query, uint(vectorBenchmarkTopK))
		if err != nil {
			b.Fatalf("usearch search: %v", err)
		}
		if len(keys) == 0 {
			b.Fatal("usearch search returned no results")
		}
	}
}

func BenchmarkCollectionVectorUSearchFilteredBaseline(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	index := openVectorUSearchBenchmarkIndex(b, docs, dims, 16, 128, 128)
	defer func() {
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch index: %v", err)
		}
	}()
	queryDoc := docs / 3
	query := vectorBenchmarkEmbedding(queryDoc, dims)
	group := queryDoc % 16
	handler := &usearch.FilteredSearchHandler{
		Callback: func(key usearch.Key, _ *usearch.FilteredSearchHandler) int {
			if int(key)%16 == group {
				return 1
			}
			return 0
		},
	}

	keys, _, err := index.FilteredSearch(query, uint(vectorBenchmarkTopK), handler)
	if err != nil {
		b.Fatalf("warm filtered usearch search: %v", err)
	}
	if len(keys) == 0 {
		b.Fatal("warm filtered usearch search returned no results")
	}
	memory, _ := index.MemoryUsage()
	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(group), "filter_group")
	b.ReportMetric(float64(memory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys, _, err := index.FilteredSearch(query, uint(vectorBenchmarkTopK), handler)
		if err != nil {
			b.Fatalf("filtered usearch search: %v", err)
		}
		if len(keys) == 0 {
			b.Fatal("filtered usearch search returned no results")
		}
	}
}

func BenchmarkCollectionVectorTinyBERTUSearchBaseline(b *testing.B) {
	path := os.Getenv("TREEDB_VECTOR_BENCH_JSONL")
	if path == "" {
		b.Skip("set TREEDB_VECTOR_BENCH_JSONL to a tiny_bert demo JSONL export")
	}
	fixture := loadVectorBenchmarkFixture(b, path)
	if len(fixture) == 0 {
		b.Fatal("tiny BERT fixture has no records")
	}
	index := openVectorUSearchFixtureIndex(b, fixture, 8, 128, 64)
	defer func() {
		if err := index.Destroy(); err != nil {
			b.Fatalf("destroy usearch fixture index: %v", err)
		}
	}()
	query := fixture[0].Embedding
	topK := minInt(vectorBenchmarkTopK, len(fixture))

	keys, _, err := index.Search(query, uint(topK))
	if err != nil {
		b.Fatalf("warm tiny BERT usearch search: %v", err)
	}
	if len(keys) == 0 {
		b.Fatal("warm tiny BERT usearch search returned no results")
	}
	memory, _ := index.MemoryUsage()
	b.ReportMetric(float64(len(fixture)), "docs/index")
	b.ReportMetric(float64(len(query)), "dims")
	b.ReportMetric(float64(memory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys, _, err := index.Search(query, uint(topK))
		if err != nil {
			b.Fatalf("tiny BERT usearch search: %v", err)
		}
		if len(keys) == 0 {
			b.Fatal("tiny BERT usearch search returned no results")
		}
	}
}

func openVectorUSearchBenchmarkIndex(tb testing.TB, docs, dims, m, efConstruction, efSearch int) *usearch.Index {
	tb.Helper()
	index := newVectorUSearchIndex(tb, dims, docs, m, efConstruction, efSearch)
	for i := 0; i < docs; i++ {
		if err := index.Add(usearch.Key(i), vectorBenchmarkEmbedding(i, dims)); err != nil {
			tb.Fatalf("add usearch vector %d: %v", i, err)
		}
	}
	return index
}

func openVectorUSearchFixtureIndex(tb testing.TB, fixture []vectorBenchmarkFixtureRecord, m, efConstruction, efSearch int) *usearch.Index {
	tb.Helper()
	dims := len(fixture[0].Embedding)
	index := newVectorUSearchIndex(tb, dims, len(fixture), m, efConstruction, efSearch)
	for i, record := range fixture {
		if len(record.Embedding) != dims {
			tb.Fatalf("fixture record %q dimension=%d want %d", record.ID, len(record.Embedding), dims)
		}
		if err := index.Add(usearch.Key(i), record.Embedding); err != nil {
			tb.Fatalf("add usearch fixture vector %q: %v", record.ID, err)
		}
	}
	return index
}

func newVectorUSearchIndex(tb testing.TB, dims, docs, m, efConstruction, efSearch int) *usearch.Index {
	tb.Helper()
	conf := usearch.DefaultConfig(uint(dims))
	conf.Quantization = usearch.F32
	conf.Metric = usearch.Cosine
	conf.Connectivity = uint(m)
	conf.ExpansionAdd = uint(efConstruction)
	conf.ExpansionSearch = uint(efSearch)
	index, err := usearch.NewIndex(conf)
	if err != nil {
		tb.Fatalf("new usearch index: %v", err)
	}
	if err := index.Reserve(uint(docs)); err != nil {
		_ = index.Destroy()
		tb.Fatalf("reserve usearch index: %v", err)
	}
	_ = index.ChangeThreadsAdd(uint(runtime.NumCPU()))
	_ = index.ChangeThreadsSearch(1)
	return index
}
