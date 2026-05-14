//go:build usearch_bench && cgo

package usearchbench

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"testing"

	usearch "github.com/unum-cloud/usearch/golang"
)

const (
	// Keep these defaults aligned with ../vector_search_bench_test.go so USearch
	// and native benchmark comparisons use the same synthetic corpus.
	defaultVectorBenchmarkDocs = 10000
	defaultVectorBenchmarkDims = 64
	vectorBenchmarkTopK        = 10
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

func vectorBenchmarkDocs(tb testing.TB) int {
	return vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_DOCS", defaultVectorBenchmarkDocs)
}

func vectorBenchmarkDims(tb testing.TB) int {
	return vectorBenchmarkPositiveEnvInt(tb, "TREEDB_VECTOR_BENCH_DIMS", defaultVectorBenchmarkDims)
}

func vectorBenchmarkPositiveEnvInt(tb testing.TB, name string, fallback int) int {
	tb.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		tb.Fatalf("%s=%q must be a positive integer", name, raw)
	}
	return value
}

type vectorBenchmarkFixtureRecord struct {
	ID         string    `json:"id"`
	Text       string    `json:"text"`
	Model      string    `json:"model"`
	Pooling    string    `json:"pooling"`
	Normalized bool      `json:"normalized"`
	Embedding  []float32 `json:"embedding"`
}

func loadVectorBenchmarkFixture(tb testing.TB, path string) []vectorBenchmarkFixtureRecord {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open vector benchmark fixture: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			tb.Fatalf("close vector benchmark fixture: %v", err)
		}
	}()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var out []vectorBenchmarkFixtureRecord
	line := 0
	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		var record vectorBenchmarkFixtureRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			tb.Fatalf("decode vector benchmark fixture line %d: %v", line, err)
		}
		if record.ID == "" {
			tb.Fatalf("vector benchmark fixture line %d missing id", line)
		}
		if len(record.Embedding) == 0 {
			tb.Fatalf("vector benchmark fixture line %d missing embedding", line)
		}
		validateFloat32Vector(tb, record.Embedding, fmt.Sprintf("vector benchmark fixture line %d", line))
		out = append(out, record)
	}
	if err := scanner.Err(); err != nil {
		tb.Fatalf("scan vector benchmark fixture: %v", err)
	}
	return out
}

func vectorBenchmarkEmbedding(id, dims int) []float32 {
	out := make([]float32, dims)
	var norm float64
	x := float64(id + 1)
	for i := range out {
		d := float64(i + 1)
		value := math.Sin(x*d*0.013) + math.Cos((x+17)*d*0.007) + math.Sin(float64((id%31)+1)*d*0.019)
		out[i] = float32(value)
		norm += value * value
	}
	scale := 1 / math.Sqrt(norm)
	for i := range out {
		out[i] = float32(float64(out[i]) * scale)
	}
	return out
}

func validateFloat32Vector(tb testing.TB, vector []float32, label string) {
	tb.Helper()
	for i, value := range vector {
		f := float64(value)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			tb.Fatalf("%s invalid embedding element %d", label, i)
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
