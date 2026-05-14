package collections

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	defaultVectorBenchmarkDocs = 10000
	defaultVectorBenchmarkDims = 64
	vectorBenchmarkTopK        = 10
)

func BenchmarkCollectionVectorSearchExact(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	query := vectorBenchmarkEmbedding(docs/3, dims)

	b.ReportMetric(float64(docs), "docs/search")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := col.SearchVectorsExact(query, VectorSearchOptions{
			Field:  "embedding",
			Metric: VectorMetricCosine,
			TopK:   vectorBenchmarkTopK,
		})
		if err != nil {
			b.Fatalf("exact vector search: %v", err)
		}
		if len(results) != vectorBenchmarkTopK {
			b.Fatalf("exact result count=%d want %d", len(results), vectorBenchmarkTopK)
		}
	}
}

func BenchmarkCollectionVectorIndexBuild(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index, err := col.BuildVectorIndex(VectorIndexOptions{
			Name:   "embedding_build",
			Field:  "embedding",
			Metric: VectorMetricCosine,
			M:      16,
		})
		if err != nil {
			b.Fatalf("build vector index: %v", err)
		}
		stats := index.Stats()
		if stats.LiveDocs != docs {
			b.Fatalf("built index live docs=%d want %d", stats.LiveDocs, docs)
		}
		b.ReportMetric(float64(stats.BytesMemory), "index_bytes")
	}
}

func BenchmarkCollectionVectorIndexBuildInt8(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index, err := col.BuildVectorIndex(VectorIndexOptions{
			Name:     "embedding_build_i8",
			Field:    "embedding",
			Metric:   VectorMetricCosine,
			M:        16,
			Encoding: VectorIndexEncodingInt8,
		})
		if err != nil {
			b.Fatalf("build int8 vector index: %v", err)
		}
		stats := index.Stats()
		if stats.LiveDocs != docs {
			b.Fatalf("built int8 index live docs=%d want %d", stats.LiveDocs, docs)
		}
		b.ReportMetric(float64(stats.BytesMemory), "index_bytes")
	}
}

func BenchmarkCollectionVectorIndexSearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      16,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	queries := [][]float32{
		vectorBenchmarkEmbedding(17, dims),
		vectorBenchmarkEmbedding(docs/3, dims),
		vectorBenchmarkEmbedding(docs/2, dims),
		vectorBenchmarkEmbedding(docs-11, dims),
	}
	recall, err := index.CheckRecall(queries, VectorIndexSearchOptions{
		TopK:            vectorBenchmarkTopK,
		EfSearch:        128,
		FetchMultiplier: 16,
	})
	if err != nil {
		b.Fatalf("check recall: %v", err)
	}
	query := queries[1]

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(recall.Recall*100, "recall_at_10_pct")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, _, err := index.Search(query, VectorIndexSearchOptions{
			TopK:                 vectorBenchmarkTopK,
			EfSearch:             128,
			FetchMultiplier:      16,
			DisableExactFallback: true,
		})
		if err != nil {
			b.Fatalf("indexed vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("indexed vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexGraphOnlySearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding_graph_only",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      16,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm graph-only search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm graph-only search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			b.Fatalf("graph-only vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("graph-only vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexSearchInt8(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:     "embedding_i8",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		M:        16,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		b.Fatalf("build int8 vector index: %v", err)
	}
	queries := [][]float32{
		vectorBenchmarkEmbedding(17, dims),
		vectorBenchmarkEmbedding(docs/3, dims),
		vectorBenchmarkEmbedding(docs/2, dims),
		vectorBenchmarkEmbedding(docs-11, dims),
	}
	recall, err := index.CheckRecall(queries, VectorIndexSearchOptions{
		TopK:            vectorBenchmarkTopK,
		EfSearch:        128,
		FetchMultiplier: 16,
	})
	if err != nil {
		b.Fatalf("check int8 recall: %v", err)
	}
	query := queries[1]

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(recall.Recall*100, "recall_at_10_pct")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, _, err := index.Search(query, VectorIndexSearchOptions{
			TopK:                 vectorBenchmarkTopK,
			EfSearch:             128,
			FetchMultiplier:      16,
			DisableExactFallback: true,
		})
		if err != nil {
			b.Fatalf("int8 indexed vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("int8 indexed vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexGraphOnlySearchInt8(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollection(b, docs, dims)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:     "embedding_graph_only_i8",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		M:        16,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		b.Fatalf("build int8 vector index: %v", err)
	}
	query := vectorBenchmarkEmbedding(docs/3, dims)
	warm, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
	if err != nil {
		b.Fatalf("warm int8 graph-only search: %v", err)
	}
	if len(warm) == 0 {
		b.Fatal("warm int8 graph-only search returned no results")
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := index.searchGraphOnly(query, vectorBenchmarkTopK, 128)
		if err != nil {
			b.Fatalf("int8 graph-only vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("int8 graph-only vector search returned no results")
		}
	}
}

func BenchmarkCollectionVectorIndexFilteredSearch(b *testing.B) {
	docs := vectorBenchmarkDocs(b)
	dims := vectorBenchmarkDims(b)
	d, col := openVectorBenchmarkCollectionWithIndexes(b, docs, dims, IndexDefinition{Name: "group_idx", Field: "group", ValueType: IndexValueInt64})
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      16,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	queryDoc := docs / 3
	query := vectorBenchmarkEmbedding(queryDoc, dims)
	group := int64(queryDoc % 16)
	filter := &VectorIndexRangeFilter{
		IndexName: "group_idx",
		Range: IndexRangeOptions{
			Lower: IndexRangeBound{Value: group, Inclusive: true},
			Upper: IndexRangeBound{Value: group, Inclusive: true},
		},
	}

	b.ReportMetric(float64(docs), "docs/index")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(group), "filter_group")
	b.ReportMetric(float64(index.Stats().BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, trace, err := index.Search(query, VectorIndexSearchOptions{
			TopK:               vectorBenchmarkTopK,
			EfSearch:           128,
			FetchMultiplier:    16,
			ExactFilterMaxDocs: 32,
			IndexRangeFilter:   filter,
		})
		if err != nil {
			b.Fatalf("filtered indexed vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("filtered indexed vector search returned no results")
		}
		if trace.CandidatesAfterFilter > 0 {
			b.ReportMetric(float64(trace.CandidatesAfterFilter), "candidates_after_filter")
		}
	}
}

func BenchmarkCollectionVectorTinyBERTFixture(b *testing.B) {
	path := os.Getenv("TREEDB_VECTOR_BENCH_JSONL")
	if path == "" {
		b.Skip("set TREEDB_VECTOR_BENCH_JSONL to a tiny_bert demo JSONL export")
	}
	fixture := loadVectorBenchmarkFixture(b, path)
	if len(fixture) == 0 {
		b.Fatal("tiny BERT fixture has no records")
	}
	d, col := openVectorFixtureCollection(b, fixture)
	defer func() { _ = d.Close() }()
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:   "embedding",
		Field:  "embedding",
		Metric: VectorMetricCosine,
		M:      8,
	})
	if err != nil {
		b.Fatalf("build vector index: %v", err)
	}
	topK := minInt(vectorBenchmarkTopK, len(fixture))
	query := fixture[0].Embedding
	recall, err := index.CheckRecall([][]float32{query}, VectorIndexSearchOptions{
		TopK:            topK,
		EfSearch:        64,
		FetchMultiplier: 8,
	})
	if err != nil {
		b.Fatalf("check tiny BERT recall: %v", err)
	}

	stats := index.Stats()
	b.ReportMetric(float64(len(fixture)), "docs/index")
	b.ReportMetric(float64(len(query)), "dims")
	b.ReportMetric(recall.Recall*100, "recall_pct")
	b.ReportMetric(float64(stats.BytesMemory), "index_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, _, err := index.Search(query, VectorIndexSearchOptions{
			TopK:                 topK,
			EfSearch:             64,
			FetchMultiplier:      8,
			DisableExactFallback: true,
		})
		if err != nil {
			b.Fatalf("tiny BERT vector search: %v", err)
		}
		if len(results) == 0 {
			b.Fatal("tiny BERT vector search returned no results")
		}
	}
}

func TestLoadVectorBenchmarkFixtureJSONL(t *testing.T) {
	path := t.TempDir() + "/fixture.jsonl"
	if err := os.WriteFile(path, []byte(
		`{"id":"D01","text":"alpha","model":"demo","pooling":"mean","normalized":true,"embedding":[1,0]}`+"\n"+
			`{"id":"D02","text":"beta","model":"demo","pooling":"mean","normalized":true,"embedding":[0,1]}`+"\n",
	), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	records := loadVectorBenchmarkFixture(t, path)
	if len(records) != 2 || records[0].ID != "D01" || len(records[1].Embedding) != 2 {
		t.Fatalf("unexpected fixture records: %+v", records)
	}
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

func openVectorBenchmarkCollection(tb testing.TB, docs, dims int) (*backenddb.DB, *Collection) {
	tb.Helper()
	return openVectorBenchmarkCollectionWithIndexes(tb, docs, dims)
}

func openVectorBenchmarkCollectionWithIndexes(tb testing.TB, docs, dims int, indexes ...IndexDefinition) (*backenddb.DB, *Collection) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Indexes: indexes}); err != nil {
		_ = d.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	const batchSize = 512
	for start := 0; start < docs; start += batchSize {
		end := start + batchSize
		if end > docs {
			end = docs
		}
		ids := make([][]byte, 0, end-start)
		documents := make([][]byte, 0, end-start)
		for i := start; i < end; i++ {
			ids = append(ids, []byte(fmt.Sprintf("doc-%06d", i)))
			documents = append(documents, vectorBenchmarkDocument(i, dims))
		}
		if _, err := col.InsertBatch(ids, documents); err != nil {
			_ = d.Close()
			tb.Fatalf("insert benchmark batch %d-%d: %v", start, end, err)
		}
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("flush benchmark collection: %v", err)
	}
	return d, col
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
		if err := validateFloat32Vector(record.Embedding); err != nil {
			tb.Fatalf("vector benchmark fixture line %d invalid embedding: %v", line, err)
		}
		out = append(out, record)
	}
	if err := scanner.Err(); err != nil {
		tb.Fatalf("scan vector benchmark fixture: %v", err)
	}
	return out
}

func openVectorFixtureCollection(tb testing.TB, fixture []vectorBenchmarkFixtureRecord) (*backenddb.DB, *Collection) {
	tb.Helper()
	if len(fixture) == 0 {
		tb.Fatal("empty vector benchmark fixture")
	}
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		_ = d.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	ids := make([][]byte, 0, len(fixture))
	documents := make([][]byte, 0, len(fixture))
	dims := len(fixture[0].Embedding)
	for _, record := range fixture {
		if len(record.Embedding) != dims {
			_ = d.Close()
			tb.Fatalf("fixture record %q dimension=%d want %d", record.ID, len(record.Embedding), dims)
		}
		document, err := json.Marshal(map[string]any{
			"text":       record.Text,
			"model":      record.Model,
			"pooling":    record.Pooling,
			"normalized": record.Normalized,
			"embedding":  record.Embedding,
		})
		if err != nil {
			_ = d.Close()
			tb.Fatalf("encode fixture document %q: %v", record.ID, err)
		}
		ids = append(ids, []byte(record.ID))
		documents = append(documents, document)
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		_ = d.Close()
		tb.Fatalf("insert vector fixture: %v", err)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("flush vector fixture: %v", err)
	}
	return d, col
}

func vectorBenchmarkDocument(id, dims int) []byte {
	vector := vectorBenchmarkEmbedding(id, dims)
	out := make([]byte, 0, 32+dims*10)
	out = append(out, fmt.Sprintf(`{"group":%d,"embedding":[`, id%16)...)
	for i, value := range vector {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, fmt.Sprintf("%.7g", value)...)
	}
	out = append(out, ']', '}')
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
