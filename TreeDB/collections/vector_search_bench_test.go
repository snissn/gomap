package collections

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	defaultVectorBenchmarkDocs = 10000
	defaultVectorBenchmarkDims = 64
	vectorBenchmarkTopK        = 10
)

var vectorDistanceBenchmarkSink float32

func BenchmarkVectorDistanceToFloat32NodeCosinePrepared(b *testing.B) {
	docs := minInt(vectorBenchmarkDocs(b), 4096)
	dims := vectorBenchmarkDims(b)
	query := vectorBenchmarkEmbedding(docs/3, dims)
	queryNorm := vectorNormSquared(query)
	candidates := make([]vectorIndexNode, docs)
	for i := range candidates {
		candidates[i] = vectorIndexNode{
			documentID: []byte(fmt.Sprintf("doc-%06d", i)),
			vector:     vectorBenchmarkEmbedding(i, dims),
		}
		candidates[i].cacheVectorNorms()
	}
	prepared, err := prepareFloat32CosineQuery(query, queryNorm)
	if err != nil {
		b.Fatalf("prepare query: %v", err)
	}

	b.ReportMetric(float64(docs), "candidates")
	b.ReportMetric(float64(dims), "dims")
	b.Run("wrapper", func(b *testing.B) {
		var sum float32
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range candidates {
				distance, err := vectorDistanceToFloat32NodeCosine(query, queryNorm, &candidates[j])
				if err != nil {
					b.Fatalf("distance: %v", err)
				}
				sum += distance
			}
		}
		vectorDistanceBenchmarkSink = sum
	})
	b.Run("prepared_unchecked", func(b *testing.B) {
		var sum float32
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range candidates {
				sum += vectorDistanceToFloat32NodeCosineUnchecked(prepared, &candidates[j])
			}
		}
		vectorDistanceBenchmarkSink = sum
	})
}

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

func removeVectorBenchmarkDirAfterClose(b *testing.B, dir string) {
	b.Helper()
	if err := os.RemoveAll(dir); err != nil {
		if runtime.GOOS != "windows" {
			b.Fatalf("remove db dir: %v", err)
		}
		b.Logf("best-effort remove db dir %q: %v", dir, err)
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

func openEmptyVectorBenchmarkCollection(tb testing.TB) (*backenddb.DB, *Collection) {
	tb.Helper()
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
	return d, col
}

func openVectorBenchmarkCollectionWithIndexes(tb testing.TB, docs, dims int, indexes ...IndexDefinition) (*backenddb.DB, *Collection) {
	tb.Helper()
	var d *backenddb.DB
	var col *Collection
	if len(indexes) == 0 {
		d, col = openEmptyVectorBenchmarkCollection(tb)
	} else {
		var err error
		d, err = backenddb.Open(backenddb.Options{Dir: tb.TempDir()})
		if err != nil {
			tb.Fatalf("open db: %v", err)
		}
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Indexes: indexes}); err != nil {
			_ = d.Close()
			tb.Fatalf("create collection: %v", err)
		}
		col, err = mgr.OpenCollection("docs")
		if err != nil {
			_ = d.Close()
			tb.Fatalf("open collection: %v", err)
		}
	}
	ids, documents := vectorBenchmarkWriteBatch(docs, dims)
	vectorBenchmarkInsertBatches(tb, col, ids, documents, 512)
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("flush benchmark collection: %v", err)
	}
	return d, col
}

func openVectorBenchmarkCollectionWithVectorIndex(tb testing.TB, dir string, docs, dims int, def VectorIndexDefinition) (*backenddb.DB, *Collection) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		_ = d.Close()
		tb.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("open collection: %v", err)
	}
	ids, documents := vectorBenchmarkWriteBatch(docs, dims)
	vectorBenchmarkInsertBatches(tb, col, ids, documents, 512)
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("flush benchmark collection: %v", err)
	}
	return d, col
}

func vectorBenchmarkWriteBatch(docs, dims int) ([][]byte, [][]byte) {
	ids := make([][]byte, docs)
	documents := make([][]byte, docs)
	for i := 0; i < docs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		documents[i] = vectorBenchmarkDocument(i, dims)
	}
	return ids, documents
}

func runParallelVectorSearchBenchmark(b *testing.B, search func() error) {
	b.Helper()
	var failureMu sync.Mutex
	var firstFailure error
	recordFailure := func(err error) {
		failureMu.Lock()
		defer failureMu.Unlock()
		if firstFailure == nil {
			firstFailure = err
		}
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := search(); err != nil {
				recordFailure(err)
				return
			}
		}
	})
	if firstFailure != nil {
		b.Fatal(firstFailure)
	}
}

func vectorBenchmarkInsertBatches(tb testing.TB, col *Collection, ids, documents [][]byte, batchSize int) {
	tb.Helper()
	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		if _, err := col.InsertBatch(ids[start:end], documents[start:end]); err != nil {
			tb.Fatalf("insert benchmark batch %d-%d: %v", start, end, err)
		}
	}
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
