package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

const (
	defaultDocs         = 10000
	defaultDimensions   = 64
	defaultQueries      = 10000
	defaultTopK         = 10
	maxDatasetVectors   = 1_000_000
	maxDatasetDims      = 4_096
	maxDatasetBytes     = int64(4 << 30)
	maxTruthComparisons = int64(200_000_000)
)

type config struct {
	out        string
	docs       int
	dimensions int
	queries    int
	topK       int
	jsonOut    bool
}

type manifest struct {
	Version             int                     `json:"version"`
	Generator           string                  `json:"generator"`
	CreatedAt           string                  `json:"created_at"`
	Docs                int                     `json:"docs"`
	Dimensions          int                     `json:"dimensions"`
	Queries             int                     `json:"queries"`
	TopK                int                     `json:"top_k"`
	Metric              string                  `json:"metric"`
	Normalized          bool                    `json:"normalized"`
	DocumentIDPattern   string                  `json:"document_id_pattern"`
	GroupModulo         int                     `json:"group_modulo"`
	DocumentVectorsFile string                  `json:"document_vectors_file"`
	QueryVectorsFile    string                  `json:"query_vectors_file"`
	DocumentsJSONLFile  string                  `json:"documents_jsonl_file"`
	QueriesJSONLFile    string                  `json:"queries_jsonl_file"`
	FloatFormat         string                  `json:"float_format"`
	ExactTruthFile      string                  `json:"exact_truth_file"`
	ExactTruthKind      string                  `json:"exact_truth_kind"`
	Files               map[string]fileManifest `json:"files"`
}

type fileManifest struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type exportResult struct {
	Dir      string   `json:"dir"`
	Manifest manifest `json:"manifest"`
}

type documentJSONL struct {
	Index     int       `json:"index"`
	ID        string    `json:"id"`
	Group     int       `json:"group"`
	Embedding []float32 `json:"embedding"`
}

type queryJSONL struct {
	Index         int       `json:"index"`
	ID            string    `json:"id"`
	DocumentIndex int       `json:"document_index"`
	Embedding     []float32 `json:"embedding"`
}

type truthNeighbor struct {
	DocumentID string  `json:"document_id"`
	Distance   float64 `json:"distance"`
}
type exactTruthJSONL struct {
	QueryID   string          `json:"query_id"`
	Neighbors []truthNeighbor `json:"neighbors"`
	Kind      string          `json:"kind"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "treedb-vector-dataset-export: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	result, err := exportDataset(cfg)
	if err != nil {
		return err
	}
	if cfg.jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	fmt.Fprintf(stdout, "TreeDB vector dataset exported\n")
	fmt.Fprintf(stdout, "dir=%s docs=%d dims=%d queries=%d top_k=%d\n", result.Dir, cfg.docs, cfg.dimensions, cfg.queries, cfg.topK)
	for name, file := range result.Manifest.Files {
		fmt.Fprintf(stdout, "file=%s bytes=%d sha256=%s\n", name, file.Bytes, file.SHA256)
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		docs:       defaultDocs,
		dimensions: defaultDimensions,
		queries:    defaultQueries,
		topK:       defaultTopK,
	}
	fs := flag.NewFlagSet("treedb_vector_dataset_export", flag.ContinueOnError)
	fs.StringVar(&cfg.out, "out", "", "Output dataset directory")
	fs.IntVar(&cfg.docs, "docs", cfg.docs, "Number of synthetic documents to export")
	fs.IntVar(&cfg.dimensions, "dims", cfg.dimensions, "Vector dimensions per document")
	fs.IntVar(&cfg.queries, "queries", cfg.queries, "Number of query vectors to export")
	fs.IntVar(&cfg.topK, "top-k", cfg.topK, "Nearest-neighbor result count intended for consumers")
	fs.BoolVar(&cfg.jsonOut, "json", false, "Emit JSON summary")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.out == "" {
		return config{}, errors.New("-out is required")
	}
	if cfg.docs <= 0 {
		return config{}, errors.New("-docs must be positive")
	}
	if cfg.dimensions <= 0 {
		return config{}, errors.New("-dims must be positive")
	}
	if cfg.queries <= 0 {
		return config{}, errors.New("-queries must be positive")
	}
	if cfg.topK <= 0 {
		return config{}, errors.New("-top-k must be positive")
	}
	if cfg.topK > cfg.docs {
		return config{}, errors.New("-top-k cannot exceed -docs")
	}
	if cfg.docs > maxDatasetVectors || cfg.queries > maxDatasetVectors || cfg.dimensions > maxDatasetDims {
		return config{}, fmt.Errorf("docs, queries, and dims exceed capped local-corpus limits")
	}
	if _, err := checkedVectorBytes(cfg.docs, cfg.dimensions); err != nil {
		return config{}, err
	}
	if _, err := checkedVectorBytes(cfg.queries, cfg.dimensions); err != nil {
		return config{}, err
	}
	if _, err := checkedCombinedVectorBytes(cfg.docs, cfg.queries, cfg.dimensions); err != nil {
		return config{}, err
	}
	if int64(cfg.docs) > maxTruthComparisons/int64(cfg.queries) {
		return config{}, errors.New("exact truth comparison cap exceeded before allocation; reduce -queries or export in bounded shards")
	}
	return cfg, nil
}

func exportDataset(cfg config) (exportResult, error) {
	out := filepath.Clean(cfg.out)
	if err := prepareOutputDir(out); err != nil {
		return exportResult{}, err
	}
	files := make(map[string]fileManifest)
	if err := writeVectorFile(filepath.Join(out, "documents.f32"), cfg.docs, cfg.dimensions, func(i int) []float32 {
		return embedding(i, cfg.dimensions)
	}, files, "documents.f32"); err != nil {
		return exportResult{}, err
	}
	queryStride := queryDocStride(cfg.docs)
	if err := writeVectorFile(filepath.Join(out, "queries.f32"), cfg.queries, cfg.dimensions, func(i int) []float32 {
		return embedding(queryDocIndex(i, cfg.docs, queryStride), cfg.dimensions)
	}, files, "queries.f32"); err != nil {
		return exportResult{}, err
	}
	if err := writeDocumentsJSONL(filepath.Join(out, "documents.jsonl"), cfg, files); err != nil {
		return exportResult{}, err
	}
	if err := writeQueriesJSONL(filepath.Join(out, "queries.jsonl"), cfg, files); err != nil {
		return exportResult{}, err
	}
	if err := writeExactTruthJSONL(filepath.Join(out, "exact_truth.jsonl"), cfg, files); err != nil {
		return exportResult{}, err
	}
	createdAt, err := manifestCreatedAt()
	if err != nil {
		return exportResult{}, err
	}
	m := manifest{
		Version:             1,
		Generator:           "treedb_vector_synthetic_v1",
		CreatedAt:           createdAt,
		Docs:                cfg.docs,
		Dimensions:          cfg.dimensions,
		Queries:             cfg.queries,
		TopK:                cfg.topK,
		Metric:              "cosine",
		Normalized:          true,
		DocumentIDPattern:   "doc-%06d",
		GroupModulo:         16,
		DocumentVectorsFile: "documents.f32",
		QueryVectorsFile:    "queries.f32",
		DocumentsJSONLFile:  "documents.jsonl",
		QueriesJSONLFile:    "queries.jsonl",
		FloatFormat:         "float32_le_row_major",
		ExactTruthFile:      "exact_truth.jsonl",
		ExactTruthKind:      "exhaustive_cosine_distance_ascending_then_id_top_k_v1",
		Files:               files,
	}
	if err := writeManifest(filepath.Join(out, "manifest.json"), m); err != nil {
		return exportResult{}, err
	}
	return exportResult{Dir: out, Manifest: m}, nil
}

func checkedVectorBytes(count, dims int) (int64, error) {
	if count < 1 || dims < 1 || int64(count) > maxDatasetBytes/4/int64(dims) {
		return 0, errors.New("vector byte product exceeds cap before allocation")
	}
	return int64(count) * int64(dims) * 4, nil
}

func checkedCombinedVectorBytes(docs, queries, dims int) (int64, error) {
	if docs < 1 || queries < 1 || dims < 1 {
		return 0, errors.New("combined vector byte product requires positive counts and dimensions")
	}
	combined := int64(docs) + int64(queries)
	if combined > maxDatasetBytes/4/int64(dims) {
		return 0, errors.New("combined document/query vector byte product exceeds cap before allocation")
	}
	return combined * int64(dims) * 4, nil
}

func manifestCreatedAt() (string, error) {
	return "1970-01-01T00:00:00Z", nil
}

func prepareOutputDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(path, 0o755)
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("output path %q exists and is not a directory", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("output directory %q already exists and is not empty", path)
	}
	return nil
}

func writeVectorFile(path string, count, dims int, vector func(int) []float32, files map[string]fileManifest, name string) error {
	if _, err := checkedVectorBytes(count, dims); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	h := sha256.New()
	w := io.MultiWriter(f, h)
	row := make([]byte, dims*4)
	for i := 0; i < count; i++ {
		v := vector(i)
		if len(v) != dims {
			_ = f.Close()
			return fmt.Errorf("vector %d dimensions=%d want %d", i, len(v), dims)
		}
		for j, value := range v {
			binary.LittleEndian.PutUint32(row[j*4:], math.Float32bits(value))
		}
		if _, err := w.Write(row); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return recordFile(path, h, files, name)
}

func writeExactTruthJSONL(path string, cfg config, files map[string]fileManifest) error {
	return writeJSONL(path, files, "exact_truth.jsonl", func(enc *json.Encoder) error {
		stride := queryDocStride(cfg.docs)
		for i := 0; i < cfg.queries; i++ {
			query := embedding(queryDocIndex(i, cfg.docs, stride), cfg.dimensions)
			neighbors := exactTruthForQuery(query, cfg.docs, cfg.dimensions, cfg.topK)
			if err := enc.Encode(exactTruthJSONL{QueryID: fmt.Sprintf("query-%06d", i), Neighbors: neighbors, Kind: "exhaustive_cosine_distance_ascending_then_id_top_k_v1"}); err != nil {
				return err
			}
		}
		return nil
	})
}

func exactTruthForQuery(query []float32, docs, dims, topK int) []truthNeighbor {
	neighbors := make([]truthNeighbor, docs)
	for document := 0; document < docs; document++ {
		var dot float64
		for dimension, value := range embedding(document, dims) {
			dot += float64(value) * float64(query[dimension])
		}
		neighbors[document] = truthNeighbor{
			DocumentID: documentID(document),
			Distance:   1 - dot,
		}
	}
	sort.Slice(neighbors, func(i, j int) bool {
		return truthNeighborLess(neighbors[i], neighbors[j])
	})
	return neighbors[:topK]
}

func truthNeighborLess(a, b truthNeighbor) bool {
	if a.Distance == b.Distance {
		return a.DocumentID < b.DocumentID
	}
	return a.Distance < b.Distance
}

func writeDocumentsJSONL(path string, cfg config, files map[string]fileManifest) error {
	return writeJSONL(path, files, "documents.jsonl", func(enc *json.Encoder) error {
		for i := 0; i < cfg.docs; i++ {
			if err := enc.Encode(documentJSONL{
				Index:     i,
				ID:        documentID(i),
				Group:     i % 16,
				Embedding: embedding(i, cfg.dimensions),
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeQueriesJSONL(path string, cfg config, files map[string]fileManifest) error {
	return writeJSONL(path, files, "queries.jsonl", func(enc *json.Encoder) error {
		stride := queryDocStride(cfg.docs)
		for i := 0; i < cfg.queries; i++ {
			docIndex := queryDocIndex(i, cfg.docs, stride)
			if err := enc.Encode(queryJSONL{
				Index:         i,
				ID:            fmt.Sprintf("query-%06d", i),
				DocumentIndex: docIndex,
				Embedding:     embedding(docIndex, cfg.dimensions),
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeJSONL(path string, files map[string]fileManifest, name string, write func(*json.Encoder) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	h := sha256.New()
	bw := bufio.NewWriter(io.MultiWriter(f, h))
	enc := json.NewEncoder(bw)
	if err := write(enc); err != nil {
		_ = f.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return recordFile(path, h, files, name)
}

func writeManifest(path string, m manifest) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := encodeManifest(f, m); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func recordFile(path string, h hash.Hash, files map[string]fileManifest, name string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	files[name] = fileManifest{Bytes: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}
	return nil
}

func encodeManifest(w io.Writer, m manifest) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(m)
}

func documentID(id int) string {
	return fmt.Sprintf("doc-%06d", id)
}

func queryDocStride(docs int) int {
	stride := 7919
	if docs > 0 {
		stride %= docs
	}
	if stride <= 0 {
		stride = 1
	}
	for gcd(stride, docs) != 1 {
		stride++
	}
	return stride
}

func queryDocIndex(i, docs, stride int) int {
	return (i*stride + docs/3 + 17) % docs
}

func gcd(a, b int) int {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func embedding(id, dims int) []float32 {
	out := make([]float32, dims)
	var norm float64
	x := float64(id + 1)
	for i := range out {
		d := float64(i + 1)
		value := math.Sin(x*d*0.013) + math.Cos((x+17)*d*0.007) + math.Sin(float64((id%31)+1)*d*0.019)
		out[i] = float32(value)
		norm += value * value
	}
	if norm == 0 || math.IsNaN(norm) || math.IsInf(norm, 0) {
		out[0] = 1
		return out
	}
	scale := 1 / math.Sqrt(norm)
	for i := range out {
		out[i] = float32(float64(out[i]) * scale)
	}
	return out
}
