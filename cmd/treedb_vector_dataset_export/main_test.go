package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestExportDatasetSmoke(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dataset")
	res, err := exportDataset(config{
		out:        dir,
		docs:       8,
		dimensions: 4,
		queries:    3,
		topK:       2,
	})
	if err != nil {
		t.Fatalf("exportDataset: %v", err)
	}
	if res.Manifest.Docs != 8 || res.Manifest.Dimensions != 4 || res.Manifest.Queries != 3 {
		t.Fatalf("unexpected manifest: %+v", res.Manifest)
	}
	for _, name := range []string{"manifest.json", "documents.f32", "queries.f32", "documents.jsonl", "queries.jsonl", "exact_truth.jsonl"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s is empty", name)
		}
		if name != "manifest.json" && res.Manifest.Files[name].Bytes == 0 {
			t.Fatalf("manifest missing %s file stats: %+v", name, res.Manifest.Files)
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var parsed manifest
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if parsed.Generator != "treedb_vector_synthetic_v1" || parsed.Metric != "cosine" || !parsed.Normalized {
		t.Fatalf("unexpected parsed manifest: %+v", parsed)
	}
	if len(parsed.Files) != len(res.Manifest.Files) {
		t.Fatalf("on-disk manifest file count differs from result: parsed=%d result=%d", len(parsed.Files), len(res.Manifest.Files))
	}
	for name, parsedFile := range parsed.Files {
		if parsedFile != res.Manifest.Files[name] {
			t.Fatalf("on-disk manifest file stats differ for %s: parsed=%+v result=%+v", name, parsedFile, res.Manifest.Files[name])
		}
	}
	if _, ok := parsed.Files["manifest.json"]; ok {
		t.Fatalf("manifest should not include self-referential manifest.json stats: %+v", parsed.Files["manifest.json"])
	}
	if parsed.ExactTruthFile != "exact_truth.jsonl" || parsed.ExactTruthKind != "exhaustive_cosine_distance_ascending_then_id_top_k_v1" {
		t.Fatalf("missing declared exact truth: %+v", parsed)
	}
}

func TestExportExactTruthIncludesTopK(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "truth")
	if _, err := exportDataset(config{out: dir, docs: 8, dimensions: 4, queries: 2, topK: 3}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "exact_truth.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var row exactTruthJSONL
	if err := json.Unmarshal([]byte(strings.Split(strings.TrimSpace(string(raw)), "\n")[0]), &row); err != nil {
		t.Fatal(err)
	}
	if len(row.Neighbors) != 3 || row.Kind != "exhaustive_cosine_distance_ascending_then_id_top_k_v1" {
		t.Fatalf("truth=%+v", row)
	}
}

func TestExportExactTruthMatchesIndependentSmallCorpusRecomputation(t *testing.T) {
	const (
		docs    = 9
		queries = 3
		dims    = 5
		topK    = 4
	)
	dir := filepath.Join(t.TempDir(), "truth")
	if _, err := exportDataset(config{out: dir, docs: docs, dimensions: dims, queries: queries, topK: topK}); err != nil {
		t.Fatal(err)
	}
	documentVectors := readFloat32RowsForTest(t, filepath.Join(dir, "documents.f32"), docs, dims)
	queryVectors := readFloat32RowsForTest(t, filepath.Join(dir, "queries.f32"), queries, dims)
	raw, err := os.ReadFile(filepath.Join(dir, "exact_truth.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != queries {
		t.Fatalf("truth rows=%d want %d", len(lines), queries)
	}
	for queryIndex := 0; queryIndex < queries; queryIndex++ {
		var got exactTruthJSONL
		if err := json.Unmarshal([]byte(lines[queryIndex]), &got); err != nil {
			t.Fatal(err)
		}
		want := make([]truthNeighbor, docs)
		for documentIndex, document := range documentVectors {
			var dot float64
			for d, value := range document {
				dot += float64(value) * float64(queryVectors[queryIndex][d])
			}
			want[documentIndex] = truthNeighbor{
				DocumentID: fmt.Sprintf("doc-%06d", documentIndex),
				Distance:   1 - dot,
			}
		}
		sort.Slice(want, func(i, j int) bool {
			if want[i].Distance == want[j].Distance {
				return want[i].DocumentID < want[j].DocumentID
			}
			return want[i].Distance < want[j].Distance
		})
		want = want[:topK]
		if len(got.Neighbors) != len(want) {
			t.Fatalf("query %d neighbors=%d want %d", queryIndex, len(got.Neighbors), len(want))
		}
		for rank := range want {
			if got.Neighbors[rank].DocumentID != want[rank].DocumentID || math.Float64bits(got.Neighbors[rank].Distance) != math.Float64bits(want[rank].Distance) {
				t.Fatalf("query %d rank %d got=%+v want=%+v", queryIndex, rank, got.Neighbors[rank], want[rank])
			}
		}
	}
}

func readFloat32RowsForTest(t *testing.T, path string, count, dims int) [][]float32 {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != count*dims*4 {
		t.Fatalf("%s bytes=%d want %d", path, len(raw), count*dims*4)
	}
	rows := make([][]float32, count)
	for i := range rows {
		rows[i] = make([]float32, dims)
		for d := range rows[i] {
			offset := (i*dims + d) * 4
			rows[i][d] = math.Float32frombits(binary.LittleEndian.Uint32(raw[offset:]))
		}
	}
	return rows
}

func TestTruthNeighborLessUsesExplicitDistanceThenIDTieOrder(t *testing.T) {
	a := truthNeighbor{DocumentID: "doc-000001", Distance: 0.25}
	b := truthNeighbor{DocumentID: "doc-000002", Distance: 0.25}
	if !truthNeighborLess(a, b) || truthNeighborLess(b, a) {
		t.Fatalf("tie helper does not order stable IDs: a=%+v b=%+v", a, b)
	}
	if !truthNeighborLess(truthNeighbor{DocumentID: "doc-z", Distance: 0.1}, a) {
		t.Fatal("tie helper does not prioritize smaller cosine distance")
	}
}

func TestExportDatasetIsByteStableAndTruthUsesAscendingDistance(t *testing.T) {
	a, b := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")
	cfg := config{docs: 8, dimensions: 4, queries: 2, topK: 3}
	cfg.out = a
	if _, err := exportDataset(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.out = b
	if _, err := exportDataset(cfg); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"manifest.json", "documents.f32", "queries.f32", "documents.jsonl", "queries.jsonl", "exact_truth.jsonl"} {
		x, _ := os.ReadFile(filepath.Join(a, name))
		y, _ := os.ReadFile(filepath.Join(b, name))
		if string(x) != string(y) {
			t.Fatalf("%s is not stable", name)
		}
	}
	raw, _ := os.ReadFile(filepath.Join(a, "exact_truth.jsonl"))
	var row exactTruthJSONL
	_ = json.Unmarshal([]byte(strings.Split(strings.TrimSpace(string(raw)), "\n")[0]), &row)
	for i := 1; i < len(row.Neighbors); i++ {
		if row.Neighbors[i-1].Distance > row.Neighbors[i].Distance {
			t.Fatal("truth distances are not ascending")
		}
	}
}

func TestParseConfigRejectsCorpusCapsBeforeAllocation(t *testing.T) {
	if _, err := parseConfig([]string{"-out", t.TempDir(), "-docs", "1000001"}); err == nil {
		t.Fatal("accepted vector cap")
	}
	if _, err := checkedVectorBytes(maxDatasetVectors, maxDatasetDims); err == nil {
		t.Fatal("accepted byte cap")
	}
	if _, err := checkedCombinedVectorBytes(maxDatasetVectors, maxDatasetVectors, maxDatasetDims); err == nil {
		t.Fatal("accepted combined document/query byte cap")
	}
}

func TestParseConfigRejectsMissingOut(t *testing.T) {
	if _, err := parseConfig([]string{"-docs", "4"}); err == nil {
		t.Fatal("parseConfig accepted missing -out")
	}
}

func TestManifestCreatedAtUsesSourceDateEpoch(t *testing.T) {
	t.Setenv("SOURCE_DATE_EPOCH", "0")
	got, err := manifestCreatedAt()
	if err != nil {
		t.Fatalf("manifestCreatedAt: %v", err)
	}
	if got != "1970-01-01T00:00:00Z" {
		t.Fatalf("created_at=%q", got)
	}
}

func TestExportDatasetRejectsNonEmptyOut(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dataset")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("do not delete"), 0o644); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	_, err := exportDataset(config{
		out:        dir,
		docs:       8,
		dimensions: 4,
		queries:    3,
		topK:       2,
	})
	if err == nil {
		t.Fatal("exportDataset accepted non-empty output directory")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "keep")); statErr != nil {
		t.Fatalf("sentinel was removed: %v", statErr)
	}
}
