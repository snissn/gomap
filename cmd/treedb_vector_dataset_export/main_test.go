package main

import (
	"encoding/json"
	"os"
	"path/filepath"
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
	if parsed.ExactTruthFile != "exact_truth.jsonl" || parsed.ExactTruthKind != "exhaustive_cosine_distance_then_id_top_k_v1" {
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
	if len(row.Neighbors) != 3 || row.Kind != "exhaustive_cosine_distance_then_id_top_k_v1" {
		t.Fatalf("truth=%+v", row)
	}
}

func TestParseConfigRejectsCorpusCapsBeforeAllocation(t *testing.T) {
	if _, err := parseConfig([]string{"-out", t.TempDir(), "-docs", "1000001"}); err == nil {
		t.Fatal("accepted vector cap")
	}
	if _, err := checkedVectorBytes(maxDatasetVectors, maxDatasetDims); err == nil {
		t.Fatal("accepted byte cap")
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
