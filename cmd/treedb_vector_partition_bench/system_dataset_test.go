package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestVectorPartitionSystemExportDatasetV1(t *testing.T) {
	dataset, cache := filepath.Join(t.TempDir(), "dataset"), t.TempDir()
	if err := run([]string{"generate-fixture", "-out", dataset, "-vectors", "12", "-queries", "2", "-dimensions", "5", "-seed", "7"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	var truthOut strings.Builder
	if err := run([]string{"generate-truth-cache", "-dataset", dataset, "-out", cache, "-top-k", "10", "-seed", "7", "-max-vectors", "12", "-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes, 10), "-max-exact-truth-visits", "24"}, &truthOut); err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(truthOut.String())
	if len(fields) < 2 {
		t.Fatalf("truth output=%q", truthOut.String())
	}
	truthSHA := strings.TrimPrefix(fields[1], "artifact_sha256=")
	out := filepath.Join(t.TempDir(), "export")
	if err := run([]string{"system-export-dataset", "-dataset", dataset, "-truth-cache", cache, "-truth-cache-sha256", truthSHA, "-out", out}, io.Discard); err != nil {
		t.Fatal(err)
	}
	var manifest vectorPartitionSystemDatasetManifestV1
	raw, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Docs != 12 || manifest.Queries != 2 || manifest.Dimensions != 5 || manifest.ExactTruthQueries != 2 || manifest.TruthArtifactSHA256 != truthSHA {
		t.Fatalf("manifest=%+v", manifest)
	}
	for _, name := range []string{"documents.f32", "queries.f32", "exact_truth.jsonl"} {
		contents, err := os.ReadFile(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(contents)
		if got := manifest.Files[name]; got.Bytes != int64(len(contents)) || got.SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatalf("%s manifest=%+v", name, got)
		}
	}
	f, err := os.Open(filepath.Join(out, "exact_truth.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	rows := 0
	for scanner.Scan() {
		var row struct {
			QueryID     string   `json:"query_id"`
			DocumentIDs []string `json:"document_ids"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if row.QueryID != fmt.Sprintf("query-%06d", rows) {
			t.Fatalf("query id=%q", row.QueryID)
		}
		if len(row.DocumentIDs) != 10 {
			t.Fatalf("document ids=%d", len(row.DocumentIDs))
		}
		rows++
	}
	if err := scanner.Err(); err != nil || rows != 2 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	if err := run([]string{"system-export-dataset", "-dataset", dataset, "-truth-cache", cache, "-truth-cache-sha256", truthSHA, "-out", out}, io.Discard); err == nil {
		t.Fatal("overwrote existing export")
	}
	manifestPath := filepath.Join(dataset, "fixture_manifest.json")
	var changed map[string]any
	raw, err = os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &changed); err != nil {
		t.Fatal(err)
	}
	changed["seed"] = float64(8)
	raw, err = json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"system-export-dataset", "-dataset", dataset, "-truth-cache", cache, "-truth-cache-sha256", truthSHA, "-out", filepath.Join(t.TempDir(), "changed-export")}, io.Discard); err == nil {
		t.Fatal("exported regenerated data with a stale fixture checksum")
	}
}
