package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOraclePartsUsesTruthMembershipNotCentroids(t *testing.T) {
	// A centroid route could prefer partition 0 for a query near its mean. The
	// oracle must instead choose the partition containing the exact truth IDs.
	assignment := []int{1, 1, 0, 0}
	got := oracleParts([]int{0, 1, 2}, assignment, 2, 1)
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("oracle=%v want truth partition 1", got)
	}
}
func TestCorpusPathAndChecksumGuards(t *testing.T) {
	for _, p := range []string{"../x", "/tmp/x", "a/b", "a\\b", ""} {
		if safeCorpusName(p) {
			t.Fatalf("unsafe path accepted %q", p)
		}
	}
	if !safeCorpusName("documents.f32") {
		t.Fatal("safe name rejected")
	}
	if _, err := readF32("missing", fileInfo{Bytes: 4, SHA256: strings.Repeat("A", 64)}, 1, 1); err == nil {
		t.Fatal("uppercase checksum accepted")
	}
}

func TestLoadFailsClosedOnMalformedManifestAndCorpusMetadata(t *testing.T) {
	base := manifest{Version: 1, Generator: "treedb_vector_synthetic_v1", Docs: 1, Dimensions: 1, Queries: 1, TopK: 10, GroupModulo: 1, Metric: "cosine", Normalized: true, DocumentIDPattern: "doc-%06d", DocumentVectorsFile: "documents.f32", QueryVectorsFile: "queries.f32", FloatFormat: "float32_le_row_major", Files: map[string]fileInfo{"documents.f32": {Bytes: 4, SHA256: strings.Repeat("0", 64)}, "queries.f32": {Bytes: 4, SHA256: strings.Repeat("0", 64)}}}
	write := func(t *testing.T, m manifest, suffix string) string {
		t.Helper()
		dir := t.TempDir()
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, suffix...), 0600); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	if _, _, _, err := load(write(t, base, " {}")); err == nil {
		t.Fatal("trailing manifest JSON accepted")
	}
	badPath := base
	badPath.DocumentVectorsFile = "../documents.f32"
	if _, _, _, err := load(write(t, badPath, "")); err == nil {
		t.Fatal("path traversal accepted")
	}
	notNormalized := base
	notNormalized.Normalized = false
	if _, _, _, err := load(write(t, notNormalized, "")); err == nil {
		t.Fatal("unnormalized corpus accepted")
	}
	missing := base
	missing.Files = map[string]fileInfo{}
	if _, _, _, err := load(write(t, missing, "")); err == nil {
		t.Fatal("missing corpus metadata accepted")
	}
	duplicate := base
	duplicate.QueryVectorsFile = duplicate.DocumentVectorsFile
	if _, _, _, err := load(write(t, duplicate, "")); err == nil {
		t.Fatal("shared document/query corpus accepted")
	}
	badQueries := base
	badQueries.Queries = 0
	if _, _, _, err := load(write(t, badQueries, "")); err == nil {
		t.Fatal("zero query count accepted")
	}
	truncated := write(t, base, "")
	if err := os.WriteFile(filepath.Join(truncated, "documents.f32"), []byte{0, 0}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := load(truncated); err == nil {
		t.Fatal("truncated corpus accepted")
	}
}

func TestCorpusNormalizationAndOverflowGuards(t *testing.T) {
	if err := validateNormalized([]float32{2}, 1, 1); err == nil {
		t.Fatal("non-unit vector accepted")
	}
	if err := validateNormalized([]float32{float32(math.NaN())}, 1, 1); err == nil {
		t.Fatal("non-finite vector accepted")
	}
	if err := validateNormalized([]float32{1}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, ok := checkedByteCount(math.MaxInt, 2); ok {
		t.Fatal("overflowing corpus byte count accepted")
	}
	if err := validateQualityWork(1_000_000, maxQueries, 4096, 64); err == nil {
		t.Fatal("unbounded quality work accepted")
	}
}

func TestLoadRejectsOversizedManifestBeforeDecode(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(strings.Repeat("x", maxManifest+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := load(dir); err == nil {
		t.Fatal("oversized manifest accepted")
	}
}

func TestReportMarksUnavailableAccountingWithoutMeasuredZero(t *testing.T) {
	raw, err := json.Marshal(report{BuildCPUAvailable: false, TotalCommandCPUAvailable: false, PeakRSSAvailable: false})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"build_cpu_nanos", "total_command_cpu_nanos", "peak_rss_bytes"} {
		if _, ok := fields[name]; ok {
			t.Fatalf("unavailable accounting emitted fabricated %s: %s", name, raw)
		}
	}
	for _, name := range []string{"build_cpu_available", "total_command_cpu_available", "peak_rss_available"} {
		if got, ok := fields[name].(bool); !ok || got {
			t.Fatalf("unavailable accounting not explicit for %s: %s", name, raw)
		}
	}
}
