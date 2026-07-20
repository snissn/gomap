package main

import (
	"encoding/json"
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
	base := manifest{Version: 1, Generator: "treedb_vector_synthetic_v1", Docs: 1, Dimensions: 1, Queries: 0, Metric: "cosine", Normalized: true, DocumentIDPattern: "doc-%06d", DocumentVectorsFile: "documents.f32", QueryVectorsFile: "queries.f32", FloatFormat: "float32_le_row_major", Files: map[string]fileInfo{"documents.f32": {Bytes: 4, SHA256: strings.Repeat("0", 64)}, "queries.f32": {Bytes: 4, SHA256: strings.Repeat("0", 64)}}}
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
	truncated := write(t, base, "")
	if err := os.WriteFile(filepath.Join(truncated, "documents.f32"), []byte{0, 0}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := load(truncated); err == nil {
		t.Fatal("truncated corpus accepted")
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
