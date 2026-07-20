package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
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

func TestOracleQualityGateAllowsFullProbeParityOnly(t *testing.T) {
	for _, tc := range []struct {
		name               string
		oracle, stableHash float64
		probes, partitions int
		want               bool
	}{
		{name: "one partition parity", oracle: 1, stableHash: 1, probes: 1, partitions: 1, want: true},
		{name: "full probe parity", oracle: 1, stableHash: 1, probes: 4, partitions: 4, want: true},
		{name: "partial probe equality rejected", oracle: .8, stableHash: .8, probes: 1, partitions: 4, want: false},
		{name: "partial probe improvement accepted", oracle: .9, stableHash: .8, probes: 1, partitions: 4, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := passesOracleQualityGate(tc.oracle, tc.stableHash, tc.probes, tc.partitions); got != tc.want {
				t.Fatalf("passesOracleQualityGate(%v, %v, %d, %d)=%v want %v", tc.oracle, tc.stableHash, tc.probes, tc.partitions, got, tc.want)
			}
		})
	}
}

func TestSingleDocumentBuildWritesFiniteReport(t *testing.T) {
	dataset, out := t.TempDir(), t.TempDir()
	unit := []byte{0, 0, 128, 63}
	digest := sha256.Sum256(unit)
	for _, name := range []string{"documents.f32", "queries.f32"} {
		if err := os.WriteFile(filepath.Join(dataset, name), unit, 0600); err != nil {
			t.Fatal(err)
		}
	}
	m := manifest{Version: 1, Generator: "treedb_vector_synthetic_v1", Docs: 1, Dimensions: 1, Queries: 1, TopK: 1, GroupModulo: 1, Metric: "cosine", Normalized: true, DocumentIDPattern: "doc-%06d", DocumentVectorsFile: "documents.f32", QueryVectorsFile: "queries.f32", FloatFormat: "float32_le_row_major", Files: map[string]fileInfo{"documents.f32": {Bytes: 4, SHA256: fmt.Sprintf("%x", digest)}, "queries.f32": {Bytes: 4, SHA256: fmt.Sprintf("%x", digest)}}}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataset, "manifest.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-dataset", dataset, "-out", out, "-partitions", "1", "-probes", "1", "-repetitions", "1", "-pivots", "2", "-max-leaf-bucket", "2", "-degree", "1"}); err != nil {
		t.Fatal(err)
	}
	reports, err := filepath.Glob(filepath.Join(out, "vector_partition_report_*.json"))
	if err != nil || len(reports) != 1 {
		t.Fatalf("reports=%v err=%v", reports, err)
	}
	reportRaw, err := os.ReadFile(reports[0])
	if err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(reportRaw, &got); err != nil {
		t.Fatal(err)
	}
	if got.GraphNeighborRecall != 0 || math.IsNaN(got.GraphNeighborRecall) || math.IsInf(got.GraphNeighborRecall, 0) {
		t.Fatalf("single-document graph recall=%v", got.GraphNeighborRecall)
	}
	if _, err := os.Stat(got.ArtifactPath); err != nil {
		t.Fatalf("artifact was not retained with report: %v", err)
	}
}

func TestRunPreflightsManifestShapeAndConfigBeforeCorpusIO(t *testing.T) {
	writeManifestOnly := func(t *testing.T, docs, dimensions int) string {
		t.Helper()
		dataset := t.TempDir()
		m := manifest{Version: 1, Generator: "treedb_vector_synthetic_v1", Docs: docs, Dimensions: dimensions, Queries: 1, TopK: 1, GroupModulo: 1, Metric: "cosine", Normalized: true, DocumentIDPattern: "doc-%06d", DocumentVectorsFile: "missing-documents.f32", QueryVectorsFile: "missing-queries.f32", FloatFormat: "float32_le_row_major", Files: map[string]fileInfo{"missing-documents.f32": {Bytes: 4, SHA256: strings.Repeat("0", 64)}, "missing-queries.f32": {Bytes: 4, SHA256: strings.Repeat("0", 64)}}}
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dataset, "manifest.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		return dataset
	}
	for _, tc := range []struct {
		name string
		docs int
		dims int
		args []string
		want string
	}{
		{name: "over-cap shape", docs: 1_000_000, dims: 4096, want: "configured graph scalar-work bound exceeded before allocation"},
		{name: "invalid requested config", docs: 1, dims: 1, args: []string{"-partitions", "1", "-probes", "1", "-repetitions", "33"}, want: "invalid vector partition configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataset := writeManifestOnly(t, tc.docs, tc.dims)
			args := []string{"-dataset", dataset, "-out", t.TempDir()}
			args = append(args, tc.args...)
			err := run(args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run error=%v want %q before missing corpus I/O", err, tc.want)
			}
		})
	}
}
