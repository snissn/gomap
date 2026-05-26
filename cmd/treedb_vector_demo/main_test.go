package main

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseConfigPresetAndOverrides(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-preset", "perf-engineer",
		"-rows", "123",
		"-dims", "12",
		"-vectors", "typed-column",
		"-metadata", "document",
		"-queries", "4",
		"-top-k", "3",
		"-metadata-filter",
		"-final-fetch",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Preset != "perf-engineer" || cfg.Rows != 123 || cfg.Dims != 12 || cfg.MetadataMode != "document" || cfg.Queries != 4 || cfg.TopK != 3 || !cfg.MetadataFilter || !cfg.FinalFetch {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestParseConfigDoubleDashPresetDefaults(t *testing.T) {
	cfg, err := parseConfig([]string{"--preset", "perf-engineer"})
	if err != nil {
		t.Fatalf("parseConfig --preset: %v", err)
	}
	if cfg.Preset != "perf-engineer" || cfg.Rows != 10000 || cfg.Queries != 100 {
		t.Fatalf("--preset did not apply perf-engineer defaults: %+v", cfg)
	}
	cfg, err = parseConfig([]string{"--preset=vector-rag"})
	if err != nil {
		t.Fatalf("parseConfig --preset=: %v", err)
	}
	if cfg.Preset != "vector-rag" || cfg.Rows != 1000 || cfg.Queries != 10 {
		t.Fatalf("--preset= did not apply vector-rag defaults: %+v", cfg)
	}
}

func TestParseConfigRejectsUnsupportedVectorMode(t *testing.T) {
	_, err := parseConfig([]string{"-vectors", "document"})
	if err == nil || !strings.Contains(err.Error(), "typed-column dense vector sections only") {
		t.Fatalf("err=%v, want clear typed-column-only error", err)
	}
}

func TestDeterministicVectorFixture(t *testing.T) {
	a := generateRows(8, 6, 99)
	b := generateRows(8, 6, 99)
	c := generateRows(8, 6, 100)
	if len(a) != len(b) || len(a[0].Vector) != 6 {
		t.Fatalf("bad fixture shape: %d dims=%d", len(a), len(a[0].Vector))
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Tenant != b[i].Tenant {
			t.Fatalf("row metadata not deterministic: %+v vs %+v", a[i], b[i])
		}
		for d := range a[i].Vector {
			if a[i].Vector[d] != b[i].Vector[d] {
				t.Fatalf("vector[%d][%d] not deterministic: %v vs %v", i, d, a[i].Vector[d], b[i].Vector[d])
			}
		}
	}
	if a[0].Vector[0] == c[0].Vector[0] {
		t.Fatalf("different seeds produced identical first component: %v", a[0].Vector[0])
	}
	queries := generateQueries(a, 3, 7)
	if len(queries) != 3 || len(queries[0].Vector) != 6 || queries[0].Name != "query-0000" {
		t.Fatalf("bad query fixture: %+v", queries)
	}
}

func TestSetSearchTimingAvoidsNonFiniteOps(t *testing.T) {
	var res result
	setSearchTiming(&res, 2, 0, 0)
	if res.OpsPerSecond <= 0 || math.IsInf(res.OpsPerSecond, 0) || math.IsNaN(res.OpsPerSecond) {
		t.Fatalf("ops/sec=%v want finite positive", res.OpsPerSecond)
	}
}

func TestExactTopKShape(t *testing.T) {
	rows := generateRows(32, 8, 1)
	query := append([]float32(nil), rows[0].Vector...)
	hits := exactTopK(rows, query, 5, nil)
	if len(hits) != 5 {
		t.Fatalf("len(hits)=%d want 5", len(hits))
	}
	for i := 1; i < len(hits); i++ {
		if hits[i-1].Score < hits[i].Score {
			t.Fatalf("hits not sorted: %+v", hits)
		}
	}
	filtered := exactTopK(rows, query, 4, func(row vectorRow) bool { return row.Tenant == defaultFilterTenant })
	if len(filtered) != 4 {
		t.Fatalf("filtered len=%d want 4", len(filtered))
	}
	for _, hit := range filtered {
		idx := -1
		_, _ = idx, hit
		for i := range rows {
			if rows[i].ID == hit.ID {
				idx = i
				break
			}
		}
		if idx < 0 || rows[idx].Tenant != defaultFilterTenant {
			t.Fatalf("filtered hit %+v not in tenant %s", hit, defaultFilterTenant)
		}
	}
}

func TestExecuteColumnGraphTopKSmoke(t *testing.T) {
	res, err := execute(context.Background(), config{
		Rows:         64,
		Dims:         8,
		VectorMode:   "typed-column",
		MetadataMode: "typed-row",
		Queries:      2,
		TopK:         3,
		Dir:          filepath.Join(t.TempDir(), "db"),
		Seed:         1,
		Preset:       "vector-rag",
		EfSearch:     32,
		MaxDecoded:   1,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.SearchPath != "column_graph_native_reader" || len(res.FirstResults) != 3 || res.Rows != 64 || res.Dims != 8 || res.Queries != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if res.BatchSize != defaultVectorBatchSize {
		t.Fatalf("batch_size=%d want default %d", res.BatchSize, defaultVectorBatchSize)
	}
	if res.VectorDirectViews == 0 || res.TypedColumnFallbacks != 0 || res.VectorBytesRead == 0 || res.AdjacencyBytesRead == 0 || res.VisitedNodes == 0 {
		t.Fatalf("typed-column vector counters missing/fallback: %+v", res)
	}
}

func TestExecuteMetadataFilterCountsFilteredVectors(t *testing.T) {
	res, err := execute(context.Background(), config{
		Rows:           64,
		Dims:           8,
		VectorMode:     "typed-column",
		MetadataMode:   "typed-row",
		Queries:        2,
		TopK:           3,
		MetadataFilter: true,
		Dir:            filepath.Join(t.TempDir(), "db"),
		Seed:           1,
		Preset:         "vector-rag",
		EfSearch:       32,
		MaxDecoded:     1,
	})
	if err != nil {
		t.Fatalf("execute metadata filter: %v", err)
	}
	// The fixture has eight tenants, so tenant-00 appears rows/8 times.
	wantScored := uint64((64 / 8) * 2)
	if res.ScoredVectors != wantScored {
		t.Fatalf("scored_vectors=%d want %d", res.ScoredVectors, wantScored)
	}
	if res.CandidateRows != wantScored || res.Candidates != wantScored || res.VisitedNodes != wantScored || res.VectorBytesRead == 0 {
		t.Fatalf("filtered counters candidate_rows=%d candidates=%d visited=%d vector_bytes=%d want scored %d", res.CandidateRows, res.Candidates, res.VisitedNodes, res.VectorBytesRead, wantScored)
	}
	if res.CandidatesPerSearch != float64(64/8) {
		t.Fatalf("candidates_per_search=%f want %f", res.CandidatesPerSearch, float64(64/8))
	}
}

func TestExplicitDirMustBeFresh(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	_, _, _, err := prepareFreshDir(dir, false)
	if err == nil {
		t.Fatal("prepareFreshDir accepted non-empty explicit dir")
	}
	if got, err := os.ReadFile(filepath.Join(dir, "keep.txt")); err != nil || string(got) != "keep" {
		t.Fatalf("sentinel changed: got=%q err=%v", got, err)
	}
}

func TestValidateFreshDemoDirRejectsRawParentTraversal(t *testing.T) {
	if _, err := validateFreshDemoDir("safe/../other"); err == nil {
		t.Fatal("validateFreshDemoDir accepted raw parent traversal")
	}
}

func TestValidateFreshDemoDirAllowsShortExplicitPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("absolute /tmp/db shape is Unix-specific")
	}
	got, err := validateFreshDemoDir("/tmp/db")
	if err != nil {
		t.Fatalf("validateFreshDemoDir rejected short explicit path: %v", err)
	}
	if got != "/tmp/db" {
		t.Fatalf("path=%q want /tmp/db", got)
	}
}

func TestRunProfileArtifacts(t *testing.T) {
	profileDir := filepath.Join(t.TempDir(), "profiles")
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"-rows", "32",
		"-dims", "8",
		"-queries", "1",
		"-top-k", "2",
		"-profile-dir", profileDir,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	for _, name := range []string{"vector_demo_cpu.pprof", "vector_demo_allocs.pprof", "vector_demo_summary.json", "vector_demo_summary.md"} {
		path := filepath.Join(profileDir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("missing profile artifact %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile artifact %s is empty", name)
		}
	}
	out := stdout.String()
	if !strings.Contains(out, "profile_artifacts=vector_demo_cpu.pprof,vector_demo_allocs.pprof,vector_demo_summary.json,vector_demo_summary.md") {
		t.Fatalf("stdout missing artifact names:\n%s", out)
	}
}
