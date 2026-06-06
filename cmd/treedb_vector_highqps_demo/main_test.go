package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := parseConfig([]string{"-docs", "128", "-dims", "16", "-queries", "7", "-warmup-queries", "3", "-top-k", "4"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Docs != 128 || cfg.Dims != 16 || cfg.Queries != 7 || cfg.WarmupQueries != 3 || cfg.TopK != 4 {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
	if cfg.M != defaultM || cfg.EfConstruction != defaultEfConstruction || cfg.EfSearch != defaultEfSearch {
		t.Fatalf("defaults not preserved: %+v", cfg)
	}
	_, err = parseConfig([]string{"-docs", "8", "-top-k", "9"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "exceeds -docs") {
		t.Fatalf("top-k validation err=%v, want exceeds -docs", err)
	}
	_, err = parseConfig([]string{"-warmup-queries", "0"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "setup stays outside the timed loop") {
		t.Fatalf("warmup validation err=%v, want setup boundary guidance", err)
	}
}

func TestExecuteSmokeExactBufferedFastPath(t *testing.T) {
	res, err := execute(context.Background(), config{
		Dir:            filepath.Join(t.TempDir(), "db"),
		Docs:           96,
		Dims:           8,
		Queries:        5,
		WarmupQueries:  2,
		TopK:           3,
		BatchSize:      24,
		M:              8,
		EfConstruction: 64,
		EfSearch:       32,
		Seed:           1,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.API != apiName || !res.CheckpointReopen || !res.BufferReused {
		t.Fatalf("unexpected API/reopen/buffer result: %+v", res)
	}
	if res.RouteEvidence.SearchRouteHNSWSearchPack != 1 || res.RouteEvidence.HNSWSearchPackActive != 1 {
		t.Fatalf("route evidence did not prove hnsw_search_pack: %+v", res.RouteEvidence)
	}
	if res.RouteEvidence.DocumentsFetched != 0 || res.RouteEvidence.GraphRowFallbacks != 0 || res.RouteEvidence.TypedColumnVectorFallbacks != 0 || res.RouteEvidence.VectorScratchDecodes != 0 {
		t.Fatalf("guardrails not clean: %+v", res.RouteEvidence)
	}
	if len(res.FirstResults) != 3 || res.ResultsPerSearch != 3 || res.OpsPerSecond <= 0 || res.AvgMicros <= 0 {
		t.Fatalf("unexpected search results/timing: %+v", res)
	}
}

func TestRunTextOutputSmoke(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"-dir", filepath.Join(t.TempDir(), "db"),
		"-docs", "64",
		"-dims", "8",
		"-queries", "3",
		"-warmup-queries", "2",
		"-top-k", "3",
		"-batch-size", "16",
		"-m", "8",
		"-ef-construction", "64",
		"-ef-search", "32",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run text: %v\nstderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"TreeDB high-QPS exact vector search demo",
		"api: Collection.SearchVectorIndexWithBuffer",
		"checkpoint_reopen: true",
		"buffer_reuse: one caller-owned VectorIndexSearchBuffer reused",
		"timed_loop: queries=3 stats_mode=production",
		"top_k:",
		"search_route_hnsw_search_pack/search=1",
		"hnsw_search_pack_active/search=1",
		"docs_fetched/search=0",
		"graph_row_fallbacks/search=0",
		"typed_column_vector_fallbacks/search=0",
		"vector_scratch_decodes/search=0",
		"Collection.SearchVectorIndex: no-document convenience call with response-owned allocation",
		"IncludeDocuments=true: separate document materialization path",
		"OpenVectorIndexSearcher + SearchWithBuffer: reusable low-level serving",
		benchmarkGuide,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("text output missing %q:\n%s", want, out)
		}
	}
}

func TestRunJSONOutputSmoke(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"-dir", filepath.Join(t.TempDir(), "db"),
		"-docs", "64",
		"-dims", "8",
		"-queries", "2",
		"-warmup-queries", "1",
		"-top-k", "2",
		"-batch-size", "16",
		"-m", "8",
		"-ef-construction", "64",
		"-ef-search", "32",
		"-json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run json: %v\nstderr=%s", err, stderr.String())
	}
	var res result
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("decode json: %v\n%s", err, stdout.String())
	}
	if res.API != apiName || res.RouteEvidence.SearchRouteHNSWSearchPack != 1 || res.RouteEvidence.DocumentsFetched != 0 {
		t.Fatalf("unexpected json result: %+v", res)
	}
	if len(res.FirstResults) != 2 || !res.InstructionalNonBench {
		t.Fatalf("json did not include expected first results/non-benchmark flag: %+v", res)
	}
}
