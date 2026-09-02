package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseGoBenchAggregatesMetricsAndDerivesOps(t *testing.T) {
	raw := `goos: darwin
BenchmarkIndexInsertSearch2564/search_text_v2_candidates_no_docs-8          3   4000000 ns/op   1200 B/op   12 allocs/op   10000 docs_fixture   64 candidate_budget/source   10 topk/search   0 docs_fetched/search   0 full_doc_fallbacks/search   0 fail_closed/search   0 text_state_lookups/search   0 text_match_details/search   812 posting_blocks_visited/search
BenchmarkIndexInsertSearch2564/search_text_v2_candidates_no_docs-8          3   5000000 ns/op   1400 B/op   14 allocs/op   10000 docs_fixture   64 candidate_budget/source   10 topk/search   0 docs_fetched/search   0 full_doc_fallbacks/search   0 fail_closed/search   0 text_state_lookups/search   0 text_match_details/search   820 posting_blocks_visited/search
`
	samples, err := parseGoBench(bytes.NewBufferString(raw))
	if err != nil {
		t.Fatalf("parseGoBench: %v", err)
	}
	if got, want := len(samples), 2; got != want {
		t.Fatalf("samples=%d want %d", got, want)
	}
	aggs := aggregateGoBenchSamples(samples)
	if got, want := len(aggs), 1; got != want {
		t.Fatalf("aggregates=%d want %d", got, want)
	}
	agg := aggs[0]
	if agg.MeanNsPerOp != 4500000 || *agg.BytesPerOp != 1300 || *agg.AllocsPerOp != 13 {
		t.Fatalf("aggregate=%+v want averaged ns/bytes/allocs", agg)
	}
	row := classifyGoBenchmark(namedPath{Name: "treedb_10k", Path: "bench.txt"}, agg)
	if row.Modality != "text_only" || row.Engine != "treedb_text_v2" {
		t.Fatalf("row modality/engine=%s/%s", row.Modality, row.Engine)
	}
	if row.OpsPerSec == nil || *row.OpsPerSec < 222 || *row.OpsPerSec > 223 {
		t.Fatalf("ops/sec=%v want about 222", row.OpsPerSec)
	}
	if !strings.Contains(row.Dataset, "docs=10000") || !strings.Contains(row.Dataset, "candidate_budget=64") {
		t.Fatalf("dataset=%q", row.Dataset)
	}
}

func TestCounterValidationFailsClosedForZeroDocRows(t *testing.T) {
	row := scoreboardRow{
		SourceLabel: "treedb_bad",
		System:      "TreeDB",
		Modality:    "text_only",
		Boundary:    "No-document text-v2 candidate generation",
		Benchmark:   "BenchmarkIndexInsertSearch2564/search_text_v2_candidates_no_docs",
		Metrics: map[string]float64{
			"docs_fetched/search":       1,
			"full_doc_fallbacks/search": 0,
			"fail_closed/search":        0,
			"text_state_lookups/search": 0,
			"text_match_details/search": 0,
		},
	}
	checks := validateCounterRows([]scoreboardRow{row})
	if got, want := len(checks), 1; got != want {
		t.Fatalf("checks=%d want %d", got, want)
	}
	if checks[0].OK || !strings.Contains(strings.Join(checks[0].Failures, " "), "docs_fetched/search=1 want 0") {
		t.Fatalf("check=%+v want docs_fetched failure", checks[0])
	}
}

func TestRowsFromGoBenchRejectsEmptyArtifacts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty_bench.txt")
	if err := os.WriteFile(path, []byte("PASS\nok\tgithub.com/snissn/gomap/TreeDB/collections\t0.01s\n"), 0o644); err != nil {
		t.Fatalf("write empty bench: %v", err)
	}
	_, err := rowsFromGoBench(namedPath{Name: "empty", Path: path})
	if err == nil || !strings.Contains(err.Error(), "no benchmark rows found") {
		t.Fatalf("rowsFromGoBench err=%v want no benchmark rows found", err)
	}
}

func TestBuildReportUsesCapturedGoVersionFromContext(t *testing.T) {
	dir := t.TempDir()
	contextPath := filepath.Join(dir, "context.txt")
	wantGo := "go version go9.99.0 custom/arch"
	if err := os.WriteFile(contextPath, []byte("timestamp=unit-test\ngo="+wantGo+"\npython=Python 3.x\n"), 0o644); err != nil {
		t.Fatalf("write context: %v", err)
	}
	rep, err := buildReport(config{
		outDir:      dir,
		repoRoot:    dir,
		contextPath: contextPath,
	})
	if err != nil {
		t.Fatalf("buildReport: %v", err)
	}
	if got := rep.Context.GoVersion; got != wantGo {
		t.Fatalf("go version=%q want captured context version %q", got, wantGo)
	}
}

func TestBuildReportAppendsCustomCaveatsToDefaults(t *testing.T) {
	rep, err := buildReport(config{
		outDir:      t.TempDir(),
		unavailable: namedValues{{Name: "Lucene", Value: "not run in unit test"}},
		caveats:     multiValues{"custom 100k smoke caveat"},
	})
	if err != nil {
		t.Fatalf("buildReport: %v", err)
	}
	md := renderMarkdown(rep)
	for _, want := range []string{
		"TreeDB rows are same-host local benchmark rows",
		"No-document candidate rows must keep docs_fetched/search=0",
		"custom 100k smoke caveat",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("caveats missing %q: %#v\n%s", want, rep.Caveats, md)
		}
	}
}

func TestHybridCloseoutOverrideLabelDrivesDatasetDocs(t *testing.T) {
	dir := t.TempDir()
	goBench := filepath.Join(dir, "hybrid.txt")
	if err := os.WriteFile(goBench, []byte(`BenchmarkSearchHybridCloseout2506/mode_hybrid_no_docs/topK_10/candidates_64/filter_none_100pct-8 1 1000 ns/op 10 B/op 1 allocs/op 0 docs_fetched/search 0 full_doc_fallbacks/search 0 fail_closed/search 0 text_state_lookups/search 0 text_match_details/search
`), 0o644); err != nil {
		t.Fatalf("write go bench: %v", err)
	}
	rep, err := buildReport(config{
		outDir:    dir,
		goBenches: namedPaths{{Name: "treedb_hybrid_closeout_docs_64", Path: goBench}},
	})
	if err != nil {
		t.Fatalf("buildReport: %v", err)
	}
	if got, want := len(rep.Rows), 1; got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	row := rep.Rows[0]
	if !strings.Contains(row.Dataset, "docs=64") || strings.Contains(row.Dataset, "docs=10000") {
		t.Fatalf("dataset=%q want docs=64 from source label override", row.Dataset)
	}
	md := renderMarkdown(rep)
	if !strings.Contains(md, "docs=64") || strings.Contains(md, "docs=10000") {
		t.Fatalf("markdown dataset did not use override docs:\n%s", md)
	}
}

func TestBuildReportParsesExternalAndRendersUnavailable(t *testing.T) {
	dir := t.TempDir()
	goBench := filepath.Join(dir, "go.txt")
	if err := os.WriteFile(goBench, []byte(`BenchmarkIndexInsertSearch2564/search_hybrid_v2_no_docs_scalar_filter-8 1 1200000 ns/op 2048 B/op 20 allocs/op 10000 docs_fixture 16 vector_dims 64 candidate_budget/source 10 topk/search 0 docs_fetched/search 0 full_doc_fallbacks/search 0 fail_closed/search 0 text_state_lookups/search 0 text_match_details/search 625 scalar_prefilter_ids/search 64 text_candidates/search
`), 0o644); err != nil {
		t.Fatalf("write go bench: %v", err)
	}
	external := filepath.Join(dir, "sqlite_fts5.json")
	if err := os.WriteFile(external, []byte(`{
  "schema_version":"treedb_text_hybrid_external/v1",
  "status":"ok",
  "system":"SQLite FTS5",
  "engine":"sqlite_fts5",
  "modality":"text_only",
  "dataset":{"docs":10000,"queries":1000,"top_k":10},
  "query_shape":"MATCH refund policy bm25 top-k",
  "boundary":"no-document rowid+bm25 only; no full document fetch",
  "benchmark":"sqlite_fts5/search",
  "search":{"avg_nanos":250000,"ops_per_second":4000},
  "build":{"seconds":0.75},
  "storage":{"total_bytes":1048576,"bytes_per_doc":104.8},
  "metrics":{"docs_fetched/search":0,"full_doc_fallbacks/search":0,"fail_closed/search":0}
}`), 0o644); err != nil {
		t.Fatalf("write external: %v", err)
	}
	rep, err := buildReport(config{
		outDir:      dir,
		goBenches:   namedPaths{{Name: "treedb_10k", Path: goBench}},
		externals:   namedPaths{{Name: "sqlite_fts5_10k", Path: external}},
		unavailable: namedValues{{Name: "Lucene", Value: "not run in unit test"}},
	})
	if err != nil {
		t.Fatalf("buildReport: %v", err)
	}
	if got, want := len(rep.Rows), 2; got != want {
		t.Fatalf("rows=%d want %d", got, want)
	}
	var sawSQLiteTopK bool
	for _, row := range rep.Rows {
		if row.System == "SQLite FTS5" && row.TopK == 10 {
			sawSQLiteTopK = true
		}
	}
	if !sawSQLiteTopK {
		t.Fatalf("SQLite FTS5 topK not propagated from dataset: %#v", rep.Rows)
	}
	if got, want := len(rep.CounterValidations), 2; got != want {
		t.Fatalf("counter validations=%d want %d", got, want)
	}
	md := renderMarkdown(rep)
	for _, want := range []string{"SQLite FTS5", "text_scalar", "Lucene", "not run in unit test", "scoreboard"} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q:\n%s", want, md)
		}
	}
}

func TestRunRejectsMissingInputsEvenWhenCounterFailuresAllowed(t *testing.T) {
	dir := t.TempDir()
	err := run(config{
		outDir:               filepath.Join(dir, "out"),
		allowCounterFailures: true,
		goBenches: namedPaths{{
			Name: "missing",
			Path: filepath.Join(dir, "missing.txt"),
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "read go bench") {
		t.Fatalf("run err=%v want missing input error", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "out", "scoreboard.json")); !os.IsNotExist(statErr) {
		t.Fatalf("scoreboard.json exists for missing input: %v", statErr)
	}
}

func TestRunWritesReportBeforeReturningCounterFailure(t *testing.T) {
	dir := t.TempDir()
	goBench := filepath.Join(dir, "bad.txt")
	if err := os.WriteFile(goBench, []byte(`BenchmarkIndexInsertSearch2564/search_text_v2_candidates_no_docs-8 1 1000 ns/op 1 B/op 1 allocs/op 1 docs_fetched/search 0 full_doc_fallbacks/search 0 fail_closed/search 0 text_state_lookups/search 0 text_match_details/search
`), 0o644); err != nil {
		t.Fatalf("write bad bench: %v", err)
	}
	out := filepath.Join(dir, "out")
	err := run(config{outDir: out, goBenches: namedPaths{{Name: "bad", Path: goBench}}})
	if err == nil {
		t.Fatal("run err=nil want counter validation failure")
	}
	if _, statErr := os.Stat(filepath.Join(out, "scoreboard.json")); statErr != nil {
		t.Fatalf("scoreboard.json not written before failure: %v", statErr)
	}
}

func TestPhase2SynthesisGolden(t *testing.T) {
	treedbNS := 1_100_000.0
	sqliteNS := 1_000_000.0
	treedbStorage := 144.0
	sqliteStorage := 128.0
	syn := buildPhase2Synthesis([]scoreboardRow{
		{
			SourceLabel:        "treedb_common_10k",
			System:             "TreeDB",
			Engine:             "treedb_text_v2",
			Modality:           "text_only",
			QueryShape:         "common term BM25F top-k with block-max pruning",
			Boundary:           "No-document text-v2 score-only BM25F search",
			Benchmark:          "BenchmarkTextV2BlockMaxCommonTerm2628/blockmax_common_topk",
			NsPerOp:            &treedbNS,
			StorageBytesPerDoc: &treedbStorage,
		},
		{
			SourceLabel:        "sqlite_common_10k",
			System:             "SQLite FTS5",
			Engine:             "sqlite_fts5",
			Modality:           "text_only",
			QueryShape:         "common term FTS5 MATCH top-k",
			Boundary:           "no-document rowid+bm25 retrieval only",
			Benchmark:          "sqlite_fts5/common_term_no_docs",
			NsPerOp:            &sqliteNS,
			StorageBytesPerDoc: &sqliteStorage,
		},
	}, []unavailableRow{
		{System: "Bleve", Reason: "not run in unit test"},
		{System: "Tantivy", Reason: "not run in unit test"},
		{System: "Lucene", Reason: "not run in unit test"},
	})
	payload, err := json.MarshalIndent(syn, "", "  ")
	if err != nil {
		t.Fatalf("marshal synthesis: %v", err)
	}
	payload = append(payload, '\n')
	goldenPath := filepath.Join("testdata", "phase2_synthesis_golden.json")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(goldenPath, payload, 0o644); err != nil {
			t.Fatalf("update golden %s: %v", goldenPath, err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden %s: %v", goldenPath, err)
	}
	if string(payload) != string(want) {
		t.Fatalf("phase2 synthesis golden mismatch\n--- got ---\n%s\n--- want ---\n%s", payload, want)
	}
}

func TestPhase2SynthesisDoesNotClaimParityFromSQLiteOnly(t *testing.T) {
	treedbNS := 700_000.0
	sqliteNS := 1_000_000.0
	syn := buildPhase2Synthesis([]scoreboardRow{
		{
			SourceLabel: "treedb_common_10k",
			System:      "TreeDB",
			Engine:      "treedb_text_v2",
			Modality:    "text_only",
			QueryShape:  "common term BM25F top-k with block-max pruning",
			Boundary:    "No-document text-v2 score-only BM25F search",
			Benchmark:   "BenchmarkTextV2BlockMaxCommonTerm2628/blockmax_common_topk",
			NsPerOp:     &treedbNS,
		},
		{
			SourceLabel: "sqlite_common_10k",
			System:      "SQLite FTS5",
			Engine:      "sqlite_fts5",
			Modality:    "text_only",
			QueryShape:  "common term FTS5 MATCH top-k",
			Boundary:    "no-document rowid+bm25 retrieval only",
			Benchmark:   "sqlite_fts5/common_term_no_docs",
			NsPerOp:     &sqliteNS,
		},
	}, []unavailableRow{{System: "Lucene", Reason: "not run"}, {System: "Tantivy", Reason: "not run"}, {System: "Bleve", Reason: "not run"}})
	var common phase2GapClassification
	for _, got := range syn.GapClassifications {
		if got.ShapeID == "single_term_common" {
			common = got
			break
		}
	}
	if common.Classification == "ahead" || common.Classification == "near_parity" {
		t.Fatalf("single_term_common classification=%q from SQLite-only evidence; want evidence gap", common.Classification)
	}
	if !strings.Contains(common.ExternalEvidence, "sqlite_common_10k") {
		t.Fatalf("single_term_common external evidence=%q should still document SQLite row availability", common.ExternalEvidence)
	}
}

func TestPhase2SynthesisRequiresExplicitComparablePair(t *testing.T) {
	treedbNS := 700_000.0
	luceneNS := 1_000_000.0
	rows := []scoreboardRow{
		{
			SourceLabel: "treedb_common_10k",
			System:      "TreeDB",
			Engine:      "treedb_text_v2",
			Modality:    "text_only",
			Dataset:     "docs=10000",
			TopK:        10,
			QueryShape:  "common term BM25F top-k with block-max pruning",
			Boundary:    "No-document text-v2 score-only BM25F search",
			Benchmark:   "BenchmarkTextV2BlockMaxCommonTerm2628/blockmax_common_topk",
			NsPerOp:     &treedbNS,
		},
		{
			SourceLabel: "lucene_common_1m",
			System:      "Lucene",
			Engine:      "lucene",
			Modality:    "text_only",
			Dataset:     "docs=1000000",
			TopK:        10,
			QueryShape:  "common term BM25 top-k",
			Boundary:    "no-document scorer top-k",
			Benchmark:   "lucene/common_term_no_docs",
			NsPerOp:     &luceneNS,
		},
	}
	syn := buildPhase2Synthesis(rows, nil)
	common := phase2ClassificationByID(t, syn, "single_term_common")
	if common.Classification == "ahead" || common.Classification == "near_parity" {
		t.Fatalf("single_term_common classification=%q from unpaired rows; want default evidence gap", common.Classification)
	}

	rows[0].Metrics = map[string]float64{"phase2_comparable": 1}
	rows[1].Metrics = map[string]float64{"phase2_comparable": 1}
	rows[1].Dataset = rows[0].Dataset + " queries=1000"
	syn = buildPhase2Synthesis(rows, nil)
	common = phase2ClassificationByID(t, syn, "single_term_common")
	if common.Classification != "ahead" {
		t.Fatalf("single_term_common classification=%q with explicit comparable pair; want ahead", common.Classification)
	}
}

func TestPhase2SynthesisComparesExplicitBuildPairs(t *testing.T) {
	treeBuildNS := 800_000_000.0
	luceneBuild := 1.0
	syn := buildPhase2Synthesis([]scoreboardRow{
		{
			SourceLabel: "treedb_build_10k",
			System:      "TreeDB",
			Engine:      "treedb_text_v2",
			Modality:    "build_storage",
			Dataset:     "docs=10000",
			TopK:        0,
			QueryShape:  "text-v2 index build",
			Boundary:    "checkpointed text index build",
			Benchmark:   "BenchmarkCreateTextIndex/build",
			NsPerOp:     &treeBuildNS,
			Metrics:     map[string]float64{"phase2_comparable": 1},
		},
		{
			SourceLabel:  "lucene_build_10k",
			System:       "Lucene",
			Engine:       "lucene",
			Modality:     "build_storage",
			Dataset:      "docs=10000",
			TopK:         0,
			QueryShape:   "Lucene index build",
			Boundary:     "optimized index build",
			Benchmark:    "lucene/build",
			BuildSeconds: &luceneBuild,
			Metrics:      map[string]float64{"phase2_comparable": 1},
		},
	}, nil)
	build := phase2ClassificationByID(t, syn, "index_build_ingest")
	if build.Classification != "ahead" {
		t.Fatalf("index_build_ingest classification=%q rationale=%q; want build metric comparison", build.Classification, build.Rationale)
	}
	if !strings.Contains(build.Rationale, "build pair") {
		t.Fatalf("index_build_ingest rationale=%q missing build pair evidence", build.Rationale)
	}
}

func phase2ClassificationByID(t *testing.T, syn phase2Synthesis, shapeID string) phase2GapClassification {
	t.Helper()
	for _, got := range syn.GapClassifications {
		if got.ShapeID == shapeID {
			return got
		}
	}
	t.Fatalf("missing phase2 classification %q", shapeID)
	return phase2GapClassification{}
}

func TestPhase2IndexSizeIgnoresIncidentalRetrievalStorage(t *testing.T) {
	treedbNS := 1_100_000.0
	sqliteNS := 1_000_000.0
	treedbStorage := 144.0
	sqliteStorage := 128.0
	syn := buildPhase2Synthesis([]scoreboardRow{
		{
			SourceLabel:        "treedb_common_10k",
			System:             "TreeDB",
			Engine:             "treedb_text_v2",
			Modality:           "text_only",
			QueryShape:         "common term BM25F top-k with block-max pruning",
			Boundary:           "No-document text-v2 score-only BM25F search",
			Benchmark:          "BenchmarkTextV2BlockMaxCommonTerm2628/blockmax_common_topk",
			NsPerOp:            &treedbNS,
			StorageBytesPerDoc: &treedbStorage,
		},
		{
			SourceLabel:        "sqlite_common_10k",
			System:             "SQLite FTS5",
			Engine:             "sqlite_fts5",
			Modality:           "text_only",
			QueryShape:         "common term FTS5 MATCH top-k",
			Boundary:           "no-document rowid+bm25 retrieval only",
			Benchmark:          "sqlite_fts5/common_term_no_docs",
			NsPerOp:            &sqliteNS,
			StorageBytesPerDoc: &sqliteStorage,
		},
	}, nil)
	var indexSize phase2GapClassification
	for _, got := range syn.GapClassifications {
		if got.ShapeID == "index_size" {
			indexSize = got
			break
		}
	}
	if indexSize.Classification != "far_behind" {
		t.Fatalf("index_size classification=%q evidence=%q; incidental retrieval storage must not become index-size parity evidence", indexSize.Classification, indexSize.TreeDBEvidence)
	}
	if !strings.Contains(indexSize.TreeDBEvidence, "not captured") {
		t.Fatalf("index_size TreeDB evidence=%q want explicit missing footprint evidence", indexSize.TreeDBEvidence)
	}
}

func TestRunWritesPhase2SynthesisArtifacts(t *testing.T) {
	dir := t.TempDir()
	goBench := filepath.Join(dir, "go.txt")
	if err := os.WriteFile(goBench, []byte(`BenchmarkTextV2BlockMaxCommonTerm2628/blockmax_common_topk-8 1 1000 ns/op 16 B/op 1 allocs/op 0 docs_fetched/search 0 full_doc_fallbacks/search 0 fail_closed/search 0 state_lookups/search 0 match_details/search 4 posting_blocks_visited/search 2 posting_blocks_skipped/search
`), 0o644); err != nil {
		t.Fatalf("write go bench: %v", err)
	}
	out := filepath.Join(dir, "out")
	if err := run(config{
		outDir:      out,
		goBenches:   namedPaths{{Name: "treedb_text_blockmax_10k", Path: goBench}},
		unavailable: namedValues{{Name: "Lucene", Value: "not run in artifact smoke"}, {Name: "Tantivy", Value: "not run in artifact smoke"}, {Name: "Bleve", Value: "not run in artifact smoke"}},
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	jsonPayload, err := os.ReadFile(filepath.Join(out, "scoreboard.json"))
	if err != nil {
		t.Fatalf("read scoreboard.json: %v", err)
	}
	var rep report
	if err := json.Unmarshal(jsonPayload, &rep); err != nil {
		t.Fatalf("parse scoreboard.json: %v", err)
	}
	if rep.Phase2Synthesis.SchemaVersion != phase2SynthesisVersion {
		t.Fatalf("phase2 synthesis schema=%q", rep.Phase2Synthesis.SchemaVersion)
	}
	mdPayload, err := os.ReadFile(filepath.Join(out, "scoreboard.md"))
	if err != nil {
		t.Fatalf("read scoreboard.md: %v", err)
	}
	md := string(mdPayload)
	for _, want := range []string{"## Phase-2 gap classification", "single_term_common", "External baseline coverage", "Non-equivalent analyzer/query semantics"} {
		if !strings.Contains(md, want) {
			t.Fatalf("scoreboard.md missing %q:\n%s", want, md)
		}
	}
}

func TestPhase2GapReadoutDocContainsRequiredTaxonomy(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "benchmarks", "treedb_text_v2_phase2_gap_classification.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	doc := string(data)
	for _, want := range []string{
		"/tmp/gomap_2727_scoreboard_candidate_20260613_090425",
		"scripts/bench_text_hybrid_scoreboard.sh",
		"single_term_common",
		"single_term_rare",
		"multi_term_and",
		"multi_term_or_wand",
		"phrase",
		"hybrid_text_scalar",
		"index_build_ingest",
		"index_size",
		"reopen",
		"maintenance_rewrite",
		"ahead",
		"near_parity",
		"behind_but_tractable",
		"far_behind",
		"SQLite FTS5",
		"Bleve",
		"Tantivy",
		"Lucene",
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("phase2 readout missing %q", want)
		}
	}
	if strings.Contains(strings.ToLower(doc), "slab") {
		t.Fatalf("phase2 readout should use value-log/storage-native wording, not slab")
	}
}
