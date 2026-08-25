package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestScaleCommandSmokeReport2731(t *testing.T) {
	outDir := t.TempDir()
	cfg := config{
		outDir:             outDir,
		rows:               96,
		batchSize:          48,
		dims:               4,
		m:                  4,
		efConstruction:     32,
		efSearch:           32,
		topK:               5,
		candidateLimit:     16,
		queries:            2,
		readers:            2,
		includeVector:      true,
		runBackfill:        true,
		backfillRows:       48,
		runReopen:          true,
		runConcurrent:      true,
		concurrentWrites:   4,
		runRewrite:         true,
		maintenanceUpdates: 4,
		maintenanceDeletes: 2,
		baseRef:            "origin/main",
	}
	rep, err := run(cfg)
	if err != nil {
		t.Fatalf("run scale command smoke: %v", err)
	}
	if rep.SchemaVersion != scaleSchemaVersion {
		t.Fatalf("schema=%q want %q", rep.SchemaVersion, scaleSchemaVersion)
	}
	if rep.Load.Rows != cfg.rows || rep.Load.TextStorage.V2LiveDocuments != uint64(cfg.rows) {
		t.Fatalf("load=%+v want %d live docs", rep.Load, cfg.rows)
	}
	if rep.Backfill == nil || rep.Backfill.Rows != cfg.backfillRows {
		t.Fatalf("backfill=%+v want rows=%d", rep.Backfill, cfg.backfillRows)
	}
	if rep.Reopen == nil || rep.Concurrent == nil || rep.Maintenance == nil {
		t.Fatalf("missing reopen/concurrent/maintenance: reopen=%v concurrent=%v maintenance=%v", rep.Reopen != nil, rep.Concurrent != nil, rep.Maintenance != nil)
	}
	if len(rep.Queries) == 0 {
		t.Fatal("no query rows")
	}
	for _, guard := range rep.Guardrails {
		if !guard.OK {
			t.Fatalf("guardrail failed: %+v", guard)
		}
	}
	jsonPath := filepath.Join(outDir, "scale_report.json")
	markdownPath := filepath.Join(outDir, "scale_report.md")
	if _, err := os.Stat(jsonPath); err != nil {
		t.Fatalf("missing json report: %v", err)
	}
	if _, err := os.Stat(markdownPath); err != nil {
		t.Fatalf("missing markdown report: %v", err)
	}
	payload, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json report: %v", err)
	}
	var decoded report
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal json report: %v", err)
	}
	if decoded.SchemaVersion != scaleSchemaVersion || len(decoded.Queries) != len(rep.Queries) {
		t.Fatalf("decoded schema/queries mismatch: %q/%d", decoded.SchemaVersion, len(decoded.Queries))
	}
	if _, err := os.Stat(rep.Artifacts.DBDir); !os.IsNotExist(err) {
		t.Fatalf("primary db dir kept unexpectedly err=%v", err)
	}
}

func TestRetrievalPhaseSelectorSkipsUnrelatedPhases2731(t *testing.T) {
	cfg, err := parseFlags([]string{"-out-dir", t.TempDir(), "-phases", "retrieval"})
	if err != nil {
		t.Fatalf("parse retrieval phases: %v", err)
	}
	got := selectedPhaseNames(cfg.selectedPhases)
	want := []string{"load", "queries", "reopen"}
	if len(got) != len(want) {
		t.Fatalf("selected phases=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selected phases=%v want %v", got, want)
		}
	}
	if _, err := parseFlags([]string{"-out-dir", t.TempDir(), "-phases", "queries"}); err == nil {
		t.Fatal("parseFlags accepted an unknown/incomplete phase selector")
	}
	for _, phases := range []string{"all,typo", "typo,all"} {
		if _, err := parseFlags([]string{"-out-dir", t.TempDir(), "-phases", phases}); err == nil || !strings.Contains(err.Error(), "unknown -phases value") {
			t.Fatalf("parseFlags(%q) error=%v, want invalid token rejection", phases, err)
		}
	}
	all, err := parsePhaseSelector("all,retrieval")
	if err != nil || strings.Join(selectedPhaseNames(all), ",") != "load,queries,reopen,concurrent,maintenance,backfill" {
		t.Fatalf("parsePhaseSelector(all,retrieval) phases=%v err=%v", selectedPhaseNames(all), err)
	}
}

func TestRetrievalQualificationExcludesDisabledProbeFromCompletion2731(t *testing.T) {
	outDir := t.TempDir()
	cfg, err := parseFlags([]string{"-out-dir", outDir, "-phases", "retrieval", "-run-reopen=false"})
	if err != nil {
		t.Fatalf("parse retrieval phases: %v", err)
	}
	cfg.rows, cfg.batchSize, cfg.dims, cfg.m, cfg.efConstruction, cfg.efSearch = 96, 48, 4, 4, 32, 32
	cfg.topK, cfg.candidateLimit, cfg.queries, cfg.readers = 5, 16, 1, 2
	rep, err := run(cfg)
	if err != nil {
		t.Fatalf("run retrieval qualification: %v", err)
	}
	want := []string{"load", "queries"}
	if !rep.Complete || rep.Reopen != nil || len(rep.Queries) == 0 || len(rep.SelectedPhases) != len(want) || len(rep.CompletedPhases) != len(want) {
		t.Fatalf("unexpected retrieval-only report: %+v", rep)
	}
	for i := range want {
		if rep.SelectedPhases[i] != want[i] || rep.CompletedPhases[i] != want[i] {
			t.Fatalf("phases selected=%v completed=%v want %v", rep.SelectedPhases, rep.CompletedPhases, want)
		}
	}
}

func TestPartialReportIsAtomicallyLabeledIncomplete2731(t *testing.T) {
	outDir := t.TempDir()
	rep := report{
		SchemaVersion: scaleSchemaVersion,
		Artifacts: reportArtifacts{
			OutDir: outDir, JSONReport: filepath.Join(outDir, "scale_report.json"), Markdown: filepath.Join(outDir, "scale_report.md"),
		},
		SelectedPhases:  []string{"load", "queries", "reopen"},
		CompletedPhases: []string{"load"},
		Complete:        false,
		Guardrails:      []guardrailResult{{Name: "queries", OK: false, Failure: "fail closed"}},
	}
	if err := writeReports(rep); err != nil {
		t.Fatalf("write partial report: %v", err)
	}
	payload, err := os.ReadFile(rep.Artifacts.JSONReport)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var decoded report
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("partial report is invalid JSON: %v", err)
	}
	if decoded.Complete || len(decoded.CompletedPhases) != 1 {
		t.Fatalf("partial report incorrectly complete: %+v", decoded)
	}
	markdown, err := os.ReadFile(rep.Artifacts.Markdown)
	if err != nil {
		t.Fatalf("read markdown: %v", err)
	}
	if !strings.Contains(string(markdown), "INCOMPLETE (partial/resumable evidence; not a completed qualification)") {
		t.Fatalf("markdown did not fail closed: %s", markdown)
	}
}

func TestFailedQueryGuardrailsLeavePersistedReportIncomplete4327(t *testing.T) {
	outDir := t.TempDir()
	rep := report{
		SchemaVersion: scaleSchemaVersion,
		Artifacts: reportArtifacts{
			OutDir: outDir, JSONReport: filepath.Join(outDir, "scale_report.json"), Markdown: filepath.Join(outDir, "scale_report.md"),
		},
		SelectedPhases:  []string{"load", "queries"},
		CompletedPhases: []string{"load"},
	}
	guard := guardrailResult{Name: "hybrid_scalar", OK: false, Failure: "fail closed"}
	rep.Guardrails = append(rep.Guardrails, guard)
	if err := completeQueryPhase(&rep, []guardrailResult{guard}, false); err == nil {
		t.Fatal("completeQueryPhase accepted a failed guardrail")
	}
	if rep.Complete || strings.Join(rep.CompletedPhases, ",") != "load" {
		t.Fatalf("failed query phase was marked complete: %+v", rep)
	}
	payload, err := os.ReadFile(rep.Artifacts.JSONReport)
	if err != nil {
		t.Fatalf("read partial report: %v", err)
	}
	var decoded report
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal partial report: %v", err)
	}
	if decoded.Complete || strings.Join(decoded.CompletedPhases, ",") != "load" {
		t.Fatalf("persisted failed query phase was marked complete: %+v", decoded)
	}
}

func TestHybridFailureRowsPreserveFailClosedStats2731(t *testing.T) {
	resp := collections.HybridSearchResponse{Stats: collections.HybridSearchStats{FailClosed: 1, FailClosedReason: collections.HybridFailClosedReasonTextIndexUnavailable}}
	row := failedHybridQueryRow(config{rows: 1_000_000, topK: 10, candidateLimit: 64}, "hybrid_common", "common", resp, errors.New("bounded generation failed"))
	if row.GuardrailOK || row.GuardrailFailure == "" {
		t.Fatalf("row guardrail=%v failure=%q", row.GuardrailOK, row.GuardrailFailure)
	}
	if row.HybridStats == nil || row.HybridStats.FailClosed != 1 || row.HybridStats.FailClosedReason != collections.HybridFailClosedReasonTextIndexUnavailable {
		t.Fatalf("row stats=%+v want fail-closed stats preserved", row.HybridStats)
	}
	if row.Samples != 0 || row.Results != 0 || row.Rows != 1_000_000 || row.CandidateBudget != 64 {
		t.Fatalf("row metadata=%+v", row)
	}
}

func TestRecordReopenProbeFailurePreservesReport2731(t *testing.T) {
	rep := report{Load: loadReport{TotalSeconds: 2}}
	recordReopenProbeFailure(&rep, errors.New("reopen hybrid vector probe: fail closed"))
	if len(rep.Guardrails) != 1 || rep.Guardrails[0].Name != "reopen_probe" || rep.Guardrails[0].OK || rep.Guardrails[0].Failure == "" {
		t.Fatalf("guardrails=%+v want failed reopen_probe", rep.Guardrails)
	}
	if len(rep.Caveats) == 0 {
		t.Fatal("missing caveat for skipped probes")
	}
	if len(rep.Bottlenecks) == 0 {
		t.Fatal("missing bottlenecks for partial report")
	}
}

func TestDBDirContainingOutDirRejected2731(t *testing.T) {
	root := t.TempDir()
	cases := []struct {
		name string
		db   string
		out  string
		want bool
	}{
		{name: "same", db: root, out: root, want: true},
		{name: "db_parent", db: root, out: filepath.Join(root, "reports"), want: true},
		{name: "db_child_allowed", db: filepath.Join(root, "reports", "primary_db"), out: filepath.Join(root, "reports"), want: false},
		{name: "sibling_allowed", db: filepath.Join(root, "db"), out: filepath.Join(root, "reports"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dbDirContainsOutDir(tc.db, tc.out)
			if err != nil {
				t.Fatalf("dbDirContainsOutDir: %v", err)
			}
			if got != tc.want {
				t.Fatalf("dbDirContainsOutDir(%q,%q)=%v want %v", tc.db, tc.out, got, tc.want)
			}
		})
	}
}

func TestRankBottlenecksNormalizesUnits2731(t *testing.T) {
	rep := report{
		Load: loadReport{TotalSeconds: 100, VectorRebuildSeconds: 50},
		Queries: []queryReport{
			{Name: "fast_query", Latency: latencySummary{P95NS: int64(time.Millisecond)}},
			{Name: "slow_query", Latency: latencySummary{P95NS: int64(200 * time.Second)}},
		},
		Maintenance: &maintenanceReport{RewriteSeconds: 75},
	}
	got := rankBottlenecks(rep)
	if len(got) < 4 {
		t.Fatalf("rankBottlenecks returned %d rows", len(got))
	}
	if got[0].Name != "slow_query" {
		t.Fatalf("first bottleneck=%q want slow_query", got[0].Name)
	}
	if got[1].Name != "fixture_load" || got[2].Name != "text_rewrite" || got[3].Name != "vector_rebuild" {
		t.Fatalf("unexpected normalized order: %+v", got[:4])
	}
}

func TestQueryRowFlagContracts4327(t *testing.T) {
	base := []string{"-out-dir", t.TempDir()}
	tests := []struct {
		name       string
		args       []string
		wantErr    string
		wantQuery  string
		wantCPU    string
		wantAllocs string
	}{
		{name: "unknown row", args: []string{"-query-rows", "not_a_query_row"}, wantErr: "unknown -query-rows value"},
		{name: "profile text row", args: []string{"-query-rows", queryRowTextCommon, "-cpu-profile", "cpu.pprof"}, wantErr: "exactly one hybrid -query-rows value"},
		{name: "vector row with vectors disabled", args: []string{"-query-rows", queryRowHybridTextVector, "-include-vector=false"}, wantErr: "requires -include-vector=true"},
		{name: "valid scalar hybrid profile row", args: []string{"-query-rows", queryRowHybridTextScalar, "-cpu-profile", "cpu.pprof"}, wantQuery: queryRowHybridTextScalar, wantCPU: "cpu.pprof"},
		{name: "valid vector hybrid profile row", args: []string{"-query-rows", queryRowHybridTextVector, "-alloc-profile", "allocs.pprof"}, wantQuery: queryRowHybridTextVector, wantAllocs: "allocs.pprof"},
		{name: "identical profile paths", args: []string{"-query-rows", queryRowHybridTextScalar, "-cpu-profile", "profiles/cpu.pprof", "-alloc-profile", "profiles/cpu.pprof"}, wantErr: "must not resolve to the same path"},
		{name: "equivalent relative profile paths", args: []string{"-query-rows", queryRowHybridTextScalar, "-cpu-profile", "profiles/cpu.pprof", "-alloc-profile", "./profiles/../profiles/cpu.pprof"}, wantErr: "must not resolve to the same path"},
		{name: "distinct profile paths", args: []string{"-query-rows", queryRowHybridTextScalar, "-cpu-profile", "profiles/cpu.pprof", "-alloc-profile", "profiles/allocs.pprof"}, wantQuery: queryRowHybridTextScalar, wantCPU: "profiles/cpu.pprof", wantAllocs: "profiles/allocs.pprof"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := append(append([]string{}, base...), tc.args...)
			cfg, err := parseFlags(args)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseFlags error=%v want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			if !cfg.queryRows[tc.wantQuery] || len(cfg.queryRows) != 1 {
				t.Fatalf("queryRows=%v want only %q", cfg.queryRows, tc.wantQuery)
			}
			wantCPU, err := filepath.Abs(tc.wantCPU)
			if err != nil {
				t.Fatalf("resolve expected CPU profile: %v", err)
			}
			wantAllocs, err := filepath.Abs(tc.wantAllocs)
			if err != nil {
				t.Fatalf("resolve expected allocation profile: %v", err)
			}
			if tc.wantCPU == "" {
				wantCPU = ""
			}
			if tc.wantAllocs == "" {
				wantAllocs = ""
			}
			if cfg.cpuProfile != wantCPU || cfg.allocProfile != wantAllocs {
				t.Fatalf("profiles cpu=%q allocs=%q want cpu=%q allocs=%q", cfg.cpuProfile, cfg.allocProfile, wantCPU, wantAllocs)
			}
		})
	}
}

func TestProfilePathsDoNotContaminateReportsOrDB4327(t *testing.T) {
	root := t.TempDir()
	outDir := filepath.Join(root, "out")
	customDBDir := filepath.Join(root, "custom_db")
	tests := []struct {
		name    string
		args    []string
		wantErr string
		wantCPU string
		wantMem string
	}{
		{name: "JSON report collision", args: []string{"-cpu-profile", filepath.Join(outDir, "scale_report.json")}, wantErr: "must not resolve to a scale report artifact"},
		{name: "Markdown report collision", args: []string{"-alloc-profile", filepath.Join(outDir, "scale_report.md")}, wantErr: "must not resolve to a scale report artifact"},
		{name: "default DB descendant", args: []string{"-cpu-profile", filepath.Join(outDir, "primary_db", "profiles", "cpu.pprof")}, wantErr: "must not resolve to the effective -db-dir or its descendant"},
		{name: "custom DB descendant", args: []string{"-db-dir", customDBDir, "-alloc-profile", filepath.Join(customDBDir, "profiles", "allocs.pprof")}, wantErr: "must not resolve to the effective -db-dir or its descendant"},
		{name: "distinct profiles under output directory", args: []string{"-cpu-profile", filepath.Join(outDir, "profiles", "cpu.pprof"), "-alloc-profile", filepath.Join(outDir, "profiles", "allocs.pprof")}, wantCPU: filepath.Join(outDir, "profiles", "cpu.pprof"), wantMem: filepath.Join(outDir, "profiles", "allocs.pprof")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"-out-dir", outDir, "-query-rows", queryRowHybridTextScalar}
			args = append(args, tc.args...)
			cfg, err := parseFlags(args)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseFlags error=%v want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			if cfg.cpuProfile != tc.wantCPU || cfg.allocProfile != tc.wantMem {
				t.Fatalf("profiles cpu=%q allocs=%q want cpu=%q allocs=%q", cfg.cpuProfile, cfg.allocProfile, tc.wantCPU, tc.wantMem)
			}
		})
	}
}

func TestProfiledWarmupFailureCannotBeAllowed4327(t *testing.T) {
	warmErr := errors.New("warm hybrid_scalar returned no results")
	if err := profiledWarmupError(config{allowGuardrailFails: true}, warmErr); err != nil {
		t.Fatalf("unprofiled allowed warm-up failure rejected: %v", err)
	}
	for _, cfg := range []config{
		{allowGuardrailFails: true, cpuProfile: "cpu.pprof"},
		{allowGuardrailFails: true, allocProfile: "allocs.pprof"},
	} {
		err := profiledWarmupError(cfg, warmErr)
		if err == nil || !strings.Contains(err.Error(), "cannot produce requested profile") {
			t.Fatalf("profiled warm-up failure err=%v", err)
		}
	}
}

func TestAllocationProfilesRequireStartupExactRate4327(t *testing.T) {
	if err := requireExactMemProfileRate(true, 1); err != nil {
		t.Fatalf("exact startup rate rejected: %v", err)
	}
	err := requireExactMemProfileRate(true, 512*1024)
	if err == nil || !strings.Contains(err.Error(), "GODEBUG=memprofilerate=1") {
		t.Fatalf("rate precondition error=%v", err)
	}
	if err := requireExactMemProfileRate(false, 512*1024); err != nil {
		t.Fatalf("CPU-only profiling unexpectedly requires allocation rate: %v", err)
	}
}

func TestAllocationProfileWritesCumulativeOutputAndStopsIdempotently4327(t *testing.T) {
	dir := t.TempDir()
	alloc := filepath.Join(dir, "profiles", "allocs.pprof")
	stop, err := startQueryProfilesAtMemProfileRate(config{allocProfile: alloc}, 1)
	if err != nil {
		t.Fatalf("start allocation profile: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("stop allocation profile: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("second stop allocation profile: %v", err)
	}
	info, err := os.Stat(alloc)
	if err != nil || info.Size() == 0 {
		t.Fatalf("profile %q info=%v err=%v", alloc, info, err)
	}
}

func TestAllocationProfileFocusesTimedHybridStack4327(t *testing.T) {
	root := t.TempDir()
	outDir := filepath.Join(root, "out")
	alloc := filepath.Join(root, "allocs.pprof")
	cmd := exec.Command("go", "run", ".",
		"-out-dir", outDir,
		"-rows", "96", "-batch-size", "48", "-dims", "4", "-m", "4",
		"-ef-construction", "32", "-ef-search", "32", "-top-k", "5", "-candidate-limit", "16", "-queries", "3", "-readers", "2",
		"-include-vector=false", "-phases=retrieval", "-run-reopen=false",
		"-query-rows", queryRowHybridTextScalar, "-alloc-profile", alloc,
	)
	cmd.Env = append(os.Environ(), "GOWORK=off", "GODEBUG=memprofilerate=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("small allocation-profile CLI smoke: %v\n%s", err, output)
	}
	info, err := os.Stat(alloc)
	if err != nil || info.Size() == 0 {
		t.Fatalf("allocation profile %q info=%v err=%v", alloc, info, err)
	}
	pprof := exec.Command("go", "tool", "pprof", "-top", "-focus=main.runProfiledHybridSamples", alloc)
	output, err := pprof.CombinedOutput()
	if err != nil {
		t.Fatalf("focused allocation profile: %v\n%s", err, output)
	}
	top := string(output)
	if !strings.Contains(top, "SearchHybrid") {
		t.Fatalf("focused profile lacks timed hybrid query path:\n%s", top)
	}
	if strings.Contains(top, "loadPrimaryFixture") || strings.Contains(top, "makeScaleBatch") {
		t.Fatalf("focused profile retained fixture construction:\n%s", top)
	}
}

func TestTenMPlanPropagatesPhaseSelector4327(t *testing.T) {
	runDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "scripts/bench_text_hybrid_scale.sh")
	cmd.Dir = filepath.Join("..", "..")
	cmd.Env = append(os.Environ(), "RUN_DIR="+runDir, "RUN_SMOKE=false", "RUN_1M=false", "RUN_10M=false", "RUN_GO_BENCH=false", "PHASES=retrieval", "GO_BIN=true")
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("generate 10M plan timed out: %v\n%s", ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("generate 10M plan: %v\n%s", err, output)
	}
	plan, err := os.ReadFile(filepath.Join(runDir, "10m_selected_matrix_commands.md"))
	if err != nil {
		t.Fatalf("read generated plan: %v", err)
	}
	if got := strings.Count(string(plan), "-phases \"retrieval\""); got != 1 {
		t.Fatalf("direct command phase selector count=%d plan:\n%s", got, plan)
	}
	if got := strings.Count(string(plan), "PHASES=retrieval"); got != 1 {
		t.Fatalf("wrapper command phase selector count=%d plan:\n%s", got, plan)
	}
}

func TestRetrievalRepetitionsRequireRetrievalPhase4327(t *testing.T) {
	for _, tc := range []struct {
		name    string
		phases  string
		wantErr bool
	}{
		{name: "reject all", phases: "all", wantErr: true},
		{name: "accept retrieval", phases: "retrieval"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "scripts/bench_text_hybrid_scale.sh")
			cmd.Dir = filepath.Join("..", "..")
			cmd.Env = append(os.Environ(), "RUN_DIR="+t.TempDir(), "RUN_SMOKE=false", "RUN_1M=false", "RUN_10M=false", "RUN_GO_BENCH=false", "PHASES="+tc.phases, "RETRIEVAL_REPETITIONS=2", "GO_BIN=true")
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("repetition contract timed out: %v\n%s", ctx.Err(), output)
			}
			if tc.wantErr {
				if err == nil || !strings.Contains(string(output), "RETRIEVAL_REPETITIONS>1 requires PHASES=retrieval") {
					t.Fatalf("expected repetition rejection, err=%v output=%s", err, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected retrieval repetition acceptance: %v\n%s", err, output)
			}
		})
	}
}

func TestRetrievalRepetitionsRejectNonpositive4327(t *testing.T) {
	for _, repetitions := range []string{"0", "-1"} {
		t.Run(repetitions, func(t *testing.T) {
			runDir := t.TempDir()
			cmd := exec.Command("bash", "scripts/bench_text_hybrid_scale.sh")
			cmd.Dir = filepath.Join("..", "..")
			cmd.Env = append(os.Environ(), "RUN_DIR="+runDir, "RUN_SMOKE=false", "RUN_1M=false", "RUN_10M=false", "RUN_GO_BENCH=false", "PHASES=retrieval", "RETRIEVAL_REPETITIONS="+repetitions, "GO_BIN=true")
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "RETRIEVAL_REPETITIONS must be a positive integer") {
				t.Fatalf("expected nonpositive repetition rejection, err=%v output=%s", err, output)
			}
			if _, err := os.Stat(filepath.Join(runDir, "10m_selected_matrix_commands.md")); !os.IsNotExist(err) {
				t.Fatalf("nonpositive repetition count wrote a plan, err=%v", err)
			}
		})
	}
}

func TestScaleCommandFlagValidation2731(t *testing.T) {
	if _, err := parseFlags([]string{"-out-dir", t.TempDir(), "-rows", "0"}); err == nil {
		t.Fatal("parseFlags accepted zero rows")
	}
	for _, flagName := range []string{"-backfill-rows", "-concurrent-writes", "-maintenance-updates", "-maintenance-deletes"} {
		if _, err := parseFlags([]string{"-out-dir", t.TempDir(), "-rows", "10", flagName, "-1"}); err == nil {
			t.Fatalf("parseFlags accepted negative %s", flagName)
		}
	}
	cfg, err := parseFlags([]string{"-out-dir", t.TempDir(), "-rows", "10", "-include-vector=false", "-run-backfill=false", "-run-rewrite=false", "-run-concurrent=false", "-run-reopen=false"})
	if err != nil {
		t.Fatalf("parseFlags valid args: %v", err)
	}
	if cfg.includeVector || cfg.runBackfill || cfg.runRewrite || cfg.runConcurrent || cfg.runReopen {
		t.Fatalf("bool flags not parsed: %+v", cfg)
	}
	wantSelected := []string{"load", "queries"}
	if got := selectedPhaseNames(cfg.selectedPhases); strings.Join(got, ",") != strings.Join(wantSelected, ",") {
		t.Fatalf("selected phases=%v want %v", got, wantSelected)
	}
}
