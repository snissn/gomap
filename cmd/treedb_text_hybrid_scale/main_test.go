package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
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
	for _, row := range rep.Queries {
		if len(row.RawLatencyNS) != row.Samples {
			t.Fatalf("row %q raw samples=%d want %d", row.Name, len(row.RawLatencyNS), row.Samples)
		}
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
	want := []string{"load", "queries", "reopen"}
	if got := selectedPhaseNames(cfg.selectedPhases); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("selected phases=%v want %v", got, want)
	}
	for _, phases := range []string{"queries", "all,typo", "typo,all", ""} {
		if _, err := parseFlags([]string{"-out-dir", t.TempDir(), "-phases", phases}); err == nil {
			t.Fatalf("parseFlags accepted invalid selector %q", phases)
		}
	}
	all, err := parsePhaseSelector("all,retrieval")
	if err != nil || strings.Join(selectedPhaseNames(all), ",") != "load,queries,reopen,concurrent,maintenance,backfill" {
		t.Fatalf("parsePhaseSelector(all,retrieval) phases=%v err=%v", selectedPhaseNames(all), err)
	}
	for _, phases := range []string{"all,typo", "typo,all"} {
		if _, err := parseFlags([]string{"-out-dir", t.TempDir(), "-phases", phases}); err == nil || !strings.Contains(err.Error(), "unknown -phases value") {
			t.Fatalf("parseFlags(%q) error=%v, want invalid token rejection", phases, err)
		}
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
	if !rep.Complete || rep.Reopen != nil || len(rep.Queries) == 0 || strings.Join(rep.SelectedPhases, ",") != strings.Join(want, ",") || strings.Join(rep.CompletedPhases, ",") != strings.Join(want, ",") {
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
		SchemaVersion:  scaleSchemaVersion,
		Artifacts:      reportArtifacts{OutDir: outDir, JSONReport: filepath.Join(outDir, "scale_report.json"), Markdown: filepath.Join(outDir, "scale_report.md")},
		SelectedPhases: []string{"load", "queries", "reopen"}, CompletedPhases: []string{"load"},
		Guardrails: []guardrailResult{{Name: "queries", OK: false, Failure: "fail closed"}},
	}
	if err := writeReports(rep); err != nil {
		t.Fatalf("write partial report: %v", err)
	}
	payload, err := os.ReadFile(rep.Artifacts.JSONReport)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var decoded report
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded.Complete || len(decoded.CompletedPhases) != 1 {
		t.Fatalf("partial report invalid or complete: decoded=%+v err=%v", decoded, err)
	}
	markdown, err := os.ReadFile(rep.Artifacts.Markdown)
	if err != nil || !strings.Contains(string(markdown), "INCOMPLETE (partial evidence; not a completed qualification)") {
		t.Fatalf("markdown did not fail closed: err=%v content=%s", err, markdown)
	}
}

func TestCaptureContextUsesInvocationProvenance4327(t *testing.T) {
	ctx := captureContext(config{baseRef: "origin/main"})
	if ctx.VCSStatus == "" || ctx.BinaryState == "" || !strings.HasPrefix(ctx.Command, "process_argv=") || ctx.Corpus == "" || ctx.Cache == "" || ctx.Durability == "" || ctx.NoisePolicy == "" {
		t.Fatalf("missing or mislabeled provenance: %+v", ctx)
	}
	if ctx.RepoRoot != "" || ctx.Branch != "" || ctx.Commit != "" {
		t.Fatalf("context used ambient checkout state: %+v", ctx)
	}
}

func TestInvocationProvenanceUsesEmbeddedBuildMetadata4327(t *testing.T) {
	info := &debug.BuildInfo{Main: debug.Module{Path: "example.com/scale"}, GoVersion: "go1.26.0", Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc123"}, {Key: "vcs.modified", Value: "false"}}}
	state, revision, clean, status := invocationProvenance("/tmp/scale", info)
	if revision != "abc123" || !clean || status != "clean (embedded build metadata)" || !strings.Contains(state, "executable=/tmp/scale") || !strings.Contains(state, "vcs_revision=abc123") {
		t.Fatalf("embedded provenance state=%q revision=%q clean=%v status=%q", state, revision, clean, status)
	}
	state, revision, clean, status = invocationProvenance("/tmp/scale", &debug.BuildInfo{})
	if revision != "" || clean || status != "unknown (incomplete embedded VCS metadata)" || !strings.Contains(state, "vcs=unknown") {
		t.Fatalf("incomplete metadata did not fail closed: state=%q revision=%q clean=%v status=%q", state, revision, clean, status)
	}
}

func TestStrictQueryFailurePersistsPartialEvidence4327(t *testing.T) {
	outDir := t.TempDir()
	cfg := config{
		outDir:         outDir,
		rows:           96,
		batchSize:      48,
		dims:           4,
		topK:           5,
		candidateLimit: -1,
		queries:        2,
		includeVector:  false,
		selectedPhases: map[string]bool{"load": true, "queries": true},
	}
	rep, err := run(cfg)
	if err == nil || !strings.Contains(err.Error(), "warm hybrid_text_only_no_docs") {
		t.Fatalf("strict query failure=%v want preserved hybrid error", err)
	}
	if rep.Complete || strings.Join(rep.CompletedPhases, ",") != "load" {
		t.Fatalf("strict failure qualified report: %+v", rep)
	}
	if len(rep.Queries) != 5 || len(rep.Guardrails) != 5 {
		t.Fatalf("partial query evidence rows/guards=%d/%d want 5/5", len(rep.Queries), len(rep.Guardrails))
	}
	for _, row := range rep.Queries[:4] {
		if len(row.RawLatencyNS) != cfg.queries {
			t.Fatalf("prior row %q lost raw samples=%d want %d", row.Name, len(row.RawLatencyNS), cfg.queries)
		}
	}
	failed := rep.Queries[len(rep.Queries)-1]
	if failed.Name != "hybrid_text_only_no_docs" || failed.GuardrailOK || failed.HybridStats == nil || failed.HybridStats.FailClosed == 0 || failed.GuardrailFailure == "" {
		t.Fatalf("failed query evidence=%+v", failed)
	}
	payload, readErr := os.ReadFile(rep.Artifacts.JSONReport)
	if readErr != nil {
		t.Fatalf("read strict partial json: %v", readErr)
	}
	var persisted report
	if err := json.Unmarshal(payload, &persisted); err != nil {
		t.Fatalf("unmarshal strict partial json: %v", err)
	}
	if persisted.Complete || strings.Join(persisted.CompletedPhases, ",") != "load" || len(persisted.Queries) != len(rep.Queries) || len(persisted.Guardrails) != len(rep.Guardrails) {
		t.Fatalf("persisted strict evidence incomplete/lost: %+v", persisted)
	}
	markdown, readErr := os.ReadFile(rep.Artifacts.Markdown)
	if readErr != nil || !strings.Contains(string(markdown), "INCOMPLETE (partial evidence; not a completed qualification)") || !strings.Contains(string(markdown), failed.Name) || !strings.Contains(string(markdown), failed.GuardrailFailure) {
		t.Fatalf("markdown did not preserve strict query evidence: err=%v content=%s", readErr, markdown)
	}
}

func TestGuardrailFailurePersistsIncompletePhase4327(t *testing.T) {
	outDir := t.TempDir()
	rep := report{
		SchemaVersion:  scaleSchemaVersion,
		Artifacts:      reportArtifacts{OutDir: outDir, JSONReport: filepath.Join(outDir, "scale_report.json"), Markdown: filepath.Join(outDir, "scale_report.md")},
		SelectedPhases: []string{"load", "queries"}, CompletedPhases: []string{"load"},
		Queries: []queryReport{{Name: "failed_query"}}, Guardrails: []guardrailResult{{Name: "queries", OK: false, Failure: "fail closed"}},
	}
	err := completeGuardedPhase(&rep, "queries", rep.Guardrails, false)
	if err == nil || !strings.Contains(err.Error(), "guardrail queries failed: fail closed") || rep.Complete || strings.Join(rep.CompletedPhases, ",") != "load" {
		t.Fatalf("failed guardrail marked phase complete or lost strict error: report=%+v err=%v", rep, err)
	}
	payload, readErr := os.ReadFile(rep.Artifacts.JSONReport)
	if readErr != nil {
		t.Fatalf("read partial report: %v", readErr)
	}
	var persisted report
	if err := json.Unmarshal(payload, &persisted); err != nil || persisted.Complete || strings.Join(persisted.CompletedPhases, ",") != "load" || len(persisted.Guardrails) != 1 {
		t.Fatalf("persisted report was not honest partial evidence: report=%+v err=%v", persisted, err)
	}
}

func TestAllowedGuardrailFailurePersistsIncompletePhase4327(t *testing.T) {
	outDir := t.TempDir()
	rep := report{
		SchemaVersion: scaleSchemaVersion,
		Artifacts: reportArtifacts{
			OutDir:     outDir,
			JSONReport: filepath.Join(outDir, "scale_report.json"),
			Markdown:   filepath.Join(outDir, "scale_report.md"),
		},
		SelectedPhases:  []string{"load", "queries", "reopen"},
		CompletedPhases: []string{"load"},
		Queries:         []queryReport{{Name: "failed_query"}},
		Guardrails:      []guardrailResult{{Name: "queries", OK: false, Failure: "fail closed"}},
	}
	if err := completeGuardedPhase(&rep, "queries", rep.Guardrails, true); err != nil {
		t.Fatalf("allowed diagnostic guardrail failure stopped execution: %v", err)
	}
	if err := completePhase(&rep, "reopen"); err != nil {
		t.Fatalf("complete eligible later phase: %v", err)
	}
	if rep.Complete || strings.Join(rep.CompletedPhases, ",") != "load,reopen" {
		t.Fatalf("allowed failed guardrail qualified report: %+v", rep)
	}
	payload, err := os.ReadFile(rep.Artifacts.JSONReport)
	if err != nil {
		t.Fatalf("read persisted diagnostic report: %v", err)
	}
	var persisted report
	if err := json.Unmarshal(payload, &persisted); err != nil || persisted.Complete || strings.Join(persisted.CompletedPhases, ",") != "load,reopen" || len(persisted.Guardrails) != 1 || persisted.Guardrails[0].OK {
		t.Fatalf("persisted allowed diagnostic report was qualified: report=%+v err=%v", persisted, err)
	}
}

func TestTextFailureRowsPreserveEvidence4327(t *testing.T) {
	resp := collections.TextSearchResponse{Stats: collections.TextSearchStats{FailClosed: 1, FailClosedReason: "text_index_unavailable"}}
	durations := []int64{11, 17}
	row := failedTextQueryRow(config{rows: 1_000_000, topK: 10}, "text_common", "common", resp, durations, errors.New("bounded generation failed"))
	if row.GuardrailOK || row.GuardrailFailure == "" {
		t.Fatalf("row guardrail=%v failure=%q", row.GuardrailOK, row.GuardrailFailure)
	}
	if row.TextStats == nil || row.TextStats.FailClosed != 1 || row.TextStats.FailClosedReason != "text_index_unavailable" {
		t.Fatalf("row stats=%+v want fail-closed stats preserved", row.TextStats)
	}
	if row.Samples != len(durations) || len(row.RawLatencyNS) != len(durations) || row.Results != 0 || row.Rows != 1_000_000 || row.CandidateBudget != 1_000_000 {
		t.Fatalf("row metadata=%+v", row)
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
	if err := completeGuardedPhase(&rep, "queries", []guardrailResult{guard}, false); err == nil {
		t.Fatal("completeGuardedPhase accepted a failed guardrail")
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
	durations := []int64{11, 17}
	row := failedHybridQueryRow(config{rows: 1_000_000, topK: 10, candidateLimit: 64}, "hybrid_common", "common", resp, durations, errors.New("bounded generation failed"))
	if row.GuardrailOK || row.GuardrailFailure == "" {
		t.Fatalf("row guardrail=%v failure=%q", row.GuardrailOK, row.GuardrailFailure)
	}
	if row.HybridStats == nil || row.HybridStats.FailClosed != 1 || row.HybridStats.FailClosedReason != collections.HybridFailClosedReasonTextIndexUnavailable {
		t.Fatalf("row stats=%+v want fail-closed stats preserved", row.HybridStats)
	}
	if row.Samples != len(durations) || len(row.RawLatencyNS) != len(durations) || row.Results != 0 || row.Rows != 1_000_000 || row.CandidateBudget != 64 {
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
		{name: "maintenance DB descendant", args: []string{"-cpu-profile", filepath.Join(outDir, "maintenance_db", "cpu.pprof")}, wantErr: "maintenance database directory"},
		{name: "backfill DB descendant", args: []string{"-alloc-profile", filepath.Join(outDir, "backfill_db", "profiles", "allocs.pprof")}, wantErr: "backfill database directory"},
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
			wantCPU, wantMem := tc.wantCPU, tc.wantMem
			if wantCPU != "" {
				wantCPU, err = resolvePathSymlinks(wantCPU)
			}
			if err == nil && wantMem != "" {
				wantMem, err = resolvePathSymlinks(wantMem)
			}
			if err != nil {
				t.Fatalf("resolve expected profile path: %v", err)
			}
			if cfg.cpuProfile != wantCPU || cfg.allocProfile != wantMem {
				t.Fatalf("profiles cpu=%q allocs=%q want cpu=%q allocs=%q", cfg.cpuProfile, cfg.allocProfile, wantCPU, wantMem)
			}
		})
	}
}

func TestProfilePathsResolveSymlinkAliases4327(t *testing.T) {
	root := t.TempDir()
	outDir := filepath.Join(root, "out")
	primaryDir := filepath.Join(outDir, "primary_db")
	if err := os.MkdirAll(primaryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	dbAlias := filepath.Join(outDir, "profile-link")
	if err := os.Symlink(primaryDir, dbAlias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := parseFlags([]string{"-out-dir", outDir, "-query-rows", queryRowHybridTextScalar, "-cpu-profile", filepath.Join(dbAlias, "cpu.pprof")}); err == nil || !strings.Contains(err.Error(), "effective -db-dir") {
		t.Fatalf("database symlink alias error=%v", err)
	}
	maintenanceTarget := filepath.Join(root, "maintenance-target")
	if err := os.MkdirAll(maintenanceTarget, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(maintenanceTarget, filepath.Join(outDir, "maintenance_db")); err != nil {
		t.Fatal(err)
	}
	if _, err := parseFlags([]string{"-out-dir", outDir, "-query-rows", queryRowHybridTextScalar, "-cpu-profile", filepath.Join(maintenanceTarget, "cpu.pprof")}); err == nil || !strings.Contains(err.Error(), "maintenance database directory") {
		t.Fatalf("reserved database symlink alias error=%v", err)
	}

	reportAlias := filepath.Join(root, "report-link.pprof")
	if err := os.Symlink(filepath.Join(outDir, "scale_report.json"), reportAlias); err != nil {
		t.Fatal(err)
	}
	if _, err := parseFlags([]string{"-out-dir", outDir, "-query-rows", queryRowHybridTextScalar, "-cpu-profile", reportAlias}); err == nil || !strings.Contains(err.Error(), "scale report artifact") {
		t.Fatalf("report symlink alias error=%v", err)
	}
}
func TestProfilePathsRejectExistingHardLinks4327(t *testing.T) {
	root := t.TempDir()
	cpuPath := filepath.Join(root, "cpu.pprof")
	allocPath := filepath.Join(root, "allocs.pprof")
	if err := os.WriteFile(cpuPath, []byte("existing profile"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(cpuPath, allocPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	_, err := parseFlags([]string{"-out-dir", filepath.Join(root, "out"), "-query-rows", queryRowHybridTextScalar, "-cpu-profile", cpuPath, "-alloc-profile", allocPath})
	if err == nil || !strings.Contains(err.Error(), "destination must not already exist") {
		t.Fatalf("hard-linked profile destinations error=%v", err)
	}
}

func TestProfileFilesUseExclusiveCreation4327(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	f, err := createProfileFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("first run"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := createProfileFile(path); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second profile create error=%v want os.ErrExist", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "first run" {
		t.Fatalf("exclusive create changed existing profile: content=%q err=%v", got, err)
	}
}

func TestProfileFinalizationAttemptsBothOutputs4327(t *testing.T) {
	cpuErr := errors.New("close CPU profile")
	allocErr := errors.New("write allocation profile")
	calls := 0
	err := finalizeQueryProfiles(func() error {
		calls++
		return cpuErr
	}, func() error {
		calls++
		return allocErr
	})
	if calls != 2 || !errors.Is(err, cpuErr) || !errors.Is(err, allocErr) {
		t.Fatalf("finalization calls=%d err=%v", calls, err)
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
	baseInfo, err := os.Stat(alloc + ".base")
	if err != nil || baseInfo.Size() == 0 {
		t.Fatalf("baseline profile %q info=%v err=%v", alloc+".base", baseInfo, err)
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
		"-include-vector=true", "-phases=retrieval", "-run-reopen=false",
		"-query-rows", queryRowHybridTextVector, "-alloc-profile", alloc,
	)
	cmd.Env = append(os.Environ(), "GOWORK=off", "GODEBUG=memprofilerate=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("small allocation-profile CLI smoke: %v\n%s", err, output)
	}
	info, err := os.Stat(alloc)
	if err != nil || info.Size() == 0 {
		t.Fatalf("allocation profile %q info=%v err=%v", alloc, info, err)
	}
	payload, err := os.ReadFile(filepath.Join(outDir, "scale_report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rep report
	if err := json.Unmarshal(payload, &rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Queries) != 1 || rep.Queries[0].AllocBytes == 0 || rep.Queries[0].AllocObjects == 0 || rep.Queries[0].BytesPerOp == 0 || rep.Queries[0].AllocsPerOp == 0 {
		t.Fatalf("missing exact timed allocation counters: %+v", rep.Queries)
	}
	pprof := exec.Command("go", "tool", "pprof", "-top", "-base", alloc+".base", "-ignore=runtime/pprof", "-focus=SearchHybrid|searchHybridVectorCandidatesWithAllowSetBudget", alloc)
	output, err := pprof.CombinedOutput()
	if err != nil {
		t.Fatalf("focused allocation profile: %v\n%s", err, output)
	}
	top := string(output)
	if !strings.Contains(top, "SearchHybrid") || !strings.Contains(top, "searchHybridVectorCandidatesWithAllowSetBudget") {
		t.Fatalf("differential profile lacks timed hybrid caller or vector worker path:\n%s", top)
	}
	if strings.Contains(top, "loadPrimaryFixture") || strings.Contains(top, "makeScaleBatch") {
		t.Fatalf("differential profile retained fixture construction:\n%s", top)
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
