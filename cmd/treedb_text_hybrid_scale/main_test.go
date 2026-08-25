package main

import (
	"encoding/json"
	"errors"
	"os"
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

func TestHybridFailureRowsPreserveFailClosedStats2731(t *testing.T) {
	resp := collections.HybridSearchResponse{Stats: collections.HybridSearchStats{FailClosed: 1, FailClosedReason: collections.HybridFailClosedReasonTextIndexUnavailable}}
	row := failedHybridQueryRow(config{rows: 1_000_000, topK: 10, candidateLimit: 64}, "hybrid_common", "common", resp, nil, errors.New("bounded generation failed"))
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
			if cfg.cpuProfile != tc.wantCPU || cfg.allocProfile != tc.wantAllocs {
				t.Fatalf("profiles cpu=%q allocs=%q want cpu=%q allocs=%q", cfg.cpuProfile, cfg.allocProfile, tc.wantCPU, tc.wantAllocs)
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
}
