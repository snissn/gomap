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
	if err != nil || !strings.Contains(string(markdown), "INCOMPLETE (partial/resumable evidence; not a completed qualification)") {
		t.Fatalf("markdown did not fail closed: err=%v content=%s", err, markdown)
	}
}

func TestCaptureContextUsesInvocationProvenance4327(t *testing.T) {
	ctx := captureContext(config{baseRef: "origin/main"})
	if ctx.VCSStatus == "" || ctx.BinaryState == "" || ctx.Command == "" || ctx.Corpus == "" || ctx.Cache == "" || ctx.Durability == "" || ctx.NoisePolicy == "" {
		t.Fatalf("missing provenance: %+v", ctx)
	}
	if ctx.RepoRoot != "" || ctx.Branch != "" || ctx.Commit != "" {
		t.Fatalf("context used ambient checkout state: %+v", ctx)
	}
}

func TestInvocationProvenanceUsesEmbeddedBuildMetadata4327(t *testing.T) {
	info := &debug.BuildInfo{Main: debug.Module{Path: "example.com/scale"}, GoVersion: "go1.26.0", Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc123"}, {Key: "vcs.modified", Value: "false"}}}
	state, clean, status := invocationProvenance("/tmp/scale", info)
	if !clean || status != "clean (embedded build metadata)" || !strings.Contains(state, "executable=/tmp/scale") || !strings.Contains(state, "vcs_revision=abc123") {
		t.Fatalf("embedded provenance state=%q clean=%v status=%q", state, clean, status)
	}
	state, clean, status = invocationProvenance("/tmp/scale", &debug.BuildInfo{})
	if clean || status != "unknown (incomplete embedded VCS metadata)" || !strings.Contains(state, "vcs=unknown") {
		t.Fatalf("incomplete metadata did not fail closed: state=%q clean=%v status=%q", state, clean, status)
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
	if err == nil || rep.Complete || strings.Join(rep.CompletedPhases, ",") != "load" {
		t.Fatalf("failed guardrail marked phase complete: report=%+v err=%v", rep, err)
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
		name, phases string
		wantErr      bool
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
