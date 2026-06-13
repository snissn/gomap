package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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

func TestScaleCommandFlagValidation2731(t *testing.T) {
	if _, err := parseFlags([]string{"-out-dir", t.TempDir(), "-rows", "0"}); err == nil {
		t.Fatal("parseFlags accepted zero rows")
	}
	cfg, err := parseFlags([]string{"-out-dir", t.TempDir(), "-rows", "10", "-include-vector=false", "-run-backfill=false", "-run-rewrite=false", "-run-concurrent=false", "-run-reopen=false"})
	if err != nil {
		t.Fatalf("parseFlags valid args: %v", err)
	}
	if cfg.includeVector || cfg.runBackfill || cfg.runRewrite || cfg.runConcurrent || cfg.runReopen {
		t.Fatalf("bool flags not parsed: %+v", cfg)
	}
}
