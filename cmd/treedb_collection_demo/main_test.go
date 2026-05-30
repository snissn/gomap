package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseConfigRejectsPositionals(t *testing.T) {
	_, err := parseConfig([]string{"unexpected"})
	if err == nil || !strings.Contains(err.Error(), "unexpected positional arguments") {
		t.Fatalf("err=%v want unexpected positional rejection", err)
	}
}

func TestParseConfigPresetOverride(t *testing.T) {
	cfg, err := parseConfig([]string{"-preset", "event-analytics", "-rows", "123", "-batch-size", "7", "-mode", "document"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.Preset != "event-analytics" || cfg.Workload != workloadRangeAggregate {
		t.Fatalf("preset not applied: %+v", cfg)
	}
	if cfg.Rows != 123 || cfg.BatchSize != 7 || cfg.Mode != modeDocument {
		t.Fatalf("explicit flags did not override preset: %+v", cfg)
	}
}

func TestGenerateFixtureRowsDeterministic(t *testing.T) {
	cfg := defaultConfig()
	cfg.Rows = 8
	cfg.PayloadBytes = 17
	cfg.ExtraFields = 3
	cfg.Int64Distribution = "random"
	cfg.Seed = 99

	first := generateFixtureRows(cfg)
	second := generateFixtureRows(cfg)
	if len(first) != len(second) {
		t.Fatalf("length mismatch")
	}
	for i := range first {
		if !bytes.Equal(first[i].ID, second[i].ID) || !bytes.Equal(first[i].Doc, second[i].Doc) || first[i].TimeUS != second[i].TimeUS {
			t.Fatalf("row %d differs:\n%+v\n%+v", i, first[i], second[i])
		}
	}
}

func TestDeterministicPayloadHandlesNegativeSeed(t *testing.T) {
	got := deterministicPayload(64, -99, 3)
	if len(got) != 64 {
		t.Fatalf("payload len=%d want 64", len(got))
	}
	if got != deterministicPayload(64, -99, 3) {
		t.Fatalf("negative-seed payload is not deterministic")
	}
}

func TestAggregateParityAcrossModes(t *testing.T) {
	modes := []string{modeDocument, modeTypedRow, modeTypedColumn, modeHybridDocumentRow, modeHybridDocumentCol, modeHybridRowColumn}
	var wantCount, wantSum int64
	for i, mode := range modes {
		cfg := defaultConfig()
		cfg.Mode = mode
		cfg.Rows = 48
		cfg.BatchSize = 16
		cfg.PayloadBytes = 8
		cfg.ExtraFields = 1
		cfg.Dir = filepath.Join(t.TempDir(), "db")
		got, err := runDemo(cfg)
		if err != nil {
			t.Fatalf("runDemo mode=%s: %v", mode, err)
		}
		if i == 0 {
			wantCount, wantSum = got.Aggregate.Count, got.Aggregate.Sum
			continue
		}
		if got.Aggregate.Count != wantCount || got.Aggregate.Sum != wantSum {
			t.Fatalf("mode=%s aggregate count=%d sum=%d want count=%d sum=%d", mode, got.Aggregate.Count, got.Aggregate.Sum, wantCount, wantSum)
		}
	}
}

func TestReopenReadSmoke(t *testing.T) {
	cfg := defaultConfig()
	cfg.Mode = modeTypedColumn
	cfg.Workload = workloadReopenRead
	cfg.Rows = 64
	cfg.BatchSize = 32
	cfg.PayloadBytes = 8
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	got, err := runDemo(cfg)
	if err != nil {
		t.Fatalf("runDemo reopen-read: %v", err)
	}
	if got.Matches == 0 || got.Aggregate.Count == 0 {
		t.Fatalf("unexpected reopen-read result: %+v", got)
	}
}

func TestInsertWorkloadReportsSetupThroughput(t *testing.T) {
	cfg := defaultConfig()
	cfg.Mode = modeTypedColumn
	cfg.Workload = workloadInsert
	cfg.Rows = 32
	cfg.BatchSize = 16
	cfg.PayloadBytes = 8
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	got, err := runDemo(cfg)
	if err != nil {
		t.Fatalf("runDemo insert: %v", err)
	}
	if got.Ops != int64(cfg.Rows) || got.OpsSec <= 0 || got.RowsSec <= 0 {
		t.Fatalf("insert counters not populated from setup timing: %+v", got)
	}
	if got.QueryMS < 0 {
		t.Fatalf("query_ms should not be negative: %+v", got)
	}
}

func TestExplicitDirMustBeFresh(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	cfg := defaultConfig()
	cfg.Rows = 8
	cfg.Dir = dir
	if _, err := runDemo(cfg); err == nil {
		t.Fatal("runDemo accepted non-empty explicit dir")
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

func TestProfileArtifactsCreated(t *testing.T) {
	profileDir := t.TempDir()
	cfg := defaultConfig()
	cfg.Mode = modeTypedColumn
	cfg.Rows = 128
	cfg.BatchSize = 64
	cfg.PayloadBytes = 8
	cfg.Dir = filepath.Join(t.TempDir(), "db")
	cfg.ProfileDir = profileDir
	if _, err := runDemo(cfg); err != nil {
		t.Fatalf("runDemo profile: %v", err)
	}
	for _, name := range []string{"cpu.pprof", "allocs.pprof", "summary.json", "summary.md"} {
		path := filepath.Join(profileDir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("missing profile artifact %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile artifact %s is empty", name)
		}
	}
}
