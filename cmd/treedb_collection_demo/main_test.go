package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

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
