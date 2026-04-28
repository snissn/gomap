package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestParseConfigDefaultsToInspectableTwoIndexTemplateFixture(t *testing.T) {
	cfg, err := parseConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("parse defaults: %v", err)
	}
	if cfg.DocumentFormat != collections.DocumentFormatTemplateV1 {
		t.Fatalf("document format=%q want template-v1", cfg.DocumentFormat)
	}
	if cfg.IndexCount != 2 {
		t.Fatalf("index count=%d want 2", cfg.IndexCount)
	}
	if !cfg.DataOuterLeavesInValueLog {
		t.Fatal("expected data outer leaves in value log by default")
	}
	if !cfg.IndexOuterLeavesInValueLog {
		t.Fatal("expected index outer leaves in value log by default")
	}
	if !cfg.Checkpoint || !cfg.ReopenVerify {
		t.Fatalf("checkpoint=%t reopen_verify=%t want both true", cfg.Checkpoint, cfg.ReopenVerify)
	}
}

func TestParseConfigRejectsExplicitEmptyFormat(t *testing.T) {
	if _, err := parseConfig([]string{"-format", ""}, io.Discard); err == nil {
		t.Fatal("expected explicit empty -format to fail")
	}
}

func TestRunFixtureKeepsTemplateV1TwoIndexDatabase(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fixture")
	cfg, err := parseConfig([]string{
		"-dir", dir,
		"-docs", "24",
		"-batch-size", "7",
		"-progress=false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	summary, err := runFixture(cfg)
	if err != nil {
		t.Fatalf("run fixture: %v", err)
	}
	if summary.Dir != dir {
		t.Fatalf("dir=%q want %q", summary.Dir, dir)
	}
	if summary.Docs != 24 || summary.Batches != 4 {
		t.Fatalf("docs=%d batches=%d want docs=24 batches=4", summary.Docs, summary.Batches)
	}
	if summary.Verify.Samples == 0 {
		t.Fatal("expected reopen verification samples")
	}
	if summary.DiskUsageFinal.TotalBytes == 0 {
		t.Fatal("expected final disk usage")
	}
	if _, err := os.Stat(filepath.Join(dir, "maindb", "index.db")); err != nil {
		t.Fatalf("expected kept maindb/index.db: %v", err)
	}
}

func TestRunFixtureRejectsNonEmptyDirectoryUnlessReset(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sentinel"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	cfg, err := parseConfig([]string{"-dir", dir, "-docs", "1", "-progress=false"}, io.Discard)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if _, err := runFixture(cfg); err == nil {
		t.Fatal("expected non-empty fixture dir to fail without -reset")
	}
}
