package main

import (
	"bytes"
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

func TestRunFixtureKeepsTemplateV1ThreeIndexDatabase(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fixture")
	cfg, err := parseConfig([]string{
		"-dir", dir,
		"-docs", "16",
		"-batch-size", "5",
		"-indexes", "3",
		"-progress=false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	summary, err := runFixture(cfg)
	if err != nil {
		t.Fatalf("run fixture: %v", err)
	}
	if summary.IndexCount != 3 {
		t.Fatalf("index count=%d want 3", summary.IndexCount)
	}
	if summary.Verify.Samples == 0 {
		t.Fatal("expected reopen verification samples")
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

func TestContainsDocumentID(t *testing.T) {
	ids := [][]byte{[]byte("u-000000001"), []byte("u-000000064")}
	if !containsDocumentID(ids, []byte("u-000000064")) {
		t.Fatal("expected matching id")
	}
	if containsDocumentID(ids, []byte("u-000000002")) {
		t.Fatal("unexpected non-matching id")
	}
}

func TestTemplateV1StoredDocumentExtractsInputEnvelope(t *testing.T) {
	var encoder collections.TemplateV1Encoder
	raw, err := document(collections.DocumentFormatTemplateV1, &encoder, 7)
	if err != nil {
		t.Fatalf("document: %v", err)
	}
	stored, err := templateV1StoredDocument(raw)
	if err != nil {
		t.Fatalf("stored document: %v", err)
	}
	if !bytes.HasPrefix(stored, []byte("TD1D")) {
		t.Fatalf("stored document prefix=%q want TD1D", string(stored[:4]))
	}
	if bytes.HasPrefix(stored, []byte("TD1I")) {
		t.Fatal("expected template-v1 input envelope to be stripped")
	}
	again, err := templateV1StoredDocument(stored)
	if err != nil {
		t.Fatalf("stored document idempotence: %v", err)
	}
	if !bytes.Equal(again, stored) {
		t.Fatal("stored document conversion changed an already-stored payload")
	}
}
