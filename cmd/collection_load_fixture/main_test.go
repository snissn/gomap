package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
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
	if cfg.KeepRecent != 1 {
		t.Fatalf("keep_recent=%d want 1", cfg.KeepRecent)
	}
	if cfg.PreferAppendAlloc {
		t.Fatal("expected prefer append allocation to be disabled by default")
	}
	if cfg.IndexVacuum != indexVacuumModeAuto {
		t.Fatalf("index vacuum mode=%q want auto", cfg.IndexVacuum)
	}
}

func TestParseConfigRejectsExplicitEmptyFormat(t *testing.T) {
	if _, err := parseConfig([]string{"-format", ""}, io.Discard); err == nil {
		t.Fatal("expected explicit empty -format to fail")
	}
}

func TestParseConfigRejectsInvalidIndexVacuumMode(t *testing.T) {
	if _, err := parseConfig([]string{"-index-vacuum", "sometimes"}, io.Discard); err == nil {
		t.Fatal("expected invalid -index-vacuum to fail")
	}
}

func TestEffectiveIndexVacuumAutoOnlyRunsAfterRewriteMaintenance(t *testing.T) {
	if got := effectiveIndexVacuumMode(config{IndexVacuum: indexVacuumModeAuto}); got != indexVacuumModeNone {
		t.Fatalf("auto without rewrite maintenance=%q want none", got)
	}
	if got := effectiveIndexVacuumMode(config{IndexVacuum: indexVacuumModeAuto, ValueLogRewrite: true}); got != indexVacuumModeOffline {
		t.Fatalf("auto with value-log rewrite=%q want offline", got)
	}
	if got := effectiveIndexVacuumMode(config{IndexVacuum: indexVacuumModeAuto, LeafGenerationPackGC: true}); got != indexVacuumModeOffline {
		t.Fatalf("auto with leafgen pack=%q want offline", got)
	}
	if got := effectiveIndexVacuumMode(config{IndexVacuum: indexVacuumModeNone, LeafGenerationPackGC: true}); got != indexVacuumModeNone {
		t.Fatalf("explicit none with leafgen pack=%q want none", got)
	}
}

func TestReopenVerifyReadOnlyRequiresFinalCheckpoint(t *testing.T) {
	if !reopenVerifyReadOnly(config{Checkpoint: true}) {
		t.Fatal("expected read-only reopen verification after final checkpoint")
	}
	if reopenVerifyReadOnly(config{Checkpoint: false}) {
		t.Fatal("expected read-write reopen verification without final checkpoint for WAL replay")
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
	if summary.IndexStorageFinal.IndexDBBytes == 0 {
		t.Fatal("expected final index storage summary")
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

func TestRunFixtureReportsBufferedIndexedWritesOnlyWhenIndexesUseThem(t *testing.T) {
	noIndexDir := filepath.Join(t.TempDir(), "no-index")
	noIndexCfg, err := parseConfig([]string{
		"-dir", noIndexDir,
		"-docs", "8",
		"-batch-size", "4",
		"-indexes", "0",
		"-buffered-indexed-writes",
		"-buffered-indexed-write-max-docs", "16",
		"-buffered-indexed-write-max-bytes", "1024",
		"-reopen-verify=false",
		"-progress=false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse no-index config: %v", err)
	}
	noIndexSummary, err := runFixture(noIndexCfg)
	if err != nil {
		t.Fatalf("run no-index fixture: %v", err)
	}
	if noIndexSummary.BufferedIndexedWrites {
		t.Fatal("no-index summary reported buffered indexed writes enabled")
	}
	if noIndexSummary.BufferedIndexedWriteMaxDocs != 0 || noIndexSummary.BufferedIndexedWriteMaxBytes != 0 {
		t.Fatalf("no-index buffered limits docs=%d bytes=%d want both zero", noIndexSummary.BufferedIndexedWriteMaxDocs, noIndexSummary.BufferedIndexedWriteMaxBytes)
	}

	indexedDir := filepath.Join(t.TempDir(), "indexed")
	indexedCfg, err := parseConfig([]string{
		"-dir", indexedDir,
		"-docs", "8",
		"-batch-size", "4",
		"-indexes", "1",
		"-buffered-indexed-writes",
		"-buffered-indexed-write-max-docs", "16",
		"-buffered-indexed-write-max-bytes", "1024",
		"-progress=false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse indexed config: %v", err)
	}
	indexedSummary, err := runFixture(indexedCfg)
	if err != nil {
		t.Fatalf("run indexed fixture: %v", err)
	}
	if !indexedSummary.BufferedIndexedWrites {
		t.Fatal("indexed summary reported buffered indexed writes disabled")
	}
	if indexedSummary.BufferedIndexedWriteMaxDocs != 16 || indexedSummary.BufferedIndexedWriteMaxBytes != 1024 {
		t.Fatalf("indexed buffered limits docs=%d bytes=%d want 16/1024", indexedSummary.BufferedIndexedWriteMaxDocs, indexedSummary.BufferedIndexedWriteMaxBytes)
	}
}

func TestLeafGenerationSummaryOmittedWhenDisabled(t *testing.T) {
	leafGeneration, err := maybePackLeafGenerations(config{}, nil)
	if err != nil {
		t.Fatalf("maybePackLeafGenerations disabled: %v", err)
	}
	if leafGeneration != nil {
		t.Fatalf("disabled leaf generation summary=%+v, want nil", leafGeneration)
	}
	blob, err := json.Marshal(loadSummary{LeafGeneration: leafGeneration})
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	if bytes.Contains(blob, []byte("leaf_generation")) {
		t.Fatalf("leaf_generation should be omitted when disabled: %s", string(blob))
	}
}

func TestRunFixtureLeafGenerationReportsDiskTotals(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fixture")
	cfg, err := parseConfig([]string{
		"-dir", dir,
		"-docs", "24",
		"-batch-size", "24",
		"-leafgen-pack-gc",
		"-progress=false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	summary, err := runFixture(cfg)
	if err != nil {
		t.Fatalf("run fixture: %v", err)
	}
	if summary.LeafGeneration == nil || !summary.LeafGeneration.Enabled {
		t.Fatal("expected enabled leaf generation summary")
	}
	if summary.LeafGeneration.DiskBytesAfterPack == 0 {
		t.Fatal("expected non-zero disk bytes after leaf-generation pack path")
	}
	if summary.LeafGeneration.DiskBytesAfterGC == 0 {
		t.Fatal("expected non-zero disk bytes after leaf-generation GC")
	}
	if summary.LeafGeneration.CandidateGenerations == 0 && summary.LeafGeneration.DiskBytesAfterPack != summary.LeafGeneration.DiskBytesBefore {
		t.Fatalf("no-candidate after pack bytes=%d want before bytes=%d", summary.LeafGeneration.DiskBytesAfterPack, summary.LeafGeneration.DiskBytesBefore)
	}
	if !summary.IndexVacuum.Enabled || summary.IndexVacuum.Mode != indexVacuumModeOffline || summary.IndexVacuum.RequestedMode != indexVacuumModeAuto {
		t.Fatalf("expected auto offline index vacuum after leafgen pack, got %+v", summary.IndexVacuum)
	}
}

func TestVacuumIndexOfflinePreservesTemplateV1TwoIndexDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("loads a large enough fixture to exercise multi-batch collection roots")
	}

	dir := filepath.Join(t.TempDir(), "fixture")
	cfg, err := parseConfig([]string{
		"-dir", dir,
		"-docs", "100000",
		"-batch-size", "80",
		"-verify-samples", "16",
		"-progress=false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}

	summary, err := runFixture(cfg)
	if err != nil {
		t.Fatalf("run fixture: %v", err)
	}
	if summary.IndexCount != 2 {
		t.Fatalf("index count=%d want 2", summary.IndexCount)
	}

	indexPath := filepath.Join(dir, "maindb", "index.db")
	beforeVacuum := regularFileSize(t, indexPath)
	if beforeVacuum == 0 {
		t.Fatal("expected non-empty index.db before vacuum")
	}

	if samples, err := verifyReopen(cfg); err != nil {
		t.Fatalf("pre-vacuum smoke verify: %v", err)
	} else if samples == 0 {
		t.Fatal("expected pre-vacuum smoke verification samples")
	}

	if err := treedb.VacuumIndexOffline(treedb.Options{Dir: dir}); err != nil {
		t.Fatalf("vacuum index offline: %v", err)
	}

	afterVacuum := regularFileSize(t, indexPath)
	if afterVacuum == 0 {
		t.Fatal("expected non-empty index.db after vacuum")
	}
	t.Logf("index.db vacuum size: before=%d after=%d", beforeVacuum, afterVacuum)

	if samples, err := verifyReopen(cfg); err != nil {
		t.Fatalf("post-vacuum smoke verify: %v", err)
	} else if samples == 0 {
		t.Fatal("expected post-vacuum smoke verification samples")
	}
}

func TestVacuumIndexOnlinePreservesTemplateV1TwoIndexDatabase(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	if testing.Short() {
		t.Skip("loads a large enough fixture to exercise multi-batch collection roots")
	}

	dir := filepath.Join(t.TempDir(), "fixture")
	cfg, err := parseConfig([]string{
		"-dir", dir,
		"-docs", "100000",
		"-batch-size", "80",
		"-verify-samples", "16",
		"-progress=false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}

	if _, err := runFixture(cfg); err != nil {
		t.Fatalf("run fixture: %v", err)
	}

	backend, cleanup, err := openBackend(cfg)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	if err := backend.VacuumIndexOnline(context.Background()); err != nil {
		_ = cleanup()
		t.Fatalf("vacuum index online: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	if samples, err := verifyReopen(cfg); err != nil {
		t.Fatalf("post-online-vacuum smoke verify: %v", err)
	} else if samples == 0 {
		t.Fatal("expected post-online-vacuum smoke verification samples")
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

func regularFileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("%s is not a regular file", path)
	}
	return info.Size()
}
