package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestSummarizeLatencyNearestRank(t *testing.T) {
	summary := summarizeLatency([]time.Duration{
		10 * time.Microsecond,
		50 * time.Microsecond,
		20 * time.Microsecond,
		40 * time.Microsecond,
		30 * time.Microsecond,
	})
	if summary.P50 != 30 {
		t.Fatalf("p50=%v want 30", summary.P50)
	}
	if summary.P95 != 50 {
		t.Fatalf("p95=%v want 50", summary.P95)
	}
	if summary.P99 != 50 {
		t.Fatalf("p99=%v want 50", summary.P99)
	}
}

func TestCollectDiskSnapshotBreakdown(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.db"), []byte("1234"), 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "leaf_vlog"), 0o700); err != nil {
		t.Fatalf("mkdir leaf_vlog: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "leaf_vlog", "value.log"), []byte("123456"), 0o600); err != nil {
		t.Fatalf("write value log: %v", err)
	}

	snapshot, err := collectDiskSnapshot(dir)
	if err != nil {
		t.Fatalf("collect disk snapshot: %v", err)
	}
	if snapshot.TotalBytes != 10 {
		t.Fatalf("total=%d want 10", snapshot.TotalBytes)
	}
	if snapshot.Paths["index.db"] != 4 {
		t.Fatalf("index.db=%d want 4", snapshot.Paths["index.db"])
	}
	if snapshot.Paths["leaf_vlog"] != 6 {
		t.Fatalf("leaf_vlog=%d want 6", snapshot.Paths["leaf_vlog"])
	}
	if snapshot.Paths["leaf_vlog/value.log"] != 6 {
		t.Fatalf("leaf_vlog/value.log=%d want 6", snapshot.Paths["leaf_vlog/value.log"])
	}
}

func TestCollectDiskSnapshotRootLayoutBreakdown(t *testing.T) {
	dir := t.TempDir()
	mainDir := filepath.Join(dir, "maindb")
	if err := os.Mkdir(mainDir, 0o700); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "index.db"), []byte("1234"), 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}

	snapshot, err := collectDiskSnapshot(dir)
	if err != nil {
		t.Fatalf("collect disk snapshot: %v", err)
	}
	if snapshot.TotalBytes != 4 {
		t.Fatalf("total=%d want 4", snapshot.TotalBytes)
	}
	if snapshot.Paths["maindb"] != 4 {
		t.Fatalf("maindb=%d want 4", snapshot.Paths["maindb"])
	}
	if snapshot.Paths["maindb/index.db"] != 4 {
		t.Fatalf("maindb/index.db=%d want 4", snapshot.Paths["maindb/index.db"])
	}
}

func TestCollectDiskSnapshotEmptyLeavesPathsNil(t *testing.T) {
	snapshot, err := collectDiskSnapshot(t.TempDir())
	if err != nil {
		t.Fatalf("collect empty disk snapshot: %v", err)
	}
	if snapshot.TotalBytes != 0 || snapshot.Paths != nil {
		t.Fatalf("empty snapshot=%+v want zero total and nil paths", snapshot)
	}
}

func TestValidateResettableTreeDBDirRejectsDangerousPaths(t *testing.T) {
	for _, dir := range []string{"", ".", "..", string(os.PathSeparator), os.TempDir()} {
		if _, err := validateResettableTreeDBDir(dir); err == nil {
			t.Fatalf("validateResettableTreeDBDir(%q) err=nil want error", dir)
		}
	}
	if cwd, err := os.Getwd(); err == nil {
		if _, err := validateResettableTreeDBDir(filepath.Join(cwd, "unsafe-treedb")); err == nil {
			t.Fatal("validateResettableTreeDBDir accepted checkout child")
		}
	}
	safeRoot := t.TempDir()
	if realSafeRoot, err := filepath.EvalSymlinks(safeRoot); err == nil {
		safeRoot = realSafeRoot
	}
	safe := filepath.Join(safeRoot, "treedb")
	if got, err := validateResettableTreeDBDir(safe); err != nil || got == "" {
		t.Fatalf("validate safe dir got/err=%q/%v", got, err)
	}
}

func TestCloseBenchTargetIsIdempotent(t *testing.T) {
	var calls int32
	target := &benchTarget{
		cleanup: func(context.Context) error {
			atomic.AddInt32(&calls, 1)
			return nil
		},
	}

	if err := closeBenchTarget(context.Background(), target); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := closeBenchTarget(context.Background(), target); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("cleanup calls=%d want 1", got)
	}
	if target.cleanup != nil {
		t.Fatal("cleanup was not cleared")
	}
}

func TestCheckoutPathCandidatesIncludeResolvedCWD(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if err := os.Mkdir(realDir, 0o700); err != nil {
		t.Fatalf("mkdir real: %v", err)
	}
	linkDir := filepath.Join(root, "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	wantReal := realDir
	if evaluated, err := filepath.EvalSymlinks(realDir); err == nil {
		wantReal = evaluated
	}
	candidates := checkoutPathCandidates(linkDir)
	foundReal := false
	for _, candidate := range candidates {
		if candidate == wantReal {
			foundReal = true
		}
	}
	if !foundReal {
		t.Fatalf("checkoutPathCandidates(%q)=%v missing real dir %q", linkDir, candidates, wantReal)
	}
}

func TestIsPathDescendant(t *testing.T) {
	parent := filepath.Join("tmp", "repo")
	if !isPathDescendant(parent, filepath.Join(parent, "bench")) {
		t.Fatal("child path not recognized as descendant")
	}
	if isPathDescendant(parent, parent) {
		t.Fatal("parent path recognized as descendant")
	}
	if isPathDescendant(parent, filepath.Join("tmp", "repo-sibling")) {
		t.Fatal("sibling path recognized as descendant")
	}
}

func TestValidateResettableTreeDBDirRejectsSymlinkComponents(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := validateResettableTreeDBDir(filepath.Join(link, "treedb")); err == nil {
		t.Fatal("symlinked treedb-dir accepted")
	}
}

func TestUnsafeResetPathModeRejectsLinksAndReparsePoints(t *testing.T) {
	for _, mode := range []os.FileMode{os.ModeSymlink, os.ModeIrregular, os.ModeDir | os.ModeIrregular} {
		if !unsafeResetPathMode(mode) {
			t.Fatalf("unsafeResetPathMode(%v)=false want true", mode)
		}
	}
	if unsafeResetPathMode(os.ModeDir) {
		t.Fatal("plain directory marked unsafe")
	}
}

func TestRedactMongoURI(t *testing.T) {
	got := redactMongoURI("mongodb://user:secret@127.0.0.1:27017/db?authSource=admin")
	want := "mongodb://user@127.0.0.1:27017/db?authSource=admin"
	if got != want {
		t.Fatalf("redacted URI=%q want %q", got, want)
	}
}

func TestParseConfigValidation(t *testing.T) {
	if _, err := parseConfig([]string{"-bad"}); err == nil || !strings.Contains(err.Error(), "Usage of mongo_gateway_bench") {
		t.Fatalf("bad flag err=%v want usage", err)
	}
	if _, err := parseConfig([]string{"-target", "bad"}); err == nil {
		t.Fatal("bad target accepted")
	}
	cfg, err := parseConfig([]string{
		"-target", "mongo",
		"-documents", "10",
		"-secondary-indexes", "1",
		"-format", "json",
		"-concurrent-readers", "4",
		"-concurrent-reads", "20",
		"-concurrent-writers", "2",
		"-concurrent-writes", "10",
	})
	if err != nil {
		t.Fatalf("parse valid config: %v", err)
	}
	if cfg.Target != "mongo" || cfg.Documents != 10 || cfg.SecondaryIndexes != 1 || cfg.Format != "json" ||
		cfg.ConcurrentReaders != 4 || cfg.ConcurrentReads != 20 || cfg.ConcurrentWriters != 2 || cfg.ConcurrentWrites != 10 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if _, err := parseConfig([]string{"-timeout", "0"}); err != nil {
		t.Fatalf("timeout 0 should disable deadline: %v", err)
	}
	if _, err := parseConfig([]string{"-timeout", "-1s"}); err == nil {
		t.Fatal("negative timeout accepted")
	}
	if _, err := parseConfig([]string{"-concurrent-readers", "-1"}); err == nil {
		t.Fatal("negative concurrent-readers accepted")
	}
	if _, err := parseConfig([]string{"-concurrent-readers", "1"}); err == nil {
		t.Fatal("concurrent-readers without concurrent-reads accepted")
	}
	if _, err := parseConfig([]string{"-concurrent-reads", "1"}); err == nil {
		t.Fatal("concurrent-reads without concurrent-readers accepted")
	}
}

func TestParseConfigTreeDBCorrectnessDefaults(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parse defaults: %v", err)
	}
	if cfg.TreeDBProfile != treedb.ProfileWALOnFast {
		t.Fatalf("TreeDBProfile=%q want %q", cfg.TreeDBProfile, treedb.ProfileWALOnFast)
	}
	if got := string(cfg.TreeDBDocumentFormat); got != "template-v1" {
		t.Fatalf("TreeDBDocumentFormat=%q want template-v1", got)
	}
	if got := string(cfg.TreeDBDataRootStorage); got != "compressed" {
		t.Fatalf("TreeDBDataRootStorage=%q want compressed", got)
	}
	if got := string(cfg.TreeDBIndexStateRootStorage); got != "compressed" {
		t.Fatalf("TreeDBIndexStateRootStorage=%q want compressed", got)
	}
	if got := string(cfg.TreeDBIndexRootStorage); got != "compressed" {
		t.Fatalf("TreeDBIndexRootStorage=%q want compressed", got)
	}
	if cfg.TreeDBMaintenance != treeDBMaintenanceFull {
		t.Fatalf("TreeDBMaintenance=%q want %q", cfg.TreeDBMaintenance, treeDBMaintenanceFull)
	}
}

func TestTreeDBProfileSmokeFastAndWALOnFast(t *testing.T) {
	if testing.Short() {
		t.Skip("profile smoke benchmark skipped in short mode")
	}
	fast := runTreeDBProfileSmoke(t, treedb.ProfileFast)
	walOnFast := runTreeDBProfileSmoke(t, treedb.ProfileWALOnFast)
	ratio := fast / walOnFast
	if ratio < 1 {
		ratio = 1 / ratio
	}
	t.Logf("fast load_insert_many ops/sec=%.1f wal_on_fast ops/sec=%.1f max ratio=%.2fx", fast, walOnFast, ratio)
	if ratio > 4.0 {
		t.Fatalf("fast and wal_on_fast write smoke diverged by %.2fx; fast=%.1f wal_on_fast=%.1f", ratio, fast, walOnFast)
	}
}

func runTreeDBProfileSmoke(t *testing.T, profile treedb.Profile) float64 {
	t.Helper()
	cfg, err := parseConfig([]string{
		"-target", "treedb",
		"-documents", "1000",
		"-batch-size", "500",
		"-reads", "0",
		"-range-reads", "0",
		"-updates", "0",
		"-secondary-indexes", "2",
		"-treedb-profile", string(profile),
		"-treedb-maintenance", treeDBMaintenanceCheckpoint,
		"-timeout", "0",
		"-format", "json",
	})
	if err != nil {
		t.Fatalf("parse smoke config: %v", err)
	}
	target, err := openTarget(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open target for %s: %v", profile, err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := target.cleanup(cleanupCtx); err != nil {
			t.Errorf("cleanup %s: %v", profile, err)
		}
	}()
	result, err := runBenchmark(context.Background(), cfg, target)
	if err != nil {
		t.Fatalf("run benchmark for %s: %v", profile, err)
	}
	for _, phase := range result.Phases {
		if phase.Name == "load_insert_many" {
			if phase.OpsPerSecond <= 0 {
				t.Fatalf("%s load_insert_many ops/sec=%f", profile, phase.OpsPerSecond)
			}
			return phase.OpsPerSecond
		}
	}
	t.Fatalf("%s load_insert_many phase missing: %+v", profile, result.Phases)
	return 0
}

func TestRunEmailFindPhaseRequiresEmailIndex(t *testing.T) {
	if runEmailFindPhase(config{Reads: 10, SecondaryIndexes: 0}) {
		t.Fatal("email phase should be skipped without an email index")
	}
	if !runEmailFindPhase(config{Reads: 10, SecondaryIndexes: 1}) {
		t.Fatal("email phase should run when the email index exists")
	}
	if runEmailFindPhase(config{Reads: 0, SecondaryIndexes: 1}) {
		t.Fatal("email phase should be skipped when reads are disabled")
	}
}

func TestRunConcurrentOperationsRunsAllOpsOnce(t *testing.T) {
	var count atomic.Int64
	seen := make([]atomic.Int64, 25)
	err := runConcurrentOperations(context.Background(), 4, len(seen), func(op int) error {
		if op < 0 || op >= len(seen) {
			return os.ErrInvalid
		}
		seen[op].Add(1)
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("run concurrent operations: %v", err)
	}
	if got := count.Load(); got != int64(len(seen)) {
		t.Fatalf("operation count=%d want %d", got, len(seen))
	}
	for op := range seen {
		if got := seen[op].Load(); got != 1 {
			t.Fatalf("op %d ran %d times, want once", op, got)
		}
	}
}

func TestRunConcurrentOperationsReturnsFirstError(t *testing.T) {
	sentinel := errors.New("boom")
	err := runConcurrentOperations(context.Background(), 4, 100, func(op int) error {
		if op == 3 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v want sentinel", err)
	}
}

func TestWriteResultSupportsGenericWriter(t *testing.T) {
	result := &benchmarkResult{
		Target:           "treedb",
		Database:         "bench",
		Collection:       "docs",
		Documents:        1,
		SecondaryIndexes: 1,
		Phases: []phaseResult{{
			Name:           "load_insert_many",
			Operations:     1,
			DriverCalls:    1,
			DurationMillis: 1,
			OpsPerSecond:   1000,
		}},
	}
	var out bytes.Buffer
	if err := writeResult(&out, "text", result); err != nil {
		t.Fatalf("writeResult: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("target=treedb")) {
		t.Fatalf("text output missing target: %q", out.String())
	}
}

func TestWriteResultIncludesRedactedMongoURI(t *testing.T) {
	result := &benchmarkResult{
		Target:           "mongo",
		MongoURI:         "mongodb://user@127.0.0.1:27017",
		Database:         "bench",
		Collection:       "docs",
		Documents:        1,
		SecondaryIndexes: 1,
	}
	var out bytes.Buffer
	if err := writeResult(&out, "text", result); err != nil {
		t.Fatalf("writeResult: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("mongo_uri=mongodb://user@127.0.0.1:27017")) {
		t.Fatalf("text output missing mongo_uri: %q", out.String())
	}
}
