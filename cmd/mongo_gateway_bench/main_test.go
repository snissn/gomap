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
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
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

func TestCloseBenchTargetKeepDirPreservesTempDir(t *testing.T) {
	dir := t.TempDir()
	target := &benchTarget{
		treedbDir:       dir,
		removeTreeDBDir: true,
		cleanup: func(context.Context) error {
			return nil
		},
	}

	if err := closeBenchTargetKeepDir(context.Background(), target); err != nil {
		t.Fatalf("close keep dir: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("dir removed by keep-dir close: %v", err)
	}
	if !target.removeTreeDBDir {
		t.Fatal("removeTreeDBDir was cleared before final cleanup")
	}
	if err := closeBenchTarget(context.Background(), target); err != nil {
		t.Fatalf("final close: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("dir still exists after final close: %v", err)
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
	} else if !strings.Contains(err.Error(), "default, fast, or compressed") {
		t.Fatalf("bad flag usage did not document root-storage default: %v", err)
	}
	if _, err := parseConfig([]string{"-target", "bad"}); err == nil {
		t.Fatal("bad target accepted")
	}
	cfg, err := parseConfig([]string{
		"-target", "mongo",
		"-documents", "10",
		"-batch-size", "5",
		"-insert-producers", "4",
		"-mongo-max-pool-size", "32",
		"-mongo-min-pool-size", "8",
		"-mongo-max-connecting", "16",
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
		cfg.ClientMode != clientModeDriver ||
		cfg.BatchSize != 5 || cfg.InsertProducers != 4 ||
		cfg.MongoMaxPoolSize != 32 || cfg.MongoMinPoolSize != 8 || cfg.MongoMaxConnecting != 16 ||
		cfg.ConcurrentReaders != 4 || cfg.ConcurrentReads != 20 || cfg.ConcurrentWriters != 2 || cfg.ConcurrentWrites != 10 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	rawWireCfg, err := parseConfig([]string{"-target", "treedb", "-client-mode", "raw-wire"})
	if err != nil {
		t.Fatalf("parse raw-wire config: %v", err)
	}
	if rawWireCfg.ClientMode != clientModeRawWire {
		t.Fatalf("ClientMode=%q want %q", rawWireCfg.ClientMode, clientModeRawWire)
	}
	rawWireTCPCfg, err := parseConfig([]string{"-target", "treedb", "-client-mode", "raw-wire-tcp"})
	if err != nil {
		t.Fatalf("parse raw-wire-tcp config: %v", err)
	}
	if rawWireTCPCfg.ClientMode != clientModeRawWireTCP {
		t.Fatalf("ClientMode=%q want %q", rawWireTCPCfg.ClientMode, clientModeRawWireTCP)
	}
	commandCfg, err := parseConfig([]string{"-target", "mongo", "-client-mode", "driver-command"})
	if err != nil {
		t.Fatalf("parse driver-command config: %v", err)
	}
	if commandCfg.ClientMode != clientModeDriverCommand {
		t.Fatalf("ClientMode=%q want %q", commandCfg.ClientMode, clientModeDriverCommand)
	}
	commandRawCfg, err := parseConfig([]string{"-target", "mongo", "-client-mode", "driver-command-raw"})
	if err != nil {
		t.Fatalf("parse driver-command-raw config: %v", err)
	}
	if commandRawCfg.ClientMode != clientModeDriverCommandRaw {
		t.Fatalf("ClientMode=%q want %q", commandRawCfg.ClientMode, clientModeDriverCommandRaw)
	}
	unackCfg, err := parseConfig([]string{"-target", "mongo", "-client-mode", "driver-unack"})
	if err != nil {
		t.Fatalf("parse driver-unack config: %v", err)
	}
	if unackCfg.ClientMode != clientModeDriverUnack {
		t.Fatalf("ClientMode=%q want %q", unackCfg.ClientMode, clientModeDriverUnack)
	}
	if _, err := parseConfig([]string{"-client-mode", "bad"}); err == nil {
		t.Fatal("bad client-mode accepted")
	}
	if _, err := parseConfig([]string{"-target", "mongo", "-client-mode", "raw-wire"}); err == nil {
		t.Fatal("raw-wire client-mode accepted for mongo target")
	}
	if _, err := parseConfig([]string{"-target", "mongo", "-client-mode", "raw-wire-tcp"}); err == nil {
		t.Fatal("raw-wire-tcp client-mode accepted for mongo target")
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
	if _, err := parseConfig([]string{"-insert-producers", "0"}); err == nil {
		t.Fatal("zero insert-producers accepted")
	}
	if _, err := parseConfig([]string{"-mongo-max-pool-size", "-1"}); err == nil {
		t.Fatal("negative mongo-max-pool-size accepted")
	}
}

func TestRawInsertCommandBuildsBSONCommand(t *testing.T) {
	docs := []bson.Raw{
		mustTestBSON(t, bson.D{{Key: "_id", Value: "a"}, {Key: "email", Value: "a@example.test"}}),
		mustTestBSON(t, bson.D{{Key: "_id", Value: "b"}, {Key: "email", Value: "b@example.test"}}),
	}
	command, err := rawInsertCommand("docs", 0, len(docs), nil, docs)
	if err != nil {
		t.Fatalf("rawInsertCommand: %v", err)
	}
	var out struct {
		Insert    string     `bson:"insert"`
		Documents []bson.Raw `bson:"documents"`
		Ordered   bool       `bson:"ordered"`
	}
	if err := bson.Unmarshal(command, &out); err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	if out.Insert != "docs" || !out.Ordered || len(out.Documents) != len(docs) {
		t.Fatalf("unexpected command: %+v", out)
	}
	for i := range docs {
		if !bytes.Equal(out.Documents[i], docs[i]) {
			t.Fatalf("document %d mismatch: got %v want %v", i, out.Documents[i], docs[i])
		}
	}
}

func mustTestBSON(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal test BSON: %v", err)
	}
	return raw
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
	if cfg.InsertProducers != 1 {
		t.Fatalf("InsertProducers=%d want 1", cfg.InsertProducers)
	}
}

func TestParseConfigAcceptsTreeDBBSONDocumentFormat(t *testing.T) {
	cfg, err := parseConfig([]string{"-treedb-document-format", "bson"})
	if err != nil {
		t.Fatalf("parse BSON document format: %v", err)
	}
	if got := string(cfg.TreeDBDocumentFormat); got != "bson" {
		t.Fatalf("TreeDBDocumentFormat=%q want bson", got)
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

func TestTreeDBClientModeSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("client mode smoke benchmark skipped in short mode")
	}
	for _, mode := range []string{clientModeDriver, clientModeDriverCommand, clientModeDriverCommandRaw, clientModeDriverUnack, clientModeRawWire, clientModeRawWireTCP} {
		t.Run(mode, func(t *testing.T) {
			opsPerSecond := runTreeDBClientModeSmoke(t, mode)
			t.Logf("%s load_insert_many ops/sec=%.1f", mode, opsPerSecond)
		})
	}
}

func TestTreeDBRawWireLoadPhaseHonorsCanceledContext(t *testing.T) {
	for _, mode := range []string{clientModeRawWire, clientModeRawWireTCP} {
		t.Run(mode, func(t *testing.T) {
			cfg, err := parseConfig([]string{
				"-target", "treedb",
				"-client-mode", mode,
				"-documents", "10",
				"-batch-size", "5",
				"-reads", "0",
				"-range-reads", "0",
				"-updates", "0",
				"-secondary-indexes", "0",
				"-treedb-maintenance", treeDBMaintenanceNone,
				"-timeout", "0",
			})
			if err != nil {
				t.Fatalf("parse raw-wire config: %v", err)
			}
			target, err := openTarget(context.Background(), cfg)
			if err != nil {
				t.Fatalf("open target: %v", err)
			}
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := closeBenchTarget(cleanupCtx, target); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			canceledCtx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = runLoadPhase(canceledCtx, cfg, target, nil, nil, nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("runLoadPhase err=%v want context.Canceled", err)
			}
		})
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
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
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

func runTreeDBClientModeSmoke(t *testing.T, clientMode string) float64 {
	t.Helper()
	cfg, err := parseConfig([]string{
		"-target", "treedb",
		"-client-mode", clientMode,
		"-documents", "300",
		"-batch-size", "100",
		"-reads", "0",
		"-range-reads", "0",
		"-updates", "0",
		"-secondary-indexes", "2",
		"-treedb-document-format", string(collections.DocumentFormatBSON),
		"-treedb-maintenance", treeDBMaintenanceNone,
		"-prebuild-documents",
		"-timeout", "0",
		"-format", "json",
	})
	if err != nil {
		t.Fatalf("parse smoke config: %v", err)
	}
	target, err := openTarget(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open target for client mode %s: %v", clientMode, err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			t.Errorf("cleanup client mode %s: %v", clientMode, err)
		}
	}()
	result, err := runBenchmark(context.Background(), cfg, target)
	if err != nil {
		t.Fatalf("run benchmark for client mode %s: %v", clientMode, err)
	}
	if result.ClientMode != clientMode {
		t.Fatalf("result client mode=%q want %q", result.ClientMode, clientMode)
	}
	for _, phase := range result.Phases {
		if phase.Name == "load_insert_many" {
			if phase.OpsPerSecond <= 0 {
				t.Fatalf("%s load_insert_many ops/sec=%f", clientMode, phase.OpsPerSecond)
			}
			if phase.SampledOpsPerSecond <= 0 || phase.SampledNsPerOp <= 0 {
				t.Fatalf("%s sampled load metrics missing: %+v", clientMode, phase)
			}
			return phase.OpsPerSecond
		}
	}
	t.Fatalf("%s load_insert_many phase missing: %+v", clientMode, result.Phases)
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

func TestMakeLoadBatchesSplitsDocumentRange(t *testing.T) {
	batches := makeLoadBatches(10, 4)
	want := []loadBatch{{start: 0, end: 4}, {start: 4, end: 8}, {start: 8, end: 10}}
	if len(batches) != len(want) {
		t.Fatalf("len(batches)=%d want %d: %+v", len(batches), len(want), batches)
	}
	for i := range want {
		if batches[i] != want[i] {
			t.Fatalf("batch %d=%+v want %+v", i, batches[i], want[i])
		}
	}
}

func TestEffectiveLoadProducersCapsAtBatchCount(t *testing.T) {
	if got := effectiveLoadProducers(10, 4, 8); got != 3 {
		t.Fatalf("effectiveLoadProducers=%d want 3", got)
	}
	if got := effectiveLoadProducers(10, 4, 2); got != 2 {
		t.Fatalf("effectiveLoadProducers=%d want 2", got)
	}
}

func TestLoadVisibilitySentinelIDsUseBatchBoundaries(t *testing.T) {
	got := loadVisibilitySentinelIDs(10, 4)
	want := []string{benchmarkID(3), benchmarkID(7), benchmarkID(9)}
	if len(got) != len(want) {
		t.Fatalf("len(sentinels)=%d want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sentinel %d=%q want %q", i, got[i], want[i])
		}
	}
}

func TestMeasureLoadPhaseReportsProducerResults(t *testing.T) {
	cfg := config{Documents: 12, BatchSize: 2, InsertProducers: 3}
	seen := make([]atomic.Int64, cfg.Documents)
	phase, err := measureLoadPhase(context.Background(), cfg, func(producer, start, end int) error {
		if producer < 0 || producer >= cfg.InsertProducers {
			return os.ErrInvalid
		}
		for i := start; i < end; i++ {
			seen[i].Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("measureLoadPhase: %v", err)
	}
	if phase.Name != "load_insert_many" || phase.Operations != cfg.Documents || phase.DriverCalls != 6 {
		t.Fatalf("unexpected phase summary: %+v", phase)
	}
	if phase.EffectiveProducers != cfg.InsertProducers {
		t.Fatalf("EffectiveProducers=%d want %d", phase.EffectiveProducers, cfg.InsertProducers)
	}
	if len(phase.ProducerResults) != cfg.InsertProducers {
		t.Fatalf("producer results=%d want %d: %+v", len(phase.ProducerResults), cfg.InsertProducers, phase.ProducerResults)
	}
	var producerOps, producerCalls int
	for _, producer := range phase.ProducerResults {
		producerOps += producer.Operations
		producerCalls += producer.DriverCalls
	}
	if producerOps != cfg.Documents || producerCalls != phase.DriverCalls {
		t.Fatalf("producer totals ops/calls=%d/%d want %d/%d", producerOps, producerCalls, cfg.Documents, phase.DriverCalls)
	}
	for doc := range seen {
		if got := seen[doc].Load(); got != 1 {
			t.Fatalf("doc %d seen %d times, want once", doc, got)
		}
	}
}

func TestMeasureLoadPhaseErrorReportsCompletedOperations(t *testing.T) {
	sentinel := errors.New("load failed")
	cfg := config{Documents: 6, BatchSize: 2, InsertProducers: 1}
	phase, err := measureLoadPhase(context.Background(), cfg, func(producer, start, end int) error {
		if start >= 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v want sentinel", err)
	}
	if phase.Operations != 2 {
		t.Fatalf("Operations=%d want completed operations 2", phase.Operations)
	}
	if phase.DriverCalls != 2 {
		t.Fatalf("DriverCalls=%d want 2", phase.DriverCalls)
	}
}

func TestMongoPoolStatsCapsCheckoutSamples(t *testing.T) {
	stats := newMongoPoolStats()
	total := maxMongoPoolCheckoutDurationSamples + 3
	for i := 0; i < total; i++ {
		stats.record(&event.PoolEvent{Type: event.ConnectionCheckedOut, Duration: time.Microsecond})
	}
	snapshot := stats.Snapshot()
	if snapshot.ConnectionCheckedOut != int64(total) {
		t.Fatalf("ConnectionCheckedOut=%d want %d", snapshot.ConnectionCheckedOut, total)
	}
	if snapshot.CheckoutSamples != int64(maxMongoPoolCheckoutDurationSamples) {
		t.Fatalf("CheckoutSamples=%d want %d", snapshot.CheckoutSamples, maxMongoPoolCheckoutDurationSamples)
	}
	if snapshot.CheckoutSamplesDropped != 3 {
		t.Fatalf("CheckoutSamplesDropped=%d want 3", snapshot.CheckoutSamplesDropped)
	}
	if snapshot.CheckoutMeanLatencyMicros != 1 {
		t.Fatalf("CheckoutMeanLatencyMicros=%f want 1", snapshot.CheckoutMeanLatencyMicros)
	}
}

func TestWriteResultSupportsGenericWriter(t *testing.T) {
	result := &benchmarkResult{
		Target:           "treedb",
		Database:         "bench",
		Collection:       "docs",
		Documents:        1,
		BatchSize:        1,
		InsertProducers:  2,
		SecondaryIndexes: 1,
		Phases: []phaseResult{{
			Name:           "load_insert_many",
			Operations:     1,
			DriverCalls:    1,
			DurationMillis: 1,
			OpsPerSecond:   1000,
			ProducerResults: []producerResult{{
				Producer:       0,
				Operations:     1,
				DriverCalls:    1,
				DurationMillis: 1,
				OpsPerSecond:   1000,
			}},
		}},
	}
	var out bytes.Buffer
	if err := writeResult(&out, "text", result); err != nil {
		t.Fatalf("writeResult: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("target=treedb")) {
		t.Fatalf("text output missing target: %q", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("insert_producers=2")) || !bytes.Contains(out.Bytes(), []byte("producer=0")) {
		t.Fatalf("text output missing producer metadata: %q", out.String())
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
