package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
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

func TestSummarizePhaseZeroOperationsWithSamples(t *testing.T) {
	phase := summarizePhase("load_insert_many", 0, 1, time.Millisecond, []time.Duration{time.Millisecond})
	if phase.DriverMeanLatencyMicros == 0 {
		t.Fatal("driver mean latency should still be reported for sampled driver calls")
	}
	if phase.SampledOpsPerSecond != 0 {
		t.Fatalf("sampled ops/sec=%v want 0 for zero completed operations", phase.SampledOpsPerSecond)
	}
	if phase.SampledNsPerOp != 0 {
		t.Fatalf("sampled ns/op=%v want 0 for zero completed operations", phase.SampledNsPerOp)
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

func TestSelectedTreeDBStatsKeepsExpectedKeys(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{name: "exact commit seq", key: "treedb.commit_seq", want: true},
		{name: "ordered root prefix", key: "treedb.publish.ordered_root_delta_group.calls_total", want: true},
		{name: "watermark prefix", key: "treedb.publish.watermark.latency_p99_ms", want: true},
		{name: "non match", key: "treedb.vlog.reads_total", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectedTreeDBStats(map[string]string{tt.key: "1"})
			if kept := got != nil && got[tt.key] == "1"; kept != tt.want {
				t.Fatalf("selectedTreeDBStats kept %q=%v want %v (stats=%v)", tt.key, kept, tt.want, got)
			}
		})
	}
}

func TestSelectedTreeDBStats(t *testing.T) {
	if got := selectedTreeDBStats(nil); got != nil {
		t.Fatalf("nil stats selected=%v want nil", got)
	}
	if got := selectedTreeDBStats(map[string]string{"treedb.vlog.reads_total": "7"}); got != nil {
		t.Fatalf("unselected stats=%v want nil", got)
	}
	got := selectedTreeDBStats(map[string]string{
		"treedb.commit_seq": "11",
		"treedb.publish.ordered_root_delta_group.calls_total":    "3",
		"treedb.publish.ordered_root_delta_group.latency_p99_ms": "1.5",
		"treedb.publish.watermark.latency_p99_ms":                "2.5",
		"treedb.vlog.reads_total":                                "7",
	})
	want := map[string]string{
		"treedb.commit_seq": "11",
		"treedb.publish.ordered_root_delta_group.calls_total":    "3",
		"treedb.publish.ordered_root_delta_group.latency_p99_ms": "1.5",
		"treedb.publish.watermark.latency_p99_ms":                "2.5",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selected stats=%v want %v", got, want)
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
		"-secondary-indexes", "2",
		"-format", "json",
		"-concurrent-readers", "4",
		"-concurrent-reads", "20",
		"-concurrent-writers", "2",
		"-concurrent-writes", "10",
		"-update-indexed-field",
		"-range-index",
	})
	if err != nil {
		t.Fatalf("parse valid config: %v", err)
	}
	if cfg.Target != "mongo" || cfg.Documents != 10 || cfg.SecondaryIndexes != 2 || cfg.Format != "json" ||
		cfg.ClientMode != clientModeDriver ||
		cfg.BatchSize != 5 || cfg.InsertProducers != 4 ||
		cfg.MongoMaxPoolSize != 32 || cfg.MongoMinPoolSize != 8 || cfg.MongoMaxConnecting != 16 ||
		cfg.ConcurrentReaders != 4 || cfg.ConcurrentReads != 20 || cfg.ConcurrentWriters != 2 || cfg.ConcurrentWrites != 10 ||
		!cfg.UpdateIndexedField || !cfg.RangeIndex {
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
	oneIndexCfg, err := parseConfig([]string{"-secondary-indexes", "1"})
	if err != nil {
		t.Fatalf("parse secondary-indexes=1 config: %v", err)
	}
	if oneIndexCfg.SecondaryIndexes != 1 {
		t.Fatalf("SecondaryIndexes=%d want 1", oneIndexCfg.SecondaryIndexes)
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
	if _, err := parseConfig([]string{"-mongo-max-pool-size", "4", "-mongo-min-pool-size", "8"}); err == nil {
		t.Fatal("mongo-min-pool-size greater than mongo-max-pool-size accepted")
	}
	if _, err := parseConfig([]string{"-mongo-min-pool-size", "101"}); err == nil {
		t.Fatal("mongo-min-pool-size greater than default mongo-max-pool-size accepted")
	}
	if _, err := parseConfig([]string{"-mongo-min-pool-size", "100"}); err != nil {
		t.Fatalf("mongo-min-pool-size equal to default mongo-max-pool-size rejected: %v", err)
	}
	if _, err := parseConfig([]string{"-secondary-indexes", "1", "-update-indexed-field"}); err == nil {
		t.Fatal("update-indexed-field accepted without city index")
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

func TestParseConfigProfileOptions(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-profile-dir", "/tmp/mongo-gateway-bench-profiles",
		"-profile-block-rate", "7",
		"-profile-mutex-fraction", "11",
		"-profile-trace",
		"-profile-heap-gc",
	})
	if err != nil {
		t.Fatalf("parse profile options: %v", err)
	}
	if cfg.ProfileDir != "/tmp/mongo-gateway-bench-profiles" {
		t.Fatalf("ProfileDir=%q", cfg.ProfileDir)
	}
	if cfg.ProfileBlockRate != 7 {
		t.Fatalf("ProfileBlockRate=%d want 7", cfg.ProfileBlockRate)
	}
	if cfg.ProfileMutexFraction != 11 {
		t.Fatalf("ProfileMutexFraction=%d want 11", cfg.ProfileMutexFraction)
	}
	if !cfg.ProfileTrace {
		t.Fatal("ProfileTrace=false want true")
	}
	if !cfg.ProfileHeapGC {
		t.Fatal("ProfileHeapGC=false want true")
	}
	if _, err := parseConfig([]string{"-profile-trace"}); err == nil {
		t.Fatal("profile-trace without profile-dir accepted")
	}
	if _, err := parseConfig([]string{"-profile-heap-gc"}); err == nil {
		t.Fatal("profile-heap-gc without profile-dir accepted")
	}
	if _, err := parseConfig([]string{"-profile-block-rate", "-1"}); err == nil {
		t.Fatal("negative profile-block-rate accepted")
	}
	if _, err := parseConfig([]string{"-profile-mutex-fraction", "-1"}); err == nil {
		t.Fatal("negative profile-mutex-fraction accepted")
	}
}

func TestProfileRecorderWritesPhaseArtifactsAndManifest(t *testing.T) {
	dir := t.TempDir()
	cfg, err := parseConfig([]string{
		"-profile-dir", dir,
		"-profile-block-rate", "1",
		"-profile-mutex-fraction", "1",
	})
	if err != nil {
		t.Fatalf("parse profile config: %v", err)
	}
	recorder, err := newProfileRecorder(cfg)
	if err != nil {
		t.Fatalf("newProfileRecorder: %v", err)
	}
	defer recorder.Close()

	phase, err := recorder.RunPhase("unit phase", func() (phaseResult, error) {
		var sink uint64
		deadline := time.Now().Add(25 * time.Millisecond)
		for time.Now().Before(deadline) {
			sink++
		}
		runtime.KeepAlive(sink)
		return summarizePhase("unit phase", 1, 1, time.Millisecond, []time.Duration{time.Millisecond}), nil
	})
	if err != nil {
		t.Fatalf("RunPhase: %v", err)
	}
	result := &benchmarkResult{
		Target:        "treedb",
		Database:      "db",
		Collection:    "docs",
		Documents:     1,
		BatchSize:     1,
		Phases:        []phaseResult{phase},
		ProfileDir:    recorder.Dir(),
		ProfileResult: profileResultFile,
	}
	if err := recorder.WriteResult(result); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}
	if err := recorder.WriteManifest(result, nil); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	for _, name := range []string{
		"unit_phase.cpu.pprof",
		"unit_phase.heap.pprof",
		"unit_phase.allocs.pprof",
		"unit_phase.block.pprof",
		"unit_phase.mutex.pprof",
		"unit_phase.goroutine.pprof",
		profileManifestFile,
		profileResultFile,
	} {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s is empty", name)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			t.Fatalf("%s permissions=%#o want 0600", name, info.Mode().Perm())
		}
	}
}

func TestProfileRecorderWritesTraceArtifactAndManifest(t *testing.T) {
	dir := t.TempDir()
	cfg, err := parseConfig([]string{
		"-profile-dir", dir,
		"-profile-trace",
	})
	if err != nil {
		t.Fatalf("parse profile config: %v", err)
	}
	recorder, err := newProfileRecorder(cfg)
	if err != nil {
		t.Fatalf("newProfileRecorder: %v", err)
	}
	defer recorder.Close()

	if _, err := recorder.RunPhase("trace phase", func() (phaseResult, error) {
		time.Sleep(time.Millisecond)
		return summarizePhase("trace phase", 1, 1, time.Millisecond, []time.Duration{time.Millisecond}), nil
	}); err != nil {
		t.Fatalf("RunPhase: %v", err)
	}
	if err := recorder.WriteManifest(nil, nil); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	traceName := "trace_phase.trace.out"
	info, err := os.Stat(filepath.Join(dir, traceName))
	if err != nil {
		t.Fatalf("stat trace artifact: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("trace artifact is empty")
	}
	raw, err := os.ReadFile(filepath.Join(dir, profileManifestFile))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest profileManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if len(manifest.Artifacts) != 1 {
		t.Fatalf("manifest artifacts=%d want 1", len(manifest.Artifacts))
	}
	if manifest.Artifacts[0].Trace != traceName {
		t.Fatalf("manifest trace=%q want %q", manifest.Artifacts[0].Trace, traceName)
	}
}

func TestSanitizeProfileNameAvoidsHiddenArtifacts(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
	}{
		{name: ".", want: "phase"},
		{name: ".hidden", want: "hidden"},
		{name: "..phase", want: "phase"},
		{name: "load.insert", want: "load.insert"},
	} {
		if got := sanitizeProfileName(tc.name); got != tc.want {
			t.Fatalf("sanitizeProfileName(%q)=%q want %q", tc.name, got, tc.want)
		}
	}
}

func TestNewProfileRecorderTightensProfileDirPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("profile directory chmod is not enforced on windows")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod setup dir: %v", err)
	}
	cfg, err := parseConfig([]string{"-profile-dir", dir})
	if err != nil {
		t.Fatalf("parse profile config: %v", err)
	}
	recorder, err := newProfileRecorder(cfg)
	if err != nil {
		t.Fatalf("newProfileRecorder: %v", err)
	}
	defer recorder.Close()
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat profile dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("profile dir permissions=%#o want 0700", got)
	}
}

func TestNewProfileRecorderRejectsNonEmptyProfileDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "stale.cpu.pprof"), []byte("old"), 0o600); err != nil {
		t.Fatalf("write stale artifact: %v", err)
	}
	cfg, err := parseConfig([]string{"-profile-dir", dir})
	if err != nil {
		t.Fatalf("parse profile config: %v", err)
	}
	recorder, err := newProfileRecorder(cfg)
	if err == nil {
		recorder.Close()
		t.Fatal("newProfileRecorder accepted non-empty profile dir")
	}
	if !strings.Contains(err.Error(), "must be empty") {
		t.Fatalf("newProfileRecorder err=%v want empty-dir error", err)
	}
}

func TestCreateProfileFileRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.pprof")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(dir, "link.pprof")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	file, err := createProfileFile(link)
	if err == nil {
		_ = file.Close()
		t.Fatal("createProfileFile accepted symlink")
	}
}

func TestCreateProfileFileRejectsExistingRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.pprof")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatalf("write existing file: %v", err)
	}
	file, err := createProfileFile(path)
	if err == nil {
		_ = file.Close()
		t.Fatal("createProfileFile accepted existing regular file")
	}
}

func TestProfileRecorderManifestRecordsProfileStopError(t *testing.T) {
	dir := t.TempDir()
	cfg, err := parseConfig([]string{"-profile-dir", dir})
	if err != nil {
		t.Fatalf("parse profile config: %v", err)
	}
	recorder, err := newProfileRecorder(cfg)
	if err != nil {
		t.Fatalf("newProfileRecorder: %v", err)
	}
	defer recorder.Close()

	if _, err := recorder.RunPhase("unit phase", func() (phaseResult, error) {
		if err := os.Mkdir(filepath.Join(dir, "unit_phase.heap.pprof"), 0o700); err != nil {
			return phaseResult{}, err
		}
		return summarizePhase("unit phase", 1, 1, time.Millisecond, []time.Duration{time.Millisecond}), nil
	}); err == nil {
		t.Fatal("RunPhase succeeded despite profile artifact write failure")
	}
	if err := recorder.WriteManifest(nil, nil); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, profileManifestFile))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest profileManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if len(manifest.Artifacts) != 1 {
		t.Fatalf("manifest artifacts=%d want 1", len(manifest.Artifacts))
	}
	if manifest.Artifacts[0].Error == "" {
		t.Fatal("manifest artifact error is empty")
	}
	if !strings.Contains(manifest.Artifacts[0].Error, "unit_phase.heap.pprof") {
		t.Fatalf("manifest artifact error=%q want heap path context", manifest.Artifacts[0].Error)
	}
}

func TestProfileRecorderSkipsDisabledBlockAndMutexProfiles(t *testing.T) {
	dir := t.TempDir()
	cfg, err := parseConfig([]string{
		"-profile-dir", dir,
		"-profile-block-rate", "0",
		"-profile-mutex-fraction", "0",
	})
	if err != nil {
		t.Fatalf("parse profile config: %v", err)
	}
	recorder, err := newProfileRecorder(cfg)
	if err != nil {
		t.Fatalf("newProfileRecorder: %v", err)
	}
	defer recorder.Close()

	if _, err := recorder.RunPhase("unit phase", func() (phaseResult, error) {
		return summarizePhase("unit phase", 1, 1, time.Millisecond, []time.Duration{time.Millisecond}), nil
	}); err != nil {
		t.Fatalf("RunPhase: %v", err)
	}
	if err := recorder.WriteManifest(nil, nil); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	for _, name := range []string{
		"unit_phase.cpu.pprof",
		"unit_phase.heap.pprof",
		"unit_phase.allocs.pprof",
		"unit_phase.goroutine.pprof",
		profileManifestFile,
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
	}
	for _, name := range []string{
		"unit_phase.block.pprof",
		"unit_phase.mutex.pprof",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("stat %s err=%v want not-exist", name, err)
		}
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
	result, err := runBenchmark(context.Background(), cfg, target, nil)
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
	result, err := runBenchmark(context.Background(), cfg, target, nil)
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
	phase, err := measureLoadPhase(context.Background(), cfg, func(ctx context.Context, producer, start, end int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
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
	phase, err := measureLoadPhase(context.Background(), cfg, func(ctx context.Context, producer, start, end int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
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

func TestRunLoadBatchesCancelsInFlightBatchContext(t *testing.T) {
	sentinel := errors.New("load failed")
	releaseFailure := make(chan struct{})
	started := make(chan int, 2)
	done := make(chan error, 1)
	go func() {
		done <- runLoadBatches(
			context.Background(),
			[]loadBatch{{start: 0, end: 1}, {start: 1, end: 2}},
			2,
			func(ctx context.Context, producer, start, end int) error {
				started <- start
				if start == 0 {
					<-releaseFailure
					return sentinel
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					return errors.New("in-flight batch context was not canceled")
				}
			},
			func(producer, operations int, duration time.Duration) {},
			func(producer int, duration time.Duration) {},
		)
	}()
	seen := map[int]bool{}
	for len(seen) < 2 {
		select {
		case start := <-started:
			seen[start] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for both batches to start; seen=%v", seen)
		}
	}
	close(releaseFailure)
	var err error
	select {
	case err = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runLoadBatches to return after failure")
	}
	if err == nil {
		t.Fatal("runLoadBatches returned nil before failure released")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v want sentinel", err)
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

func TestBenchmarkSetUpdateCanExerciseIndexedField(t *testing.T) {
	updatedCityValues := buildBenchmarkUpdatedCityValues()

	updateSet := func(update bson.D) bson.Raw {
		t.Helper()
		raw, err := bson.Marshal(update)
		if err != nil {
			t.Fatalf("marshal update: %v", err)
		}
		doc := bson.Raw(raw)
		set, ok := doc.Lookup("$set").DocumentOK()
		if !ok {
			t.Fatalf("$set missing or not a document: %v", doc.Lookup("$set").Type)
		}
		return set
	}

	set := updateSet(benchmarkSetUpdate(benchmarkSetUpdateParams{
		Operation:          3,
		DocumentOrdinal:    7,
		DocumentCount:      100,
		ConcurrentPhase:    true,
		UpdateIndexedField: true,
		UpdatedCityValues:  updatedCityValues,
	}))
	if got, ok := set.Lookup("concurrent_update_seq").Int64OK(); !ok || got != 3 {
		t.Fatalf("concurrent_update_seq=%d ok=%t want 3", got, ok)
	}
	if city, ok := set.Lookup("city").StringValueOK(); !ok || city != benchmarkUpdatedCity(3, 7, 100) {
		t.Fatalf("city=%q ok=%t want %q", city, ok, benchmarkUpdatedCity(3, 7, 100))
	}

	set = updateSet(benchmarkSetUpdate(benchmarkSetUpdateParams{
		Operation:       4,
		DocumentOrdinal: 7,
		DocumentCount:   100,
		ConcurrentPhase: true,
	}))
	if _, ok := set.Lookup("city").StringValueOK(); ok {
		t.Fatalf("city present when updateIndexedField=false: %v", set.Lookup("city"))
	}

	set = updateSet(benchmarkSetUpdate(benchmarkSetUpdateParams{
		Operation:          5,
		DocumentOrdinal:    11,
		DocumentCount:      100,
		UpdateIndexedField: true,
		UpdatedCityValues:  updatedCityValues,
	}))
	if got, ok := set.Lookup("update_seq").Int64OK(); !ok || got != 5 {
		t.Fatalf("update_seq=%d ok=%t want 5", got, ok)
	}
	if _, ok := set.Lookup("concurrent_update_seq").Int64OK(); ok {
		t.Fatalf("concurrent_update_seq present in non-concurrent update: %v", set.Lookup("concurrent_update_seq"))
	}
	if city, ok := set.Lookup("city").StringValueOK(); !ok || city != benchmarkUpdatedCity(5, 11, 100) {
		t.Fatalf("city=%q ok=%t want %q", city, ok, benchmarkUpdatedCity(5, 11, 100))
	}
	set = updateSet(benchmarkSetUpdate(benchmarkSetUpdateParams{
		Operation:          6,
		DocumentOrdinal:    12,
		DocumentCount:      100,
		UpdateIndexedField: true,
		UpdatedCityValues:  []string{},
	}))
	if city, ok := set.Lookup("city").StringValueOK(); !ok || city != "" {
		t.Fatalf("city=%q ok=%t want explicit empty value list to stay empty", city, ok)
	}
	if got := len(updatedCityValues); got != benchmarkUpdatedCityValueCount {
		t.Fatalf("updatedCityValues len=%d want %d", got, benchmarkUpdatedCityValueCount)
	}
	if first, revisited := benchmarkUpdatedCity(13, 42, benchmarkUpdatedCityValueCount), benchmarkUpdatedCity(13+benchmarkUpdatedCityValueCount, 42, benchmarkUpdatedCityValueCount); first == revisited {
		t.Fatalf("benchmarkUpdatedCity repeated value %q when revisiting same document after full value cycle", first)
	}
	if got := avoidBenchmarkUpdatedCityRepeat(3, 3, len(updatedCityValues)); got == 3 {
		t.Fatalf("avoidBenchmarkUpdatedCityRepeat did not advance colliding index")
	}
	const documents = 65520
	for op := 0; op < 1024; op++ {
		documentOrdinal := benchmarkDocumentOrdinal(op, 31, documents)
		if first, revisited := benchmarkUpdatedCity(op, documentOrdinal, documents), benchmarkUpdatedCity(op+documents, documentOrdinal, documents); first == revisited {
			t.Fatalf("benchmarkUpdatedCity repeated value %q when revisiting document %d at op+documents for op=%d documents=%d", first, documentOrdinal, op, documents)
		}
	}
}

func TestBenchmarkDocumentOrdinalAvoidsIntOverflow(t *testing.T) {
	const documents = 100000
	op := int(^uint(0) >> 1)
	got := benchmarkDocumentOrdinal(op, 37, documents)
	want := int((uint64(op) * 37) % documents)
	if got != want {
		t.Fatalf("ordinal=%d want %d", got, want)
	}
	if got < 0 || got >= documents {
		t.Fatalf("ordinal=%d out of range [0,%d)", got, documents)
	}
}

func TestBenchmarkUpdatedCityIndexAvoidsIntOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	cycle := 65521
	got := benchmarkUpdatedCityIndex(maxInt, maxInt-1, maxInt-2, cycle)
	seed := (uint64(maxInt)+1)*0x9e3779b185ebca87 ^
		bits.RotateLeft64((uint64(maxInt-1)+1)*0xc2b2ae3d27d4eb4f, 17) ^
		bits.RotateLeft64((uint64(maxInt-2)+1)*0x165667b19e3779f9, 31)
	seed += 0x9e3779b97f4a7c15
	seed = (seed ^ (seed >> 30)) * 0xbf58476d1ce4e5b9
	seed = (seed ^ (seed >> 27)) * 0x94d049bb133111eb
	seed ^= seed >> 31
	want := int(seed % uint64(cycle))
	if got != want {
		t.Fatalf("updated city index=%d want %d", got, want)
	}
}

func TestMergeTreeDBPersistentStatsPreservesProcessCounters(t *testing.T) {
	base := map[string]string{
		"treedb.commit_seq": "10",
		"treedb.publish.ordered_root_delta_group.calls_total": "7",
		"treedb.publish.watermark.latency_p99_ms":             "1.250",
	}
	refreshed := map[string]string{
		"treedb.commit_seq": "12",
		"treedb.publish.ordered_root_delta_group.calls_total": "0",
		"treedb.publish.watermark.latency_p99_ms":             "0.000",
	}
	got := mergeTreeDBPersistentStats(base, refreshed)
	if got["treedb.commit_seq"] != "12" {
		t.Fatalf("commit_seq=%q want refreshed value", got["treedb.commit_seq"])
	}
	if got["treedb.publish.ordered_root_delta_group.calls_total"] != "7" {
		t.Fatalf("calls_total=%q want in-memory counter preserved", got["treedb.publish.ordered_root_delta_group.calls_total"])
	}
	if got["treedb.publish.watermark.latency_p99_ms"] != "1.250" {
		t.Fatalf("latency_p99_ms=%q want in-memory counter preserved", got["treedb.publish.watermark.latency_p99_ms"])
	}
	got["treedb.commit_seq"] = "mutated"
	if base["treedb.commit_seq"] != "10" {
		t.Fatalf("base map was aliased: %v", base)
	}
	if refreshed["treedb.commit_seq"] != "12" {
		t.Fatalf("refreshed map was aliased: %v", refreshed)
	}
}

func TestMergeTreeDBPersistentStatsClonesFastPaths(t *testing.T) {
	base := map[string]string{"treedb.publish.ordered_root_delta_group.calls_total": "7"}
	got := mergeTreeDBPersistentStats(base, nil)
	got["treedb.publish.ordered_root_delta_group.calls_total"] = "mutated"
	if base["treedb.publish.ordered_root_delta_group.calls_total"] != "7" {
		t.Fatalf("base-only merge aliased input: %v", base)
	}

	refreshed := map[string]string{"treedb.commit_seq": "12"}
	got = mergeTreeDBPersistentStats(nil, refreshed)
	got["treedb.commit_seq"] = "mutated"
	if refreshed["treedb.commit_seq"] != "12" {
		t.Fatalf("refreshed-only merge aliased input: %v", refreshed)
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

func TestRangePhaseNameDistinguishesScanAndIndexedRuns(t *testing.T) {
	if got := rangePhaseName(config{}); got != "age_range_scan_limit_10" {
		t.Fatalf("default range phase name=%q want scan", got)
	}
	if got := rangePhaseName(config{RangeIndex: true}); got != "age_range_indexed_limit_10" {
		t.Fatalf("indexed range phase name=%q want indexed", got)
	}
}

func TestWriteResultKeepsTextHeaderStableForIndexedUpdateKnob(t *testing.T) {
	result := &benchmarkResult{
		Target:          "treedb",
		Database:        "bench",
		Collection:      "docs",
		Documents:       1,
		BatchSize:       1,
		ClientMode:      "driver-command-raw",
		ConcurrentReads: 2,
	}
	var out bytes.Buffer
	if err := writeResult(&out, "text", result); err != nil {
		t.Fatalf("writeResult false: %v", err)
	}
	firstLine := strings.SplitN(out.String(), "\n", 2)[0]
	if strings.Contains(firstLine, "update_indexed_field") {
		t.Fatalf("text header should not include update_indexed_field by default: %q", firstLine)
	}
	lines := strings.Split(out.String(), "\n")
	if len(lines) < 2 || lines[1] != "update_indexed_field=false" {
		t.Fatalf("text output missing separate update_indexed_field=false line: %q", out.String())
	}
	if len(lines) < 3 || lines[2] != "range_index=false" {
		t.Fatalf("text output missing separate range_index=false line: %q", out.String())
	}

	out.Reset()
	result.UpdateIndexedField = true
	result.RangeIndex = true
	if err := writeResult(&out, "text", result); err != nil {
		t.Fatalf("writeResult true: %v", err)
	}
	lines = strings.Split(out.String(), "\n")
	if len(lines) < 2 || lines[1] != "update_indexed_field=true" {
		t.Fatalf("text output missing separate update_indexed_field line: %q", out.String())
	}
	if len(lines) < 3 || lines[2] != "range_index=true" {
		t.Fatalf("text output missing separate range_index line: %q", out.String())
	}
}
