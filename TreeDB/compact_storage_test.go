package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCompactStoragePublicCommandWALRelaxedExhaustiveFailsClosedForCachedWrapper(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.DisableSideStores = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open command_wal_relaxed public cached DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.SetSync([]byte("canonical"), bytes.Repeat([]byte("v"), 512)); err != nil {
		t.Fatalf("SetSync canonical public cached write: %v", err)
	}

	classification := db.CompactStorageLeafPageLogOwnerClassification(treedb.CompactStorageLifecycleQuiescedMaintenance)
	if classification.OwnerClass != treedb.CompactStorageLeafPageLogOwnerCachedWrapper {
		t.Fatalf("owner=%q want %q (classification=%+v)", classification.OwnerClass, treedb.CompactStorageLeafPageLogOwnerCachedWrapper, classification)
	}
	if classification.Status != treedb.CompactStorageOwnerStatusLiveWriterFailClosed {
		t.Fatalf("status=%q want %q (classification=%+v)", classification.Status, treedb.CompactStorageOwnerStatusLiveWriterFailClosed, classification)
	}
	if !classification.RequiresQuiescence {
		t.Fatalf("RequiresQuiescence=false, want true (classification=%+v)", classification)
	}
	if classification.Replaceable {
		t.Fatalf("Replaceable=true, want false (classification=%+v)", classification)
	}
	for _, want := range []string{"background flush/apply workers", "checkpoint/close drains", "cached backlog"} {
		if !strings.Contains(classification.Detail, want) {
			t.Fatalf("classification detail=%q, want %q", classification.Detail, want)
		}
	}

	if _, err := db.CompactStorage(context.Background(), treedb.CompactStorageOptions{Mode: treedb.CompactStorageExhaustive}); !errors.Is(err, treedb.ErrCompactStorageLeafPageLogOwnerUnsupported) {
		t.Fatalf("CompactStorage exhaustive error=%v, want owner unsupported", err)
	}
	got, err := db.Get([]byte("canonical"))
	if err != nil {
		t.Fatalf("Get canonical after refused compact: %v", err)
	}
	want := bytes.Repeat([]byte("v"), 512)
	if !bytes.Equal(got, want) {
		t.Fatalf("canonical value mismatch after refused compact: got %q want %q", got, want)
	}
	if err := db.SetSync([]byte("post-refusal"), []byte("ok")); err != nil {
		t.Fatalf("post-refusal SetSync through original cached owner: %v", err)
	}
	got, err = db.Get([]byte("post-refusal"))
	if err != nil {
		t.Fatalf("Get post-refusal: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("post-refusal value=%q want ok", got)
	}
}

func TestCompactStorageFullPacksLeafGenerationDebtOffline(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableSideStores = true
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 64 << 10
	opts.ValueLog.Generational.HotSegmentTargetBytes = 64 << 10
	opts.ValueLog.Generational.WarmSegmentTargetBytes = 64 << 10
	opts.ValueLog.Generational.ColdSegmentTargetBytes = 64 << 10

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaintenancePhase(treedb.MaintenancePhaseRestore)
	writeLeafGenerationChurnWorkload(t, db, 20000, 5000, 4, 96)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	backend, cleanup, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	cleanupDone := false
	defer func() {
		if !cleanupDone {
			_ = cleanup()
		}
	}()

	compactOpts := treedb.CompactStorageOptions{
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	}
	before, err := backend.CompactStoragePlan(context.Background(), compactOpts)
	if err != nil {
		t.Fatalf("CompactStoragePlan before: %v", err)
	}
	if before.RemainingDebt.LeafPackGenerations == 0 || before.RemainingDebt.LeafPackBytes == 0 {
		t.Fatalf("expected leaf-pack debt before compaction, debt=%+v", before.RemainingDebt)
	}

	stats, err := backend.CompactStorage(context.Background(), compactOpts)
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized || stats.RemainingDebt.LeafGCGenerations == 0 {
		t.Fatalf("retained leaf generation overstated completion: %+v", stats)
	}
	if len(stats.LeafGenerationPacks) == 0 || !stats.LeafGenerationPacks[0].Ran {
		t.Fatalf("expected at least one leaf-generation pack run, packs=%+v", stats.LeafGenerationPacks)
	}

	again, err := backend.CompactStoragePlan(context.Background(), compactOpts)
	if err != nil {
		t.Fatalf("CompactStoragePlan after: %v", err)
	}
	if again.RemainingDebt.LeafPackGenerations != 0 || again.RemainingDebt.LeafPackBytes != 0 {
		t.Fatalf("leaf-pack debt remains after compaction, debt=%+v", again.RemainingDebt)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	cleanupDone = true

	reopened, err := treedb.Open(treedb.Options{Dir: dir, DisableSideStores: true})
	if err != nil {
		t.Fatalf("reopen after CompactStorage: %v", err)
	}
	value, err := reopened.Get([]byte("k00000000"))
	closeErr := reopened.Close()
	if err != nil {
		t.Fatalf("get after CompactStorage reopen: %v", err)
	}
	if len(value) == 0 {
		t.Fatal("empty value after CompactStorage reopen")
	}
	if closeErr != nil {
		t.Fatalf("close reopened: %v", closeErr)
	}
}

func TestCompactStorageCachedRefreshesProtectedPathsAcrossPhases(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableSideStores = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.SetSync([]byte("k"), bytes.Repeat([]byte("v"), 256)); err != nil {
		t.Fatalf("set: %v", err)
	}

	calls := 0
	if _, err := db.CompactStorage(context.Background(), treedb.CompactStorageOptions{
		ValueLogProtectedPathsFunc: func() []string {
			calls++
			return []string{"user-protected-path"}
		},
	}); err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if calls < 3 {
		t.Fatalf("protected path callback calls=%d want at least 3", calls)
	}
}

func TestCompactStorageCachedPlanReportsZeroByteValueLogDebt(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableSideStores = true

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	valueLogDir := backenddb.ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	emptyPath := filepath.Join(valueLogDir, "value-l42-000001.log")
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}

	stats, err := db.CompactStoragePlan(context.Background(), treedb.CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := stats.RemainingDebt.ZeroByteValueLogFiles; got != 1 {
		t.Fatalf("zero-byte debt=%d want 1", got)
	}
	if _, err := os.Stat(emptyPath); err != nil {
		t.Fatalf("plan mutated empty value-log file: %v", err)
	}
}

func TestCompactStorageCachedDeletesZeroByteValueLogFiles(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableSideStores = true

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	valueLogDir := backenddb.ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	emptyPath := filepath.Join(valueLogDir, "value-l42-000001.log")
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}

	stats, err := db.CompactStorage(context.Background(), treedb.CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if got := stats.ZeroByteValueLogFilesDeleted; got != 1 {
		t.Fatalf("deleted zero-byte files=%d want 1", got)
	}
	if got := stats.RemainingDebt.ZeroByteValueLogFiles; got != 0 {
		t.Fatalf("remaining zero-byte debt=%d want 0", got)
	}
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("empty value-log file still exists or stat failed: %v", err)
	}
}

func TestCachedValueLogWritersAreLazy(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if total, zero, _ := countValueLogSegmentFiles(t, dir); total != 0 || zero != 0 {
		_ = db.Close()
		t.Fatalf("fresh read/write open created value-log files: total=%d zero=%d", total, zero)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fresh open: %v", err)
	}
	if total, zero, _ := countValueLogSegmentFiles(t, dir); total != 0 || zero != 0 {
		t.Fatalf("fresh close left value-log files: total=%d zero=%d", total, zero)
	}

	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen for write: %v", err)
	}
	if err := db.SetSync([]byte("large"), bytes.Repeat([]byte("v"), 256)); err != nil {
		_ = db.Close()
		t.Fatalf("set large value: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after write: %v", err)
	}
	total, zero, nonzero := countValueLogSegmentFiles(t, dir)
	if nonzero == 0 {
		t.Fatalf("expected one written value-log segment, total=%d zero=%d nonzero=%d", total, zero, nonzero)
	}
	if zero != 0 {
		t.Fatalf("write created inactive zero-byte value-log files: total=%d zero=%d nonzero=%d", total, zero, nonzero)
	}
	if total > 4 {
		t.Fatalf("write created unexpected value-log fan-out: total=%d zero=%d nonzero=%d", total, zero, nonzero)
	}

	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatalf("maintenance-style reopen: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close maintenance-style reopen: %v", err)
	}
	afterTotal, afterZero, afterNonzero := countValueLogSegmentFiles(t, dir)
	if afterTotal != total || afterZero != 0 || afterNonzero != nonzero {
		t.Fatalf("read/write reopen changed value-log files: before total=%d nonzero=%d after total=%d zero=%d nonzero=%d",
			total, nonzero, afterTotal, afterZero, afterNonzero)
	}
}

func countValueLogSegmentFiles(t *testing.T, rootDir string) (total, zero, nonzero int) {
	t.Helper()
	valueLogDir := filepath.Join(rootDir, "maindb", "value_vlog")
	entries, err := os.ReadDir(valueLogDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, 0
		}
		t.Fatalf("read value_vlog dir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matched, err := filepath.Match("value-l*.log", entry.Name())
		if err != nil {
			t.Fatalf("match value-log name: %v", err)
		}
		if !matched {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("stat value-log segment: %v", err)
		}
		total++
		if info.Size() == 0 {
			zero++
		} else {
			nonzero++
		}
	}
	return total, zero, nonzero
}
