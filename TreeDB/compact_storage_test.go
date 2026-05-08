package treedb_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCompactStorageFullPacksLeafGenerationDebtOffline(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
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

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir, DisableSideStores: true})
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
	if !stats.FullyCompacted {
		t.Fatalf("FullyCompacted=false remaining debt=%+v", stats.RemainingDebt)
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
