package treedb_test

import (
	"context"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestCompactStorageFullPacksLeafGenerationDebtOffline(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
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

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	defer func() { _ = cleanup() }()

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
}
