package db

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestRelaxedFinalizeDefersAdvisoryRecordLengthIndexSyncUntilCheckpoint(t *testing.T) {
	db, leafLog, opts := openLeafLogLaneGCTestDB(t, 32<<10)
	t.Cleanup(func() { closeLeafLogLaneGCTestDB(t, db, leafLog) })

	var lenidxSyncs atomic.Int64
	var manifestSyncs atomic.Int64
	uninstall := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeDependencyFileSync && strings.Contains(e.Path, leafGenerationRecordLengthIndexSuffix) {
			lenidxSyncs.Add(1)
		}
		if (e.Point == durabilitycut.BeforeDependencyFileSync || e.Point == durabilitycut.BeforeNewFileDirectorySync) &&
			(strings.Contains(e.Path, leafGenerationManifestFileName) || strings.Contains(e.Path, "manifest.durable.")) {
			manifestSyncs.Add(1)
		}
		return nil
	})
	defer uninstall()

	// Write enough keys with relaxed batches to roll over the 32 KiB leaf-log
	// segment and seal at least one generation during relaxed finalizeCommitPostWork.
	for round := 0; round < 4; round++ {
		b := db.NewBatch()
		for i := 0; i < 512; i++ {
			if err := b.Set(leafGenerationKey("gen", round*512+i), []byte("val-relaxed-defer-lenidx-payload-0123456789")); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatal(err)
		}
		_ = b.Close()
	}

	if got := lenidxSyncs.Load(); got != 0 {
		t.Fatalf("relaxed commits synchronously synced %d advisory .lenidx files; want 0", got)
	}
	if got := manifestSyncs.Load(); got != 0 {
		t.Fatalf("relaxed commits synchronously synced %d operational leaf-generation manifest files; want 0", got)
	}

	// Maintenance footprint admission must reject oversized requests BEFORE
	// flushing any deferred manifest or advisory .lenidx files.
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{
		MaintenanceLimits: LeafGenerationMaintenanceLimits{NativeEntries: 1, NativeBytes: 1, PagerPages: 1},
	}); err == nil {
		t.Fatal("expected LeafGenerationPlan with tight MaintenanceLimits to fail admission")
	}
	if got := manifestSyncs.Load(); got != 0 {
		t.Fatalf("rejected LeafGenerationPlan flushed %d deferred operational manifest updates; want 0", got)
	}
	if got := lenidxSyncs.Load(); got != 0 {
		t.Fatalf("rejected LeafGenerationPlan flushed %d deferred .lenidx files; want 0", got)
	}

	// Checkpoint must flush both the deferred manifest and advisory .lenidx files to disk.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := manifestSyncs.Load(); got == 0 {
		t.Fatalf("Checkpoint flushed %d deferred operational manifest updates; want >= 1", got)
	}
	if got := lenidxSyncs.Load(); got == 0 {
		t.Fatalf("Checkpoint flushed %d deferred .lenidx files; want >= 1", got)
	}

	plan, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	foundSealed := false
	for _, gen := range plan.Generations {
		if gen.State == leafGenerationStateSealed && len(gen.FileIDs) > 0 {
			foundSealed = true
			path := leafGenerationRecordLengthIndexPath(opts.Dir, gen.FileIDs[0])
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("expected persisted .lenidx %q after Checkpoint: %v", path, err)
			}
		}
	}
	if !foundSealed {
		t.Fatalf("expected at least one sealed generation in plan: %+v", plan.Generations)
	}
}
