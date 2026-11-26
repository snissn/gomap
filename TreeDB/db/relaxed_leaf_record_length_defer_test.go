package db

import (
	"bytes"
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

func TestRelaxedDeferredManifestSurvivesCrashReopenWithoutFlush(t *testing.T) {
	db, leafLog, opts := openLeafLogLaneGCTestDB(t, 32<<10)
	closed := false
	t.Cleanup(func() {
		if !closed {
			closeLeafLogLaneGCTestDB(t, db, leafLog)
		}
	})

	const rounds = 4
	const keysPerRound = 512
	val := []byte("val-relaxed-crash-reopen-unflushed-manifest-0123456789")
	for round := 0; round < rounds; round++ {
		b := db.NewBatch()
		for i := 0; i < keysPerRound; i++ {
			if err := b.Set(leafGenerationKey("crash", round*keysPerRound+i), val); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatal(err)
		}
		_ = b.Close()
	}

	if !db.leafGenerationManifestDirty.Load() {
		t.Fatal("expected deferred operational leaf-generation manifest to remain dirty after relaxed writes")
	}

	// Verify the in-memory manifest tracks newly created leaf-log segments while
	// the on-disk operational manifest has not yet been flushed.
	db.mu.RLock()
	inMemIDs := leafGenerationManifestRawFileIDSet(db.leafGenerationManifest.clone())
	db.mu.RUnlock()
	onDiskBefore := leafGenerationManifestRawFileIDSet(loadLeafGenerationManifestOrFatal(t, opts.Dir))
	if len(inMemIDs) <= len(onDiskBefore) {
		t.Fatalf("expected in-memory manifest file IDs (%d) to exceed unflushed on-disk manifest (%d)", len(inMemIDs), len(onDiskBefore))
	}

	// Wait for the background root publisher to persist the durable root meta
	// pointing at the split leaf-log segments WITHOUT calling Checkpoint,
	// leaving the deferred operational manifest dirty and unflushed.
	if runtime := db.rootPublication; runtime != nil && runtime.coordinator != nil {
		visibleSeq := db.state.Load().CommitSeq
		if err := runtime.coordinator.WaitThrough(context.Background(), visibleSeq); err != nil {
			t.Fatalf("WaitThrough(%d): %v", visibleSeq, err)
		}
	} else {
		if err := leafLog.Sync(); err != nil {
			t.Fatalf("leafLog.Sync: %v", err)
		}
		if gen := db.idx.Load(); gen != nil && gen.pager != nil {
			if err := gen.pager.Sync(); err != nil {
				t.Fatalf("pager.Sync: %v", err)
			}
		}
	}
	if !db.leafGenerationManifestDirty.Load() {
		t.Fatal("expected operational manifest to still be unflushed after durable root publication")
	}

	// Simulate an abrupt exit before Close can flush the deferred operational
	// manifest and advisory .lenidx files, preserving exact physical file inodes
	// on disk so reopen exercises true reconciliation against the stale manifest.
	db.leafGenerationManifestDirty.Store(false)
	db.leafGenerationRecordLengthMu.Lock()
	clear(db.leafGenerationDeferredRecordLengthFileIDs)
	db.leafGenerationRecordLengthMu.Unlock()
	if err := db.Close(); err != nil {
		t.Fatalf("Close DB without manifest flush: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}
	closed = true

	onDiskAtReopen := leafGenerationManifestRawFileIDSet(loadLeafGenerationManifestOrFatal(t, opts.Dir))
	if len(onDiskAtReopen) != len(onDiskBefore) {
		t.Fatalf("expected on-disk manifest to remain unflushed before reopen: got %d file IDs, want %d", len(onDiskAtReopen), len(onDiskBefore))
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen with unflushed operational manifest: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened db: %v", err)
		}
	}()

	// Reopen must reconcile every unrecorded leaf-log segment into manifest.json
	// and keep every reachable outer-leaf pointer readable.
	reconciledDisk := leafGenerationManifestRawFileIDSet(loadLeafGenerationManifestOrFatal(t, opts.Dir))
	for rawFileID := range inMemIDs {
		if _, ok := reconciledDisk[rawFileID]; !ok {
			t.Fatalf("reopened on-disk manifest missing reconciled leaf-log file ID %d (have=%v)", rawFileID, reconciledDisk)
		}
	}
	for i := 0; i < rounds*keysPerRound; i++ {
		got, err := reopened.Get(leafGenerationKey("crash", i))
		if err != nil {
			t.Fatalf("reopened Get(%d): %v", i, err)
		}
		if !bytes.Equal(got, val) {
			t.Fatalf("reopened value mismatch at key %d: got %q want %q", i, got, val)
		}
	}
	plan, err := reopened.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{})
	if err != nil {
		t.Fatalf("reopened LeafGenerationPlan: %v", err)
	}
	if len(plan.Generations) < 2 {
		t.Fatalf("expected reconciled generations after crash reopen, got %d", len(plan.Generations))
	}
}
