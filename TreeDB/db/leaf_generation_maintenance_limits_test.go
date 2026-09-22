package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func generousLeafGenerationMaintenanceLimits() LeafGenerationMaintenanceLimits {
	return LeafGenerationMaintenanceLimits{NativeEntries: 1 << 20, NativeBytes: 1 << 40, PagerPages: 1 << 30}
}

func TestLeafGenerationMaintenanceLimitsRejectBeforeNativeWork(t *testing.T) {
	db, _, _ := openLeafGenerationPackTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 128, 'a')
	var indexScans, treeScans atomic.Int64
	defer registerLeafGenerationRecordLengthIndexScanHook(func(uint32) { indexScans.Add(1) })()
	defer registerLeafGenerationSubtreeCacheMissHook(func(uint64) { treeScans.Add(1) })()
	before := db.State().CommitSeq
	refreshes := db.valueLogManager.RefreshScanCount()
	operations := []struct {
		name string
		run  func(LeafGenerationMaintenanceLimits) error
	}{
		{"plan", func(l LeafGenerationMaintenanceLimits) error {
			_, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{MaintenanceLimits: l})
			return err
		}},
		{"pack-run-once", func(l LeafGenerationMaintenanceLimits) error {
			_, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{MaintenanceLimits: l, MaxGenerations: 1})
			return err
		}},
		{"gc", func(l LeafGenerationMaintenanceLimits) error {
			_, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{MaintenanceLimits: l})
			return err
		}},
	}
	for _, op := range operations {
		for _, field := range []string{"entries", "bytes", "pages", "partial"} {
			t.Run(op.name+"/"+field, func(t *testing.T) {
				limits := generousLeafGenerationMaintenanceLimits()
				switch field {
				case "entries":
					limits.NativeEntries = 1
				case "bytes":
					limits.NativeBytes = 1
				case "pages":
					limits.PagerPages = 1
				case "partial":
					limits.PagerPages = 0
				}
				if err := op.run(limits); !errors.Is(err, ErrLeafGenerationMaintenanceLimit) {
					t.Fatalf("error=%v, want footprint rejection", err)
				}
				if indexScans.Load() != 0 || treeScans.Load() != 0 {
					t.Fatalf("rejection scanned indexes=%d trees=%d", indexScans.Load(), treeScans.Load())
				}
				if got := db.valueLogManager.RefreshScanCount(); got != refreshes {
					t.Fatalf("rejection refreshed directories: got %d want %d", got, refreshes)
				}
				if got := db.State().CommitSeq; got != before {
					t.Fatalf("rejection published commit %d, want %d", got, before)
				}
			})
		}
	}
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 0), 'a')
}

func TestLeafGenerationMaintenanceLimitsDirectoryBoundary(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	limits := LeafGenerationMaintenanceLimits{NativeEntries: 1, NativeBytes: 4, PagerPages: 1}
	if err := limits.admitDirectory(context.Background(), root); err != nil {
		t.Fatalf("exact boundary: %v", err)
	}
	limits.NativeBytes--
	if err := limits.admitDirectory(context.Background(), root); !errors.Is(err, ErrLeafGenerationMaintenanceLimit) {
		t.Fatalf("byte overflow: %v", err)
	}
	limits.NativeBytes++
	if err := os.WriteFile(filepath.Join(root, "extra"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := limits.admitDirectory(context.Background(), root); !errors.Is(err, ErrLeafGenerationMaintenanceLimit) {
		t.Fatalf("entry overflow: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := limits.admitDirectory(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if err := (LeafGenerationMaintenanceLimits{}).admitDirectory(ctx, root); err != nil {
		t.Fatalf("zero must preserve legacy behavior: %v", err)
	}
}

func TestLeafGenerationMaintenanceLimitsManifestBeforeClone(t *testing.T) {
	limits := LeafGenerationMaintenanceLimits{NativeEntries: 1, NativeBytes: 4096, PagerPages: 1}
	manifest := &leafGenerationManifest{Generations: []leafGenerationRecord{{FileIDs: []uint32{1, 2}}}}
	if err := limits.admitManifest(manifest); !errors.Is(err, ErrLeafGenerationMaintenanceLimit) {
		t.Fatalf("file references: %v", err)
	}
	manifest.Generations = append(manifest.Generations, leafGenerationRecord{})
	if err := limits.admitManifest(manifest); !errors.Is(err, ErrLeafGenerationMaintenanceLimit) {
		t.Fatalf("generation table: %v", err)
	}
}

func TestLeafGenerationMaintenanceLimitsCapturedFiles(t *testing.T) {
	db, _, _ := openLeafGenerationPackTestDB(t)
	snap := db.AcquireSnapshot()
	defer snap.Close()
	file, err := os.CreateTemp(t.TempDir(), "captured-file-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(8); err != nil {
		t.Fatal(err)
	}
	// Exercise retained snapshot resources independently of the live directory:
	// a recovery snapshot can still own an older, unlinked native generation.
	state := *snap.state
	state.LeafGenerations = nil
	state.ValueLogSet = &valuelog.Set{Files: map[uint32]*valuelog.File{1: {File: file}}}
	captured := &Snapshot{state: &state, idx: snap.idx}
	limits := generousLeafGenerationMaintenanceLimits()
	limits.NativeEntries = 1
	limits.NativeBytes = int64(snap.idx.pager.PageCount())*page.PageSize + 8
	if err := limits.admitSnapshot(context.Background(), captured); err != nil {
		t.Fatalf("exact captured byte boundary: %v", err)
	}
	if err := file.Truncate(9); err != nil {
		t.Fatal(err)
	}
	if err := limits.admitSnapshot(context.Background(), captured); !errors.Is(err, ErrLeafGenerationMaintenanceLimit) {
		t.Fatalf("captured file growth: %v", err)
	}
	state.ValueLogSet.Files[2] = &valuelog.File{File: file}
	if err := limits.admitSnapshot(context.Background(), captured); !errors.Is(err, ErrLeafGenerationMaintenanceLimit) {
		t.Fatalf("captured file table: %v", err)
	}
}

func TestLeafGenerationMaintenanceLimitsPackRechecksRewriteSnapshot(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	db, leafLog, _ := openLeafGenerationPackTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')
	limits := generousLeafGenerationMaintenanceLimits()
	snap := db.AcquireSnapshot()
	limits.PagerPages = snap.idx.pager.PageCount()
	_ = snap.Close()
	var copies atomic.Int64
	defer registerLeafGenerationPackCopyHook(func(leafGenerationPackCopyEvent) { copies.Add(1) })()
	oldScanner := leafGenerationPackRIDStartScanner
	t.Cleanup(func() { leafGenerationPackRIDStartScanner = oldScanner })
	injected := false
	leafGenerationPackRIDStartScanner = func(set *valuelog.Set) (uint64, error) {
		start, err := oldScanner(set)
		if err == nil && !injected {
			injected = true
			// Planning and phase admission already passed. The rewrite must
			// admit its own later snapshot before walking or copying it.
			// Reserve one new pager page without relying on foreground writes
			// to exhaust the allocator's reusable free pages.
			growth := db.AcquireSnapshot()
			_, err = growth.idx.pager.Alloc(1)
			_ = growth.Close()
		}
		return start, err
	}
	_, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{
		Force: true, MaxGenerations: 1, MaintenanceLimits: limits,
	})
	if !injected || !errors.Is(err, ErrLeafGenerationMaintenanceLimit) {
		t.Fatalf("injected=%v error=%v, want late snapshot rejection", injected, err)
	}
	if copies.Load() != 0 {
		t.Fatalf("copied after oversized snapshot: %d", copies.Load())
	}
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 0), 'b')
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 1500), 'a')
}

func TestLeafGenerationMaintenanceLimitsRecoverableSnapshot(t *testing.T) {
	db, _, _ := openLeafGenerationPackTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 128, 'a')
	roots, err := db.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer roots.Release()
	limits := generousLeafGenerationMaintenanceLimits()
	limits.PagerPages = 1
	var scans atomic.Int64
	defer registerLeafGenerationSubtreeCacheMissHook(func(uint64) { scans.Add(1) })()
	_, err = db.collectRecoverableLeafGenerationIDs(context.Background(), roots, db.State().LeafGenerations, limits)
	if !errors.Is(err, ErrLeafGenerationMaintenanceLimit) || scans.Load() != 0 {
		t.Fatalf("error=%v scans=%d, want rejection before recovery walk", err, scans.Load())
	}
}

func TestLeafGenerationMaintenanceLimitsAdmittedPackAndGC(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	db, leafLog, dir := openLeafGenerationPackTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')
	limits := generousLeafGenerationMaintenanceLimits()
	packed, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{
		Force: true, MaxGenerations: 1, MaintenanceLimits: limits,
	})
	if err != nil || !packed.Ran {
		t.Fatalf("admitted pack: ran=%v error=%v skip=%q", packed.Ran, err, packed.SkipReason)
	}
	advanceLeafGenerationPackDurableRootHorizon(t, db, "bounded maintenance test")
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{MaintenanceLimits: limits}); err != nil {
		t.Fatalf("admitted GC: %v", err)
	}
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 0), 'b')
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 1500), 'a')
	closeNoErr(t, db)
	closeNoErr(t, leafLog)
	reopened, err := Open(Options{
		Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true,
		IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true,
		IndexColumnarLeaves: true, IndexPackedValuePtr: true,
	})
	if err != nil {
		t.Fatalf("reopen after admitted pack/GC: %v", err)
	}
	defer closeNoErr(t, reopened)
	expectLeafGenerationValue(t, reopened, leafGenerationKey("k", 0), 'b')
	expectLeafGenerationValue(t, reopened, leafGenerationKey("k", 1500), 'a')
}
