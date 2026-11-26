package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func openCommandWALLeafGCTestDB(t *testing.T) (*DB, *rewriteWriter, string) {
	t.Helper()
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		CommandWAL:                 true,
		Durability:                 DurabilityWALOnRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		IndexPackedValuePtr:        true,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	d.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = writer.Close()
		_ = d.Close()
	})
	return d, writer, dir
}

func TestLeafGenerationGC_ConvergesFallbackSlotAndReclaimsDeadGeneration(t *testing.T) {
	d, leafLog, dir := openCommandWALLeafGCTestDB(t)

	writeLeafGenerationKeys(t, d, "k", 64, 'a')
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint gen1: %v", err)
	}
	if err := d.refreshDurableRootFallback(); err != nil {
		t.Fatalf("refreshDurableRootFallback gen1: %v", err)
	}
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	indexPath1 := leafGenerationRecordLengthIndexPath(dir, rawFileID1)

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	// Single synchronous write batch publishes gen2 into one alternating durable
	// meta slot while deterministically leaving the alternate durable slot on
	// the gen1 root established above.
	writeLeafGenerationKeys(t, d, "k", 64, 'b')
	path2, fileID2 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID2 := page.ValueLogSegmentID(fileID2)
	d.maintenanceMu.Lock()
	roots, err := d.captureRecoverableRootSetForInspectionWithMaintenanceLockHeld(context.Background())
	d.maintenanceMu.Unlock()
	if err != nil {
		t.Fatalf("captureRecoverableRootSetForInspection: %v", err)
	}
	if len(roots.roots) < 2 {
		roots.Release()
		t.Fatalf("expected distinct current and fallback recoverable roots, got %d (%+v)", len(roots.roots), roots.roots)
	}
	roots.Release()

	// Dry-run must not mutate durable slots and must report the older
	// fallback-protected generation as retiring rather than eligible.
	dryStats, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run: %v", err)
	}
	if got, want := dryStats.GenerationsRetiring, 1; got != want {
		t.Fatalf("dry-run GenerationsRetiring=%d want %d (stats=%+v)", got, want, dryStats)
	}
	if dryStats.GenerationsEligible != 0 || dryStats.BytesEligible != 0 {
		t.Fatalf("dry-run unexpectedly marked fallback-protected generation eligible: %+v", dryStats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("dry-run disturbed older generation: %v", err)
	}

	// Ordinary GC (ConvergeFallback: false) must preserve the fallback root
	// and leave the generation in retiring rather than deleting it.
	ordinaryStats, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("ordinary LeafGenerationGC: %v", err)
	}
	if got, want := ordinaryStats.GenerationsRetiring, 1; got != want {
		t.Fatalf("ordinary GenerationsRetiring=%d want %d (stats=%+v)", got, want, ordinaryStats)
	}
	if ordinaryStats.GenerationsDeleted != 0 || ordinaryStats.FilesDeleted != 0 {
		t.Fatalf("ordinary GC unexpectedly deleted fallback-protected generation: %+v", ordinaryStats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("ordinary GC removed fallback-protected generation: %v", err)
	}

	// A single destructive LeafGenerationGC call with ConvergeFallback: true
	// must converge the stale durable fallback slot, drop conservative
	// outer-leaf pins if present, and reclaim the unreachable generation without
	// requiring a manual fallback refresh or extra writes.
	stats, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{ConvergeFallback: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if got, want := stats.GenerationsDeleted, 1; got != want {
		t.Fatalf("GenerationsDeleted=%d want %d (stats=%+v)", got, want, stats)
	}
	if got, want := stats.FilesDeleted, 1; got != want {
		t.Fatalf("FilesDeleted=%d want %d (stats=%+v)", got, want, stats)
	}
	if stats.BytesDeleted <= 0 {
		t.Fatalf("BytesDeleted=%d want > 0 (stats=%+v)", stats.BytesDeleted, stats)
	}
	if stats.GenerationsRetiring != 0 {
		t.Fatalf("GenerationsRetiring=%d want 0 (stats=%+v)", stats.GenerationsRetiring, stats)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
	if err := waitForPathRemoval(indexPath1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", indexPath1, err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("live leaf segment removed: %v", err)
	}
	manifestAfter := loadLeafGenerationManifestOrFatal(t, dir)
	if got, want := len(manifestAfter.Generations), 1; got != want {
		t.Fatalf("len(manifestAfter.Generations)=%d want %d (%+v)", got, want, manifestAfter.Generations)
	}
	if got := manifestAfter.Generations[0].FileIDs[0]; got != rawFileID2 {
		t.Fatalf("remaining fileID=%d want %d", got, rawFileID2)
	}

	// Verify fallback-slot durability: corrupt the newest meta page and reopen.
	newestSlot := d.metaPageID
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("leafLog Close: %v", err)
	}
	corruptIndexPageByte(t, dir, newestSlot)

	reopened, err := Open(Options{
		Dir:                        dir,
		CommandWAL:                 true,
		Durability:                 DurabilityWALOnRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("fallback reopen after GC: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	wantVal := bytes.Repeat([]byte{'b'}, 32)
	for i := 0; i < 64; i++ {
		got, err := reopened.Get([]byte(fmt.Sprintf("k-%04d", i)))
		if err != nil {
			t.Fatalf("Get k-%04d after fallback reopen: %v", i, err)
		}
		if !bytes.Equal(got, wantVal) {
			t.Fatalf("k-%04d value mismatch after fallback reopen", i)
		}
	}
}

func TestLeafGenerationGC_PinnedSnapshotStillRetainsGenerationAcrossFallback(t *testing.T) {
	d, leafLog, _ := openCommandWALLeafGCTestDB(t)

	writeLeafGenerationKeys(t, d, "pin", 64, 'a')
	path1, _ := currentLeafSegmentOrFatal(t, leafLog)

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() {
		if snap != nil {
			_ = snap.Close()
		}
	}()

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, d, "pin", 64, 'b')
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	heldStats, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{ConvergeFallback: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC while snapshot pinned: %v", err)
	}
	if heldStats.GenerationsDeleted != 0 || heldStats.FilesDeleted != 0 || heldStats.BytesDeleted != 0 {
		t.Fatalf("GC deleted generation pinned by snapshot: %+v", heldStats)
	}
	if got, want := heldStats.GenerationsRetiring, 1; got != want {
		t.Fatalf("GenerationsRetiring=%d want %d while pinned", got, want)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("pinned leaf segment missing: %v", err)
	}
	oldVal, err := snap.Get([]byte("pin-0000"))
	if err != nil || !bytes.Equal(oldVal, bytes.Repeat([]byte{'a'}, 32)) {
		t.Fatalf("pinned snapshot read=%q err=%v", oldVal, err)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	snap = nil

	stats, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{ConvergeFallback: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC after snapshot release: %v", err)
	}
	if got, want := stats.GenerationsDeleted, 1; got != want {
		t.Fatalf("GenerationsDeleted=%d want %d after release (stats=%+v)", got, want, stats)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
}

type advancePackedSourcesTestLog struct {
	current       []LeafPageLogSegment
	advancedToSeq uint32
	advanceCalls  int
}

func (l *advancePackedSourcesTestLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, nil
}

func (l *advancePackedSourcesTestLog) Flush() error { return nil }
func (l *advancePackedSourcesTestLog) Sync() error  { return nil }

func (l *advancePackedSourcesTestLog) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	return append([]LeafPageLogSegment(nil), l.current...), nil
}

func (l *advancePackedSourcesTestLog) AdvanceCompactStorageLeafPageLogSeqAtLeast(seq uint32) error {
	l.advanceCalls++
	l.advancedToSeq = seq
	return nil
}

func TestAdvanceLeafPageLogPastPackedSourcesRotatesMatchingLanes(t *testing.T) {
	fileID2, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 2)
	if err != nil {
		t.Fatalf("EncodeFileID(2): %v", err)
	}
	fileID10, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 10)
	if err != nil {
		t.Fatalf("EncodeFileID(10): %v", err)
	}
	fileID11, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 11)
	if err != nil {
		t.Fatalf("EncodeFileID(11): %v", err)
	}
	raw2 := page.ValueLogSegmentID(fileID2)
	raw10 := page.ValueLogSegmentID(fileID10)
	raw11 := page.ValueLogSegmentID(fileID11)

	log := &advancePackedSourcesTestLog{
		current: []LeafPageLogSegment{
			{Path: filepath.Join(t.TempDir(), "value-l255-000002.log"), FileID: fileID2},
		},
	}
	d := &DB{leafPageLog: log}

	// Disjoint source IDs must not trigger any rotation.
	if err := d.advanceLeafPageLogPastPackedSources([]uint32{raw10}, []uint32{raw11}); err != nil {
		t.Fatalf("disjoint advanceLeafPageLogPastPackedSources: %v", err)
	}
	if log.advanceCalls != 0 {
		t.Fatalf("disjoint advanceCalls=%d want 0", log.advanceCalls)
	}

	// Overlapping active lane writer must advance past the highest created seq.
	if err := d.advanceLeafPageLogPastPackedSources([]uint32{raw2, raw10}, []uint32{raw11}); err != nil {
		t.Fatalf("overlapping advanceLeafPageLogPastPackedSources: %v", err)
	}
	if log.advanceCalls != 1 || log.advancedToSeq != 11 {
		t.Fatalf("advanceCalls=%d advancedToSeq=%d want 1 / 11", log.advanceCalls, log.advancedToSeq)
	}
}

func TestLeafGenerationPackAndGC_ReclaimsSourcesWithoutManualFallbackRefresh(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, dir := openCommandWALLeafGCTestDB(t)

	writeLeafGenerationKeys(t, d, "pack", 1024, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "pack", 0, 512, 'b')
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint before pack: %v", err)
	}

	packStats, err := d.LeafGenerationPackFromPlan(context.Background(), LeafGenerationPackFromPlanOptions{
		Force:          true,
		MaxGenerations: 0,
		Sync:           true,
		LeafFrameK:     16,
		ReserveRIDs:    leafLog.reserveRecordRIDs,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPackFromPlan: %v", err)
	}
	if packStats.GenerationsMatched == 0 || len(packStats.CreatedFileIDs) == 0 {
		t.Fatalf("unexpected pack stats: %+v", packStats)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after pack: %v", err)
	}

	gcStats, err := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{ConvergeFallback: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC after pack: %v", err)
	}
	if gcStats.GenerationsDeleted == 0 || gcStats.FilesDeleted == 0 || gcStats.BytesDeleted <= 0 {
		t.Fatalf("expected LeafGenerationGC to reclaim packed source without manual fallback refresh: %+v", gcStats)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("packed source %s was not removed: %v", path1, err)
	}
	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	for _, gen := range manifest.Generations {
		for _, fid := range gen.FileIDs {
			if fid == rawFileID1 {
				t.Fatalf("deleted packed source file %d still in manifest: %+v", rawFileID1, gen)
			}
		}
	}

	newestSlot := d.metaPageID
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("leafLog Close: %v", err)
	}
	corruptIndexPageByte(t, dir, newestSlot)

	reopened, err := Open(Options{
		Dir:                        dir,
		CommandWAL:                 true,
		Durability:                 DurabilityWALOnRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("fallback reopen after pack+GC: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	got0, err := reopened.Get([]byte("pack-0000"))
	if err != nil || !bytes.Equal(got0, bytes.Repeat([]byte{'b'}, 32)) {
		t.Fatalf("pack-0000=%q err=%v", got0, err)
	}
	got900, err := reopened.Get([]byte("pack-0900"))
	if err != nil || !bytes.Equal(got900, bytes.Repeat([]byte{'a'}, 32)) {
		t.Fatalf("pack-0900=%q err=%v", got900, err)
	}
}
