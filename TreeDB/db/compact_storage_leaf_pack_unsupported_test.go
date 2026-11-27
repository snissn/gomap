package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompactStorageNonExhaustiveHandlesUnsupportedLeafPackPromotion(t *testing.T) {
	// Inject ErrNamespacePersistenceUnsupported on the pack promotion target
	// probe so this contract is verified on Linux as well as darwin/windows.
	origProbe := leafGenerationPackPromotionTargetProbe
	leafGenerationPackPromotionTargetProbe = func(*leafGenerationPackPromotionAuthority) error {
		return fmt.Errorf("%w: injected unsupported leaf pack target", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	t.Cleanup(func() {
		leafGenerationPackPromotionTargetProbe = origProbe
	})

	newTestDB := func(t *testing.T) (*DB, *rewriteWriter) {
		t.Helper()
		dir := t.TempDir()
		d, err := Open(Options{
			Dir:                        dir,
			IndexOuterLeavesInValueLog: true,
			KeepRecent:                 1,
			DisableBackgroundPrune:     true,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = d.Close() })

		leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0, d.valueLogIdentityPins)
		leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
		d.SetLeafPageLog(leafLog)
		t.Cleanup(func() { _ = leafLog.Close() })

		writeBatch := func(start, end int, fill byte) {
			b := d.NewBatch()
			val := bytes.Repeat([]byte{fill}, 220)
			for i := start; i < end; i++ {
				if err := b.Set([]byte(fmt.Sprintf("k/%06d", i)), val); err != nil {
					t.Fatalf("Set: %v", err)
				}
			}
			if err := b.Write(); err != nil {
				t.Fatalf("Write: %v", err)
			}
			_ = b.Close()
		}

		// Create a partially-dead sealed generation (gen 1) so leaf-generation-pack
		// is admitted and attempts promotion, plus a 100%-dead sealed generation
		// (gen 2) so leaf-generation-gc has real work after pack is marked unsupported.
		writeBatch(0, 600, 'a')
		if err := leafLog.rotateLeaf(); err != nil {
			t.Fatalf("rotateLeaf 1: %v", err)
		}
		writeBatch(1000, 1400, 'b')
		if err := leafLog.rotateLeaf(); err != nil {
			t.Fatalf("rotateLeaf 2: %v", err)
		}
		// Partially overwrite gen 1 (leaving some keys live) and fully overwrite gen 2.
		writeBatch(0, 450, 'c')
		writeBatch(1000, 1400, 'd')
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		return d, leafLog
	}

	for _, mode := range []CompactStorageMode{CompactStorageFull, CompactStorageQuick} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			d, _ := newTestDB(t)
			stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{
				Mode:                            mode,
				LeafPackMinExpectedReclaimBytes: 1,
				LeafPackMinReclaimPerCopyPPM:    1,
			})
			if err != nil {
				t.Fatalf("CompactStorage(%s) returned error on unsupported leaf pack: %v", mode, err)
			}
			sawUnsupportedPack := 0
			sawSettlePack := false
			sawLeafGC := false
			for _, phase := range stats.Phases {
				if strings.HasPrefix(phase.Name, "leaf-generation-pack-") {
					if phase.Status != CompactStoragePhaseStatusUnsupported || !phase.Skipped {
						t.Fatalf("unexpected status on %s in %s mode: %+v", phase.Name, mode, phase)
					}
					sawUnsupportedPack++
				}
				if strings.HasPrefix(phase.Name, "settle-leaf-generation-pack-") {
					sawSettlePack = true
				}
				if phase.Name == "leaf-generation-gc" {
					sawLeafGC = true
				}
			}
			if sawUnsupportedPack != 1 {
				t.Fatalf("sawUnsupportedPack=%d want 1 in %s mode, phases=%+v", sawUnsupportedPack, mode, stats.Phases)
			}
			if sawSettlePack {
				t.Fatalf("unexpected settle-leaf-generation-pack retry after unsupported pack in %s mode, phases=%+v", mode, stats.Phases)
			}
			if !sawLeafGC {
				t.Fatalf("expected leaf-generation-gc to still run after unsupported pack in %s mode, phases=%+v", mode, stats.Phases)
			}
			if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
				t.Fatalf("expected compaction flags false while pack debt remains in %s mode: fully=%v policy=%v min=%v debt=%+v",
					mode, stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized, stats.RemainingDebt)
			}
			if stats.RemainingDebt.LeafPackGenerations == 0 {
				t.Fatalf("expected RemainingDebt.LeafPackGenerations > 0 in %s mode, debt=%+v", mode, stats.RemainingDebt)
			}
			// Depending on whether indexVacuumLeafGuard pins gen 2 through vacuum or
			// vacuum is unsupported (Windows), leaf-generation-gc marks gen 2 retiring,
			// eligible, or deleted during CompactStorage.
			if stats.LeafGenerationGC.GenerationsDeleted == 0 && stats.LeafGenerationGC.GenerationsEligible == 0 && stats.LeafGenerationGC.GenerationsRetiring == 0 && stats.LeafGenerationGC.BytesDeleted == 0 {
				t.Fatalf("expected dead gen 2 to be processed by leaf-generation-gc in %s mode, gc=%+v", mode, stats.LeafGenerationGC)
			}
		})
	}

	t.Run("recovery_required_joined_with_unsupported_remains_fatal", func(t *testing.T) {
		if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
			t.Skip("target probe injection requires platform preflight support")
		}
		orig := leafGenerationPackPromotionTargetProbe
		leafGenerationPackPromotionTargetProbe = func(*leafGenerationPackPromotionAuthority) error {
			return errors.Join(
				fmt.Errorf("%w: injected unsupported leaf pack target", rootpublication.ErrNamespacePersistenceUnsupported),
				ErrRecoveryRequired,
			)
		}
		t.Cleanup(func() { leafGenerationPackPromotionTargetProbe = orig })

		d, _ := newTestDB(t)
		_, err := d.CompactStorage(context.Background(), CompactStorageOptions{
			Mode:                            CompactStorageFull,
			LeafPackMinExpectedReclaimBytes: 1,
			LeafPackMinReclaimPerCopyPPM:    1,
		})
		if !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("CompactStorage(Full) err=%v want ErrRecoveryRequired", err)
		}
	})

	t.Run("exhaustive_remains_fail_closed", func(t *testing.T) {
		d, leafLog := newTestDB(t)
		if closer, ok := d.leafPageLog.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		d.SetLeafPageLog(nil)
		_ = leafLog.Close()
		_, err := d.CompactStorage(context.Background(), CompactStorageOptions{
			Mode: CompactStorageExhaustive,
		})
		if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatalf("CompactStorage(Exhaustive) err=%v want ErrNamespacePersistenceUnsupported", err)
		}
	})
}

type detachingTestLeafPageLog struct {
	current       []LeafPageLogSegment
	detachedCalls int
	detachedIDs   map[uint32]struct{}
}

func (l *detachingTestLeafPageLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, nil
}
func (l *detachingTestLeafPageLog) Flush() error { return nil }
func (l *detachingTestLeafPageLog) Sync() error  { return nil }
func (l *detachingTestLeafPageLog) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	return append([]LeafPageLogSegment(nil), l.current...), nil
}
func (l *detachingTestLeafPageLog) DetachSealedLeafPageLogSegments(sealedRawFileIDs map[uint32]struct{}) error {
	l.detachedCalls++
	if l.detachedIDs == nil {
		l.detachedIDs = make(map[uint32]struct{}, len(sealedRawFileIDs))
	}
	kept := l.current[:0]
	for _, seg := range l.current {
		rawID := page.ValueLogSegmentID(seg.FileID)
		if _, ok := sealedRawFileIDs[rawID]; ok {
			l.detachedIDs[rawID] = struct{}{}
			continue
		}
		kept = append(kept, seg)
	}
	l.current = kept
	return nil
}

func TestDetachSealedLeafPageLogSegmentsHelper(t *testing.T) {
	fileID1, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 1)
	if err != nil {
		t.Fatalf("EncodeFileID(1): %v", err)
	}
	fileID2, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 2)
	if err != nil {
		t.Fatalf("EncodeFileID(2): %v", err)
	}
	raw1 := page.ValueLogSegmentID(fileID1)
	raw2 := page.ValueLogSegmentID(fileID2)

	log := &detachingTestLeafPageLog{
		current: []LeafPageLogSegment{
			{Path: "value-l255-000001.log", FileID: fileID1},
			{Path: "value-l255-000002.log", FileID: fileID2},
		},
	}
	wrapped := &leafPageLogWithRecordLengthHints{inner: log}
	if err := detachSealedLeafPageLogSegments(wrapped, map[uint32]struct{}{raw1: {}}); err != nil {
		t.Fatalf("detachSealedLeafPageLogSegments: %v", err)
	}
	if log.detachedCalls != 1 {
		t.Fatalf("detachedCalls=%d want 1", log.detachedCalls)
	}
	if _, ok := log.detachedIDs[raw1]; !ok {
		t.Fatalf("expected raw1=%d to be detached", raw1)
	}
	if _, ok := log.detachedIDs[raw2]; ok {
		t.Fatalf("expected writable raw2=%d to remain attached", raw2)
	}
	if len(log.current) != 1 || log.current[0].FileID != fileID2 {
		t.Fatalf("remaining current=%+v want only fileID2", log.current)
	}

	// Verify *leafPageLogLaneGroup forwarding as well.
	logB := &detachingTestLeafPageLog{
		current: []LeafPageLogSegment{
			{Path: "value-l255-000001.log", FileID: fileID1},
		},
	}
	group := &leafPageLogLaneGroup{
		lanes:     []LeafPageLog{logB},
		laneLocks: []*sync.Mutex{newLeafPageLogLaneLock()},
	}
	if err := detachSealedLeafPageLogSegments(group, map[uint32]struct{}{raw1: {}}); err != nil {
		t.Fatalf("group detachSealedLeafPageLogSegments: %v", err)
	}
	if logB.detachedCalls != 1 || len(logB.current) != 0 {
		t.Fatalf("group detach did not forward to lane: calls=%d current=%+v", logB.detachedCalls, logB.current)
	}
}

func TestDemoteSealedLeafLogCurrentWritableSkipsStillAttachedSegments(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		KeepRecent:                 1,
		DisableBackgroundPrune:     true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0, d.valueLogIdentityPins)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	d.SetLeafPageLog(leafLog)
	t.Cleanup(func() {
		if closer, ok := d.leafPageLog.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		_ = leafLog.Close()
	})

	b := d.NewBatch()
	if err := b.Set([]byte("k1"), bytes.Repeat([]byte("a"), 256)); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()

	_, fileID1, ok := leafLog.CurrentValueLogSegment()
	if !ok {
		t.Fatal("missing current leaf segment 1")
	}
	raw1 := page.ValueLogSegmentID(fileID1)
	if err := d.valueLogManager.PromoteCurrentWritable(fileID1); err != nil {
		t.Fatalf("PromoteCurrentWritable(1): %v", err)
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	b2 := d.NewBatch()
	if err := b2.Set([]byte("k2"), bytes.Repeat([]byte("b"), 256)); err != nil {
		t.Fatal(err)
	}
	if err := b2.Write(); err != nil {
		t.Fatal(err)
	}
	_ = b2.Close()

	_, fileID2, ok := leafLog.CurrentValueLogSegment()
	if !ok {
		t.Fatal("missing current leaf segment 2")
	}
	raw2 := page.ValueLogSegmentID(fileID2)
	if err := d.valueLogManager.PromoteCurrentWritable(fileID2); err != nil {
		t.Fatalf("PromoteCurrentWritable(2): %v", err)
	}

	// Mark both raw1 and raw2 sealed, but raw2 is still attached in stillCurrent.
	// raw1 must be demoted while raw2 must remain currentWritable.
	d.demoteSealedLeafLogCurrentWritable(
		map[uint32]struct{}{raw1: {}, raw2: {}},
		map[uint32]struct{}{raw2: {}},
	)
	hasID := func(target uint32) bool {
		for _, id := range d.valueLogManager.CurrentWritableFileIDs() {
			if id == target {
				return true
			}
		}
		return false
	}
	if hasID(fileID1) {
		t.Fatalf("expected detached sealed fileID1=%d to be demoted", fileID1)
	}
	if !hasID(fileID2) {
		t.Fatalf("expected still-attached fileID2=%d to remain currentWritable", fileID2)
	}
}
