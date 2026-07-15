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

func waitForPathRemoval(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, err := os.Stat(path)
		if err == nil {
			if time.Now().After(deadline) {
				return fmt.Errorf("path still exists after %s: %s", timeout, path)
			}
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
}

func TestReferencedValueLogSegments_ExcludesLeafVlogSegmentsInSplitMode(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	_, fileID := currentLeafSegmentOrFatal(t, leafLog)
	leafFileID := page.ValueLogSegmentID(fileID)

	referenced, err := db.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("referencedValueLogSegments: %v", err)
	}
	if len(referenced) != 0 {
		t.Fatalf("expected no value_vlog refs for leaf-only split-log tree, got %v", referenced)
	}
	if _, ok := referenced[leafFileID]; ok {
		t.Fatalf("leaf_vlog file %d must not be tracked by generic value-log reachability", leafFileID)
	}
}

func TestValueLogGC_IgnoresLeafVlogSegmentsInSplitMode(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("stat first leaf segment: %v", err)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')
	path2, fileID2 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	rawFileID2 := page.ValueLogSegmentID(fileID2)

	probe, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{
		DryRun:                           true,
		ObservedSourceFileIDs:            []uint32{rawFileID1},
		ObservedSourceAssumeUnreferenced: true,
	})
	if err != nil {
		t.Fatalf("ValueLogGC probe: %v", err)
	}
	if probe.ObservedSourceSegments != 0 || probe.ObservedSourceSegmentsEligible != 0 || probe.ObservedSourceSegmentsDeleted != 0 {
		t.Fatalf("generic GC should ignore leaf_vlog observed sources, got %+v", probe)
	}

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted != 0 || stats.ObservedSourceSegments != 0 {
		t.Fatalf("generic GC should not delete or classify leaf_vlog segments, got %+v", stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("generic GC removed dead leaf segment unexpectedly: %v", err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("expected current leaf segment to remain, stat err=%v", err)
	}

	advancePastRetainedDurableSlotForTest(t, db)
	leafStats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if got, want := leafStats.GenerationsDeleted, 1; got != want {
		t.Fatalf("LeafGenerationGC.GenerationsDeleted=%d, want %d (stats=%+v)", got, want, leafStats)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("expected current leaf segment to remain after leaf GC, stat err=%v", err)
	}

	manifestAfter := loadLeafGenerationManifestOrFatal(t, db.dir)
	if got, want := len(manifestAfter.Generations), 1; got != want {
		t.Fatalf("len(manifestAfter.Generations)=%d, want %d", got, want)
	}
	if got, want := manifestAfter.Generations[0].FileIDs[0], rawFileID2; got != want {
		t.Fatalf("remaining generation fileID=%d, want %d", got, want)
	}
}

func TestValueLogRewriteOffline_PreservesLeafPagesInValueLogFormatConfig(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		_ = db.Close()
		t.Fatalf("mkdir wal: %v", err)
	}

	leafLog := newRewriteWriter(walDir, 0, 0, 0)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	// Keep the tree as a single leaf payload so the root internal page points
	// directly at leaf-log children.
	value := bytes.Repeat([]byte("v"), 16)
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		if err := db.Set(key, value); err != nil {
			_ = leafLog.Close()
			_ = db.Close()
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	state := db.State()
	if state == nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("missing db state")
	}
	if _, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(db.Pager(), state.RootPageID); err != nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("inspect root leaf-log children: %v", err)
	} else if !allLeafRefs {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("expected leaf-ref root page with leaf pages in vlog; root=%d", state.RootPageID)
	}

	if err := leafLog.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("leafLog close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// Preserve leaf-page-in-vlog mode via format.json (Options should not need to
	// restate it here).
	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	state2 := reopen.State()
	if state2 == nil {
		t.Fatalf("missing state after rewrite reopen")
	}
	if _, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(reopen.Pager(), state2.RootPageID); err != nil {
		t.Fatalf("inspect rewritten root leaf-log children: %v", err)
	} else if !allLeafRefs {
		t.Fatalf("expected leaf-ref root page after rewrite; root=%d", state2.RootPageID)
	}

	got, err := reopen.Get([]byte("k0000"))
	if err != nil {
		t.Fatalf("Get(k0000): %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch after rewrite")
	}
}
