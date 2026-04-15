package db

import (
	"context"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafGenerationPackRunOnce_MovesSparseGeneration(t *testing.T) {
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')

	manifestBefore := loadLeafGenerationManifestOrFatal(t, dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)

	stats, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{MaxGenerations: 1, Sync: true})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if !stats.Ran {
		t.Fatalf("expected run once to execute, skip_reason=%q plan=%+v", stats.SkipReason, stats.Plan)
	}
	if got, want := len(stats.Selection.GenerationIDs), 1; got != want || stats.Selection.GenerationIDs[0] != gen1.GenerationID {
		t.Fatalf("Selection.GenerationIDs=%v, want [%d]", stats.Selection.GenerationIDs, gen1.GenerationID)
	}
	if got, want := stats.Pack.GenerationsMatched, 1; got != want {
		t.Fatalf("Pack.GenerationsMatched=%d, want %d", got, want)
	}
	for i := 0; i < 1024; i++ {
		expectLeafGenerationValue(t, db, leafGenerationKey("k", i), 'b')
	}
	for i := 1024; i < 2048; i++ {
		expectLeafGenerationValue(t, db, leafGenerationKey("k", i), 'a')
	}
	for i := 0; i < 32; i++ {
		expectLeafGenerationValue(t, db, leafGenerationKey("z", i), 'z')
	}
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after run once: %v", err)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
}

func TestLeafGenerationPackRunOnce_SkipsDensePlan(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 32768, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1, 'b')

	stats, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{MinReclaimPerByteCopiedPPM: leafGenerationPackDefaultMinReclaimPerByteCopiedPPM})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if stats.Ran {
		t.Fatalf("expected dense plan to skip, got pack=%+v", stats.Pack)
	}
	if got, want := stats.SkipReason, "plan_admission:reclaim_per_copy_too_low"; got != want {
		t.Fatalf("SkipReason=%q, want %q", got, want)
	}
}
