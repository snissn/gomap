package db

import (
	"context"
	"sync/atomic"
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

func TestLeafGenerationPackRunOnce_SkipsTooFewCandidateGenerations(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')

	stats, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{MinCandidateGenerations: 2})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if stats.Ran {
		t.Fatalf("expected single sparse generation to skip, got pack=%+v", stats.Pack)
	}
	if got, want := stats.SkipReason, "plan_admission:too_few_generations"; got != want {
		t.Fatalf("SkipReason=%q, want %q", got, want)
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
	if got, want := stats.SkipReason, "selection:leaf generation pack selection: no candidate generations satisfy min-reclaim-per-byte-copied-ppm=10000"; got != want {
		t.Fatalf("SkipReason=%q, want %q", got, want)
	}
}

func TestLeafGenerationPackRunOnce_SkipsPlanWhenWindowYieldTooLow(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 32768, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1, 'b')

	stats, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{MinReclaimPerByteCopiedPPM: 200000})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if stats.Ran {
		t.Fatalf("expected low-yield window to skip, got pack=%+v", stats.Pack)
	}
	if got, want := stats.SkipReason, "selection:leaf generation pack selection: no candidate generations satisfy min-reclaim-per-byte-copied-ppm=200000"; got != want {
		t.Fatalf("SkipReason=%q, want %q", got, want)
	}
}

func TestLeafGenerationPackRunOnce_BoundedSelectionCanRunWhenWholePlanReclaimPerCopyIsLow(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "dense", 32768, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf dense: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "dense", 0, 1, 'b')

	writeLeafGenerationKeys(t, db, "sparse", 2048, 'c')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf sparse: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "sparse", 0, 1024, 'd')

	stats, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{
		MaxGenerations:             1,
		MinReclaimPerByteCopiedPPM: 200000,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if !stats.Ran {
		t.Fatalf("expected bounded selection to run, skip_reason=%q plan=%+v", stats.SkipReason, stats.Plan)
	}
	if got, want := len(stats.Selection.GenerationIDs), 1; got != want {
		t.Fatalf("len(Selection.GenerationIDs)=%d want %d", got, want)
	}
	if got := stats.Selection.ExpectedReclaimPerByteCopiedPPM; got < 200000 {
		t.Fatalf("ExpectedReclaimPerByteCopiedPPM=%d want >= 200000", got)
	}
}

func TestLeafGenerationPackRunOnce_CallsLeafGenerationPlanOnce(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')

	var calls atomic.Int64
	unregister := registerLeafGenerationPlanCallHook(func() {
		calls.Add(1)
	})
	defer unregister()

	stats, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{MaxGenerations: 1, Sync: true})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if !stats.Ran {
		t.Fatalf("expected run once to execute, skip_reason=%q", stats.SkipReason)
	}
	if got, want := calls.Load(), int64(1); got != want {
		t.Fatalf("LeafGenerationPlan calls=%d, want %d", got, want)
	}
}

func TestLeafGenerationPackRunOnce_UsesTranscodeFallbackWhenPlanHasNoDeadBytes(t *testing.T) {
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 32768, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1, 'b')

	manifestBefore := loadLeafGenerationManifestOrFatal(t, dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)

	prev := leafGenerationPackSelectTranscode
	leafGenerationPackSelectTranscode = func(_ *DB, _ context.Context, plan LeafGenerationPlan, _ LeafGenerationPackFromPlanOptions) (LeafGenerationPackSelection, error) {
		if got, want := plan.Admission, leafGenerationPlanAdmissionEligible; got != want {
			t.Fatalf("plan admission=%q want %q", got, want)
		}
		return LeafGenerationPackSelection{
			Mode:          leafGenerationPackSelectionModeTranscode,
			GenerationIDs: []uint64{gen1.GenerationID},
			Generations: []LeafGenerationPlanGeneration{
				{GenerationID: gen1.GenerationID, BytesTotal: 4096, BytesLive: 3584, BytesDead: 512, BytesToCopy: 4096, LivePages: 1},
			},
			BytesTotal:                         4096,
			BytesLive:                          3584,
			BytesDead:                          512,
			BytesToCopy:                        4096,
			LivePages:                          1,
			ExpectedReclaimBytes:               512,
			ExpectedReclaimRatioPPM:            ratioPPM(512, 4096),
			ExpectedReclaimPerByteCopiedPPM:    ratioPPM(512, 4096),
			ExpectedBytesAfter:                 3584,
			ExpectedBytesSaved:                 512,
			ExpectedBytesSavedRatioPPM:         ratioPPM(512, 4096),
			ExpectedBytesSavedPerByteCopiedPPM: ratioPPM(512, 4096),
		}, nil
	}
	defer func() {
		leafGenerationPackSelectTranscode = prev
	}()

	stats, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{
		MaxGenerations:             1,
		MinReclaimPerByteCopiedPPM: 900000,
		Sync:                       true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if !stats.Ran {
		t.Fatalf("expected transcode fallback to run, skip_reason=%q", stats.SkipReason)
	}
	if got, want := stats.Selection.Mode, leafGenerationPackSelectionModeTranscode; got != want {
		t.Fatalf("Selection.Mode=%q want %q", got, want)
	}
	if got, want := stats.Selection.GenerationIDs, []uint64{gen1.GenerationID}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("Selection.GenerationIDs=%v want %v", got, want)
	}
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 0), 'b')
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 1), 'a')
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after transcode fallback: %v", err)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
}

func TestLeafGenerationPackFromPlan_ForceBypassesSelectionThresholds(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')

	stats, err := db.LeafGenerationPackFromPlan(context.Background(), LeafGenerationPackFromPlanOptions{
		Force:                      true,
		MaxGenerations:             1,
		MinExpectedReclaimBytes:    1 << 50,
		MinExpectedReclaimRatioPPM: 900000,
		MinReclaimPerByteCopiedPPM: 900000,
		Sync:                       true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPackFromPlan: %v", err)
	}
	if got, want := stats.GenerationsMatched, 1; got != want {
		t.Fatalf("GenerationsMatched=%d, want %d", got, want)
	}
}
