package db

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func findLeafGenerationPlanEntry(t *testing.T, plan LeafGenerationPlan, generationID uint64) LeafGenerationPlanGeneration {
	t.Helper()
	for _, gen := range plan.Generations {
		if gen.GenerationID == generationID {
			return gen
		}
	}
	t.Fatalf("generation %d not found in plan %+v", generationID, plan.Generations)
	return LeafGenerationPlanGeneration{}
}

func TestRankLeafGenerationPlanCandidates(t *testing.T) {
	gens := []LeafGenerationPlanGeneration{
		{GenerationID: 4, BytesDead: 100, BytesLive: 50},
		{GenerationID: 1, BytesDead: 100, BytesLive: 0},
		{GenerationID: 2, BytesDead: 200, BytesLive: 100},
		{GenerationID: 3, BytesDead: 150, BytesLive: 100},
	}
	rankLeafGenerationPlanCandidates(gens)
	got := []uint64{gens[0].GenerationID, gens[1].GenerationID, gens[2].GenerationID, gens[3].GenerationID}
	want := []uint64{1, 2, 4, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ranked generation[%d]=%d, want %d (full=%v)", i, got[i], want[i], got)
		}
	}
}

func TestLeafGenerationPlan_ReportsDeadAndWritableGenerations(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')
	_, fileID2 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID2 := page.ValueLogSegmentID(fileID2)

	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)
	gen2 := findLeafGenerationByFileID(t, manifest, rawFileID2)

	plan, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	if got, want := plan.Admission, leafGenerationPlanAdmissionEligible; got != want {
		t.Fatalf("Admission=%q, want %q", got, want)
	}
	if got, want := len(plan.Generations), 2; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	if got, want := len(plan.Candidates), 1; got != want {
		t.Fatalf("len(Candidates)=%d, want %d", got, want)
	}
	if got, want := plan.CandidateGenerationIDs[0], gen1.GenerationID; got != want {
		t.Fatalf("CandidateGenerationIDs[0]=%d, want %d", got, want)
	}

	entry1 := findLeafGenerationPlanEntry(t, plan, gen1.GenerationID)
	if !entry1.Eligible {
		t.Fatalf("generation %d should be eligible: %+v", gen1.GenerationID, entry1)
	}
	if got := entry1.BytesTotal; got <= 0 {
		t.Fatalf("generation %d BytesTotal=%d, want > 0", gen1.GenerationID, got)
	}
	if got := entry1.BytesLive; got != 0 {
		t.Fatalf("generation %d BytesLive=%d, want 0", gen1.GenerationID, got)
	}
	if got, want := entry1.BytesDead, entry1.BytesTotal; got != want {
		t.Fatalf("generation %d BytesDead=%d, want %d", gen1.GenerationID, got, want)
	}
	if got := entry1.LivePages; got != 0 {
		t.Fatalf("generation %d LivePages=%d, want 0", gen1.GenerationID, got)
	}

	entry2 := findLeafGenerationPlanEntry(t, plan, gen2.GenerationID)
	if entry2.Eligible {
		t.Fatalf("current writable generation should not be eligible: %+v", entry2)
	}
	if got, want := entry2.SkipReason, leafGenerationPlanSkipWritableGeneration; got != want {
		t.Fatalf("generation %d SkipReason=%q, want %q", gen2.GenerationID, got, want)
	}
	if got := entry2.BytesLive; got <= 0 {
		t.Fatalf("generation %d BytesLive=%d, want > 0", gen2.GenerationID, got)
	}
	if got := plan.ExpectedReclaimBytes; got != entry1.BytesDead {
		t.Fatalf("ExpectedReclaimBytes=%d, want %d", got, entry1.BytesDead)
	}
}

func TestLeafGenerationPlan_AgeGateAndForce(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')

	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)

	blocked, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{MinPublishedAgeCommits: 1 << 60})
	if err != nil {
		t.Fatalf("LeafGenerationPlan blocked: %v", err)
	}
	if got, want := blocked.Admission, leafGenerationPlanAdmissionNoCandidates; got != want {
		t.Fatalf("blocked Admission=%q, want %q", got, want)
	}
	entryBlocked := findLeafGenerationPlanEntry(t, blocked, gen1.GenerationID)
	if entryBlocked.Eligible {
		t.Fatalf("fresh generation should be blocked: %+v", entryBlocked)
	}
	if got, want := entryBlocked.SkipReason, leafGenerationPlanSkipFreshGeneration; got != want {
		t.Fatalf("blocked SkipReason=%q, want %q", got, want)
	}

	forced, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{MinPublishedAgeCommits: 1 << 60, Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan forced: %v", err)
	}
	if got, want := forced.Admission, leafGenerationPlanAdmissionEligible; got != want {
		t.Fatalf("forced Admission=%q, want %q", got, want)
	}
	entryForced := findLeafGenerationPlanEntry(t, forced, gen1.GenerationID)
	if !entryForced.Eligible {
		t.Fatalf("forced generation should be eligible: %+v", entryForced)
	}
}

func TestLeafGenerationPlan_DoesNotLeaveExtraSnapshotPins(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')
	_, fileID2 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID2 := page.ValueLogSegmentID(fileID2)

	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)
	gen2 := findLeafGenerationByFileID(t, manifest, rawFileID2)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	before1 := db.leafGenerationPinCountForTesting(gen1.GenerationID)
	before2 := db.leafGenerationPinCountForTesting(gen2.GenerationID)
	if before1 == 0 || before2 == 0 {
		t.Fatalf("expected non-zero pins before plan: gen1=%d gen2=%d", before1, before2)
	}

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(gen1.GenerationID); got != before1 {
		t.Fatalf("gen1 pin count after plan=%d, want %d", got, before1)
	}
	if got := db.leafGenerationPinCountForTesting(gen2.GenerationID); got != before2 {
		t.Fatalf("gen2 pin count after plan=%d, want %d", got, before2)
	}
}
