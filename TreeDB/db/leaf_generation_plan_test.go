package db

import (
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
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

func withLeafGenerationLiveScanCounter(t *testing.T) *atomic.Uint64 {
	t.Helper()
	var counter atomic.Uint64
	unregister := registerLeafGenerationLiveScanHook(func() {
		counter.Add(1)
	})
	t.Cleanup(func() {
		unregister()
	})
	return &counter
}

func withValueLogRecordLengthHeaderReadCounter(t *testing.T) *atomic.Uint64 {
	t.Helper()
	var counter atomic.Uint64
	unregister := registerValueLogRecordLengthHeaderReadHook(func() {
		counter.Add(1)
	})
	t.Cleanup(func() {
		unregister()
	})
	return &counter
}

func withLeafGenerationRecordLengthIndexScanCounter(t *testing.T) *atomic.Uint64 {
	t.Helper()
	var counter atomic.Uint64
	unregister := registerLeafGenerationRecordLengthIndexScanHook(func(rawFileID uint32) {
		counter.Add(1)
	})
	t.Cleanup(func() {
		unregister()
	})
	return &counter
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

func TestLeafGenerationPlan_SeparatesWholeGenerationGCFromPack(t *testing.T) {
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
	if got, want := plan.Admission, leafGenerationPlanAdmissionNoCandidates; got != want {
		t.Fatalf("Admission=%q, want %q", got, want)
	}
	if got, want := len(plan.Generations), 2; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	if got, want := len(plan.Candidates), 0; got != want {
		t.Fatalf("len(Candidates)=%d, want %d", got, want)
	}
	if got := len(plan.CandidateGenerationIDs); got != 0 {
		t.Fatalf("len(CandidateGenerationIDs)=%d, want 0 (%v)", got, plan.CandidateGenerationIDs)
	}
	if got := plan.ExpectedReclaimBytes; got != 0 {
		t.Fatalf("ExpectedReclaimBytes=%d, want 0", got)
	}
	if got := plan.CandidateBytesToCopy; got != 0 {
		t.Fatalf("CandidateBytesToCopy=%d, want 0", got)
	}

	entry1 := findLeafGenerationPlanEntry(t, plan, gen1.GenerationID)
	if entry1.Eligible {
		t.Fatalf("generation %d should not be a pack candidate: %+v", gen1.GenerationID, entry1)
	}
	if !entry1.WholeGenerationGCEligible {
		t.Fatalf("generation %d should be a whole-generation GC candidate: %+v", gen1.GenerationID, entry1)
	}
	if got, want := entry1.SkipReason, leafGenerationPlanSkipWholeGenerationGC; got != want {
		t.Fatalf("generation %d SkipReason=%q, want %q", gen1.GenerationID, got, want)
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
	if got := entry1.BytesToCopy; got != 0 {
		t.Fatalf("generation %d BytesToCopy=%d, want 0", gen1.GenerationID, got)
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
}

func TestLeafGenerationPlan_AgeGateAndForceOnSparseGeneration(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 64, 'b')

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
		t.Fatalf("fresh sparse generation should be blocked: %+v", entryBlocked)
	}
	if got, want := entryBlocked.SkipReason, leafGenerationPlanSkipFreshGeneration; got != want {
		t.Fatalf("blocked SkipReason=%q, want %q", got, want)
	}
	if entryBlocked.WholeGenerationGCEligible {
		t.Fatalf("sparse generation should not be a whole-generation GC candidate: %+v", entryBlocked)
	}

	forced, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{MinPublishedAgeCommits: 1 << 60, Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan forced: %v", err)
	}
	if got, want := forced.Admission, leafGenerationPlanAdmissionEligible; got != want {
		t.Fatalf("forced Admission=%q, want %q", got, want)
	}
	if got, want := len(forced.Candidates), 1; got != want {
		t.Fatalf("len(forced.Candidates)=%d, want %d", got, want)
	}
	if got, want := forced.CandidateGenerationIDs[0], gen1.GenerationID; got != want {
		t.Fatalf("forced CandidateGenerationIDs[0]=%d, want %d", got, want)
	}
	entryForced := findLeafGenerationPlanEntry(t, forced, gen1.GenerationID)
	if !entryForced.Eligible {
		t.Fatalf("forced generation should be eligible: %+v", entryForced)
	}
	if entryForced.WholeGenerationGCEligible {
		t.Fatalf("forced sparse generation should not be whole-generation GC eligible: %+v", entryForced)
	}
	if got := entryForced.BytesDead; got <= 0 {
		t.Fatalf("forced BytesDead=%d, want > 0", got)
	}
	if got := entryForced.BytesLive; got <= 0 {
		t.Fatalf("forced BytesLive=%d, want > 0", got)
	}
	if got, want := entryForced.BytesToCopy, entryForced.BytesLive; got != want {
		t.Fatalf("forced BytesToCopy=%d, want %d", got, want)
	}
	if got, want := forced.CandidateBytesToCopy, entryForced.BytesToCopy; got != want {
		t.Fatalf("CandidateBytesToCopy=%d, want %d", got, want)
	}
	if got, want := forced.ExpectedReclaimBytes, entryForced.BytesDead; got != want {
		t.Fatalf("ExpectedReclaimBytes=%d, want %d", got, want)
	}
	if got, want := forced.ExpectedReclaimPerByteCopiedPPM, ratioPPM(entryForced.BytesDead, entryForced.BytesToCopy); got != want {
		t.Fatalf("ExpectedReclaimPerByteCopiedPPM=%d, want %d", got, want)
	}
}

func TestLeafGenerationPlan_MinReclaimPerByteCopiedAdmission(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 64, 'b')

	base, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationPlan base: %v", err)
	}
	if got, want := base.Admission, leafGenerationPlanAdmissionEligible; got != want {
		t.Fatalf("base Admission=%q, want %q", got, want)
	}
	if got := base.ExpectedReclaimPerByteCopiedPPM; got <= 0 {
		t.Fatalf("ExpectedReclaimPerByteCopiedPPM=%d, want > 0", got)
	}

	blocked, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{
		MinReclaimPerByteCopiedPPM: base.ExpectedReclaimPerByteCopiedPPM + 1,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPlan blocked: %v", err)
	}
	if got, want := blocked.Admission, leafGenerationPlanAdmissionReclaimPerCopyTooLow; got != want {
		t.Fatalf("blocked Admission=%q, want %q", got, want)
	}

	allowed, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{
		MinReclaimPerByteCopiedPPM: base.ExpectedReclaimPerByteCopiedPPM,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPlan allowed: %v", err)
	}
	if got, want := allowed.Admission, leafGenerationPlanAdmissionEligible; got != want {
		t.Fatalf("allowed Admission=%q, want %q", got, want)
	}
}

func TestLeafGenerationRecordLengthForPlan_LoadsPersistedSidecarWithoutRescan(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 64, 'b')

	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)

	db.writeMu.RLock()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	if len(snap.leafGenerationIDs) > 0 {
		db.unpinLeafGenerationIDs(snap.leafGenerationIDs)
		snap.leafGenerationIDs = snap.leafGenerationIDs[:0]
	}
	view := snap.state.LeafGenerations
	if view == nil {
		t.Fatal("expected leaf generation view")
	}
	var sealedPtr page.LeafLogPtr
	visit := func(ptr page.LeafLogPtr) error {
		if view.FileToGeneration[ptr.FileID] == gen1.GenerationID && sealedPtr.FileID == 0 {
			sealedPtr = ptr
		}
		return nil
	}
	for _, rootID := range []uint64{snap.state.RootPageID, snap.state.SystemRootPageID} {
		if rootID == 0 || sealedPtr.FileID != 0 {
			continue
		}
		if err := leafrefscan.Walk(context.Background(), rootID, snap.idx.pager.Get, nil, visit); err != nil {
			t.Fatalf("leafref walk: %v", err)
		}
	}
	if sealedPtr.FileID == 0 {
		t.Fatal("expected sealed leafref pointer")
	}

	want, err := db.valueLogRecordLengthForRewriteInSet(sealedPtr.ValuePtr(), snap.state.ValueLogSet)
	if err != nil {
		t.Fatalf("valueLogRecordLengthForRewriteInSet: %v", err)
	}
	if _, usedIndex, err := db.leafGenerationRecordLengthForPlan(sealedPtr, snap.state.ValueLogSet, view); err != nil {
		t.Fatalf("leafGenerationRecordLengthForPlan initial: %v", err)
	} else if !usedIndex {
		t.Fatal("expected initial sealed lookup to use built index")
	}
	indexPath := leafGenerationRecordLengthIndexPath(db.dir, sealedPtr.FileID)
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("Stat(%q): %v", indexPath, err)
	}

	db.leafGenerationRecordLengthMu.Lock()
	delete(db.leafGenerationRecordLengthByFile, sealedPtr.FileID)
	db.leafGenerationRecordLengthMu.Unlock()

	scanCounter := withLeafGenerationRecordLengthIndexScanCounter(t)
	headerCounter := withValueLogRecordLengthHeaderReadCounter(t)
	got, usedIndex, err := db.leafGenerationRecordLengthForPlan(sealedPtr, snap.state.ValueLogSet, view)
	if err != nil {
		t.Fatalf("leafGenerationRecordLengthForPlan reload: %v", err)
	}
	if !usedIndex {
		t.Fatal("expected persisted sidecar lookup to use index")
	}
	if got != want {
		t.Fatalf("record length=%d, want %d", got, want)
	}
	if got := scanCounter.Load(); got != 0 {
		t.Fatalf("persisted sidecar reload unexpectedly rescanned %d files", got)
	}
	if got := headerCounter.Load(); got != 0 {
		t.Fatalf("persisted sidecar reload unexpectedly read %d headers", got)
	}
}

func TestLeafGenerationRecordLengthForPlan_UsesSealedIndex(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 64, 'b')

	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)

	db.writeMu.RLock()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	if len(snap.leafGenerationIDs) > 0 {
		db.unpinLeafGenerationIDs(snap.leafGenerationIDs)
		snap.leafGenerationIDs = snap.leafGenerationIDs[:0]
	}
	view := snap.state.LeafGenerations
	if view == nil {
		t.Fatal("expected leaf generation view")
	}
	var sealedPtr page.LeafLogPtr
	visit := func(ptr page.LeafLogPtr) error {
		if view.FileToGeneration[ptr.FileID] == gen1.GenerationID && sealedPtr.FileID == 0 {
			sealedPtr = ptr
		}
		return nil
	}
	for _, rootID := range []uint64{snap.state.RootPageID, snap.state.SystemRootPageID} {
		if rootID == 0 || sealedPtr.FileID != 0 {
			continue
		}
		if err := leafrefscan.Walk(context.Background(), rootID, snap.idx.pager.Get, nil, visit); err != nil {
			t.Fatalf("leafref walk: %v", err)
		}
	}
	if sealedPtr.FileID == 0 {
		t.Fatal("expected sealed leafref pointer")
	}

	want, err := db.valueLogRecordLengthForRewriteInSet(sealedPtr.ValuePtr(), snap.state.ValueLogSet)
	if err != nil {
		t.Fatalf("valueLogRecordLengthForRewriteInSet: %v", err)
	}
	counter := withValueLogRecordLengthHeaderReadCounter(t)
	got, usedIndex, err := db.leafGenerationRecordLengthForPlan(sealedPtr, snap.state.ValueLogSet, view)
	if err != nil {
		t.Fatalf("leafGenerationRecordLengthForPlan: %v", err)
	}
	if !usedIndex {
		t.Fatal("expected sealed leaf length lookup to use index")
	}
	if got != want {
		t.Fatalf("record length=%d, want %d", got, want)
	}
	if got := counter.Load(); got != 0 {
		t.Fatalf("sealed index path unexpectedly read %d headers", got)
	}
	if idx, ok := db.loadLeafGenerationRecordLengthIndex(sealedPtr.FileID); !ok || idx == nil {
		t.Fatalf("expected cached record-length index for file %d", sealedPtr.FileID)
	}
}

func TestLeafGenerationPlan_CachesLiveStatsUntilPublishedStateChanges(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)
	counter := withLeafGenerationLiveScanCounter(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 16, 'b')

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan first: %v", err)
	}
	if got, want := counter.Load(), uint64(1); got != want {
		t.Fatalf("scan count after first plan=%d, want %d", got, want)
	}

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan second: %v", err)
	}
	if got, want := counter.Load(), uint64(1); got != want {
		t.Fatalf("scan count after cached second plan=%d, want %d", got, want)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	if len(snap.leafGenerationIDs) > 0 {
		db.unpinLeafGenerationIDs(snap.leafGenerationIDs)
		snap.leafGenerationIDs = snap.leafGenerationIDs[:0]
	}
	if _, err := collectLiveLeafGenerationIDs(context.Background(), snap); err != nil {
		_ = snap.Close()
		t.Fatalf("collectLiveLeafGenerationIDs: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got, want := counter.Load(), uint64(1); got != want {
		t.Fatalf("scan count after gc helper reuse=%d, want %d", got, want)
	}

	writeLeafGenerationKeyRange(t, db, "k", 16, 32, 'c')
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan after commit: %v", err)
	}
	if got, want := counter.Load(), uint64(2); got != want {
		t.Fatalf("scan count after published-state change=%d, want %d", got, want)
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
