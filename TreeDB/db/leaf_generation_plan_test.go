package db

import (
	"context"
	"math/big"
	"os"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafGenerationPlan_ReportsZeroBytesForMissingManifestFile(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 17)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	rawFileID := page.ValueLogSegmentID(fileID)
	manifest := db.leafGenerationManifest.clone()
	manifest.Generations[manifest.currentGenerationIndex()].FileIDs = []uint32{rawFileID}
	db.writeMu.Lock()
	db.leafGenerationManifest = manifest
	db.writeMu.Unlock()
	if err := db.publishLeafGenerationState(false); err != nil {
		t.Fatalf("publishLeafGenerationState: %v", err)
	}

	plan, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan with stale missing manifest file: %v", err)
	}
	entry := findLeafGenerationPlanEntry(t, plan, manifest.CurrentGenerationID)
	if got, want := entry.BytesTotal, int64(0); got != want {
		t.Fatalf("BytesTotal=%d, want %d", got, want)
	}
}

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

func withLeafGenerationSubtreeCacheMissCounter(t *testing.T) *atomic.Uint64 {
	t.Helper()
	var counter atomic.Uint64
	unregister := registerLeafGenerationSubtreeCacheMissHook(func(uint64) {
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

func TestDedupeMaintenanceRootsByRootIDDropsRoleAliasesForLiveStats(t *testing.T) {
	roots := []maintenanceRoot{
		{kind: maintenanceRootUser, rootID: 7},
		{kind: maintenanceRootCollection, rootID: 7, descriptorKey: []byte(maintenanceTestCollectionRootKey)},
		{kind: maintenanceRootSystem, rootID: 8},
		{kind: maintenanceRootCollection, rootID: 8, descriptorKey: []byte("collections/root/users/alias")},
		{kind: maintenanceRootCollection, rootID: 0},
	}

	got := dedupeMaintenanceRootsByRootID(roots)
	if len(got) != 2 {
		t.Fatalf("roots len=%d want 2: %+v", len(got), got)
	}
	if got[0].kind != maintenanceRootUser || got[0].rootID != 7 {
		t.Fatalf("first root=%+v want user rootID 7", got[0])
	}
	if got[1].kind != maintenanceRootSystem || got[1].rootID != 8 {
		t.Fatalf("second root=%+v want system rootID 8", got[1])
	}
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

func TestCompareDeadPerLive_OverflowSafe(t *testing.T) {
	tests := []struct {
		name  string
		aDead int64
		aLive int64
		bDead int64
		bLive int64
	}{
		{
			name:  "a_greater",
			aDead: 1<<62 - 1,
			aLive: 1<<62 - 7,
			bDead: 1<<62 - 9,
			bLive: 1<<62 - 11,
		},
		{
			name:  "b_greater",
			aDead: 1<<62 - 25,
			aLive: 1<<62 - 3,
			bDead: 1<<62 - 5,
			bLive: 1<<62 - 33,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			left := new(big.Int).Mul(big.NewInt(tc.aDead), big.NewInt(tc.bLive))
			right := new(big.Int).Mul(big.NewInt(tc.bDead), big.NewInt(tc.aLive))
			want := left.Cmp(right)
			got := compareDeadPerLive(tc.aDead, tc.aLive, tc.bDead, tc.bLive)
			if got != want {
				t.Fatalf("compareDeadPerLive(%d,%d,%d,%d)=%d, want %d", tc.aDead, tc.aLive, tc.bDead, tc.bLive, got, want)
			}
		})
	}
}

func TestCompareDeadPerLive_ClampsNegativeInputs(t *testing.T) {
	if got := compareDeadPerLive(-1, 100, 10, 100); got != -1 {
		t.Fatalf("compareDeadPerLive negative aDead=%d, want -1", got)
	}
	if got := compareDeadPerLive(10, -1, 5, 10); got != 1 {
		t.Fatalf("compareDeadPerLive negative aLive=%d, want 1", got)
	}
}

func TestLeafGenerationGroupedFrameInfo_LiveByteContributionKeepsSparseRefsNonZero(t *testing.T) {
	info := leafGenerationGroupedFrameInfo{recordLen: 2, k: 4, rawLen: 4096, offsets: make([]uint32, 5)}
	for i := 0; i <= info.k; i++ {
		info.offsets[i] = uint32(i * 1024)
	}
	for i := 0; i < info.k; i++ {
		got, ok := info.liveByteContribution(uint16(i))
		if !ok {
			t.Fatalf("liveByteContribution(%d) not ok", i)
		}
		if got == 0 {
			t.Fatalf("liveByteContribution(%d)=0, want non-zero so a referenced grouped sub-record keeps its segment live", i)
		}
	}
}

func TestLeafGenerationGroupedFrameInfo_LiveByteContributionRejectsShortOffsets(t *testing.T) {
	info := leafGenerationGroupedFrameInfo{recordLen: 2, k: 4, rawLen: 4096, offsets: make([]uint32, 4)}
	if got, ok := info.liveByteContribution(3); ok || got != 0 {
		t.Fatalf("liveByteContribution(3)=(%d,%t), want (0,false)", got, ok)
	}
}

func TestLeafGenerationGroupedFrameScanCache_BoundsEntries(t *testing.T) {
	cache := newLeafGenerationGroupedFrameScanCache(2)
	first := groupedRecordKey{fileID: 1, start: 100}
	second := groupedRecordKey{fileID: 1, start: 200}
	third := groupedRecordKey{fileID: 1, start: 300}

	cache.store(first, leafGenerationGroupedFrameInfo{recordLen: 10, k: 2})
	cache.store(second, leafGenerationGroupedFrameInfo{recordLen: 20, k: 2})
	cache.store(first, leafGenerationGroupedFrameInfo{recordLen: 11, k: 2})
	if got := cache.len(); got != 2 {
		t.Fatalf("cache len after update=%d, want 2", got)
	}
	if info, ok := cache.get(first); !ok || info.recordLen != 11 {
		t.Fatalf("updated first entry=(%+v,%t), want recordLen 11 hit", info, ok)
	}

	cache.store(third, leafGenerationGroupedFrameInfo{recordLen: 30, k: 2})
	if got := cache.len(); got != 2 {
		t.Fatalf("cache len after eviction=%d, want 2", got)
	}
	if _, ok := cache.get(first); ok {
		t.Fatalf("first entry still cached after bounded FIFO eviction")
	}
	if _, ok := cache.get(second); !ok {
		t.Fatalf("second entry missing after eviction")
	}
	if _, ok := cache.get(third); !ok {
		t.Fatalf("third entry missing after insert")
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
		snap.releaseLeafGenerationPins()
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

func TestLeafGenerationRecordLengthForPlan_UsesWritableIndex(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 256, 'a')

	db.writeMu.RLock()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}
	view := snap.state.LeafGenerations
	if view == nil {
		t.Fatal("expected leaf generation view")
	}
	currentGenID := view.CurrentGenerationID
	var writablePtr page.LeafLogPtr
	visit := func(ptr page.LeafLogPtr) error {
		if view.FileToGeneration[ptr.FileID] == currentGenID && writablePtr.FileID == 0 {
			writablePtr = ptr
		}
		return nil
	}
	for _, rootID := range []uint64{snap.state.RootPageID, snap.state.SystemRootPageID} {
		if rootID == 0 || writablePtr.FileID != 0 {
			continue
		}
		if err := leafrefscan.Walk(context.Background(), rootID, snap.idx.pager.Get, nil, visit); err != nil {
			t.Fatalf("leafref walk: %v", err)
		}
	}
	if writablePtr.FileID == 0 {
		t.Fatal("expected writable leafref pointer")
	}

	want, err := db.valueLogRecordLengthForRewriteInSet(writablePtr.ValuePtr(), snap.state.ValueLogSet)
	if err != nil {
		t.Fatalf("valueLogRecordLengthForRewriteInSet: %v", err)
	}
	counter := withValueLogRecordLengthHeaderReadCounter(t)
	got, usedIndex, err := db.leafGenerationRecordLengthForPlan(writablePtr, snap.state.ValueLogSet, view)
	if err != nil {
		t.Fatalf("leafGenerationRecordLengthForPlan: %v", err)
	}
	if !usedIndex {
		t.Fatal("expected writable leaf length lookup to use in-memory index")
	}
	if got != want {
		t.Fatalf("record length=%d, want %d", got, want)
	}
	if got := counter.Load(); got != 0 {
		t.Fatalf("writable index path unexpectedly read %d headers", got)
	}
	if idx, ok := db.loadLeafGenerationRecordLengthIndex(writablePtr.FileID); !ok || idx == nil {
		t.Fatalf("expected cached record-length index for writable file %d", writablePtr.FileID)
	}
}

func TestLeafGenerationPlan_SmallWriteKeepsWritableRecordLengthIndexWarm(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 256, 'a')
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("warmup LeafGenerationPlan: %v", err)
	}

	scanCounter := withLeafGenerationRecordLengthIndexScanCounter(t)
	writeLeafGenerationKeyRange(t, db, "k", 0, 1, 'b')
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan after small write: %v", err)
	}
	if got := scanCounter.Load(); got != 0 {
		t.Fatalf("small write planner path rescanned %d writable files, want 0", got)
	}
}

func TestLeafGenerationLiveStats_MarksPagerPagesVerified(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 256, 'a')

	db.writeMu.RLock()
	snap := db.AcquireSnapshot()
	db.writeMu.RUnlock()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}
	if snap.idx == nil || snap.idx.pager == nil {
		t.Fatal("expected pager")
	}
	rootIDs := []uint64{snap.state.RootPageID, snap.state.SystemRootPageID}
	checked := 0
	for _, rootID := range rootIDs {
		if rootID == 0 {
			continue
		}
		snap.idx.pager.MarkUnverified(rootID)
		if snap.idx.pager.IsVerified(rootID) {
			t.Fatalf("root %d unexpectedly verified before scan", rootID)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("expected at least one root page")
	}
	if _, err := db.scanLeafGenerationLiveStats(context.Background(), snap); err != nil {
		t.Fatalf("scanLeafGenerationLiveStats: %v", err)
	}
	for _, rootID := range rootIDs {
		if rootID == 0 {
			continue
		}
		if !snap.idx.pager.IsVerified(rootID) {
			t.Fatalf("root %d not marked verified after scan", rootID)
		}
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
		snap.releaseLeafGenerationPins()
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

func TestLeafGenerationPlan_UsesPhysicalFileSizeWhenCachedSizeIsStale(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	infoBefore, err := os.Stat(path1)
	if err != nil {
		t.Fatalf("stat leaf file before extension: %v", err)
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 64, 'b')

	const extraDeadBytes = int64(1 << 20)
	physicalSize := infoBefore.Size() + extraDeadBytes
	if err := os.Truncate(path1, physicalSize); err != nil {
		t.Fatalf("extend sealed leaf file: %v", err)
	}

	plan, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	gen := findLeafGenerationPlanEntry(t, plan, findLeafGenerationByFileID(t, loadLeafGenerationManifestOrFatal(t, db.dir), rawFileID1).GenerationID)
	if gen.BytesTotal < physicalSize {
		t.Fatalf("generation bytes_total=%d, want at least physical size %d", gen.BytesTotal, physicalSize)
	}
	if gen.BytesDead < extraDeadBytes {
		t.Fatalf("generation bytes_dead=%d, want at least injected dead bytes %d", gen.BytesDead, extraDeadBytes)
	}
	if !gen.Eligible {
		t.Fatalf("generation eligible=%t skip=%q, want eligible after physical dead bytes", gen.Eligible, gen.SkipReason)
	}
}

func TestLeafGenerationPlan_CachesLiveStatsPerPublishedState(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)
	counter := withLeafGenerationLiveScanCounter(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 16, 'b')

	stateBefore := db.State()
	if stateBefore == nil {
		t.Fatal("expected published state")
	}
	rootBefore := stateBefore.RootPageID

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
		snap.releaseLeafGenerationPins()
	}
	if _, err := collectLiveLeafGenerationIDs(context.Background(), snap, nil, nil); err != nil {
		_ = snap.Close()
		t.Fatalf("collectLiveLeafGenerationIDs: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got, want := counter.Load(), uint64(2); got != want {
		t.Fatalf("scan count after uncached gc helper=%d, want %d", got, want)
	}

	if err := db.ForceCommit(rootBefore); err != nil {
		t.Fatalf("Commit same root: %v", err)
	}
	stateSameRoot := db.State()
	if stateSameRoot == nil {
		t.Fatal("expected published state after same-root commit")
	}
	if stateSameRoot.CommitSeq == stateBefore.CommitSeq {
		t.Fatalf("same-root commit did not advance commit seq: before=%d after=%d", stateBefore.CommitSeq, stateSameRoot.CommitSeq)
	}
	if stateSameRoot.RootPageID != rootBefore {
		t.Fatalf("same-root commit changed root: got=%d want=%d", stateSameRoot.RootPageID, rootBefore)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan after same-root commit: %v", err)
	}
	if got, want := counter.Load(), uint64(3); got != want {
		t.Fatalf("scan count after same-root commit=%d, want %d", got, want)
	}

	writeLeafGenerationKeyRange(t, db, "k", 16, 32, 'c')
	stateAfterWrite := db.State()
	if stateAfterWrite == nil {
		t.Fatal("expected published state after write")
	}
	if stateAfterWrite.RootPageID == rootBefore {
		t.Fatalf("write did not change root: root=%d", stateAfterWrite.RootPageID)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan after root change: %v", err)
	}
	if got, want := counter.Load(), uint64(4); got != want {
		t.Fatalf("scan count after root change=%d, want %d", got, want)
	}
}

func TestLeafGenerationPlan_CachesProtectedRootsCanonically(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 16, 'b')
	state := db.State()
	if state == nil {
		t.Fatal("expected published state")
	}

	counter := withLeafGenerationLiveScanCounter(t)
	first := LeafGenerationPlanOptions{
		ProtectedRootIDs:       []uint64{state.RootPageID, state.RootPageID},
		ProtectedSystemRootIDs: []uint64{state.SystemRootPageID, state.SystemRootPageID},
	}
	if _, err := db.LeafGenerationPlan(context.Background(), first); err != nil {
		t.Fatalf("LeafGenerationPlan first: %v", err)
	}
	if got, want := counter.Load(), uint64(1); got != want {
		t.Fatalf("scan count after first protected plan=%d, want %d", got, want)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{
		ProtectedRootIDs:       []uint64{state.RootPageID},
		ProtectedSystemRootIDs: []uint64{state.SystemRootPageID},
	}); err != nil {
		t.Fatalf("LeafGenerationPlan deduplicated roots: %v", err)
	}
	if got, want := counter.Load(), uint64(1); got != want {
		t.Fatalf("scan count after reordered protected plan=%d, want %d", got, want)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{
		ProtectedRootIDs: []uint64{state.RootPageID},
	}); err != nil {
		t.Fatalf("LeafGenerationPlan changed roots: %v", err)
	}
	if got, want := counter.Load(), uint64(2); got != want {
		t.Fatalf("scan count after changed protected plan=%d, want %d", got, want)
	}
}

func TestLeafGenerationProtectedRootsHashCanonicalizesOrderAndKeepsRootKindsDistinct(t *testing.T) {
	first := leafGenerationProtectedRootsHash(
		[]uint64{9, 3, 5, 3, 0},
		[]uint64{17, 11, 17, 0},
	)
	reordered := leafGenerationProtectedRootsHash(
		[]uint64{5, 9, 3},
		[]uint64{11, 17},
	)
	if first != reordered {
		t.Fatalf("canonical hashes differ: first=%x reordered=%x", first, reordered)
	}

	swappedKinds := leafGenerationProtectedRootsHash(
		[]uint64{11, 17},
		[]uint64{3, 5, 9},
	)
	if first == swappedKinds {
		t.Fatalf("user/system root distinction lost: hash=%x", first)
	}
}

func TestLeafGenerationPlan_ReusesCachedSubtreesAcrossRootChanges(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	counter := withLeafGenerationSubtreeCacheMissCounter(t)

	writeLeafGenerationKeys(t, db, "k", 4096, 'a')
	stateBefore := db.State()
	if stateBefore == nil {
		t.Fatal("expected published state")
	}
	rootBefore := stateBefore.RootPageID

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan first: %v", err)
	}
	firstMisses := counter.Load()
	if firstMisses < 2 {
		t.Fatalf("initial subtree miss count=%d, want at least 2 cached pages", firstMisses)
	}

	counter.Store(0)
	writeLeafGenerationKeyRange(t, db, "k", 0, 1, 'b')
	stateAfterWrite := db.State()
	if stateAfterWrite == nil {
		t.Fatal("expected published state after write")
	}
	if stateAfterWrite.RootPageID == rootBefore {
		t.Fatalf("write did not change root: root=%d", stateAfterWrite.RootPageID)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan after root change: %v", err)
	}
	secondMisses := counter.Load()
	if secondMisses == 0 {
		t.Fatal("expected some subtree misses after root change")
	}
	if secondMisses >= firstMisses {
		t.Fatalf("subtree misses after root change=%d, want less than initial %d", secondMisses, firstMisses)
	}
}

func TestLeafGenerationPlan_InvalidatesLiveStatsCacheWhenLeafGenerationViewChanges(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 64, 'a')

	counter := withLeafGenerationLiveScanCounter(t)
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan first: %v", err)
	}
	if got := counter.Load(); got != 1 {
		t.Fatalf("live scan count after first plan=%d, want 1", got)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan second: %v", err)
	}
	if got := counter.Load(); got != 1 {
		t.Fatalf("live scan count after cached plan=%d, want 1", got)
	}
	if err := db.publishLeafGenerationState(false); err != nil {
		t.Fatalf("publishLeafGenerationState: %v", err)
	}
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{}); err != nil {
		t.Fatalf("LeafGenerationPlan after leaf-generation view publish: %v", err)
	}
	if got := counter.Load(); got != 2 {
		t.Fatalf("live scan count after leaf-generation view publish=%d, want 2", got)
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
	if before1 != 0 || before2 != 0 {
		t.Fatalf("expected current-view snapshot pins to stay at zero before plan: gen1=%d gen2=%d", before1, before2)
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
