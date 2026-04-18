package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func openLeafGenerationLogicalRebuildOnlineTestDB(t *testing.T, maxLeafSegmentBytes int64) (*DB, *rewriteWriter, string) {
	t.Helper()
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
		t.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, maxLeafSegmentBytes)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	t.Cleanup(func() { closeNoErr(t, leafLog) })
	t.Cleanup(func() { closeNoErr(t, db) })
	return db, leafLog, dir
}

func TestLeafGenerationLogicalRebuildRunOnce_NoCandidate(t *testing.T) {
	db, _, _ := openLeafGenerationLogicalRebuildOnlineTestDB(t, 1024)
	writeLeafGenerationKeys(t, db, "k", 32, 'a')

	_, err := db.LeafGenerationLogicalRebuildRunOnce(context.Background(), LeafGenerationLogicalRebuildRunOnceOptions{Sync: true})
	if !errors.Is(err, ErrLeafGenerationLogicalRebuildNoCandidate) {
		t.Fatalf("LeafGenerationLogicalRebuildRunOnce err=%v, want %v", err, ErrLeafGenerationLogicalRebuildNoCandidate)
	}
}

func TestBuildLeafGenerationLogicalRebuildCandidates_UsesEligibleRunUnion(t *testing.T) {
	allSources := []leafLogicalRebuildSource{
		{generationID: 11, rawFileID: 101, fileID: page.ValueLogFileID(101), firstIndex: 0, lastIndex: 11, ranges: [][2]int{{0, 1}, {10, 11}}, sourcePages: 4, sourceBytes: 200, retireable: true},
		{generationID: 12, rawFileID: 102, fileID: page.ValueLogFileID(102), firstIndex: 3, lastIndex: 4, ranges: [][2]int{{3, 4}}, sourcePages: 2, sourceBytes: 180, retireable: true},
		{generationID: 13, rawFileID: 103, fileID: page.ValueLogFileID(103), firstIndex: 6, lastIndex: 8, ranges: [][2]int{{6, 8}}, sourcePages: 3, sourceBytes: 160},
	}
	eligibleSources := allSources[:2]

	candidates := buildLeafGenerationLogicalRebuildCandidates(allSources, eligibleSources, 2)
	if len(candidates) == 0 {
		t.Fatalf("buildLeafGenerationLogicalRebuildCandidates returned no candidates")
	}
	found := false
	for _, got := range candidates {
		if len(got.rawFileIDs) != 2 {
			continue
		}
		if len(got.runRanges) != 3 {
			t.Fatalf("runRanges=%v, want three disjoint slices", got.runRanges)
		}
		if got.runRanges[0] != ([2]int{0, 1}) || got.runRanges[1] != ([2]int{3, 4}) || got.runRanges[2] != ([2]int{10, 11}) {
			continue
		}
		if got.sourceBytes != 380 {
			t.Fatalf("sourceBytes=%d, want 380", got.sourceBytes)
		}
		if got.windowPages != 6 {
			t.Fatalf("windowPages=%d, want 6", got.windowPages)
		}
		found = true
		break
	}
	if !found {
		t.Fatalf("missing expected disjoint two-file candidate in %v", candidates)
	}
}

func TestLeafGenerationLogicalRebuildSampleRanges_PicksBoundedSlices(t *testing.T) {
	const maxPages = 96
	got := leafGenerationLogicalRebuildSampleRanges([][2]int{{0, 299}}, maxPages)
	if len(got) == 0 {
		t.Fatalf("sample ranges empty")
	}
	total := 0
	for _, rr := range got {
		total += rr[1] - rr[0] + 1
	}
	if total > maxPages {
		t.Fatalf("sampled pages=%d, want <= %d", total, maxPages)
	}
	if got[0][0] != 0 {
		t.Fatalf("first sample=%v, want to include start", got[0])
	}
	last := got[len(got)-1]
	if last[1] != 299 {
		t.Fatalf("last sample=%v, want to include end", last)
	}
}

func TestLeafGenerationLogicalRebuildRunOnce_RetiresSelectedGenerationAndPreservesSnapshot(t *testing.T) {
	db, leafLog, dir := openLeafGenerationLogicalRebuildOnlineTestDB(t, 64<<20)
	writeLeafGenerationKeyRange(t, db, "k", 0, 256, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 256, 256, 'b')

	baseSnap := db.AcquireSnapshot()
	candidates, _, err := db.selectLeafGenerationLogicalRebuildCandidates(baseSnap, 0, 0, 4)
	if err != nil {
		closeNoErr(t, baseSnap)
		t.Fatalf("selectLeafGenerationLogicalRebuildCandidates: %v", err)
	}
	candidate := candidates[0]
	publishedCommitSeq := uint64(0)
	for _, gen := range db.leafGenerationManifest.Generations {
		if gen.GenerationID == candidate.generationIDs[0] {
			publishedCommitSeq = gen.PublishedCommitSeq
			break
		}
	}
	if publishedCommitSeq == 0 {
		closeNoErr(t, baseSnap)
		t.Fatalf("missing published commit seq for generation %d", candidate.generationIDs[0])
	}
	if _, _, err := db.selectLeafGenerationLogicalRebuildCandidates(baseSnap, candidate.rawFileIDs[0], publishedCommitSeq-1, 1); !errors.Is(err, ErrLeafGenerationLogicalRebuildNoCandidate) {
		closeNoErr(t, baseSnap)
		t.Fatalf("selectLeafGenerationLogicalRebuildCandidates older watermark err=%v, want %v", err, ErrLeafGenerationLogicalRebuildNoCandidate)
	}

	keyBefore := leafGenerationKey("k", 17)
	snapValue, err := baseSnap.Get(keyBefore)
	if err != nil {
		closeNoErr(t, baseSnap)
		t.Fatalf("snapshot Get before rebuild: %v", err)
	}
	wantValue := bytes.Repeat([]byte{'a'}, 32)
	if !bytes.Equal(snapValue, wantValue) {
		closeNoErr(t, baseSnap)
		t.Fatalf("snapshot value before rebuild=%x, want %x", snapValue, wantValue)
	}

	stats, err := db.LeafGenerationLogicalRebuildRunOnce(context.Background(), LeafGenerationLogicalRebuildRunOnceOptions{
		RawFileID: candidate.rawFileIDs[0],
		Sync:      true,
	})
	if err != nil {
		closeNoErr(t, baseSnap)
		t.Fatalf("LeafGenerationLogicalRebuildRunOnce: %v", err)
	}
	if got, want := stats.SelectedRawFileID, candidate.rawFileIDs[0]; got != want {
		closeNoErr(t, baseSnap)
		t.Fatalf("SelectedRawFileID=%d, want %d", got, want)
	}
	if len(stats.CreatedFileIDs) == 0 {
		closeNoErr(t, baseSnap)
		t.Fatalf("CreatedFileIDs empty in stats=%+v", stats)
	}
	if got, want := stats.RetiredGenerationIDs, []uint64{candidate.generationIDs[0]}; len(got) != len(want) || got[0] != want[0] {
		closeNoErr(t, baseSnap)
		t.Fatalf("RetiredGenerationIDs=%v, want %v", got, want)
	}

	manifestAfter := loadLeafGenerationManifestOrFatal(t, dir)
	retired := findLeafGenerationByFileID(t, manifestAfter, candidate.rawFileIDs[0])
	if got, want := retired.State, leafGenerationStateRetiring; got != want {
		closeNoErr(t, baseSnap)
		t.Fatalf("retired generation state=%q, want %q", got, want)
	}

	got, err := db.Get(keyBefore)
	if err != nil {
		closeNoErr(t, baseSnap)
		t.Fatalf("db.Get after rebuild: %v", err)
	}
	if !bytes.Equal(got, wantValue) {
		closeNoErr(t, baseSnap)
		t.Fatalf("db.Get after rebuild=%x, want %x", got, wantValue)
	}

	snapValue, err = baseSnap.Get(keyBefore)
	if err != nil {
		closeNoErr(t, baseSnap)
		t.Fatalf("snapshot Get after rebuild: %v", err)
	}
	if !bytes.Equal(snapValue, wantValue) {
		closeNoErr(t, baseSnap)
		t.Fatalf("snapshot value after rebuild=%x, want %x", snapValue, wantValue)
	}

	gcStatsPinned, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		closeNoErr(t, baseSnap)
		t.Fatalf("LeafGenerationGC while snapshot pinned: %v", err)
	}
	if gcStatsPinned.GenerationsDeleted != 0 {
		closeNoErr(t, baseSnap)
		t.Fatalf("GenerationsDeleted while pinned=%d, want 0", gcStatsPinned.GenerationsDeleted)
	}
	manifestPinned := loadLeafGenerationManifestOrFatal(t, dir)
	pinnedGen := findLeafGenerationByFileID(t, manifestPinned, candidate.rawFileIDs[0])
	if got, want := pinnedGen.State, leafGenerationStateRetiring; got != want {
		closeNoErr(t, baseSnap)
		t.Fatalf("pinned generation state=%q, want %q", got, want)
	}

	closeNoErr(t, baseSnap)

	gcStatsDeleted, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC after snapshot close: %v", err)
	}
	if gcStatsDeleted.GenerationsDeleted == 0 {
		t.Fatalf("GenerationsDeleted=%d, want > 0", gcStatsDeleted.GenerationsDeleted)
	}
	if _, err := os.Stat(leafGenerationFallbackPath(dir, candidate.rawFileIDs[0])); !os.IsNotExist(err) {
		t.Fatalf("source leaf file still present after GC: err=%v", err)
	}

	for _, fileID := range stats.CreatedFileIDs {
		if !page.IsValueLogFileID(fileID) {
			t.Fatalf("created file id %d missing value-log bit", fileID)
		}
	}
}
