package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

func openLeafGenerationGCTestDB(t *testing.T) (*DB, *rewriteWriter) {
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
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	t.Cleanup(func() { closeNoErr(t, leafLog) })
	t.Cleanup(func() { closeNoErr(t, db) })
	return db, leafLog
}

func writeLeafGenerationKeys(t *testing.T, db *DB, prefix string, count int, fill byte) {
	t.Helper()
	writeLeafGenerationKeyRange(t, db, prefix, 0, count, fill)
}

func writeLeafGenerationKeyRange(t *testing.T, db *DB, prefix string, start, count int, fill byte) {
	t.Helper()
	raw := db.NewBatch()
	b, ok := raw.(*Batch)
	if !ok {
		closeNoErr(t, raw)
		t.Fatalf("NewBatch type=%T, want *Batch", raw)
	}
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("%s-%04d", prefix, start+i))
		value := bytes.Repeat([]byte{fill}, 32)
		if err := b.Set(key, value); err != nil {
			closeNoErr(t, b)
			t.Fatalf("Set(%q): %v", key, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		closeNoErr(t, b)
		t.Fatalf("WriteSync: %v", err)
	}
	closeNoErr(t, b)
}

func currentLeafSegmentOrFatal(t *testing.T, leafLog *rewriteWriter) (string, uint32) {
	t.Helper()
	path, fileID, ok := leafLog.CurrentValueLogSegment()
	if !ok || path == "" || fileID == 0 {
		t.Fatalf("CurrentValueLogSegment ok=%v path=%q fileID=%d", ok, path, fileID)
	}
	return path, fileID
}

func findLeafGenerationByFileID(t *testing.T, manifest *leafGenerationManifest, fileID uint32) leafGenerationRecord {
	t.Helper()
	if manifest == nil {
		t.Fatal("manifest=nil")
	}
	for _, gen := range manifest.Generations {
		for _, id := range gen.FileIDs {
			if id == fileID {
				return gen
			}
		}
	}
	t.Fatalf("fileID %d not found in manifest %+v", fileID, manifest.Generations)
	return leafGenerationRecord{}
}

func loadLeafGenerationManifestOrFatal(t *testing.T, dir string) *leafGenerationManifest {
	t.Helper()
	manifest, ok, err := loadLeafGenerationManifest(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatal("expected manifest to exist")
	}
	return manifest
}

func TestLeafGenerationView_SkipsRetiringAndDeletedGenerations(t *testing.T) {
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 4,
		NextGenerationID:    5,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{101}},
			{GenerationID: 2, State: leafGenerationStateRetiring, FileIDs: []uint32{202}},
			{GenerationID: 3, State: leafGenerationStateSealed, FileIDs: []uint32{303}},
			{GenerationID: 4, State: leafGenerationStateWritable, FileIDs: []uint32{404}},
		},
	}
	view := newLeafGenerationView(manifest)
	if view == nil {
		t.Fatal("expected leaf generation view")
	}
	if got, want := len(view.GenerationOrder), 2; got != want {
		t.Fatalf("len(GenerationOrder)=%d, want %d", got, want)
	}
	if _, ok := view.Generations[1]; ok {
		t.Fatalf("deleted generation should be absent from view")
	}
	if _, ok := view.FileToGeneration[101]; ok {
		t.Fatalf("deleted file should be absent from file map")
	}
	if _, ok := view.Generations[2]; ok {
		t.Fatalf("retiring generation should be absent from view")
	}
	if _, ok := view.FileToGeneration[202]; ok {
		t.Fatalf("retiring file should be absent from file map")
	}
	if got, want := view.FileToGeneration[303], uint64(3); got != want {
		t.Fatalf("FileToGeneration[303]=%d, want %d", got, want)
	}
}

func TestLeafGenerationGC_DeletesFullyDeadGeneration(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	indexPath1 := leafGenerationRecordLengthIndexPath(db.dir, rawFileID1)
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("stat first leaf segment: %v", err)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')
	path2, fileID2 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID2 := page.ValueLogSegmentID(fileID2)
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("stat second leaf segment: %v", err)
	}

	manifestBefore := loadLeafGenerationManifestOrFatal(t, db.dir)
	if got, want := len(manifestBefore.Generations), 2; got != want {
		t.Fatalf("len(manifestBefore.Generations)=%d, want %d", got, want)
	}
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)
	if got, want := gen1.State, leafGenerationStateSealed; got != want {
		t.Fatalf("generation1 state=%q, want %q", got, want)
	}

	stats1, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC first: %v", err)
	}
	if got := stats1.GenerationsEligible; got < 1 {
		t.Fatalf("expected at least one eligible generation, got %d", got)
	}
	if got, want := stats1.GenerationsDeleted, 1; got != want {
		t.Fatalf("GenerationsDeleted=%d, want %d (stats=%+v)", got, want, stats1)
	}
	if got, want := stats1.FilesDeleted, 1; got != want {
		t.Fatalf("FilesDeleted=%d, want %d (stats=%+v)", got, want, stats1)
	}
	if got := stats1.BytesEligible; got <= 0 {
		t.Fatalf("BytesEligible=%d, want > 0 (stats=%+v)", got, stats1)
	}
	if got := stats1.BytesDeleted; got <= 0 {
		t.Fatalf("BytesDeleted=%d, want > 0 (stats=%+v)", got, stats1)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
	if err := waitForPathRemoval(indexPath1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", indexPath1, err)
	}
	if _, err := os.Stat(path1); !os.IsNotExist(err) {
		t.Fatalf("expected first leaf segment removed, stat err=%v", err)
	}
	if _, err := os.Stat(indexPath1); !os.IsNotExist(err) {
		t.Fatalf("expected first leaf segment record-length index removed, stat err=%v", err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("expected second leaf segment to remain, stat err=%v", err)
	}

	manifestAfter := loadLeafGenerationManifestOrFatal(t, db.dir)
	if got, want := len(manifestAfter.Generations), 1; got != want {
		t.Fatalf("len(manifestAfter.Generations)=%d, want %d", got, want)
	}
	remaining := manifestAfter.Generations[0]
	if got, want := remaining.FileIDs[0], rawFileID2; got != want {
		t.Fatalf("remaining generation fileID=%d, want %d", got, want)
	}
}

func TestLeafGenerationGC_ProtectedRootIDsKeepDetachedRootLive(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	rootTable := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	rootID, err := db.PublishOrderedRootIterator(0, rootTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	if rootID == 0 {
		t.Fatal("expected non-zero detached root")
	}
	if state := db.State(); state.RootPageID == rootID || state.SystemRootPageID == rootID {
		t.Fatalf("test requires detached root, got state roots user=%d system=%d detached=%d", state.RootPageID, state.SystemRootPageID, rootID)
	}

	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	refs := collectLeafRefIDsFromRoot(t, db, rootID)
	if len(refs) == 0 {
		t.Fatalf("expected detached root %d to contain leaf-log refs", rootID)
	}
	refsFile := false
	for ptr := range refs {
		if ptr.FileID == rawFileID1 {
			refsFile = true
			break
		}
	}
	if !refsFile {
		t.Fatalf("detached root refs do not include raw file id %d: %+v", rawFileID1, refs)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "current", 1, 'z')

	probe, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run: %v", err)
	}
	if got := probe.GenerationsEligible; got == 0 {
		t.Fatalf("GenerationsEligible=%d want detached generation eligible without protection (stats=%+v)", got, probe)
	}

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{
		ProtectedRootIDs: []uint64{0, rootID, rootID},
	})
	if err != nil {
		t.Fatalf("LeafGenerationGC protected: %v", err)
	}
	if got := stats.GenerationsLive; got == 0 {
		t.Fatalf("GenerationsLive=%d want protected detached generation live (stats=%+v)", got, stats)
	}
	if got := stats.GenerationsDeleted; got != 0 {
		t.Fatalf("GenerationsDeleted=%d want 0 for protected detached root (stats=%+v)", got, stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected protected leaf segment to remain: %v", err)
	}
}

func TestLeafGenerationPlan_ProtectedOrdinaryRootDoesNotParseDescriptors(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)

	rootTable := mustFrozenSystemMemtable(
		t,
		collectionRootDescriptorPrefix+"ordinary-user-key", "not-a-root-id",
		"doc/a", "value-a",
	)
	rootID, err := db.PublishOrderedRootIterator(0, rootTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}

	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{
		ProtectedRootIDs: []uint64{rootID},
	}); err != nil {
		t.Fatalf("LeafGenerationPlan with protected ordinary root: %v", err)
	}
}

func TestLeafGenerationGC_ProtectedSystemRootDescriptorsKeepCollectionRootLive(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	collectionRootID, err := db.PublishOrderedRootIterator(0, mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator collection: %v", err)
	}
	if collectionRootID == 0 {
		t.Fatal("expected non-zero collection root")
	}
	refs := collectLeafRefIDsFromRoot(t, db, collectionRootID)
	if len(refs) == 0 {
		t.Fatalf("expected collection root %d to contain leaf-log refs", collectionRootID)
	}
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	refsFile := false
	for ptr := range refs {
		if ptr.FileID == rawFileID1 {
			refsFile = true
			break
		}
	}
	if !refsFile {
		t.Fatalf("collection root refs do not include raw file id %d: %+v", rawFileID1, refs)
	}

	systemRootID, err := db.PublishOrderedRootIterator(0, mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(collectionRootID)).NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator system: %v", err)
	}
	if systemRootID == 0 {
		t.Fatal("expected non-zero system root")
	}
	if state := db.State(); state.RootPageID == collectionRootID || state.SystemRootPageID == systemRootID {
		t.Fatalf("test requires detached roots, got state user=%d system=%d collection=%d protectedSystem=%d", state.RootPageID, state.SystemRootPageID, collectionRootID, systemRootID)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "current", 1, 'z')

	probe, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run: %v", err)
	}
	if got := probe.GenerationsEligible; got == 0 {
		t.Fatalf("GenerationsEligible=%d want descriptor collection generation eligible without protection (stats=%+v)", got, probe)
	}

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{
		ProtectedRootIDs: []uint64{systemRootID},
	})
	if err != nil {
		t.Fatalf("LeafGenerationGC protected: %v", err)
	}
	if got := stats.GenerationsLive; got == 0 {
		t.Fatalf("GenerationsLive=%d want protected descriptor collection generation live (stats=%+v)", got, stats)
	}
	if got := stats.GenerationsDeleted; got != 0 {
		t.Fatalf("GenerationsDeleted=%d want 0 for protected system descriptor root (stats=%+v)", got, stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected protected descriptor leaf segment to remain: %v", err)
	}
}

func TestLeafGenerationGC_IgnoresStaleReachabilityCache(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 512, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "z", 1, 'z')

	manifestBefore := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)
	if got, want := gen1.State, leafGenerationStateSealed; got != want {
		t.Fatalf("generation1 state=%q, want %q", got, want)
	}
	state := db.state.Load()
	cacheKey, ok := leafGenerationLiveStatsKeyForState(state)
	if !ok {
		t.Fatal("expected cacheable leaf-generation state")
	}

	db.leafGenerationLiveStatsMu.Lock()
	db.leafGenerationLiveStatsCache = leafGenerationLiveStatsCache{
		key:   cacheKey,
		stats: leafGenerationLiveScanStats{Generations: map[uint64]leafGenerationLiveTotals{}},
		ok:    true,
	}
	db.leafGenerationLiveStatsMu.Unlock()

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if got := stats.GenerationsLive; got == 0 {
		t.Fatalf("GenerationsLive=%d, want stale cache ignored (stats=%+v)", got, stats)
	}
	if got := stats.GenerationsDeleted; got != 0 {
		t.Fatalf("GenerationsDeleted=%d, want 0 for live generation (stats=%+v)", got, stats)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected live first leaf segment to remain: %v", err)
	}
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 0), 'a')
	expectLeafGenerationValue(t, db, leafGenerationKey("k", 511), 'a')
}

func TestLeafGenerationGC_RetiresPinnedGenerationUntilSnapshotCloses(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	manifestBefore := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	if got := db.leafGenerationPinCountForTesting(gen1.GenerationID); got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("pin count before republish=%d, want 0", got)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		closeNoErr(t, snap)
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')
	if got, want := db.leafGenerationPinCountForTesting(gen1.GenerationID), uint64(1); got != want {
		closeNoErr(t, snap)
		t.Fatalf("pin count after republish=%d, want %d", got, want)
	}

	stats1, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		closeNoErr(t, snap)
		t.Fatalf("LeafGenerationGC while pinned: %v", err)
	}
	if got, want := stats1.GenerationsRetiring, 1; got != want {
		closeNoErr(t, snap)
		t.Fatalf("GenerationsRetiring=%d, want %d", got, want)
	}
	if got := stats1.GenerationsDeleted; got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("GenerationsDeleted=%d, want 0 while pinned", got)
	}
	if got := stats1.BytesEligible; got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("BytesEligible=%d, want 0 while pinned", got)
	}
	if got := stats1.BytesDeleted; got != 0 {
		closeNoErr(t, snap)
		t.Fatalf("BytesDeleted=%d, want 0 while pinned", got)
	}
	if _, err := os.Stat(path1); err != nil {
		closeNoErr(t, snap)
		t.Fatalf("expected pinned segment to remain, stat err=%v", err)
	}
	manifestRetiring := loadLeafGenerationManifestOrFatal(t, db.dir)
	genRetiring := findLeafGenerationByFileID(t, manifestRetiring, rawFileID1)
	if got, want := genRetiring.State, leafGenerationStateRetiring; got != want {
		closeNoErr(t, snap)
		t.Fatalf("retiring generation state=%q, want %q", got, want)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(gen1.GenerationID); got != 0 {
		t.Fatalf("pin count after close=%d, want 0", got)
	}

	stats2, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC after close: %v", err)
	}
	if got := stats2.GenerationsEligible; got < 1 {
		t.Fatalf("expected eligible generation after snapshot close, got %d", got)
	}
	if got, want := stats2.GenerationsDeleted, 1; got != want {
		t.Fatalf("GenerationsDeleted=%d, want %d (stats=%+v)", got, want, stats2)
	}
	if got := stats2.BytesDeleted; got <= 0 {
		t.Fatalf("BytesDeleted=%d, want > 0 (stats=%+v)", got, stats2)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
	if _, err := os.Stat(path1); !os.IsNotExist(err) {
		t.Fatalf("expected retired segment removed, stat err=%v", err)
	}
}
