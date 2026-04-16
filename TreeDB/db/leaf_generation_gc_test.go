package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	if _, err := os.Stat(path1); !os.IsNotExist(err) {
		t.Fatalf("expected first leaf segment removed, stat err=%v", err)
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
	if got, want := db.leafGenerationPinCountForTesting(gen1.GenerationID), uint64(1); got != want {
		t.Fatalf("pin count=%d, want %d", got, want)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		closeNoErr(t, snap)
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')

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

func TestLeafGenerationGC_DryRunDoesNotPersistRetiringState(t *testing.T) {
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 64, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	manifestBefore := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer closeNoErr(t, snap)

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "k", 64, 'b')

	stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		t.Fatalf("LeafGenerationGC dry-run: %v", err)
	}
	if got, want := stats.GenerationsRetiring, 1; got != want {
		t.Fatalf("GenerationsRetiring=%d, want %d", got, want)
	}
	manifestAfter := loadLeafGenerationManifestOrFatal(t, db.dir)
	genAfter := findLeafGenerationByFileID(t, manifestAfter, rawFileID1)
	if got, want := genAfter.State, leafGenerationStateSealed; got != want {
		t.Fatalf("generation state after dry-run=%q, want %q", got, want)
	}
	if got, want := genAfter.RetiredCommitSeq, uint64(0); got != want {
		t.Fatalf("RetiredCommitSeq after dry-run=%d, want %d", got, want)
	}
	if got, want := genAfter.GenerationID, gen1.GenerationID; got != want {
		t.Fatalf("GenerationID after dry-run=%d, want %d", got, want)
	}
}

func TestPruneDeletedLeafGenerationRecords_CountsDistinctNonZeroFiles(t *testing.T) {
	manifest := &leafGenerationManifest{
		Generations: []leafGenerationRecord{{
			GenerationID: 7,
			State:        leafGenerationStateDeleted,
			FileIDs:      []uint32{0, 11, 11, 12},
		}},
	}
	filePaths := map[uint32]string{
		11: filepath.Join(t.TempDir(), "missing-11.log"),
		12: filepath.Join(t.TempDir(), "missing-12.log"),
	}

	pruned, changed, filesDeleted := pruneDeletedLeafGenerationRecords(manifest, filePaths)
	if !changed {
		t.Fatal("expected pruneDeletedLeafGenerationRecords to prune deleted generation")
	}
	if got, want := filesDeleted, 2; got != want {
		t.Fatalf("FilesDeleted=%d, want %d", got, want)
	}
	if got, want := len(pruned.Generations), 0; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
}
