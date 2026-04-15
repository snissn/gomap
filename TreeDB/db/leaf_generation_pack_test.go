package db

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

func expectLeafGenerationValue(t *testing.T, db *DB, key []byte, fill byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	want := bytes.Repeat([]byte{fill}, 32)
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q)=%x, want %x", key, got, want)
	}
}

func leafGenerationKey(prefix string, i int) []byte {
	return []byte(fmt.Sprintf("%s-%04d", prefix, i))
}

func openLeafGenerationPackTestDB(t *testing.T) (*DB, *rewriteWriter, string) {
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
	return db, leafLog, dir
}

func TestLeafGenerationPack_MovesSparseSealedGeneration(t *testing.T) {
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
	defer func() {
		if leafLog != nil {
			_ = leafLog.Close()
		}
		if db != nil {
			_ = db.Close()
		}
	}()

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
	planBefore, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationPlan before pack: %v", err)
	}
	if got, want := planBefore.Admission, leafGenerationPlanAdmissionEligible; got != want {
		t.Fatalf("plan Admission=%q, want %q", got, want)
	}
	entryBefore := findLeafGenerationPlanEntry(t, planBefore, gen1.GenerationID)
	if !entryBefore.Eligible {
		t.Fatalf("generation %d should be pack-eligible: %+v", gen1.GenerationID, entryBefore)
	}
	if entryBefore.WholeGenerationGCEligible {
		t.Fatalf("generation %d should not be a whole-generation GC candidate: %+v", gen1.GenerationID, entryBefore)
	}
	if got := entryBefore.BytesLive; got <= 0 {
		t.Fatalf("generation %d BytesLive=%d, want > 0 before pack", gen1.GenerationID, got)
	}
	if got := entryBefore.BytesDead; got <= 0 {
		t.Fatalf("generation %d BytesDead=%d, want > 0 before pack", gen1.GenerationID, got)
	}

	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{gen1.GenerationID},
		Sync:          true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if got, want := stats.GenerationsRequested, 1; got != want {
		t.Fatalf("GenerationsRequested=%d, want %d", got, want)
	}
	if got, want := stats.GenerationsMatched, 1; got != want {
		t.Fatalf("GenerationsMatched=%d, want %d", got, want)
	}
	if got, want := stats.SourceFilesRequested, 1; got != want {
		t.Fatalf("SourceFilesRequested=%d, want %d", got, want)
	}
	if got := stats.LeafPagesCopied; got <= 0 {
		t.Fatalf("LeafPagesCopied=%d, want > 0", got)
	}
	if got := stats.BytesCopied; got <= 0 {
		t.Fatalf("BytesCopied=%d, want > 0", got)
	}
	if got := len(stats.CreatedFileIDs); got != 1 {
		t.Fatalf("len(CreatedFileIDs)=%d, want 1", got)
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

	planAfter, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan after pack: %v", err)
	}
	entryAfter := findLeafGenerationPlanEntry(t, planAfter, gen1.GenerationID)
	if got := entryAfter.BytesLive; got != 0 {
		t.Fatalf("generation %d BytesLive=%d, want 0 after pack", gen1.GenerationID, got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close db: %v", err)
	}
	db = nil
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leafLog: %v", err)
	}
	leafLog = nil

	reopened, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeNoErr(t, reopened)
	for i := 0; i < 1024; i++ {
		expectLeafGenerationValue(t, reopened, leafGenerationKey("k", i), 'b')
	}
	for i := 1024; i < 2048; i++ {
		expectLeafGenerationValue(t, reopened, leafGenerationKey("k", i), 'a')
	}
	for i := 0; i < 32; i++ {
		expectLeafGenerationValue(t, reopened, leafGenerationKey("z", i), 'z')
	}
	if _, err := reopened.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after reopen: %v", err)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
}

func TestLeafGenerationPack_RejectsDenseGenerationByDefault(t *testing.T) {
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 32768, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1, 'b')

	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)
	plan, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{MinReclaimPerByteCopiedPPM: leafGenerationPackDefaultMinReclaimPerByteCopiedPPM})
	if err != nil {
		t.Fatalf("LeafGenerationPlan: %v", err)
	}
	entry := findLeafGenerationPlanEntry(t, plan, gen1.GenerationID)
	if got, want := plan.Admission, leafGenerationPlanAdmissionReclaimPerCopyTooLow; got != want {
		t.Fatalf("plan Admission=%q, want %q (entry=%+v)", got, want, entry)
	}
	if !entry.Eligible {
		t.Fatalf("dense generation should still be individually eligible before aggregate admission: %+v", entry)
	}

	_, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{GenerationIDs: []uint64{gen1.GenerationID}})
	if err == nil {
		t.Fatalf("expected dense generation pack to fail by default")
	}
	if !strings.Contains(err.Error(), "selection admission=reclaim_per_copy_too_low") {
		t.Fatalf("dense generation error=%v, want reclaim_per_copy_too_low", err)
	}
}

func TestLeafGenerationPack_ForceAllowsDenseGeneration(t *testing.T) {
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 128, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, db, "z", 8, 'z')

	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)

	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{gen1.GenerationID},
		Force:         true,
		Sync:          true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack force: %v", err)
	}
	if got, want := stats.GenerationsMatched, 1; got != want {
		t.Fatalf("GenerationsMatched=%d, want %d", got, want)
	}
	if got := stats.LeafPagesCopied; got <= 0 {
		t.Fatalf("LeafPagesCopied=%d, want > 0", got)
	}
}

func TestLeafGenerationPack_RejectsWritableGeneration(t *testing.T) {
	db, _ := openLeafGenerationGCTestDB(t)
	writeLeafGenerationKeys(t, db, "k", 32, 'a')
	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	current := manifest.Generations[manifest.currentGenerationIndex()]

	_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{GenerationIDs: []uint64{current.GenerationID}})
	if err == nil {
		t.Fatalf("expected writable generation pack to fail")
	}
}
