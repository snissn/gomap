package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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

func advanceLeafGenerationPackDurableRootHorizon(t *testing.T, db *DB, reason string) {
	t.Helper()
	state := db.State()
	if err := db.ForceCommit(state.RootPageID); err != nil {
		t.Fatalf("advance recoverable-root horizon (%s): %v", reason, err)
	}
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

func TestNoteCreatedLeafGenerationFileIDs_PersistsRecordLengthIndex(t *testing.T) {
	db, _, dir := openLeafGenerationPackTestDB(t)

	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	defer func() { _ = writer.Close() }()
	for i := 0; i < 4; i++ {
		if _, err := writer.AppendLeafPage(bytes.Repeat([]byte{byte('a' + i)}, page.PageSize)); err != nil {
			t.Fatalf("AppendLeafPage(%d): %v", i, err)
		}
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("writer.Sync: %v", err)
	}
	createdIDs, err := writer.createdFileIDs()
	if err != nil {
		t.Fatalf("createdFileIDs: %v", err)
	}
	if err := db.noteCreatedLeafGenerationFileIDs(55, createdIDs); err != nil {
		t.Fatalf("noteCreatedLeafGenerationFileIDs: %v", err)
	}

	for _, rawFileID := range filterLeafGenerationRawFileIDs(createdIDs) {
		indexPath := leafGenerationRecordLengthIndexPath(dir, rawFileID)
		if _, err := os.Stat(indexPath); err != nil {
			t.Fatalf("Stat(%q): %v", indexPath, err)
		}
		idx, ok, err := loadLeafGenerationRecordLengthIndexFile(indexPath, rawFileID)
		if err != nil {
			t.Fatalf("loadLeafGenerationRecordLengthIndexFile(%d): %v", rawFileID, err)
		}
		if !ok {
			t.Fatalf("expected persisted record-length index for raw file %d", rawFileID)
		}
		if idx == nil || idx.len() == 0 {
			t.Fatalf("expected non-empty record-length index for raw file %d", rawFileID)
		}
	}
}

func TestLoadLeafGenerationRecordLengthIndex_ReturnsStableSnapshot(t *testing.T) {
	db, _, _ := openLeafGenerationPackTestDB(t)
	rawFileID := uint32(77)
	db.noteLeafGenerationRecordLengthRaw(rawFileID, 4, 96)

	first, ok := db.loadLeafGenerationRecordLengthIndex(rawFileID)
	if !ok || first == nil {
		t.Fatal("expected first record-length snapshot")
	}
	if got, found := first.lookup(4); !found || got != 96 {
		t.Fatalf("first.lookup(4)=(%d,%v), want (96,true)", got, found)
	}

	db.noteLeafGenerationRecordLengthRaw(rawFileID, 128, 104)
	if _, found := first.lookup(128); found {
		t.Fatal("first snapshot observed later append")
	}

	second, ok := db.loadLeafGenerationRecordLengthIndex(rawFileID)
	if !ok || second == nil {
		t.Fatal("expected second record-length snapshot")
	}
	if got, found := second.lookup(128); !found || got != 104 {
		t.Fatalf("second.lookup(128)=(%d,%v), want (104,true)", got, found)
	}
}

func TestNoteCreatedLeafGenerationFileIDsInManifest_OnlyReturnsNewLeafFiles(t *testing.T) {
	manifest := newLeafGenerationManifest(10)
	leafFileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 7)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	valueFileID, err := valuelog.EncodeFileID(0, 9)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}

	rawFileIDs, changed, err := noteCreatedLeafGenerationFileIDsInManifest(manifest, 11, []uint32{leafFileID, leafFileID, valueFileID})
	if err != nil {
		t.Fatalf("noteCreatedLeafGenerationFileIDsInManifest: %v", err)
	}
	if !changed {
		t.Fatal("expected manifest to change")
	}
	if got, want := rawFileIDs, []uint32{page.ValueLogSegmentID(leafFileID)}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rawFileIDs=%v want=%v", got, want)
	}

	rawFileIDs, changed, err = noteCreatedLeafGenerationFileIDsInManifest(manifest, 11, []uint32{leafFileID, leafFileID})
	if err != nil {
		t.Fatalf("second noteCreatedLeafGenerationFileIDsInManifest: %v", err)
	}
	if changed {
		t.Fatal("expected duplicate registration to be ignored")
	}
	if rawFileIDs != nil {
		t.Fatalf("rawFileIDs=%v want nil on duplicate registration", rawFileIDs)
	}
}

func TestPersistLeafGenerationManifestAndRecordLengthIndexes_ReportsSidecarError(t *testing.T) {
	db, _, dir := openLeafGenerationPackTestDB(t)
	if db.leafGenerationManifest == nil {
		t.Fatal("expected leaf generation manifest")
	}
	rawFileID := uint32(77)
	db.leafGenerationRecordLengthMu.Lock()
	if db.leafGenerationRecordLengthByFile == nil {
		db.leafGenerationRecordLengthByFile = make(map[uint32]*leafGenerationRecordLengthIndex)
	}
	db.leafGenerationRecordLengthByFile[rawFileID] = &leafGenerationRecordLengthIndex{
		offsets: []uint32{4},
		lengths: nil,
	}
	db.leafGenerationRecordLengthMu.Unlock()

	var reported error
	db.notifyError = func(err error) {
		reported = err
	}

	err := db.persistLeafGenerationManifestAndRecordLengthIndexes(db.leafGenerationManifest.clone(), []uint32{rawFileID})
	if err != nil {
		t.Fatalf("persistLeafGenerationManifestAndRecordLengthIndexes: %v", err)
	}
	if reported == nil {
		t.Fatal("expected record-length sidecar persist error to be reported")
	}
	if !strings.Contains(reported.Error(), "raw file 77") {
		t.Fatalf("reported error=%q, want raw file id context", reported)
	}
	if _, err := os.Stat(leafGenerationManifestPath(LeafLogDirPath(dir))); err != nil {
		t.Fatalf("manifest stat: %v", err)
	}
	db.bgErrMu.Lock()
	db.bgErr = nil
	db.bgErrMu.Unlock()
}

func TestLeafGenerationPack_FinalizeFailpointCleansCreatedSegments(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')

	manifestBefore := loadLeafGenerationManifestOrFatal(t, dir)
	beforeFiles, err := listLeafGenerationBootstrapFiles(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("listLeafGenerationBootstrapFiles(before): %v", err)
	}
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)

	db.testFailFinalizeCommit.Store(true)
	_, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{GenerationIDs: []uint64{gen1.GenerationID}, Force: true})
	db.testFailFinalizeCommit.Store(false)
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("LeafGenerationPack failpoint err=%v, want %v", err, errTestFinalizeCommitFailpoint)
	}

	afterFiles, err := listLeafGenerationBootstrapFiles(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("listLeafGenerationBootstrapFiles(after): %v", err)
	}
	if len(afterFiles) != len(beforeFiles) {
		t.Fatalf("bootstrap file count=%d, want %d", len(afterFiles), len(beforeFiles))
	}
	for i := range beforeFiles {
		if afterFiles[i] != beforeFiles[i] {
			t.Fatalf("bootstrap files[%d]=%+v, want %+v", i, afterFiles[i], beforeFiles[i])
		}
	}
	manifestAfter := loadLeafGenerationManifestOrFatal(t, dir)
	if len(manifestAfter.Generations) != len(manifestBefore.Generations) {
		t.Fatalf("manifest generations=%d, want %d", len(manifestAfter.Generations), len(manifestBefore.Generations))
	}
}

func TestLeafGenerationPack_WriteMetaFailpointRetainsExactRetryCandidate(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')

	manifestBefore := loadLeafGenerationManifestOrFatal(t, dir)
	beforeFiles, err := listLeafGenerationBootstrapFiles(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("listLeafGenerationBootstrapFiles(before): %v", err)
	}
	gen1 := findLeafGenerationByFileID(t, manifestBefore, rawFileID1)

	db.testFailWriteMeta.Store(true)
	_, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{GenerationIDs: []uint64{gen1.GenerationID}, Force: true})
	db.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("LeafGenerationPack writeMeta failpoint err=%v, want failpoint plus recovery-required", err)
	}
	if !db.publicationPoisoned.Load() {
		t.Fatal("pre-meta retry exhaustion did not fail the writable handle closed")
	}
	runtime := db.rootPublication
	runtime.mu.Lock()
	pending := runtime.activeSeal
	pendingOwnsExactCandidate := pending != nil && pending.resources != nil && pending.token != nil && pending.prepared != nil && pending.manifest != nil
	var pendingManifestEntries []rootpublication.DependencyManifestEntryV1
	if pendingOwnsExactCandidate {
		pendingManifestEntries = pending.manifest.Entries()
	}
	runtime.mu.Unlock()
	if !pendingOwnsExactCandidate {
		t.Fatalf("retained coordinator seal=%+v, want exact resources/index/COW ownership", pending)
	}
	if stats := runtime.coordinator.Stats(); stats.PendingCommits == 0 || stats.PreMetaFailures == 0 {
		t.Fatalf("coordinator stats=%+v want retained pending debt and recorded pre-meta failure", stats)
	}

	afterFiles, err := listLeafGenerationBootstrapFiles(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("listLeafGenerationBootstrapFiles(after): %v", err)
	}
	if len(afterFiles) <= len(beforeFiles) {
		t.Fatalf("bootstrap file count=%d, want retained packed candidate beyond baseline %d", len(afterFiles), len(beforeFiles))
	}
	manifestAfter := loadLeafGenerationManifestOrFatal(t, dir)
	if len(manifestAfter.Generations) <= len(manifestBefore.Generations) {
		t.Fatalf("manifest generations=%d, want retained packed revision beyond baseline %d", len(manifestAfter.Generations), len(manifestBefore.Generations))
	}
	var retainedManifest, retainedPack bool
	for _, entry := range pendingManifestEntries {
		for _, field := range entry.Reachability {
			switch field {
			case rootpublication.ReachabilityOuterLeafGeneration:
				retainedManifest = entry.Generation == manifestAfter.ManifestRevision
			case rootpublication.ReachabilityOuterLeafPackedPointer:
				retainedPack = true
			}
		}
	}
	if !retainedManifest || !retainedPack {
		t.Fatalf("retained candidate manifest revision=%d manifest=%t pack=%t entries=%+v", manifestAfter.ManifestRevision, retainedManifest, retainedPack, pendingManifestEntries)
	}
}

func TestLeafGenerationPackRunOnce_ReserveRIDsUsesExternalAllocator(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	db, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "k", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "z", 32, 'z')

	origRIDScanner := leafGenerationPackRIDStartScanner
	ridScanCalls := 0
	leafGenerationPackRIDStartScanner = func(*valuelog.Set) (uint64, error) {
		ridScanCalls++
		return 0, fmt.Errorf("unexpected rid scan")
	}
	t.Cleanup(func() { leafGenerationPackRIDStartScanner = origRIDScanner })

	var reserveCalls int
	nextRIDBase := uint64(900_000)
	stats, err := db.LeafGenerationPackRunOnce(context.Background(), LeafGenerationPackFromPlanOptions{
		MaxGenerations: 1,
		ReserveRIDs: func(count int) (uint64, error) {
			if count <= 0 {
				t.Fatalf("ReserveRIDs count=%d, want > 0", count)
			}
			reserveCalls++
			start := nextRIDBase
			nextRIDBase += uint64(count)
			return start, nil
		},
	})
	if err != nil {
		t.Fatalf("LeafGenerationPackRunOnce: %v", err)
	}
	if !stats.Ran {
		t.Fatalf("expected run once to execute, skip_reason=%q", stats.SkipReason)
	}
	if reserveCalls == 0 {
		t.Fatal("expected ReserveRIDs to be called")
	}
	if ridScanCalls != 0 {
		t.Fatalf("expected ReserveRIDs mode to skip rid scan, calls=%d", ridScanCalls)
	}
}

func TestLeafGenerationPack_MovesSparseSealedGeneration(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
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
		LeafFrameK:    8,
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
	if got, want := len(stats.SourceGenerationIDs), 1; got != want || stats.SourceGenerationIDs[0] != gen1.GenerationID {
		t.Fatalf("SourceGenerationIDs=%v, want [%d]", stats.SourceGenerationIDs, gen1.GenerationID)
	}
	if got := stats.SourceBytesTotal; got <= 0 {
		t.Fatalf("SourceBytesTotal=%d, want > 0", got)
	}
	if got := stats.SourceBytesLive; got <= 0 {
		t.Fatalf("SourceBytesLive=%d, want > 0", got)
	}
	if got := stats.SourceBytesDead; got <= 0 {
		t.Fatalf("SourceBytesDead=%d, want > 0", got)
	}
	if got := stats.SourceBytesToCopy; got <= 0 {
		t.Fatalf("SourceBytesToCopy=%d, want > 0", got)
	}
	if got, want := stats.ExpectedReclaimBytes, stats.SourceBytesDead; got != want {
		t.Fatalf("ExpectedReclaimBytes=%d, want %d", got, want)
	}
	if got := stats.ExpectedReclaimPerByteCopiedPPM; got <= 0 {
		t.Fatalf("ExpectedReclaimPerByteCopiedPPM=%d, want > 0", got)
	}
	if got := stats.WallTimeNanos; got <= 0 {
		t.Fatalf("WallTimeNanos=%d, want > 0", got)
	}
	if got := stats.LeafPagesCopied; got <= 0 {
		t.Fatalf("LeafPagesCopied=%d, want > 0", got)
	}
	if got := stats.LeafFramesWritten; got <= 0 || got >= stats.LeafPagesCopied {
		t.Fatalf("LeafFramesWritten=%d, want >0 and < LeafPagesCopied=%d", got, stats.LeafPagesCopied)
	}
	if got := stats.MaxLeafFrameK; got <= 1 || got > 8 {
		t.Fatalf("MaxLeafFrameK=%d, want 2..8", got)
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
	advanceLeafGenerationPackDurableRootHorizon(t, reopened, "moves-sparse")
	if _, err := reopened.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after reopen: %v", err)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
}

func TestLeafGenerationPack_UsesOuterLeafDict(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()
	const dictID = uint64(9001)
	leafA, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "pack-a"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(a): %v", err)
	}
	leafB, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "pack-b"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(b): %v", err)
	}
	leafC, _, err := valuelog.MaybeCompactLeafLogPayload(buildRewriteLeafPageFixture(t, "pack-c"))
	if err != nil {
		t.Fatalf("MaybeCompactLeafLogPayload(c): %v", err)
	}
	dictBytes, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(dictID),
		Contents: [][]byte{leafA, leafB, leafC},
		History:  append([]byte(nil), leafA...),
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatalf("BuildDict: %v", err)
	}
	if len(dictBytes) == 0 {
		t.Fatal("expected non-empty outer leaf dict")
	}

	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockLZ4,
			DictLookup: func(id uint64) ([]byte, error) {
				if id != dictID {
					return nil, valuelog.ErrMissingDict
				}
				return append([]byte(nil), dictBytes...), nil
			},
			DictCurrentForClass: func(_ context.Context, class string) (uint64, error) {
				if class == "outer_leaf" {
					return dictID, nil
				}
				return 0, nil
			},
			DictLeafPayloadMode: func(_ context.Context, id uint64) (bool, bool, error) {
				if id != dictID {
					return false, false, nil
				}
				return false, true, nil
			},
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	dictionaryProvider := newTestStableDictionaryProvider(t, dictID, dictBytes)
	db.SetStableDictionaryResourceProvider(dictionaryProvider)
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	defer func() {
		closeNoErr(t, leafLog)
		closeNoErr(t, db)
	}()

	writeLeafGenerationKeys(t, db, "dict-pack", 2048, 'a')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "dict-pack", 0, 1024, 'b')
	writeLeafGenerationKeys(t, db, "dict-pack-live", 32, 'z')

	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)
	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{gen1.GenerationID},
		Force:         true,
		Sync:          true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if len(stats.CreatedFileIDs) == 0 {
		t.Fatalf("CreatedFileIDs empty, stats=%+v", stats)
	}
	if calls := dictionaryProvider.captureCalls.Load(); calls == 0 {
		t.Fatal("packed production path did not capture dictionary authority")
	}

	header := readFirstLeafGenerationFrameHeader(t, dir, stats.CreatedFileIDs[0])
	if header.DictID != dictID {
		t.Fatalf("packed leaf frame dictID=%d want %d", header.DictID, dictID)
	}
}

func readFirstLeafGenerationFrameHeader(t *testing.T, dir string, rawFileID uint32) valuelog.FrameHeader {
	t.Helper()
	path := leafLogSegmentPath(t, dir, rawFileID)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var header [valuelog.HeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		t.Fatalf("read record header: %v", err)
	}
	bodyLen := binary.LittleEndian.Uint32(header[16:20])
	body := make([]byte, int(bodyLen))
	if _, err := io.ReadFull(f, body); err != nil {
		t.Fatalf("read frame body: %v", err)
	}
	frameHeader, _, _, _, err := valuelog.DecodeFrame(body)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	return frameHeader
}

func TestLeafGenerationPack_RewritesCollectionLeafRefRoot(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	const descriptorKey = "collections/root/pack/users/primary"
	const docKey = "doc/collection"
	docValue := bytes.Repeat([]byte("collection-leaf-pack|"), 16)
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          mustFrozenRawMemtable(t, docKey, docValue).NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("rootIDs=%d want 1", len(rootIDs))
		}
		return mustFrozenRawMemtable(t, descriptorKey, encodeMaintenanceRootID(rootIDs[0])).NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatalf("publish collection leaf-ref root: %v", err)
	}
	oldRoot := rootIDs[0]
	oldLeafPtr := requireLeafLogRootChildren(t, db, oldRoot)[0]
	oldLeafPath := leafLogSegmentPath(t, dir, oldLeafPtr.FileID)
	rawFileID := page.ValueLogSegmentID(oldLeafPtr.FileID)
	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotate leaf log: %v", err)
	}
	writeLeafGenerationKeys(t, db, "pack-after", 1, 'z')

	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	gen := findLeafGenerationByFileID(t, manifest, rawFileID)
	if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true}); err != nil {
		t.Fatalf("LeafGenerationPlan before pack: %v", err)
	}
	nextRID := uint64(10_000)
	reservedRIDs := 0
	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{gen.GenerationID},
		Sync:          true,
		Force:         true,
		ReserveRIDs: func(count int) (uint64, error) {
			start := nextRID
			nextRID += uint64(count)
			reservedRIDs += count
			return start, nil
		},
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if got := stats.LeafPagesCopied; got < 2 {
		t.Fatalf("LeafPagesCopied=%d, want >= 2 for collection and system roots", got)
	}
	if reservedRIDs < stats.LeafPagesCopied {
		t.Fatalf("ReserveRIDs reserved %d records, want at least LeafPagesCopied=%d", reservedRIDs, stats.LeafPagesCopied)
	}

	newRoot := readCollectionRootID(t, db, descriptorKey)
	if newRoot == oldRoot {
		t.Fatalf("collection descriptor still points at old leaf-ref root %d", oldRoot)
	}
	newLeafPtr := requireLeafLogRootChildren(t, db, newRoot)[0]
	if newLeafPtr.FileID == oldLeafPtr.FileID && newLeafPtr.Offset == oldLeafPtr.Offset {
		t.Fatalf("collection leaf ref was not moved: %v", newLeafPtr)
	}
	got := readCollectionRootValue(t, db, descriptorKey, []byte(docKey))
	if !bytes.Equal(got, docValue) {
		t.Fatalf("collection value mismatch after pack")
	}

	planAfter, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan after pack: %v", err)
	}
	entryAfter := findLeafGenerationPlanEntry(t, planAfter, gen.GenerationID)
	if got := entryAfter.BytesLive; got != 0 {
		t.Fatalf("generation %d BytesLive=%d, want 0 after collection pack", gen.GenerationID, got)
	}
	advanceLeafGenerationPackDurableRootHorizon(t, db, "collection-leaf-root")
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if err := waitForPathRemoval(oldLeafPath, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", oldLeafPath, err)
	}
}

func TestLeafRefRewriteCtx_CollectionDescriptorDeltaIgnoresDurableInlineThreshold(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)

	const descriptorKey = "collections/root/pack/users/primary"
	if err := db.Set([]byte(descriptorKey), encodeMaintenanceRootID(101)); err != nil {
		t.Fatalf("seed descriptor: %v", err)
	}
	state := db.State()
	if state == nil {
		t.Fatal("expected state")
	}
	rootID := state.SystemRootPageID
	idx := db.idx.Load()
	if idx == nil || idx.zipper == nil {
		t.Fatal("expected zipper")
	}

	db.policy.InlineThreshold = 1
	ctx := &leafRefRewriteCtx{
		ctx:    context.Background(),
		db:     db,
		zipper: idx.zipper,
	}
	newRoot, changed, err := ctx.applySystemRootCollectionRootReplacements(rootID, []vacuumCollectionRootReplacement{{
		key:   []byte(descriptorKey),
		value: encodeMaintenanceRootID(202),
	}})
	if err != nil {
		t.Fatalf("applySystemRootCollectionRootReplacements: %v", err)
	}
	if !changed {
		t.Fatal("changed=false want true")
	}
	if err := leafLog.Flush(); err != nil {
		t.Fatalf("flush leaf log: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	got, err := snap.GetAtRoot(newRoot, []byte(descriptorKey))
	if err != nil {
		t.Fatalf("read rewritten descriptor: %v", err)
	}
	if gotID := binary.BigEndian.Uint64(got); gotID != 202 {
		t.Fatalf("descriptor root=%d want 202", gotID)
	}
}

func TestLeafGenerationPack_RewritesCollectionInternalRootsBeforeGC(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	descriptorKeys := []string{
		"collections/root/pack/users/primary",
		"collections/root/pack/users/index-state",
		"collections/root/pack/users/email_idx",
		"collections/root/pack/users/city_idx",
	}
	inputs := []OrderedRootPublishInput{
		{BaseRoot: 0, Iter: collectionPackTable(t, "doc", 0, 4096, 'a').NewIterator(nil, nil), StoragePolicy: OrderedRootStorageValueLogLeaves},
		{BaseRoot: 0, Iter: collectionPackTable(t, "state", 0, 4096, 's').NewIterator(nil, nil), StoragePolicy: OrderedRootStorageValueLogLeaves},
		{BaseRoot: 0, Iter: collectionPackTable(t, "email", 0, 4096, 'e').NewIterator(nil, nil), StoragePolicy: OrderedRootStorageValueLogLeaves},
		{BaseRoot: 0, Iter: collectionPackTable(t, "city", 0, 4096, 'c').NewIterator(nil, nil), StoragePolicy: OrderedRootStorageValueLogLeaves},
	}
	_, rootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder(inputs, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return collectionPackDescriptorIterator(t, descriptorKeys, rootIDs), nil
	})
	if err != nil {
		t.Fatalf("publish collection roots: %v", err)
	}
	if len(rootIDs) != len(descriptorKeys) {
		t.Fatalf("rootIDs=%d want %d", len(rootIDs), len(descriptorKeys))
	}
	oldLeafPath, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	oldLeafIndexPath := leafGenerationRecordLengthIndexPath(dir, rawFileID1)
	if err := leafLog.Sync(); err != nil {
		t.Fatalf("sync leaf log: %v", err)
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotate leaf log: %v", err)
	}

	_, updatedRootIDs, err := db.PublishOrderedRootDeltaGroupWithSystemBuilder([]OrderedRootDeltaPublishInput{{
		BaseRoot:      rootIDs[0],
		Iter:          collectionPackTable(t, "doc", 0, 512, 'b').NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}}, func(updatedRootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(updatedRootIDs) != 1 {
			return nil, fmt.Errorf("updatedRootIDs=%d want 1", len(updatedRootIDs))
		}
		nextRootIDs := append([]uint64(nil), rootIDs...)
		nextRootIDs[0] = updatedRootIDs[0]
		return collectionPackDescriptorIterator(t, descriptorKeys, nextRootIDs), nil
	})
	if err != nil {
		t.Fatalf("publish collection root delta: %v", err)
	}
	rootIDs[0] = updatedRootIDs[0]

	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	gen := findLeafGenerationByFileID(t, manifest, rawFileID1)
	planBefore, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan before pack: %v", err)
	}
	entryBefore := findLeafGenerationPlanEntry(t, planBefore, gen.GenerationID)
	if got := entryBefore.LivePages; got < 16 {
		t.Fatalf("generation %d LivePages=%d, want many live collection leaves before pack (entry=%+v)", gen.GenerationID, got, entryBefore)
	}
	if got := entryBefore.BytesLive; got <= 16*1024 {
		t.Fatalf("generation %d BytesLive=%d, want >16KiB before pack (entry=%+v)", gen.GenerationID, got, entryBefore)
	}
	if got := entryBefore.BytesDead; got <= 0 {
		t.Fatalf("generation %d BytesDead=%d, want >0 before pack (entry=%+v)", gen.GenerationID, got, entryBefore)
	}

	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{gen.GenerationID},
		Sync:          true,
		Force:         true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if got := stats.LeafPagesCopied; got < entryBefore.LivePages {
		t.Fatalf("LeafPagesCopied=%d, want at least live collection pages %d", got, entryBefore.LivePages)
	}
	planAfter, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true})
	if err != nil {
		t.Fatalf("LeafGenerationPlan after pack: %v", err)
	}
	entryAfter := findLeafGenerationPlanEntry(t, planAfter, gen.GenerationID)
	if got := entryAfter.BytesLive; got != 0 {
		t.Fatalf("generation %d BytesLive=%d, want 0 after pack", gen.GenerationID, got)
	}

	advanceLeafGenerationPackDurableRootHorizon(t, db, "collection-internal-roots")
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if err := waitForPathRemoval(oldLeafPath, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", oldLeafPath, err)
	}
	if err := waitForPathRemoval(oldLeafIndexPath, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", oldLeafIndexPath, err)
	}

	expectCollectionPackValue(t, db, descriptorKeys[0], "doc-0000", 'b')
	expectCollectionPackValue(t, db, descriptorKeys[0], "doc-2048", 'a')
	expectCollectionPackValue(t, db, descriptorKeys[1], "state-2048", 's')
	expectCollectionPackValue(t, db, descriptorKeys[2], "email-2048", 'e')
	expectCollectionPackValue(t, db, descriptorKeys[3], "city-2048", 'c')
}

func TestLeafGenerationPack_PrunesCachedSubtreesOutsideSelection(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	db, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, db, "a", 3072, 'a')
	writeLeafGenerationKeys(t, db, "m", 1024, 'm')
	_, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf after generation 1: %v", err)
	}
	writeLeafGenerationKeys(t, db, "z", 4096, 'z')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf after generation 2: %v", err)
	}
	writeLeafGenerationKeys(t, db, "a", 3072, 'b')

	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	gen1 := findLeafGenerationByFileID(t, manifest, rawFileID1)

	plan, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationPlan before pack: %v", err)
	}
	entry := findLeafGenerationPlanEntry(t, plan, gen1.GenerationID)
	if !entry.Eligible {
		t.Fatalf("generation %d should remain pack-eligible: %+v", gen1.GenerationID, entry)
	}
	if entry.WholeGenerationGCEligible {
		t.Fatalf("generation %d unexpectedly became whole-generation GC eligible: %+v", gen1.GenerationID, entry)
	}

	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{gen1.GenerationID},
		Sync:          true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if got := stats.LeafPagesCopied; got <= 0 {
		t.Fatalf("LeafPagesCopied=%d, want > 0", got)
	}
	if got := stats.InternalPagesVisited; got == 0 {
		t.Fatalf("InternalPagesVisited=%d, want > 0", got)
	}
	if got := stats.SubtreesPruned; got == 0 {
		t.Fatalf("SubtreesPruned=%d, want > 0", got)
	}

	for i := 0; i < 3072; i++ {
		expectLeafGenerationValue(t, db, leafGenerationKey("a", i), 'b')
	}
	for i := 0; i < 1024; i++ {
		expectLeafGenerationValue(t, db, leafGenerationKey("m", i), 'm')
	}
	for i := 0; i < 4096; i += 257 {
		expectLeafGenerationValue(t, db, leafGenerationKey("z", i), 'z')
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

func collectionPackTable(t *testing.T, keyPrefix string, start, count int, fill byte) memtable.Table {
	t.Helper()
	kvs := make([]any, 0, count*2)
	for i := 0; i < count; i++ {
		kvs = append(kvs, fmt.Sprintf("%s-%04d", keyPrefix, start+i), bytes.Repeat([]byte{fill}, 32))
	}
	return mustFrozenRawMemtable(t, kvs...)
}

func collectionPackDescriptorIterator(t *testing.T, descriptorKeys []string, rootIDs []uint64) iterator.UnsafeIterator {
	t.Helper()
	if len(descriptorKeys) != len(rootIDs) {
		t.Fatalf("descriptorKeys=%d rootIDs=%d", len(descriptorKeys), len(rootIDs))
	}
	kvs := make([]any, 0, len(descriptorKeys)*2)
	for i, key := range descriptorKeys {
		kvs = append(kvs, key, encodeMaintenanceRootID(rootIDs[i]))
	}
	return mustFrozenRawMemtable(t, kvs...).NewIterator(nil, nil)
}

func expectCollectionPackValue(t *testing.T, db *DB, descriptorKey, key string, fill byte) {
	t.Helper()
	got := readCollectionRootValue(t, db, descriptorKey, []byte(key))
	want := bytes.Repeat([]byte{fill}, 32)
	if !bytes.Equal(got, want) {
		t.Fatalf("collection root %q key %q=%x, want %x", descriptorKey, key, got, want)
	}
}

func TestLeafGenerationPack_ForceAllowsDenseGeneration(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
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
	if got := stats.ExpectedReclaimBytes; got <= 0 {
		t.Fatalf("ExpectedReclaimBytes=%d, want > 0", got)
	}
	if got := stats.ExpectedReclaimPerByteCopiedPPM; got <= 0 || got >= leafGenerationPackDefaultMinReclaimPerByteCopiedPPM {
		t.Fatalf("ExpectedReclaimPerByteCopiedPPM=%d, want > 0 and < %d", got, leafGenerationPackDefaultMinReclaimPerByteCopiedPPM)
	}
	if got := stats.WallTimeNanos; got <= 0 {
		t.Fatalf("WallTimeNanos=%d, want > 0", got)
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
