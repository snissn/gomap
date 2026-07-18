package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type leafGenerationPackTestCandidate struct {
	generation leafGenerationRecord
	sourcePath string
}

func prepareLeafGenerationPackTestCandidate(t *testing.T, db *DB, leafLog *rewriteWriter, keys int) leafGenerationPackTestCandidate {
	t.Helper()
	requireLeafGenerationPackPromotionSupport(t)
	if keys < 128 {
		keys = 128
	}
	writeLeafGenerationKeys(t, db, "pack-concurrent", keys, 'a')
	sourcePath, sourceFileID := currentLeafSegmentOrFatal(t, leafLog)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, db, "pack-concurrent", 0, keys/2, 'b')
	writeLeafGenerationKeys(t, db, "pack-tail", 32, 'z')
	manifest := loadLeafGenerationManifestOrFatal(t, db.dir)
	return leafGenerationPackTestCandidate{
		generation: findLeafGenerationByFileID(t, manifest, page.ValueLogSegmentID(sourceFileID)),
		sourcePath: sourcePath,
	}
}

func waitLeafGenerationPackTestSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func TestLeafGenerationPack_CopyAllowsConcurrentSetAndSetSync(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 1024)

	copyPaused := make(chan struct{})
	releaseCopy := make(chan struct{})
	var paused atomic.Bool
	var stagedFileIDs []uint32
	unregister := registerLeafGenerationPackCopyHook(func(event leafGenerationPackCopyEvent) {
		if event.Phase != leafGenerationPackCopyComplete || !paused.CompareAndSwap(false, true) {
			return
		}
		stagedFileIDs = append(stagedFileIDs, event.CreatedFileIDs...)
		close(copyPaused)
		<-releaseCopy
	})
	defer unregister()

	type packResult struct {
		stats LeafGenerationPackStats
		err   error
	}
	packDone := make(chan packResult, 1)
	go func() {
		stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
			GenerationIDs: []uint64{candidate.generation.GenerationID},
			Force:         true,
			Sync:          true,
		})
		packDone <- packResult{stats: stats, err: err}
	}()
	waitLeafGenerationPackTestSignal(t, copyPaused, "pack copy pause")

	if len(stagedFileIDs) == 0 {
		t.Fatal("copy hook reported no staged files")
	}
	if err := db.valueLogManager.Refresh(); err != nil {
		t.Fatalf("Refresh while copy paused: %v", err)
	}
	for _, fileID := range stagedFileIDs {
		if db.valueLogManager.HasSegment(fileID) {
			t.Fatalf("staged segment %d became visible before publish", fileID)
		}
	}

	setDone := make(chan error, 1)
	go func() { setDone <- db.Set([]byte("during-pack-set"), []byte("set-value")) }()
	select {
	case err := <-setDone:
		if err != nil {
			t.Fatalf("Set while copy paused: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Set blocked behind pack copy")
	}

	setSyncDone := make(chan error, 1)
	go func() { setSyncDone <- db.SetSync([]byte("during-pack-set-sync"), []byte("set-sync-value")) }()
	select {
	case err := <-setSyncDone:
		if err != nil {
			t.Fatalf("SetSync while copy paused: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetSync blocked behind pack copy")
	}

	close(releaseCopy)
	select {
	case result := <-packDone:
		if result.err != nil {
			t.Fatalf("LeafGenerationPack: %v", result.err)
		}
		if result.stats.CopyAttempts < 2 || result.stats.CopyAborts != 1 {
			t.Fatalf("copy attempts/aborts=%d/%d want at least 2/1", result.stats.CopyAttempts, result.stats.CopyAborts)
		}
		if result.stats.CopyTimeNanos <= 0 || result.stats.PublishHoldNanos <= 0 {
			t.Fatalf("missing phase timing stats: %+v", result.stats)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("pack did not finish after copy release")
	}

	for key, want := range map[string]string{
		"during-pack-set":      "set-value",
		"during-pack-set-sync": "set-sync-value",
	} {
		got, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%q)=%q want %q", key, got, want)
		}
	}
}

func TestLeafGenerationPack_ChangedOverlappingRootDiscardsCopyAndRetries(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 1024)

	copyPaused := make(chan struct{})
	releaseCopy := make(chan struct{})
	var paused atomic.Bool
	unregister := registerLeafGenerationPackCopyHook(func(event leafGenerationPackCopyEvent) {
		if event.Phase == leafGenerationPackCopyComplete && paused.CompareAndSwap(false, true) {
			close(copyPaused)
			<-releaseCopy
		}
	})
	defer unregister()

	type packResult struct {
		stats LeafGenerationPackStats
		err   error
	}
	packDone := make(chan packResult, 1)
	go func() {
		stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
			GenerationIDs: []uint64{candidate.generation.GenerationID},
			Force:         true,
		})
		packDone <- packResult{stats: stats, err: err}
	}()
	waitLeafGenerationPackTestSignal(t, copyPaused, "overlapping-root copy pause")

	overlapKey := leafGenerationKey("pack-concurrent", 900)
	overlapValue := []byte("newer-overlapping-write")
	if err := db.SetSync(overlapKey, overlapValue); err != nil {
		t.Fatalf("SetSync overlapping key: %v", err)
	}
	close(releaseCopy)

	select {
	case result := <-packDone:
		if result.err != nil {
			t.Fatalf("LeafGenerationPack: %v", result.err)
		}
		if result.stats.CopyAborts != 1 || result.stats.PrivatePagesDiscarded == 0 {
			t.Fatalf("stale copy cleanup stats=%+v", result.stats)
		}
		if result.stats.RetryApplyStages.TreeRewriteTimeNanos <= 0 || result.stats.RetryApplyStages.LeafSyncTimeNanos <= 0 {
			t.Fatalf("discarded attempt stages were not accounted separately: %+v", result.stats)
		}
		if result.stats.RetryApplyStages.TreeRewriteTimeNanos >= result.stats.ApplyStages.TreeRewriteTimeNanos {
			t.Fatalf("aggregate stages do not include successful retry: %+v", result.stats)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("pack did not finish after overlapping write")
	}
	got, err := db.Get(overlapKey)
	if err != nil {
		t.Fatalf("Get overlapping key: %v", err)
	}
	if !bytes.Equal(got, overlapValue) {
		t.Fatalf("overlapping value=%q want %q", got, overlapValue)
	}

	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("post-stale-%04d", i))
		if err := db.Set(key, []byte("ok")); err != nil {
			t.Fatalf("post-stale Set(%d): %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after stale retry: %v", err)
	}
}

func TestLeafGenerationPack_PrivatePagesRaceWithForegroundWriters(t *testing.T) {
	db, _, _ := openLeafGenerationPackTestDB(t)
	idx := db.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	mainPagesBefore := idx.pager.PageCount()
	staging, err := pager.NewOverlay(db.chunkSize, leafGenerationPackPrivatePageIDBase, idx.pager)
	if err != nil {
		t.Fatalf("NewOverlay: %v", err)
	}
	defer func() { _ = staging.Close() }()
	tracker := &leafGenerationPackStagingAllocator{pager: staging}

	const iterations = 256
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			id, err := tracker.Alloc(0)
			if err != nil {
				errCh <- err
				return
			}
			if id < leafGenerationPackPrivatePageIDBase {
				errCh <- fmt.Errorf("private id %d overlaps committed namespace", id)
				return
			}
			buf, err := staging.GetForWrite(id)
			if err != nil {
				errCh <- err
				return
			}
			clear(buf)
		}
		if err := staging.Sync(); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			key := []byte(fmt.Sprintf("private-race-%04d", i))
			if err := db.Set(key, []byte("foreground")); err != nil {
				errCh <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent private-page stress: %v", err)
	}
	if got := idx.pager.PageCount(); got < mainPagesBefore || got > db.meta.TotalPages {
		t.Fatalf("main pager pages=%d before=%d visible high-water=%d", got, mainPagesBefore, db.meta.TotalPages)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint foreground roots: %v", err)
	}
	if got, durable := idx.pager.PageCount(), db.durableRoot.record.TotalPages; got < durable {
		t.Fatalf("materialized pager pages=%d below durable root high-water=%d", got, durable)
	}
	for i := 0; i < iterations; i++ {
		key := []byte(fmt.Sprintf("private-race-%04d", i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
	}
}

func TestLeafGenerationPack_SourcePinnedUntilPublishThenGCReadable(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 1024)

	copyPaused := make(chan struct{})
	releaseCopy := make(chan struct{})
	var paused atomic.Bool
	unregister := registerLeafGenerationPackCopyHook(func(event leafGenerationPackCopyEvent) {
		if event.Phase == leafGenerationPackCopyComplete && paused.CompareAndSwap(false, true) {
			close(copyPaused)
			<-releaseCopy
		}
	})
	defer unregister()

	packDone := make(chan error, 1)
	go func() {
		_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
			GenerationIDs: []uint64{candidate.generation.GenerationID},
			Force:         true,
			Sync:          true,
		})
		packDone <- err
	}()
	waitLeafGenerationPackTestSignal(t, copyPaused, "source pin copy pause")
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		close(releaseCopy)
		t.Fatalf("source segment disappeared before publish: %v", err)
	}

	type gcResult struct {
		err error
	}
	gcDone := make(chan gcResult, 1)
	go func() {
		_, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
		gcDone <- gcResult{err: err}
	}()
	select {
	case result := <-gcDone:
		close(releaseCopy)
		t.Fatalf("GC completed before pack publish: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		close(releaseCopy)
		t.Fatalf("GC removed source segment before publish: %v", err)
	}
	close(releaseCopy)
	select {
	case err := <-packDone:
		if err != nil {
			t.Fatalf("LeafGenerationPack: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("pack did not finish")
	}
	select {
	case result := <-gcDone:
		if result.err != nil {
			t.Fatalf("LeafGenerationGC: %v", result.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("GC did not finish after pack publish")
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		t.Fatalf("older recoverable slot did not retain source segment: %v", err)
	}
	advanceLeafGenerationPackDurableRootHorizon(t, db, "concurrent-gc")
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after horizon advance: %v", err)
	}
	if err := waitForPathRemoval(candidate.sourcePath, 5*time.Second); err != nil {
		t.Fatalf("source GC: %v", err)
	}
	for i := 0; i < 1024; i++ {
		want := byte('a')
		if i < 512 {
			want = 'b'
		}
		expectLeafGenerationValue(t, db, leafGenerationKey("pack-concurrent", i), want)
	}
}

func TestLeafGenerationPack_CloseWaitsForCopyTeardown(t *testing.T) {
	db, leafLog, _ := openLeafGenerationPackTestDB(t)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)

	copyPaused := make(chan struct{})
	releaseCopy := make(chan struct{})
	var paused atomic.Bool
	unregister := registerLeafGenerationPackCopyHook(func(event leafGenerationPackCopyEvent) {
		if event.Phase == leafGenerationPackCopyComplete && paused.CompareAndSwap(false, true) {
			close(copyPaused)
			<-releaseCopy
		}
	})
	defer unregister()

	packDone := make(chan error, 1)
	go func() {
		_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
			GenerationIDs: []uint64{candidate.generation.GenerationID},
			Force:         true,
		})
		packDone <- err
	}()
	waitLeafGenerationPackTestSignal(t, copyPaused, "close copy pause")

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before copy released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseCopy)
	select {
	case err := <-packDone:
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("LeafGenerationPack during Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pack did not leave teardown phase")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish after copy release")
	}
}

func TestLeafGenerationPack_CheckpointReopenPreservesLeafRefsAndValuePointers(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)

	const keyCount = 768
	oldPointers := appendPointersInNewSegment(t, dir, 0, 1, 1_000_000, keyCount, func(i int) []byte {
		return bytes.Repeat([]byte{byte(1 + i%251)}, 256)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh old value pointers: %v", err)
	}
	writeLeafGenerationPointerBatch(t, db, oldPointers, 0)
	sourcePath, sourceFileID := currentLeafSegmentOrFatal(t, leafLog)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}

	newPointers := appendPointersInNewSegment(t, dir, 0, 2, 2_000_000, keyCount/2, func(i int) []byte {
		return bytes.Repeat([]byte{byte(252 - i%251)}, 384)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("refresh new value pointers: %v", err)
	}
	writeLeafGenerationPointerBatch(t, db, newPointers, 0)
	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	generation := findLeafGenerationByFileID(t, manifest, page.ValueLogSegmentID(sourceFileID))
	if _, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{generation.GenerationID},
		Force:         true,
		Sync:          true,
	}); err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	advanceLeafGenerationPackDurableRootHorizon(t, db, "checkpoint-reopen")
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if err := waitForPathRemoval(sourcePath, 5*time.Second); err != nil {
		t.Fatalf("source leaf segment GC: %v", err)
	}
	verifyLeafGenerationPointerValues(t, db, oldPointers, newPointers)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	verifyLeafGenerationPointerValues(t, reopened, oldPointers, newPointers)
}

func writeLeafGenerationPointerBatch(t *testing.T, db *DB, pointers []page.ValuePtr, keyOffset int) {
	t.Helper()
	b := db.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	for i, ptr := range pointers {
		key := []byte(fmt.Sprintf("pack-pointer-%04d", keyOffset+i))
		if err := b.SetPointer(key, ptr); err != nil {
			t.Fatalf("SetPointer(%q): %v", key, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync pointers: %v", err)
	}
}

func verifyLeafGenerationPointerValues(t *testing.T, db *DB, oldPointers, newPointers []page.ValuePtr) {
	t.Helper()
	for i := range oldPointers {
		want := bytes.Repeat([]byte{byte(1 + i%251)}, 256)
		if i < len(newPointers) {
			want = bytes.Repeat([]byte{byte(252 - i%251)}, 384)
		}
		key := []byte(fmt.Sprintf("pack-pointer-%04d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) length=%d want=%d", key, len(got), len(want))
		}
	}
}
