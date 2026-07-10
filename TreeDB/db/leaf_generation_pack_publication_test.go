package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func leafGenerationPackPublicationTestOptions(dir string) Options {
	return Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	}
}

func openLeafGenerationPackPublicationTestDB(t *testing.T, dir string) (*DB, *rewriteWriter) {
	t.Helper()
	db, err := Open(leafGenerationPackPublicationTestOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)
	return db, leafLog
}

func closeLeafGenerationPackPublicationTestDB(t *testing.T, db *DB, leafLog *rewriteWriter) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close DB: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}
}

func TestLeafGenerationPack_RetryExhaustionDoesNotLeakCommittedPages(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 768)

	var attempts atomic.Int32
	unregister := registerLeafGenerationPackCopyHook(func(event leafGenerationPackCopyEvent) {
		if event.Phase != leafGenerationPackCopyComplete {
			return
		}
		for _, id := range event.PrivatePageIDs {
			if id < leafGenerationPackPrivatePageIDBase {
				t.Errorf("private page id %d entered committed namespace", id)
			}
		}
		n := attempts.Add(1)
		if err := db.SetSync([]byte(fmt.Sprintf("retry-conflict-%d", n)), []byte("foreground")); err != nil {
			t.Errorf("SetSync conflict %d: %v", n, err)
		}
	})
	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	})
	unregister()
	if !errors.Is(err, errLeafGenerationPackPublishConflict) {
		t.Fatalf("LeafGenerationPack error=%v want conflict", err)
	}
	if stats.CopyAttempts != 2 || stats.CopyAborts != 2 || attempts.Load() != 2 {
		t.Fatalf("retry stats=%+v hook attempts=%d", stats, attempts.Load())
	}
	idx := db.idx.Load()
	if got, want := idx.pager.PageCount(), db.meta.TotalPages; got != want {
		t.Fatalf("live PageCount=%d durable TotalPages=%d", got, want)
	}
	durableAllocator := freelist.New(idx.pager, db.meta.FreelistHeadID)
	beforeStats, err := durableAllocator.Stats(db.meta.TotalPages)
	if err != nil {
		t.Fatalf("allocator Stats before close: %v", err)
	}
	wantPages := db.meta.TotalPages
	closeLeafGenerationPackPublicationTestDB(t, db, leafLog)

	reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if got := reopened.idx.Load().pager.PageCount(); got != wantPages || reopened.meta.TotalPages != wantPages {
		t.Fatalf("reopen pages=%d meta=%d want=%d", got, reopened.meta.TotalPages, wantPages)
	}
	afterStats, err := reopened.idx.Load().allocator.Stats(wantPages)
	if err != nil {
		t.Fatalf("allocator Stats after reopen: %v", err)
	}
	if afterStats.Head != beforeStats.Head || afterStats.Pages != beforeStats.Pages || afterStats.FreeIDs != beforeStats.FreeIDs {
		t.Fatalf("freelist changed across reopen: before=%+v after=%+v", beforeStats, afterStats)
	}
	for i := 1; i <= 2; i++ {
		got, err := reopened.Get([]byte(fmt.Sprintf("retry-conflict-%d", i)))
		if err != nil || string(got) != "foreground" {
			t.Fatalf("reopen foreground %d value=%q err=%v", i, got, err)
		}
	}
	staging, err := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-*"))
	if err != nil || len(staging) != 0 {
		t.Fatalf("orphan staging dirs=%v err=%v", staging, err)
	}
}

func TestLeafGenerationPack_PreMetaFailureMatrixCleansPrivateResources(t *testing.T) {
	testErr := errors.New("leaf pack publication failpoint")
	for _, phase := range []leafGenerationPackPublishPhase{
		leafGenerationPackBeforePromotion,
		leafGenerationPackAfterPromotion,
		leafGenerationPackAfterDirectorySync,
		leafGenerationPackBeforeRegistration,
		leafGenerationPackAfterRegistration,
		leafGenerationPackBeforeMetaWrite,
	} {
		phase := phase
		t.Run(fmt.Sprintf("phase-%d", phase), func(t *testing.T) {
			dir := t.TempDir()
			db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
			defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
			candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
			beforePages := db.idx.Load().pager.PageCount()
			var created []uint32
			unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
				if event.Phase != phase {
					return nil
				}
				created = append(created, event.FileIDs...)
				return testErr
			})
			_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
				GenerationIDs: []uint64{candidate.generation.GenerationID},
				Force:         true,
			})
			unregister()
			if !errors.Is(err, testErr) {
				t.Fatalf("LeafGenerationPack error=%v want failpoint", err)
			}
			if db.publicationPoisoned.Load() {
				t.Fatal("pre-meta failure poisoned DB")
			}
			if got := db.idx.Load().pager.PageCount(); got != beforePages {
				t.Fatalf("PageCount=%d want rollback to %d", got, beforePages)
			}
			if _, err := os.Stat(candidate.sourcePath); err != nil {
				t.Fatalf("source removed on pre-meta failure: %v", err)
			}
			for _, id := range created {
				if db.valueLogManager.HasSegment(id) {
					t.Fatalf("created segment %d remained registered", id)
				}
			}
			staging, err := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-*"))
			if err != nil || len(staging) != 0 {
				t.Fatalf("staging dirs=%v err=%v", staging, err)
			}
			if err := db.SetSync([]byte("after-failure"), []byte("ok")); err != nil {
				t.Fatalf("SetSync after pre-meta failure: %v", err)
			}
		})
	}
}

func TestLeafGenerationPack_ClosesStagingReaderBeforePromotion(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)

	originalClose := closeLeafGenerationPackStagingReaderFn
	defer func() { closeLeafGenerationPackStagingReaderFn = originalClose }()
	var readerClosed atomic.Bool
	closeLeafGenerationPackStagingReaderFn = func(reader *valuelog.Manager) error {
		err := originalClose(reader)
		if err == nil {
			readerClosed.Store(true)
		}
		return err
	}

	var promotionObserved atomic.Bool
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase != leafGenerationPackBeforePromotion {
			return nil
		}
		promotionObserved.Store(true)
		if !readerClosed.Load() {
			return errors.New("staging reader remained open at promotion")
		}
		return nil
	})
	defer unregister()

	if _, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	}); err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if !promotionObserved.Load() {
		t.Fatal("promotion hook was not observed")
	}
}

func TestLeafGenerationPack_PromotedDirectorySyncFailureCleansCandidates(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	beforePages := db.idx.Load().pager.PageCount()
	testErr := errors.New("leaf pack directory sync failpoint")

	originalSyncDir := syncDirFn
	defer func() { syncDirFn = originalSyncDir }()
	var failOnce atomic.Bool
	failOnce.Store(true)
	leafDir := filepath.Clean(LeafLogDirPath(dir))
	syncDirFn = func(path string) error {
		if filepath.Clean(path) == leafDir && failOnce.CompareAndSwap(true, false) {
			return testErr
		}
		return originalSyncDir(path)
	}

	_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	})
	if !errors.Is(err, testErr) {
		t.Fatalf("LeafGenerationPack error=%v want directory sync failpoint", err)
	}
	if db.publicationPoisoned.Load() {
		t.Fatal("pre-meta directory sync failure poisoned DB")
	}
	if got := db.idx.Load().pager.PageCount(); got != beforePages {
		t.Fatalf("PageCount=%d want rollback to %d", got, beforePages)
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		t.Fatalf("source removed after directory sync failure: %v", err)
	}
	if err := db.SetSync([]byte("after-directory-sync-failure"), []byte("ok")); err != nil {
		t.Fatalf("SetSync after directory sync failure: %v", err)
	}
}

func TestLeafGenerationPack_MetaSyncFailurePoisonsAndRetainsCandidates(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 768)
	beforeSeq := db.State().CommitSeq
	var promotedIDs []uint32
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase == leafGenerationPackAfterRegistration {
			promotedIDs = append(promotedIDs, event.FileIDs...)
		}
		return nil
	})
	db.testFailSyncMeta.Store(true)
	_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
		Sync:          false,
	})
	db.testFailSyncMeta.Store(false)
	unregister()
	if !errors.Is(err, errTestSyncMetaFailpoint) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("LeafGenerationPack error=%v want meta-sync and recovery-required", err)
	}
	if !db.publicationPoisoned.Load() {
		t.Fatal("meta-sync ambiguity did not poison DB")
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		t.Fatalf("source removed after ambiguous publish: %v", err)
	}
	if len(promotedIDs) == 0 {
		t.Fatal("no promoted segment IDs captured")
	}
	for _, id := range promotedIDs {
		if !db.valueLogManager.HasSegment(id) {
			t.Fatalf("candidate segment %d was unregistered after ambiguous publish", id)
		}
	}
	if snap := db.AcquireSnapshot(); snap != nil {
		_ = snap.Close()
		t.Fatal("AcquireSnapshot succeeded on poisoned handle")
	}
	for name, err := range map[string]error{
		"Get": func() error { _, err := db.Get([]byte("pack-concurrent-0000")); return err }(),
		"Pack": func() error {
			_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{Force: true})
			return err
		}(),
		"LeafGenerationGC": func() error {
			_, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
			return err
		}(),
		"SetSync":    db.SetSync([]byte("after-ambiguous"), []byte("blocked")),
		"Checkpoint": db.Checkpoint(),
		"Refresh":    db.RefreshValueLogSet(),
	} {
		if !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("%s error=%v want ErrRecoveryRequired", name, err)
		}
	}
	// Simulate a later file-wide flush after the API has correctly refused to
	// continue. Candidate pages/segments must remain sufficient for either meta.
	if err := db.idx.Load().pager.Sync(); err != nil {
		t.Fatalf("later raw pager sync: %v", err)
	}
	closeLeafGenerationPackPublicationTestDB(t, db, leafLog)

	reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
	if err != nil {
		t.Fatalf("reopen after ambiguous sync: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if seq := reopened.State().CommitSeq; seq != beforeSeq+1 {
		t.Fatalf("reopen CommitSeq=%d want later-synced new=%d", seq, beforeSeq+1)
	}
	for i := 0; i < 768; i++ {
		want := byte('a')
		if i < 384 {
			want = 'b'
		}
		expectLeafGenerationValue(t, reopened, leafGenerationKey("pack-concurrent", i), want)
	}
	if _, err := reopened.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC after reopen: %v", err)
	}
	for i := 0; i < 768; i++ {
		want := byte('a')
		if i < 384 {
			want = 'b'
		}
		expectLeafGenerationValue(t, reopened, leafGenerationKey("pack-concurrent", i), want)
	}
}

func TestLeafGenerationPack_MetaSyncFailureCanReopenOldMeta(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	beforeSeq := db.State().CommitSeq
	targetMetaPageID := uint64(0)
	if db.metaPageID == 0 {
		targetMetaPageID = 1
	}
	targetBytes, err := db.idx.Load().pager.Get(targetMetaPageID)
	if err != nil {
		t.Fatalf("read alternate meta: %v", err)
	}
	oldTargetBytes := append([]byte(nil), targetBytes...)

	db.testFailSyncMeta.Store(true)
	_, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	})
	db.testFailSyncMeta.Store(false)
	if !errors.Is(err, errTestSyncMetaFailpoint) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("LeafGenerationPack error=%v want meta-sync and recovery-required", err)
	}
	// Model the allowed old-state outcome by restoring the previous alternate
	// meta bytes before a later file-wide sync. Candidate data remains retained,
	// so either selected root is self-contained.
	if err := db.idx.Load().pager.Write(targetMetaPageID, oldTargetBytes); err != nil {
		t.Fatalf("restore old alternate meta: %v", err)
	}
	if err := db.idx.Load().pager.SyncPages([]uint64{targetMetaPageID}); err != nil {
		t.Fatalf("sync restored alternate meta: %v", err)
	}
	closeLeafGenerationPackPublicationTestDB(t, db, leafLog)

	reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
	if err != nil {
		t.Fatalf("reopen old outcome: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if got := reopened.State().CommitSeq; got != beforeSeq {
		t.Fatalf("reopen CommitSeq=%d want old=%d", got, beforeSeq)
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		t.Fatalf("old outcome lost authoritative source: %v", err)
	}
	for i := 0; i < 512; i++ {
		want := byte('a')
		if i < 256 {
			want = 'b'
		}
		expectLeafGenerationValue(t, reopened, leafGenerationKey("pack-concurrent", i), want)
	}
}

func TestLeafGenerationPack_RefreshAndSnapshotCannotObserveFailedRegistration(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	pinned := db.AcquireSnapshot()
	if pinned == nil {
		t.Fatal("AcquireSnapshot before pack")
	}
	defer func() { _ = pinned.Close() }()

	registered := make(chan struct{})
	release := make(chan struct{})
	testErr := errors.New("fail after registration")
	var once sync.Once
	var created []uint32
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase != leafGenerationPackAfterRegistration {
			return nil
		}
		created = append(created, event.FileIDs...)
		once.Do(func() { close(registered) })
		<-release
		return testErr
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
	waitLeafGenerationPackTestSignal(t, registered, "registered publication pause")

	refreshDone := make(chan error, 1)
	go func() { refreshDone <- db.RefreshValueLogSet() }()
	snapshotDone := make(chan *Snapshot, 1)
	go func() { snapshotDone <- db.AcquireSnapshot() }()
	select {
	case err := <-refreshDone:
		t.Fatalf("Refresh completed inside promotion window: %v", err)
	case snap := <-snapshotDone:
		if snap != nil {
			_ = snap.Close()
		}
		t.Fatal("snapshot completed inside promotion window")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-packDone; !errors.Is(err, testErr) {
		t.Fatalf("pack error=%v want failpoint", err)
	}
	if err := <-refreshDone; err != nil {
		t.Fatalf("Refresh after cleanup: %v", err)
	}
	snap := <-snapshotDone
	if snap == nil {
		t.Fatal("snapshot unavailable after cleanup")
	}
	defer func() { _ = snap.Close() }()
	for _, id := range created {
		if db.valueLogManager.HasSegment(id) {
			t.Fatalf("failed candidate %d visible after Refresh", id)
		}
		if pinned.state.ValueLogSet != nil {
			if _, ok := pinned.state.ValueLogSet.Files[id]; ok {
				t.Fatalf("pre-existing snapshot observed candidate %d", id)
			}
		}
		if snap.state.ValueLogSet != nil {
			if _, ok := snap.state.ValueLogSet.Files[id]; ok {
				t.Fatalf("post-cleanup snapshot observed candidate %d", id)
			}
		}
	}
}

func TestLeafGenerationPack_MetaSyncFailureRejectsQueuedLeafGC(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)

	registered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase == leafGenerationPackAfterRegistration {
			once.Do(func() { close(registered) })
			<-release
		}
		return nil
	})
	defer unregister()
	db.testFailSyncMeta.Store(true)
	defer db.testFailSyncMeta.Store(false)
	packDone := make(chan error, 1)
	go func() {
		_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
			GenerationIDs: []uint64{candidate.generation.GenerationID},
			Force:         true,
		})
		packDone <- err
	}()
	waitLeafGenerationPackTestSignal(t, registered, "registered meta-sync pause")

	gcDone := make(chan error, 1)
	go func() {
		_, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
		gcDone <- err
	}()
	close(release)
	if err := <-packDone; !errors.Is(err, errTestSyncMetaFailpoint) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("pack error=%v want meta-sync and recovery-required", err)
	}
	if err := <-gcDone; !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("queued LeafGenerationGC error=%v want ErrRecoveryRequired", err)
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		t.Fatalf("queued GC removed possible authoritative source: %v", err)
	}
}

func TestLeafGenerationPack_CloseWaitsForRegisteredFailureCleanup(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)

	registered := make(chan struct{})
	release := make(chan struct{})
	testErr := errors.New("registered cleanup failpoint")
	var once sync.Once
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase != leafGenerationPackAfterRegistration {
			return nil
		}
		once.Do(func() { close(registered) })
		<-release
		return testErr
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
	waitLeafGenerationPackTestSignal(t, registered, "registered cleanup pause")

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before registered cleanup: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-packDone; !errors.Is(err, testErr) {
		t.Fatalf("pack error=%v want cleanup failpoint", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close after registered cleanup: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}
}

func TestLeafGenerationPack_StartupCleansOrphansWithoutDeletingLiveAttempt(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	copyPaused := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unregister := registerLeafGenerationPackCopyHook(func(event leafGenerationPackCopyEvent) {
		if event.Phase == leafGenerationPackCopyComplete {
			once.Do(func() { close(copyPaused) })
			<-release
		}
	})
	packDone := make(chan error, 1)
	go func() {
		_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
			GenerationIDs: []uint64{candidate.generation.GenerationID},
			Force:         true,
		})
		packDone <- err
	}()
	waitLeafGenerationPackTestSignal(t, copyPaused, "live staging copy")
	liveDirs, err := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-*"))
	if err != nil || len(liveDirs) != 1 {
		t.Fatalf("live staging dirs=%v err=%v", liveDirs, err)
	}
	if second, err := Open(leafGenerationPackPublicationTestOptions(dir)); err == nil {
		_ = second.Close()
		t.Fatal("second opener unexpectedly acquired live DB lock")
	}
	if _, err := os.Stat(liveDirs[0]); err != nil {
		t.Fatalf("failed second open deleted live attempt: %v", err)
	}
	close(release)
	if err := <-packDone; err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	unregister()
	closeLeafGenerationPackPublicationTestDB(t, db, leafLog)

	orphan := filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-orphan")
	if err := os.MkdirAll(orphan, 0o700); err != nil {
		t.Fatalf("MkdirAll orphan: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphan, "index.pages"), []byte("orphan"), 0o600); err != nil {
		t.Fatalf("WriteFile orphan: %v", err)
	}
	reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan stat error=%v want not-exist", err)
	}
}

func TestLeafGenerationPack_SyncFalseStillDurablyPublishesBeforeGC(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	if _, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
		Sync:          false,
	}); err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if err := waitForPathRemoval(candidate.sourcePath, 5*time.Second); err != nil {
		t.Fatalf("source GC: %v", err)
	}
	closeLeafGenerationPackPublicationTestDB(t, db, leafLog)

	reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	for i := 0; i < 512; i++ {
		want := byte('a')
		if i < 256 {
			want = 'b'
		}
		expectLeafGenerationValue(t, reopened, leafGenerationKey("pack-concurrent", i), want)
	}
	if state := reopened.State(); state == nil || state.RootPageID == 0 || state.CommitSeq == 0 {
		t.Fatalf("invalid reopened state: %+v", state)
	}
	if reopened.meta.TotalPages != reopened.idx.Load().pager.PageCount() {
		t.Fatalf("reopened TotalPages=%d pager=%d", reopened.meta.TotalPages, reopened.idx.Load().pager.PageCount())
	}
}
