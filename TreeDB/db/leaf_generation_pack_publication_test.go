package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
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

func TestLeafGenerationPackPublishAllocatorSharesCOWHighWater(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(12); err != nil {
		t.Fatal(err)
	}
	allocator := freelist.New(p, 0)
	base := freelist.MustNewFreelistGenerationV1(1, 12, []uint64{5}, nil)
	if err := allocator.EnableCOWV1(base, freelist.NewReservationLedger()); err != nil {
		t.Fatalf("EnableCOWV1: %v", err)
	}
	idx := &indexGen{pager: p, allocator: allocator}
	publish := newLeafGenerationPackPublishAllocator(idx, 1)
	for range 2 {
		if _, err := publish.Alloc(0); err != nil {
			t.Fatalf("publish Alloc: %v", err)
		}
	}
	if got := publish.Pages(); len(got) != 2 || got[0] != 12 || got[1] != 13 {
		t.Fatalf("publish pages=%v want [12 13]", got)
	}
	publishedPages := publish.Pages()
	if err := publish.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	capability, err := freelist.NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatalf("NewReuseCapability: %v", err)
	}
	var candidateID freelist.CandidateIDV1
	candidateID[0] = 7
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateID, capability, 1, freelist.NewMemoryPageStoreV1())
	if err != nil {
		t.Fatalf("PrepareCOWCandidateV1: %v", err)
	}
	if got := prepared.AuxiliaryPageIDs(); len(got) != 1 || got[0] <= 13 {
		t.Fatalf("durable-root auxiliary pages=%v overlap rolled-back publish pages %v", got, publishedPages)
	}
	if got := prepared.Candidate().Generation().RetiredCount(); got != 2 {
		t.Fatalf("retired unpublished pages=%d want 2", got)
	}
}

func openLeafGenerationPackPublicationTestDB(t *testing.T, dir string) (*DB, *rewriteWriter) {
	t.Helper()
	requireLeafGenerationPackPromotionSupport(t)
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

func closePoisonedLeafGenerationPackPublicationTestDB(t *testing.T, db *DB, leafLog *rewriteWriter) {
	t.Helper()
	if err := db.Close(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Close poisoned DB error=%v want ErrRecoveryRequired", err)
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
	if got, want := idx.pager.PageCount(), db.durableRoot.record.TotalPages; got != want {
		t.Fatalf("live PageCount=%d durable record TotalPages=%d", got, want)
	}
	beforeStats, err := idx.allocator.Stats(db.durableRoot.record.TotalPages)
	if err != nil {
		t.Fatalf("allocator Stats before close: %v", err)
	}
	wantPages := db.durableRoot.record.TotalPages
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
	// Generation-COW tracks the rollback pages in the live generation before
	// close, so reopening may preserve (rather than strictly increase) the
	// reclaimable count. It must never lose those already-accounted pages.
	if afterStats.ReclaimablePages() < beforeStats.ReclaimablePages() {
		t.Fatalf("retired unpublished pages were lost across reopen: before=%+v after=%+v", beforeStats, afterStats)
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
			afterFailurePages := db.idx.Load().pager.PageCount()
			if phase < leafGenerationPackAfterDirectorySync && afterFailurePages != beforePages {
				t.Fatalf("PageCount=%d before committed-page allocation want %d", afterFailurePages, beforePages)
			}
			if afterFailurePages < beforePages {
				t.Fatalf("PageCount=%d shrank below pre-publication count %d", afterFailurePages, beforePages)
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
			if got, want := db.idx.Load().pager.PageCount(), db.durableRoot.record.TotalPages; got != want {
				t.Fatalf("post-failure publish pages=%d durable record total=%d", got, want)
			}
		})
	}
}

func TestLeafGenerationPack_PromotionBlocksRefreshUntilRollbackCompletes(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)

	wantErr := errors.New("fail after packed promotion")
	promoted := make(chan struct{})
	releasePromotion := make(chan struct{})
	var created []uint32
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase != leafGenerationPackAfterPromotion {
			return nil
		}
		created = append(created, event.FileIDs...)
		close(promoted)
		<-releasePromotion
		return wantErr
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
	select {
	case <-promoted:
	case <-time.After(10 * time.Second):
		t.Fatal("leaf pack did not reach post-promotion hook")
	}
	if db.valueLogPublicationMu.TryRLock() {
		db.valueLogPublicationMu.RUnlock()
		t.Fatal("post-promotion hook did not hold value-log publication gate")
	}

	refreshStarted := make(chan struct{})
	refreshDone := make(chan error, 1)
	go func() {
		close(refreshStarted)
		refreshDone <- db.RefreshValueLogSet()
	}()
	<-refreshStarted
	select {
	case err := <-refreshDone:
		t.Fatalf("RefreshValueLogSet completed before promotion rollback: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePromotion)
	select {
	case err := <-packDone:
		if !errors.Is(err, wantErr) || errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("LeafGenerationPack error=%v want exact rollback failure", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("LeafGenerationPack did not finish after releasing hook")
	}
	select {
	case err := <-refreshDone:
		if err != nil {
			t.Fatalf("RefreshValueLogSet after rollback: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RefreshValueLogSet remained blocked after rollback")
	}
	if db.publicationPoisoned.Load() {
		t.Fatal("exact post-promotion rollback poisoned DB")
	}
	for _, id := range created {
		if db.valueLogManager.HasSegment(id) {
			t.Fatalf("rolled-back segment %d remained registered", id)
		}
	}
}

func TestLeafGenerationPack_ManifestPreparationRunsBeforePublicationLocks(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)

	var observed atomic.Bool
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase != leafGenerationPackAfterManifestPreparation {
			return nil
		}
		if !db.writeMu.TryLock() {
			return errors.New("manifest preparation retained writeMu")
		}
		db.writeMu.Unlock()
		if !db.commitMu.TryLock() {
			return errors.New("manifest preparation retained commitMu")
		}
		db.commitMu.Unlock()
		if !db.publishPrepareMu.TryLock() {
			return errors.New("manifest preparation retained publishPrepareMu")
		}
		db.publishPrepareMu.Unlock()
		if !db.valueLogPublicationMu.TryLock() {
			return errors.New("manifest preparation retained valueLogPublicationMu")
		}
		db.valueLogPublicationMu.Unlock()
		observed.Store(true)
		return nil
	})
	defer unregister()

	if _, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	}); err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if !observed.Load() {
		t.Fatal("manifest preparation hook was not observed")
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

func TestLeafGenerationPack_PostPromotionCreateCutRollsBackExactly(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer func() {
		_ = db.Close()
		_ = leafLog.Close()
	}()
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	beforePages := db.idx.Load().pager.PageCount()

	cutErr := errors.New("injected post-promotion create cut")
	var (
		namespaceEvents     []durabilitycut.Event
		promotedPath        string
		promotedCreateIndex = -1
	)
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == "" {
			return nil
		}
		namespaceEvents = append(namespaceEvents, event)
		if event.Namespace == durabilitycut.NamespaceCreate &&
			event.Resource == durabilitycut.ResourceOuterLeaf &&
			filepath.Clean(filepath.Dir(event.NewPath)) == filepath.Clean(LeafLogDirPath(dir)) {
			if _, _, ok := parseLeafGenerationBootstrapFileName(filepath.Base(event.NewPath)); !ok {
				return nil
			}
			promotedPath = event.NewPath
			promotedCreateIndex = len(namespaceEvents) - 1
			return cutErr
		}
		return nil
	})
	_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	})
	restore()

	if !errors.Is(err, cutErr) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("LeafGenerationPack error=%v, want injected cut after exact rollback", err)
	}
	if db.publicationPoisoned.Load() {
		t.Fatal("exactly rolled-back promotion poisoned DB")
	}
	if promotedPath == "" || promotedCreateIndex < 0 {
		t.Fatalf("namespace events=%#v, want promoted create", namespaceEvents)
	}
	if _, statErr := os.Stat(promotedPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("promoted path survived exact rollback: %v", statErr)
	}
	promotionUnlinked := false
	for _, event := range namespaceEvents[promotedCreateIndex+1:] {
		if event.Namespace == durabilitycut.NamespaceUnlink && event.OldPath == promotedPath {
			promotionUnlinked = true
			break
		}
	}
	if !promotionUnlinked {
		t.Fatalf("namespace events after promoted create=%#v, want exact promotion unlink for %q", namespaceEvents[promotedCreateIndex+1:], promotedPath)
	}
	if got := db.idx.Load().pager.PageCount(); got != beforePages {
		t.Fatalf("PageCount=%d want rollback to %d", got, beforePages)
	}
	if _, statErr := os.Stat(candidate.sourcePath); statErr != nil {
		t.Fatalf("source removed after promotion rollback: %v", statErr)
	}
	staging, globErr := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-*"))
	if globErr != nil || len(staging) != 0 {
		t.Fatalf("exact rollback staging dirs=%v err=%v", staging, globErr)
	}
	if err := db.SetSync([]byte("after-promotion-cut"), []byte("allowed")); err != nil {
		t.Fatalf("SetSync after exact promotion rollback: %v", err)
	}
}

func TestLeafGenerationPackStagingAllocatorRejectsCommittedNamespace(t *testing.T) {
	base, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("open base pager: %v", err)
	}
	defer func() { _ = base.Close() }()
	overlay, err := pager.NewOverlay(64*1024, 1, base)
	if err != nil {
		t.Fatalf("new overlay: %v", err)
	}
	defer func() { _ = overlay.Close() }()

	alloc := &leafGenerationPackStagingAllocator{pager: overlay}
	if _, err := alloc.Alloc(0); err == nil || !strings.Contains(err.Error(), "committed-namespace") {
		t.Fatalf("Alloc error=%v, want committed-namespace rejection", err)
	}
	if len(alloc.pages) != 0 {
		t.Fatalf("staging allocator retained invalid pages: %v", alloc.pages)
	}
}

func TestCheckpointRechecksPublicationPoisonAfterPreflight(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	preflightDone := make(chan struct{})
	release := make(chan struct{})
	db.testCheckpointAfterPoisonPreflightHook = func() {
		close(preflightDone)
		<-release
	}

	done := make(chan error, 1)
	go func() { done <- db.Checkpoint() }()
	<-preflightDone
	db.publicationPoisoned.Store(true)
	close(release)

	if err := <-done; !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Checkpoint error=%v, want ErrRecoveryRequired", err)
	}
}

func TestLeafGenerationPack_PostPublicationCleanupFailureIsReportedOutOfBand(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)

	testErr := errors.New("staging cleanup failpoint")
	originalRemove := removeLeafGenerationPackStagingDirFn
	defer func() { removeLeafGenerationPackStagingDirFn = originalRemove }()
	removeLeafGenerationPackStagingDirFn = func(path string) error {
		if strings.HasPrefix(filepath.Base(path), ".leaf-pack-copy-") {
			return testErr
		}
		return originalRemove(path)
	}
	notified := make(chan error, 1)
	originalNotify := db.notifyError
	defer func() { db.notifyError = originalNotify }()
	db.notifyError = func(err error) { notified <- err }

	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	})
	if err != nil {
		t.Fatalf("LeafGenerationPack reported committed publication as failure: %v", err)
	}
	if stats.LeafPagesCopied == 0 {
		t.Fatal("LeafGenerationPack did not publish copied pages")
	}
	select {
	case err := <-notified:
		if !errors.Is(err, testErr) {
			t.Fatalf("NotifyError=%v, want cleanup failpoint", err)
		}
	default:
		t.Fatal("post-publication cleanup failure was not reported")
	}
}

func TestLeafGenerationPack_PromotedDirectorySyncCutRollsBackExactly(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	beforePages := db.idx.Load().pager.PageCount()
	testErr := errors.New("leaf pack directory sync failpoint")

	var failOnce atomic.Bool
	leafDir := filepath.Clean(LeafLogDirPath(dir))
	var promotedPath string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceCreate && event.Resource == durabilitycut.ResourceOuterLeaf && filepath.Clean(filepath.Dir(event.NewPath)) == leafDir {
			if _, _, ok := parseLeafGenerationBootstrapFileName(filepath.Base(event.NewPath)); ok {
				promotedPath = event.NewPath
				failOnce.Store(true)
			}
		}
		if event.Point == durabilitycut.BeforeNewFileDirectorySync && filepath.Clean(event.Path) == leafDir && failOnce.CompareAndSwap(true, false) {
			return testErr
		}
		return nil
	})
	defer restore()

	_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID},
		Force:         true,
	})
	if !errors.Is(err, testErr) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("LeafGenerationPack error=%v want directory sync cut after exact rollback", err)
	}
	if db.publicationPoisoned.Load() {
		t.Fatal("exactly rolled-back directory sync cut poisoned DB")
	}
	if got := db.idx.Load().pager.PageCount(); got != beforePages {
		t.Fatalf("PageCount=%d want rollback to %d", got, beforePages)
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		t.Fatalf("source removed after directory sync failure: %v", err)
	}
	if promotedPath == "" {
		t.Fatal("directory sync cut did not observe promoted path")
	}
	if _, statErr := os.Stat(promotedPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("promoted path survived directory sync rollback: %v", statErr)
	}
	staging, globErr := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-*"))
	if globErr != nil || len(staging) != 0 {
		t.Fatalf("directory sync rollback staging dirs=%v err=%v", staging, globErr)
	}
	if err := db.SetSync([]byte("after-directory-sync-failure"), []byte("allowed")); err != nil {
		t.Fatalf("SetSync after exact directory sync rollback: %v", err)
	}
}

func TestLeafGenerationPack_PackedPinsSurvivePromotionRegistrationAndPreMeta(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	baselinePins := db.valueLogIdentityPins.ActivePins()
	seen := make(map[leafGenerationPackPublishPhase]bool)
	promotedIdentities := make(map[uint32]rootpublication.StableIdentity)
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		switch event.Phase {
		case leafGenerationPackAfterPromotion, leafGenerationPackAfterRegistration, leafGenerationPackBeforeMetaWrite:
			seen[event.Phase] = true
			if got := db.valueLogIdentityPins.ActivePins(); got <= baselinePins {
				return fmt.Errorf("packed authority pins=%d baseline=%d at phase=%d", got, baselinePins, event.Phase)
			}
		}
		if event.Phase == leafGenerationPackAfterRegistration {
			for _, id := range event.FileIDs {
				identity, ok := db.valueLogManager.StableSegmentIdentity(id)
				if !ok {
					return fmt.Errorf("manager missing stable identity %d", id)
				}
				promotedIdentities[page.ValueLogSegmentID(id)] = identity
				if _, err := db.valueLogIdentityPins.BeginDelete(identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
					return fmt.Errorf("delete race for %d error=%v want pinned", id, err)
				}
			}
		}
		return nil
	})
	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID}, Force: true,
	})
	unregister()
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	for _, phase := range []leafGenerationPackPublishPhase{leafGenerationPackAfterPromotion, leafGenerationPackAfterRegistration, leafGenerationPackBeforeMetaWrite} {
		if !seen[phase] {
			t.Fatalf("publication phase %d was not observed", phase)
		}
	}
	if len(stats.CreatedFileIDs) == 0 {
		t.Fatal("successful pack reported no created segment IDs")
	}
	for _, id := range stats.CreatedFileIDs {
		identity, ok := promotedIdentities[id]
		if !ok {
			t.Fatalf("publication hook did not capture stable identity %d", id)
		}
		if lease, err := db.valueLogIdentityPins.BeginDelete(identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
			if lease != nil {
				lease.Abort()
			}
			t.Fatalf("packed segment %d after metadata handoff delete error=%v want pinned", id, err)
		}
	}
}

func TestLeafGenerationPack_MetaSyncFailurePoisonsAndRetainsCandidates(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	registry := db.valueLogIdentityPins
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
	if got := registry.ActivePins(); got == 0 {
		t.Fatal("ambiguous publish released packed authority before close")
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
	closePoisonedLeafGenerationPackPublicationTestDB(t, db, leafLog)
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("packed authority pins after close=%d want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("packed authority identities after close=%d want 0", got)
	}

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
	closePoisonedLeafGenerationPackPublicationTestDB(t, db, leafLog)

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
	defer closePoisonedLeafGenerationPackPublicationTestDB(t, db, leafLog)
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
	probeOrphan := filepath.Join(LeafLogDirPath(dir), rootpublication.StableChildFileInstallProbePrefix+"crash-left")
	if err := os.WriteFile(probeOrphan, nil, 0o600); err != nil {
		t.Fatalf("WriteFile probe orphan: %v", err)
	}
	reopened, err := Open(leafGenerationPackPublicationTestOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan stat error=%v want not-exist", err)
	}
	if _, err := os.Stat(probeOrphan); !os.IsNotExist(err) {
		t.Fatalf("probe orphan stat error=%v want not-exist", err)
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
	// The immediately older recoverable root still owns the source generation.
	// Advance the alternate slot once before expecting generation GC to unlink it.
	if err := db.SetSync([]byte("post-pack-horizon"), []byte("advance")); err != nil {
		t.Fatalf("SetSync after pack: %v", err)
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
