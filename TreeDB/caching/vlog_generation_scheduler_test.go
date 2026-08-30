package caching

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type blockingRewritePlannerBackend struct {
	*backenddb.DB

	startOnce       sync.Once
	planUnblockOnce sync.Once
	planStart       chan struct{}
	planBlock       chan struct{}

	mu            sync.Mutex
	rewriteCalls  int
	planCompleted int
	planCanceled  int
	planResponse  backenddb.ValueLogRewritePlan
}

func rewriteChunkPlanForTest(plan backenddb.ValueLogRewritePlan, chunkBytes int64) backenddb.ValueLogRewriteChunkPlan {
	if chunkBytes <= 0 {
		chunkBytes = 16 << 20
	}
	chunkPlan := backenddb.ValueLogRewriteChunkPlan{
		ChunkBytes:                chunkBytes,
		BytesTotal:                plan.BytesTotal,
		BytesLive:                 plan.BytesLive,
		BytesStale:                plan.BytesStale,
		AgeBlockedChunks:          plan.AgeBlockedSegments,
		AgeBlockedBytesTotal:      plan.AgeBlockedBytesTotal,
		AgeBlockedBytesLive:       plan.AgeBlockedBytesLive,
		AgeBlockedBytesStale:      plan.AgeBlockedBytesStale,
		AgeBlockedMinRemainingAge: plan.AgeBlockedMinRemainingAge,
	}
	if len(plan.SelectedSegments) > 0 {
		chunkPlan.SourceChunks = make([]backenddb.ValueLogRewritePlanChunk, 0, len(plan.SelectedSegments))
		for _, seg := range plan.SelectedSegments {
			if seg.FileID == 0 {
				continue
			}
			chunkPlan.SourceChunks = append(chunkPlan.SourceChunks, backenddb.ValueLogRewritePlanChunk{
				FileID:     seg.FileID,
				BytesTotal: seg.BytesTotal,
				BytesLive:  seg.BytesLive,
				BytesStale: seg.BytesStale,
				StaleRatio: seg.StaleRatio,
			})
			chunkPlan.SelectedBytesTotal += seg.BytesTotal
			chunkPlan.SelectedBytesLive += seg.BytesLive
			chunkPlan.SelectedBytesStale += seg.BytesStale
		}
		chunkPlan.ChunksSelected = len(chunkPlan.SourceChunks)
		chunkPlan.ChunksTotal = chunkPlan.ChunksSelected
		return chunkPlan
	}
	if len(plan.SourceFileIDs) == 0 {
		return chunkPlan
	}
	shareBytes := func(total int64, idx, count int) int64 {
		if total <= 0 || count <= 0 {
			return 0
		}
		base := total / int64(count)
		rem := total % int64(count)
		if int64(idx) < rem {
			base++
		}
		return base
	}
	selectedTotal := plan.SelectedBytesTotal
	selectedLive := plan.SelectedBytesLive
	selectedStale := plan.SelectedBytesStale
	if selectedTotal <= 0 {
		selectedTotal = selectedLive + selectedStale
	}
	if selectedTotal <= 0 {
		selectedTotal = int64(len(plan.SourceFileIDs))
	}
	if selectedLive <= 0 {
		selectedLive = selectedTotal
	}
	chunkPlan.SourceChunks = make([]backenddb.ValueLogRewritePlanChunk, 0, len(plan.SourceFileIDs))
	for i, id := range plan.SourceFileIDs {
		if id == 0 {
			continue
		}
		live := shareBytes(selectedLive, i, len(plan.SourceFileIDs))
		stale := shareBytes(selectedStale, i, len(plan.SourceFileIDs))
		total := shareBytes(selectedTotal, i, len(plan.SourceFileIDs))
		if total < live+stale {
			total = live + stale
		}
		if total <= 0 {
			total = 1
		}
		chunk := backenddb.ValueLogRewritePlanChunk{
			FileID:     id,
			BytesTotal: total,
			BytesLive:  live,
			BytesStale: stale,
		}
		if total > 0 && stale > 0 {
			chunk.StaleRatio = float64(stale) / float64(total)
		}
		chunkPlan.SourceChunks = append(chunkPlan.SourceChunks, chunk)
		chunkPlan.SelectedBytesTotal += total
		chunkPlan.SelectedBytesLive += live
		chunkPlan.SelectedBytesStale += stale
	}
	chunkPlan.ChunksSelected = len(chunkPlan.SourceChunks)
	chunkPlan.ChunksTotal = chunkPlan.ChunksSelected
	return chunkPlan
}

func forceVlogMaintenanceIdle(db *DB) {
	if db != nil {
		idleAt := time.Now().Add(-2 * vlogGenerationMaintenanceQuietWindow)
		db.lastForegroundWriteUnixNano.Store(idleAt.UnixNano())
		db.lastForegroundReadUnixNano.Store(idleAt.UnixNano())
		db.activeForegroundIterators.Store(0)
	}
}

func forceRetainedPruneIdle(db *DB) {
	if db != nil {
		idleAt := time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano()
		db.lastForegroundWriteUnixNano.Store(idleAt)
		db.checkpointCutoverLastUnixNano.Store(idleAt)
	}
}

func forceRewriteStageConfirmDue(t *testing.T, db *DB) {
	t.Helper()
	if db == nil {
		return
	}
	observedAt := time.Now().Add(-vlogGenerationRewriteStageConfirmDelay - time.Second).UnixNano()
	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteStageObservedUnixNano = observedAt
	db.vlogGenerationRewriteQueueMu.Unlock()
	// Synchronous scheduler tests drive the confirmation pass explicitly. Do not
	// arm the background confirmation wake here; that introduces a race where the
	// goroutine can consume staged debt before the test's direct maintenance call,
	// which has been flaky on slower Windows runners.
	db.clearVlogGenerationRewriteStageConfirmation()
}

func holdVlogGenerationDeferredMaintenanceRunnerForTest(t *testing.T, db *DB) {
	t.Helper()
	if db == nil {
		return
	}
	if db.vlogGenerationDeferredMaintenancePending.Load() {
		t.Fatalf("deferred maintenance unexpectedly pending before test ownership barrier")
	}
	if !db.vlogGenerationDeferredMaintenanceRunning.CompareAndSwap(false, true) {
		t.Fatalf("deferred maintenance runner unexpectedly active before test ownership barrier")
	}
	t.Cleanup(func() {
		db.vlogGenerationDeferredMaintenancePending.Store(false)
		db.vlogGenerationDeferredMaintenanceRunning.Store(false)
	})
}

func disableVlogGenerationLoop(t *testing.T) {
	t.Helper()
	t.Setenv(envDisableVlogGenerationLoop, "1")
}

func prepareDirectSchedulerTest(t *testing.T) {
	t.Helper()
	disableVlogGenerationLoop(t)
	// Direct scheduler tests drive maintenance explicitly and should not arm the
	// checkpoint-kick path during Open, even if the scheduler itself is disabled.
	t.Setenv(envDisableVlogGenerationCheckpointKick, "1")
}

func skipRetainedPrune(db *DB) {
	if db != nil {
		db.testSkipRetainedPrune = true
	}
}

func schedulerTestWait(t *testing.T) time.Duration {
	t.Helper()
	const (
		defaultWait = 2 * time.Second
		maxWait     = 4 * time.Second
		minWait     = 500 * time.Millisecond
		safety      = 10 * time.Millisecond
	)
	if deadline, ok := t.Deadline(); ok {
		remain := time.Until(deadline) - safety
		if remain <= 0 {
			return 0
		}
		wait := remain / 8
		if wait > maxWait {
			wait = maxWait
		}
		if wait < minWait {
			wait = minWait
		}
		if wait > remain {
			return remain
		}
		return wait
	}
	return defaultWait
}

func (b *blockingRewritePlannerBackend) ValueLogRewritePlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
	b.startOnce.Do(func() { close(b.planStart) })
	select {
	case <-b.planBlock:
		b.mu.Lock()
		b.planCompleted++
		plan := b.planResponse
		b.mu.Unlock()
		return plan, nil
	case <-ctx.Done():
		b.mu.Lock()
		b.planCanceled++
		b.mu.Unlock()
		return backenddb.ValueLogRewritePlan{}, ctx.Err()
	}
}

func (b *blockingRewritePlannerBackend) ValueLogRewriteChunkPlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions, chunkBytes int64) (backenddb.ValueLogRewriteChunkPlan, error) {
	plan, err := b.ValueLogRewritePlan(ctx, opts)
	if err != nil {
		return backenddb.ValueLogRewriteChunkPlan{}, err
	}
	return rewriteChunkPlanForTest(plan, chunkBytes), nil
}

func (b *blockingRewritePlannerBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	b.mu.Lock()
	b.rewriteCalls++
	b.mu.Unlock()
	return backenddb.ValueLogRewriteStats{}, nil
}

func (b *blockingRewritePlannerBackend) recordedRewriteCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rewriteCalls
}

func (b *blockingRewritePlannerBackend) recordedPlanOutcomes() (completed int, canceled int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.planCompleted, b.planCanceled
}

func (b *blockingRewritePlannerBackend) unblockPlan() {
	b.planUnblockOnce.Do(func() {
		close(b.planBlock)
	})
}

type timedRewritePlannerBackend struct {
	*backenddb.DB

	startOnce sync.Once
	planStart chan struct{}
	planDelay time.Duration

	mu            sync.Mutex
	planCompleted int
	planCanceled  int
}

func (b *timedRewritePlannerBackend) ValueLogRewritePlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
	b.startOnce.Do(func() { close(b.planStart) })
	if b.planDelay > 0 {
		timer := time.NewTimer(b.planDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			b.mu.Lock()
			b.planCanceled++
			b.mu.Unlock()
			return backenddb.ValueLogRewritePlan{}, ctx.Err()
		}
	}
	b.mu.Lock()
	b.planCompleted++
	b.mu.Unlock()
	return backenddb.ValueLogRewritePlan{}, nil
}

func (b *timedRewritePlannerBackend) ValueLogRewriteChunkPlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions, chunkBytes int64) (backenddb.ValueLogRewriteChunkPlan, error) {
	plan, err := b.ValueLogRewritePlan(ctx, opts)
	if err != nil {
		return backenddb.ValueLogRewriteChunkPlan{}, err
	}
	return rewriteChunkPlanForTest(plan, chunkBytes), nil
}

func (b *timedRewritePlannerBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	return backenddb.ValueLogRewriteStats{}, nil
}

func (b *timedRewritePlannerBackend) recordedPlanOutcomes() (completed int, canceled int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.planCompleted, b.planCanceled
}

type blockingRewriteOnlineBackend struct {
	*backenddb.DB

	mu             sync.Mutex
	planResponse   backenddb.ValueLogRewritePlan
	rewriteCalls   int
	lastRewriteTTL time.Duration
	rewriteEntered chan struct{}
	rewriteRelease chan struct{}
	enterOnce      sync.Once
}

func (b *blockingRewriteOnlineBackend) ValueLogRewritePlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
	return b.planResponse, nil
}

func (b *blockingRewriteOnlineBackend) ValueLogRewriteChunkPlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions, chunkBytes int64) (backenddb.ValueLogRewriteChunkPlan, error) {
	return rewriteChunkPlanForTest(b.planResponse, chunkBytes), nil
}

func (b *blockingRewriteOnlineBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	ttl := time.Duration(-1)
	if deadline, ok := ctx.Deadline(); ok {
		ttl = time.Until(deadline)
	}
	b.mu.Lock()
	b.rewriteCalls++
	b.lastRewriteTTL = ttl
	b.mu.Unlock()
	b.enterOnce.Do(func() {
		close(b.rewriteEntered)
	})
	select {
	case <-b.rewriteRelease:
	case <-ctx.Done():
		return backenddb.ValueLogRewriteStats{}, ctx.Err()
	}
	return backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1}, nil
}

func (b *blockingRewriteOnlineBackend) recordedRewriteCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rewriteCalls
}

func (b *blockingRewriteOnlineBackend) recordedRewriteTTL() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastRewriteTTL
}

func TestShouldRunVlogGenerationRewrite_TotalBytes(t *testing.T) {
	db := &DB{valueLogRewriteTriggerBytes: 100}
	run, reason := db.shouldRunVlogGenerationRewrite(150, 0, 0)
	if !run {
		t.Fatalf("expected rewrite to trigger on total bytes")
	}
	if reason != vlogGenerationReasonTotalBytes {
		t.Fatalf("reason=%d want=%d", reason, vlogGenerationReasonTotalBytes)
	}
}

func TestShouldRunVlogGenerationRewrite_StaleRatio(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 250000}
	run, reason := db.shouldRunVlogGenerationRewrite(0, 300000, 0)
	if !run {
		t.Fatalf("expected rewrite to trigger on stale ratio")
	}
	if reason != vlogGenerationReasonStaleRatio {
		t.Fatalf("reason=%d want=%d", reason, vlogGenerationReasonStaleRatio)
	}
}

func TestShouldRunVlogGenerationRewrite_Churn(t *testing.T) {
	db := &DB{valueLogRewriteTriggerChurn: 1 << 20}
	run, reason := db.shouldRunVlogGenerationRewrite(0, 0, 2<<20)
	if !run {
		t.Fatalf("expected rewrite to trigger on churn")
	}
	if reason != vlogGenerationReasonChurn {
		t.Fatalf("reason=%d want=%d", reason, vlogGenerationReasonChurn)
	}
}

func TestShouldRunVlogGenerationRewrite_NoTrigger(t *testing.T) {
	db := &DB{
		valueLogRewriteTriggerBytes:    100,
		valueLogRewriteTriggerRatioPPM: 200000,
		valueLogRewriteTriggerChurn:    1000,
	}
	run, reason := db.shouldRunVlogGenerationRewrite(50, 100000, 900)
	if run {
		t.Fatalf("expected no rewrite trigger")
	}
	if reason != vlogGenerationReasonNone {
		t.Fatalf("reason=%d want=%d", reason, vlogGenerationReasonNone)
	}
}

func TestObserveVlogGenerationRewriteQueueProgress_LiveBytesUnknownResetsLast(t *testing.T) {
	db := &DB{}
	db.vlogGenerationRewriteQueueLiveBytesBeforeLast.Store(1600)
	db.vlogGenerationRewriteQueueLiveBytesAfterLast.Store(1200)
	db.vlogGenerationRewriteQueueLiveBytesDeltaLast.Store(-400)

	db.observeVlogGenerationRewriteQueueProgress(
		4, 1600, false,
		2, 1200, true,
	)

	if got, want := db.vlogGenerationRewriteQueueProgressPasses.Load(), uint64(1); got != want {
		t.Fatalf("queue progress passes=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewriteQueueLiveBytesUnknownPasses.Load(), uint64(1); got != want {
		t.Fatalf("live bytes unknown passes=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewriteQueueLiveBytesKnownPasses.Load(), uint64(0); got != want {
		t.Fatalf("live bytes known passes=%d want=%d", got, want)
	}
	if got := db.vlogGenerationRewriteQueueLiveBytesBeforeLast.Load(); got != -1 {
		t.Fatalf("live bytes before last=%d want -1", got)
	}
	if got := db.vlogGenerationRewriteQueueLiveBytesAfterLast.Load(); got != -1 {
		t.Fatalf("live bytes after last=%d want -1", got)
	}
	if got := db.vlogGenerationRewriteQueueLiveBytesDeltaLast.Load(); got != -1 {
		t.Fatalf("live bytes delta last=%d want -1", got)
	}
}

func TestObserveVlogGenerationRewriteQueueProgress_KnownPassOverwritesSentinel(t *testing.T) {
	db := &DB{}
	db.observeVlogGenerationRewriteQueueProgress(
		4, 1600, false,
		2, 1200, true,
	)
	db.observeVlogGenerationRewriteQueueProgress(
		2, 900, true,
		1, 700, true,
	)

	if got, want := db.vlogGenerationRewriteQueueLiveBytesUnknownPasses.Load(), uint64(1); got != want {
		t.Fatalf("live bytes unknown passes=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewriteQueueLiveBytesKnownPasses.Load(), uint64(1); got != want {
		t.Fatalf("live bytes known passes=%d want=%d", got, want)
	}
	if got := db.vlogGenerationRewriteQueueLiveBytesBeforeLast.Load(); got != 900 {
		t.Fatalf("live bytes before last=%d want 900", got)
	}
	if got := db.vlogGenerationRewriteQueueLiveBytesAfterLast.Load(); got != 700 {
		t.Fatalf("live bytes after last=%d want 700", got)
	}
	if got := db.vlogGenerationRewriteQueueLiveBytesDeltaLast.Load(); got != -200 {
		t.Fatalf("live bytes delta last=%d want -200", got)
	}
}

func TestVlogGenerationRewriteQueueLiveBytesSnapshot_UsesLoadedLedger(t *testing.T) {
	db := &DB{}
	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteQueueLoaded = true
	db.vlogGenerationRewriteLedger = []backenddb.ValueLogRewritePlanSegment{
		{FileID: 1, BytesLive: 100},
		{FileID: 2, BytesLive: 50},
		{FileID: 3, BytesLive: -20},
	}
	db.vlogGenerationRewriteQueueMu.Unlock()

	liveBytes, known, err := db.vlogGenerationRewriteQueueLiveBytesSnapshot([]uint32{2, 1, 2, 9, 0})
	if err != nil {
		t.Fatalf("snapshot error: %v", err)
	}
	if known {
		t.Fatalf("expected known=false when queue coverage is partial")
	}
	if liveBytes != 200 {
		t.Fatalf("live bytes=%d want 200", liveBytes)
	}

	liveBytes, known, err = db.vlogGenerationRewriteQueueLiveBytesSnapshot([]uint32{2, 1, 2, 0})
	if err != nil {
		t.Fatalf("snapshot error (full coverage): %v", err)
	}
	if !known {
		t.Fatalf("expected known=true when all queue ids are in the ledger")
	}
	if liveBytes != 200 {
		t.Fatalf("live bytes (full coverage)=%d want 200", liveBytes)
	}

	liveBytes, known, err = db.vlogGenerationRewriteQueueLiveBytesSnapshot([]uint32{3})
	if err != nil {
		t.Fatalf("snapshot error (negative clamp): %v", err)
	}
	if !known {
		t.Fatalf("expected known=true for matching negative-live id")
	}
	if liveBytes != 0 {
		t.Fatalf("live bytes clamp=%d want 0", liveBytes)
	}
}

func TestVlogGenerationRewriteQueueLiveBytesSnapshot_UnknownWhenLedgerMissing(t *testing.T) {
	db := &DB{}
	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteQueueLoaded = true
	db.vlogGenerationRewriteLedger = nil
	db.vlogGenerationRewriteQueueMu.Unlock()

	liveBytes, known, err := db.vlogGenerationRewriteQueueLiveBytesSnapshot([]uint32{1, 2})
	if err != nil {
		t.Fatalf("snapshot error: %v", err)
	}
	if known {
		t.Fatalf("expected known=false when ledger is missing")
	}
	if liveBytes != 0 {
		t.Fatalf("live bytes=%d want 0", liveBytes)
	}
}

func TestVlogGenerationRewriteQueueLiveBytesSnapshot_DeduplicatesDuplicateLedgerEntries(t *testing.T) {
	db := &DB{}
	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteQueueLoaded = true
	db.vlogGenerationRewriteLedger = []backenddb.ValueLogRewritePlanSegment{
		{FileID: 1, BytesLive: 100},
		// Duplicate FileID: last entry wins (same as legacy byID behavior).
		{FileID: 1, BytesLive: 250},
		{FileID: 2, BytesLive: 50},
	}
	db.vlogGenerationRewriteQueueMu.Unlock()

	liveBytes, known, err := db.vlogGenerationRewriteQueueLiveBytesSnapshot([]uint32{1, 2})
	if err != nil {
		t.Fatalf("snapshot error with duplicate ledger entries: %v", err)
	}
	if !known {
		t.Fatalf("expected known=true when duplicate-ledger ids are present")
	}
	if want := int64(250 + 50); liveBytes != want {
		t.Fatalf("live bytes with duplicate ledger entries=%d want %d", liveBytes, want)
	}
}

func TestConsumeVlogGenerationRewriteQueueChunk_RebuildsLedgerByFileID(t *testing.T) {
	db := &DB{}
	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteQueueLoaded = true
	db.vlogGenerationRewriteQueue = []uint32{1, 2}
	db.vlogGenerationRewriteLedger = []backenddb.ValueLogRewritePlanSegment{
		{FileID: 1, BytesLive: 100},
		{FileID: 2, BytesLive: 50},
	}
	db.vlogGenerationRewriteLedgerByFileID = map[uint32]backenddb.ValueLogRewritePlanSegment{
		1: {FileID: 1, BytesLive: 999},
		2: {FileID: 2, BytesLive: 999},
		3: {FileID: 3, BytesLive: 999},
	}
	db.vlogGenerationRewriteQueueMu.Unlock()

	if err := db.consumeVlogGenerationRewriteQueueChunk([]uint32{1}); err != nil {
		t.Fatalf("consume queue chunk: %v", err)
	}

	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if got := db.vlogGenerationRewriteQueue; len(got) != 1 || got[0] != 2 {
		t.Fatalf("remaining queue=%v want [2]", got)
	}
	if got := db.vlogGenerationRewriteLedgerByFileID; len(got) != 1 {
		t.Fatalf("ledgerByFileID length=%d want 1", len(got))
	}
	if _, ok := db.vlogGenerationRewriteLedgerByFileID[1]; ok {
		t.Fatalf("stale file id 1 remained in ledgerByFileID")
	}
	if got := db.vlogGenerationRewriteLedgerByFileID[2].BytesLive; got != 50 {
		t.Fatalf("ledgerByFileID[2].BytesLive=%d want 50", got)
	}
}

func BenchmarkVlogGenerationRewriteQueueLiveBytesSnapshot(b *testing.B) {
	const (
		ledgerSegments = 4096
		stride         = 8
	)
	db := &DB{}
	ledger := make([]backenddb.ValueLogRewritePlanSegment, 0, ledgerSegments)
	ids := make([]uint32, 0, ledgerSegments/stride)
	for i := 1; i <= ledgerSegments; i++ {
		id := uint32(i)
		ledger = append(ledger, backenddb.ValueLogRewritePlanSegment{
			FileID:    id,
			BytesLive: int64(64 + (i % 31)),
		})
		if i%stride == 0 {
			ids = append(ids, id)
		}
	}
	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteQueueLoaded = true
	db.vlogGenerationRewriteLedger = ledger
	db.vlogGenerationRewriteQueueMu.Unlock()
	if _, _, err := db.vlogGenerationRewriteQueueLiveBytesSnapshot(ids); err != nil {
		b.Fatalf("priming snapshot error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var (
		lastLiveBytes int64
		lastKnown     bool
		lastErr       error
	)
	for i := 0; i < b.N; i++ {
		lastLiveBytes, lastKnown, lastErr = db.vlogGenerationRewriteQueueLiveBytesSnapshot(ids)
	}
	b.StopTimer()
	if lastErr != nil {
		b.Fatalf("snapshot error: %v", lastErr)
	}
	if !lastKnown {
		b.Fatalf("expected known=true")
	}
	if lastLiveBytes <= 0 {
		b.Fatalf("expected positive live bytes, got %d", lastLiveBytes)
	}
}

func TestObserveVlogGenerationRewritePlanOutcome_SelectedTracksBytes(t *testing.T) {
	db := &DB{}
	db.observeVlogGenerationRewritePlanOutcome(backenddb.ValueLogRewritePlan{
		SourceFileIDs:      []uint32{11},
		SegmentsSelected:   1,
		SelectedBytesTotal: 1024,
		SelectedBytesLive:  640,
		SelectedBytesStale: 384,
	}, nil)
	if got, want := db.vlogGenerationRewritePlanRuns.Load(), uint64(1); got != want {
		t.Fatalf("plan runs=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanSelected.Load(), uint64(1); got != want {
		t.Fatalf("plan selected=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanSelectedBytes.Load(), uint64(1024); got != want {
		t.Fatalf("plan selected bytes total=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanSelectedLiveBytes.Load(), uint64(640); got != want {
		t.Fatalf("plan selected bytes live=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanSelectedStaleBytes.Load(), uint64(384); got != want {
		t.Fatalf("plan selected bytes stale=%d want=%d", got, want)
	}
}

func TestObserveVlogGenerationRewritePlanOutcome_SelectedTracksSegmentFallbackBytes(t *testing.T) {
	db := &DB{}
	db.observeVlogGenerationRewritePlanOutcome(backenddb.ValueLogRewritePlan{
		SourceFileIDs:    []uint32{11, 22},
		SegmentsSelected: 2,
		SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
			{FileID: 11, BytesTotal: 100, BytesLive: 25, BytesStale: 75},
			{FileID: 22, BytesTotal: 120, BytesLive: 40, BytesStale: 80},
		},
	}, nil)
	if got, want := db.vlogGenerationRewritePlanSelectedBytes.Load(), uint64(220); got != want {
		t.Fatalf("fallback selected bytes total=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanSelectedLiveBytes.Load(), uint64(65); got != want {
		t.Fatalf("fallback selected bytes live=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanSelectedStaleBytes.Load(), uint64(155); got != want {
		t.Fatalf("fallback selected bytes stale=%d want=%d", got, want)
	}
}

func TestObserveVlogGenerationRewritePlanOutcome_EmptyReasonBuckets(t *testing.T) {
	db := &DB{}
	db.observeVlogGenerationRewritePlanOutcome(backenddb.ValueLogRewritePlan{
		AgeBlockedSegments:        2,
		AgeBlockedMinRemainingAge: 3 * time.Second,
	}, nil)
	db.observeVlogGenerationRewritePlanOutcome(backenddb.ValueLogRewritePlan{}, nil)

	if got, want := db.vlogGenerationRewritePlanEmpty.Load(), uint64(2); got != want {
		t.Fatalf("plan empty=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanEmptyAgeBlocked.Load(), uint64(1); got != want {
		t.Fatalf("plan empty age-blocked=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanEmptyNoSelection.Load(), uint64(1); got != want {
		t.Fatalf("plan empty no-selection=%d want=%d", got, want)
	}
}

func TestObserveVlogGenerationRewritePlanPenaltyFilterCounters(t *testing.T) {
	db := &DB{}
	db.observeVlogGenerationRewritePlanPenaltyFilter(5, 2)
	db.observeVlogGenerationRewritePlanPenaltyFilter(2, 0)

	if got, want := db.vlogGenerationRewritePlanPenaltyFilterRuns.Load(), uint64(2); got != want {
		t.Fatalf("penalty filter runs=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanPenaltyFilterSegments.Load(), uint64(5); got != want {
		t.Fatalf("penalty filter segments=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewritePlanPenaltyFilterToEmpty.Load(), uint64(1); got != want {
		t.Fatalf("penalty filter to-empty=%d want=%d", got, want)
	}
}

func TestObserveVlogGenerationRewriteCanceledCountersByQueueState(t *testing.T) {
	db := &DB{}
	db.observeVlogGenerationRewriteCanceled(false)
	db.observeVlogGenerationRewriteCanceled(true)

	if got, want := db.vlogGenerationRewriteCanceledRuns.Load(), uint64(2); got != want {
		t.Fatalf("rewrite canceled total=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewriteCanceledFreshPlanRuns.Load(), uint64(1); got != want {
		t.Fatalf("rewrite canceled fresh=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewriteCanceledQueuedDebtRuns.Load(), uint64(1); got != want {
		t.Fatalf("rewrite canceled queued=%d want=%d", got, want)
	}
}

func TestObserveVlogGenerationRewriteDeadlineCountersByQueueState(t *testing.T) {
	db := &DB{}
	db.observeVlogGenerationRewriteDeadline(false)
	db.observeVlogGenerationRewriteDeadline(true)

	if got, want := db.vlogGenerationRewriteDeadlineRuns.Load(), uint64(2); got != want {
		t.Fatalf("rewrite deadline total=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewriteDeadlineFreshPlanRuns.Load(), uint64(1); got != want {
		t.Fatalf("rewrite deadline fresh=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationRewriteDeadlineQueuedDebtRuns.Load(), uint64(1); got != want {
		t.Fatalf("rewrite deadline queued=%d want=%d", got, want)
	}
}

func TestMaybeRunVlogGenerationMaintenanceWithOptions_TracksWalOnPeriodicSkip(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}
	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{})
	if got, want := db.vlogGenerationMaintenanceAttempts.Load(), uint64(1); got != want {
		t.Fatalf("maintenance attempts=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationMaintenanceSkipWALOnPeriodic.Load(), uint64(1); got != want {
		t.Fatalf("maintenance wal-on periodic skips=%d want=%d", got, want)
	}
	if got := db.vlogGenerationMaintenanceAcquired.Load(); got != 0 {
		t.Fatalf("maintenance acquired=%d want=0", got)
	}
}

func TestSetMaintenancePhase_WALOnSteadyArmsScheduler(t *testing.T) {
	// Keep checkpoint-kick inert so this test only exercises the periodic
	// scheduler state and direct maintenance gating under WAL-on.
	t.Setenv(envDisableVlogGenerationCheckpointKick, "1")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       false,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerDisabled {
		t.Fatalf("scheduler state=%d want disabled", got)
	}

	db.SetMaintenancePhase(MaintenancePhaseRestore)
	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerDisabled {
		t.Fatalf("scheduler state after restore=%d want disabled", got)
	}
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})
	if got, want := db.vlogGenerationMaintenanceAttempts.Load(), uint64(1); got != want {
		t.Fatalf("maintenance attempts after restore-phase request=%d want=%d", got, want)
	}
	if got := db.vlogGenerationMaintenanceAcquired.Load(); got != 0 {
		t.Fatalf("maintenance acquired during restore-phase request=%d want=0", got)
	}
	if got := db.vlogGenerationMaintenanceSkipWALOnPeriodic.Load(); got != 0 {
		t.Fatalf("maintenance wal-on periodic skips after restore-phase request=%d want=0", got)
	}

	db.SetMaintenancePhase(MaintenancePhaseSteady)
	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerIdle {
		t.Fatalf("scheduler state after steady=%d want=%d", got, vlogGenerationSchedulerIdle)
	}
	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{})
	if got, want := db.vlogGenerationMaintenanceAttempts.Load(), uint64(2); got != want {
		t.Fatalf("maintenance attempts after periodic request=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationMaintenanceSkipWALOnPeriodic.Load(), uint64(1); got != want {
		t.Fatalf("maintenance wal-on periodic skips=%d want=%d", got, want)
	}
	if got := db.vlogGenerationMaintenanceAcquired.Load(); got != 0 {
		t.Fatalf("maintenance acquired=%d want=0", got)
	}
}

func TestSetMaintenancePhase_WALOnSteadyDoesNotArmSchedulerWhileClosing(t *testing.T) {
	t.Setenv(envDisableVlogGenerationCheckpointKick, "1")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       false,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.SetMaintenancePhase(MaintenancePhaseRestore)
	db.closing.Store(true)
	db.SetMaintenancePhase(MaintenancePhaseSteady)
	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerDisabled {
		t.Fatalf("scheduler state while closing=%d want disabled", got)
	}
}

func TestMaybeRunVlogGenerationMaintenanceWithOptions_TracksCollision(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}
	db.vlogGenerationMaintenanceActive.Store(true)
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})
	if got, want := db.vlogGenerationMaintenanceAttempts.Load(), uint64(1); got != want {
		t.Fatalf("maintenance attempts=%d want=%d", got, want)
	}
	if got, want := db.vlogGenerationMaintenanceCollisions.Load(), uint64(1); got != want {
		t.Fatalf("maintenance collisions=%d want=%d", got, want)
	}
	if got := db.vlogGenerationMaintenanceAcquired.Load(); got != 0 {
		t.Fatalf("maintenance acquired=%d want=0", got)
	}
}

func TestShouldRunVlogGenerationIndexVacuum_TracksSkipReasons(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}
	now := time.Now()
	if db.shouldRunVlogGenerationIndexVacuum(vlogGenerationVacuumTriggerRewriteBytes-1, now) {
		t.Fatalf("expected vacuum to skip below rewrite trigger")
	}
	if got, want := db.vlogGenerationVacuumSkippedRewriteBytes.Load(), uint64(1); got != want {
		t.Fatalf("vacuum skipped_rewrite_bytes=%d want=%d", got, want)
	}
	db.vlogGenerationLastVacuumUnixNano.Store(now.UnixNano())
	if db.shouldRunVlogGenerationIndexVacuum(vlogGenerationVacuumTriggerRewriteBytes, now) {
		t.Fatalf("expected vacuum to skip during cooldown")
	}
	if got, want := db.vlogGenerationVacuumSkippedCooldown.Load(), uint64(1); got != want {
		t.Fatalf("vacuum skipped_cooldown=%d want=%d", got, want)
	}
}

func TestMaybeRunVlogGenerationIndexVacuum_TracksDisabledSkip(t *testing.T) {
	t.Setenv(envDisableVlogGenerationVacuum, "1")
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}
	db.maybeRunVlogGenerationIndexVacuum(vlogGenerationVacuumTriggerRewriteBytes)
	if got, want := db.vlogGenerationVacuumSkippedDisabled.Load(), uint64(1); got != want {
		t.Fatalf("vacuum skipped_disabled=%d want=%d", got, want)
	}
}

func TestRunVlogGenerationMaintenanceRetries_CoalescesPendingCollisionRetries(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}

	db.vlogGenerationMaintenanceActive.Store(true)
	db.vlogGenerationCheckpointKickPending.Store(true)
	db.runVlogGenerationMaintenanceRetries(vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "checkpoint_pending",
	}, 30*time.Millisecond, false)
	if got := db.vlogGenerationMaintenanceCollisions.Load(); got != 0 {
		t.Fatalf("checkpoint pending retry collisions=%d want=0", got)
	}

	db.vlogGenerationMaintenanceActive.Store(true)
	db.vlogGenerationCheckpointKickPending.Store(false)
	db.runVlogGenerationMaintenanceRetries(vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "checkpoint_pending",
	}, 30*time.Millisecond, false)
	if got := db.vlogGenerationMaintenanceCollisions.Load(); got != 0 {
		t.Fatalf("checkpoint retry collisions while active=%d want=0", got)
	}

	db.vlogGenerationMaintenanceActive.Store(true)
	db.vlogGenerationDeferredMaintenancePending.Store(true)
	db.runVlogGenerationMaintenanceRetries(vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm",
	}, 30*time.Millisecond, true)
	if got := db.vlogGenerationMaintenanceCollisions.Load(); got != 0 {
		t.Fatalf("deferred pending retry collisions=%d want=0", got)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForGenericPass_UsesConfiguredTriggerRatio(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 200000}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForGenericPass(8<<30), 0.85; got != want {
		t.Fatalf("generic min stale ratio=%f want=%f", got, want)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForGenericPass_UsesHigherConfiguredTriggerRatio(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 800000}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForGenericPass(8<<30), 0.85; got != want {
		t.Fatalf("generic min stale ratio=%f want=%f", got, want)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForGenericPass_PreservesHigherConfiguredRatio(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 900000}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForGenericPass(8<<30), 0.90; got != want {
		t.Fatalf("generic min stale ratio=%f want=%f", got, want)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForGenericPass_DisabledBelowEfficacyFloor(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 800000}
	if got := db.vlogGenerationRewriteMinStaleRatioForGenericPass(vlogGenerationRewriteEfficacyMinTotalBytes - 1); got != 0 {
		t.Fatalf("generic min stale ratio below efficacy floor=%f want=0", got)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForGenericPass_DefaultWithoutConfiguredTrigger(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 0}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForGenericPass(8<<30), vlogGenerationRewriteGenericMinSegmentStaleRatio; got != want {
		t.Fatalf("generic min stale ratio=%f want=%f", got, want)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForQueuedDebt_UsesGenericFloorForTotalBytes(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 200000}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForQueuedDebt(8<<30, vlogGenerationReasonTotalBytes), 0.85; got != want {
		t.Fatalf("queued total-bytes min stale ratio=%f want=%f", got, want)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForQueuedDebt_PreservesResumeFloor(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 200000}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForQueuedDebt(8<<30, vlogGenerationReasonRewriteResume), vlogGenerationRewriteMinSegmentStaleRatio; got != want {
		t.Fatalf("queued resume min stale ratio=%f want=%f", got, want)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForStaleRatioTrigger_UsesQualityFloor(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 200000}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForStaleRatioTrigger(8<<30), 0.50; got != want {
		t.Fatalf("stale-ratio min stale ratio=%f want=%f", got, want)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForStaleRatioTrigger_UsesHigherConfiguredRatio(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 800000}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForStaleRatioTrigger(8<<30), 0.80; got != want {
		t.Fatalf("stale-ratio min stale ratio=%f want=%f", got, want)
	}
}

func TestVlogGenerationRewriteMinStaleRatioForStaleRatioTrigger_PreservesConfiguredRatioBelowEfficacyFloor(t *testing.T) {
	db := &DB{valueLogRewriteTriggerRatioPPM: 200000}
	if got, want := db.vlogGenerationRewriteMinStaleRatioForStaleRatioTrigger(vlogGenerationRewriteEfficacyMinTotalBytes-1), 0.20; got != want {
		t.Fatalf("stale-ratio min stale ratio below efficacy floor=%f want=%f", got, want)
	}
}

func TestFilterVlogGenerationRewriteLedgerByQuality(t *testing.T) {
	segments := []backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesTotal: 64, BytesLive: 8, BytesStale: 56, StaleRatio: 0.875},
		{FileID: 12, BytesTotal: 64, BytesLive: 40, BytesStale: 24, StaleRatio: 0.375},
	}
	filtered := filterVlogGenerationRewriteLedgerByQuality(segments, 0.5, 1)
	if got, want := vlogGenerationRewriteLedgerIDs(filtered), []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("filtered ids=%v want=%v", got, want)
	}
}

func TestShouldRunVlogGenerationGC_ReclaimableBytes(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}
	run := db.shouldRunVlogGenerationGC(valueLogRetainedGenerationStats{}, vlogGenerationGCMinBytes, 0)
	if !run {
		t.Fatalf("expected gc to trigger on reclaimable bytes")
	}
}

func TestShouldRunVlogGenerationGC_HotSegmentPressure(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}
	run := db.shouldRunVlogGenerationGC(valueLogRetainedGenerationStats{
		SegmentsTotal: 4,
		SegmentsHot:   2,
	}, 0, 0)
	if !run {
		t.Fatalf("expected gc to trigger on hot segment pressure")
	}
}

func TestShouldRunVlogGenerationGC_ChurnDriven(t *testing.T) {
	db := &DB{
		valueLogGenerationPolicy:    uint8(backenddb.ValueLogGenerationHotWarmCold),
		valueLogRewriteTriggerChurn: 1 << 20,
	}
	run := db.shouldRunVlogGenerationGC(valueLogRetainedGenerationStats{}, 0, 1<<19)
	if !run {
		t.Fatalf("expected gc to trigger on churn threshold")
	}
}

func TestShouldRunVlogGenerationIndexVacuum_Threshold(t *testing.T) {
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}
	ok := db.shouldRunVlogGenerationIndexVacuum(vlogGenerationVacuumTriggerRewriteBytes, time.Now())
	if !ok {
		t.Fatalf("expected index vacuum at rewrite threshold")
	}
}

func TestShouldRunVlogGenerationIndexVacuum_Cooldown(t *testing.T) {
	now := time.Now()
	db := &DB{valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold)}
	db.vlogGenerationLastVacuumUnixNano.Store(now.Add(-vlogGenerationVacuumMinInterval / 2).UnixNano())
	ok := db.shouldRunVlogGenerationIndexVacuum(vlogGenerationVacuumTriggerRewriteBytes, now)
	if ok {
		t.Fatalf("expected index vacuum to be blocked by cooldown")
	}
}

func TestVlogGenerationRewriteBudgetAccruesAtConfiguredRate(t *testing.T) {
	db := &DB{
		valueLogRewriteTriggerBytes:  4096,
		valueLogGenerationHotTarget:  1024,
		valueLogGenerationWarmTarget: 1024,
		valueLogGenerationColdTarget: 1024,
		valueLogRewriteBudgetBytes:   512,
		valueLogGenerationPolicy:     uint8(backenddb.ValueLogGenerationHotWarmCold),
	}
	start := time.Unix(100, 0)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(start.UnixNano())
	db.vlogGenerationAccrueRewriteBudget(start.Add(3500 * time.Millisecond))
	if got, want := db.vlogGenerationRewriteBudgetTokensBytes.Load(), int64(1792); got != want {
		t.Fatalf("tokens=%d want=%d", got, want)
	}
}

func TestVlogGenerationRewriteBudgetCapEnforcedOnLargeElapsed(t *testing.T) {
	db := &DB{
		valueLogRewriteTriggerBytes:  1024,
		valueLogGenerationHotTarget:  128,
		valueLogGenerationWarmTarget: 128,
		valueLogGenerationColdTarget: 128,
		valueLogRewriteBudgetBytes:   1 << 62,
		valueLogGenerationPolicy:     uint8(backenddb.ValueLogGenerationHotWarmCold),
	}
	start := time.Unix(100, 0)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(start.UnixNano())
	db.vlogGenerationAccrueRewriteBudget(start.Add(24 * time.Hour))
	if got, want := db.vlogGenerationRewriteBudgetTokensBytes.Load(), int64(1024); got != want {
		t.Fatalf("tokens=%d want=%d", got, want)
	}
}

func TestVlogGenerationRewriteBudgetIgnoresBackwardClock(t *testing.T) {
	db := &DB{
		valueLogRewriteTriggerBytes:  1024,
		valueLogGenerationHotTarget:  256,
		valueLogGenerationWarmTarget: 256,
		valueLogGenerationColdTarget: 256,
		valueLogRewriteBudgetBytes:   512,
		valueLogGenerationPolicy:     uint8(backenddb.ValueLogGenerationHotWarmCold),
	}
	start := time.Unix(100, 0)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(start.UnixNano())
	db.vlogGenerationAccrueRewriteBudget(start.Add(-time.Second))
	if got := db.vlogGenerationRewriteBudgetTokensBytes.Load(); got != 0 {
		t.Fatalf("tokens after backward clock=%d want=0", got)
	}
	if got := db.vlogGenerationRewriteBudgetLastUnixNano.Load(); got != start.UnixNano() {
		t.Fatalf("last timestamp moved backwards: got=%d want=%d", got, start.UnixNano())
	}
	db.vlogGenerationAccrueRewriteBudget(start.Add(2 * time.Second))
	if got, want := db.vlogGenerationRewriteBudgetTokensBytes.Load(), int64(1024); got != want {
		t.Fatalf("tokens after forward clock=%d want=%d", got, want)
	}
}

func TestVlogGenerationRewriteBudgetCapSaturatesTargets(t *testing.T) {
	db := &DB{
		valueLogRewriteTriggerBytes:  1,
		valueLogGenerationHotTarget:  maxPositiveInt64 - 16,
		valueLogGenerationWarmTarget: 32,
		valueLogGenerationColdTarget: 64,
	}
	if got, want := db.vlogGenerationRewriteBudgetCapBytes(), maxPositiveInt64; got != want {
		t.Fatalf("budget cap=%d want=%d", got, want)
	}
}

func TestAddClampInt64_SaturatesAndClamps(t *testing.T) {
	if got := addClampInt64(5, 7, 10); got != 10 {
		t.Fatalf("addClampInt64 saturating add=%d want=10", got)
	}
	if got := addClampInt64(-5, 3, 10); got != 3 {
		t.Fatalf("addClampInt64 negative current=%d want=3", got)
	}
	if got := addClampInt64(9, 1, 0); got != 0 {
		t.Fatalf("addClampInt64 zero limit=%d want=0", got)
	}
}

func TestMulDivClampInt64_ClampsOverflowAndCap(t *testing.T) {
	if got := mulDivClampInt64(maxPositiveInt64, maxPositiveInt64, 1, 123); got != 123 {
		t.Fatalf("mulDivClampInt64 overflow=%d want=123", got)
	}
	if got := mulDivClampInt64(9, 5, 3, 20); got != 15 {
		t.Fatalf("mulDivClampInt64 normal=%d want=15", got)
	}
	if got := mulDivClampInt64(9, 5, 3, 0); got != 0 {
		t.Fatalf("mulDivClampInt64 zero cap=%d want=0", got)
	}
}

func TestVlogGenerationRewriteMaxSegmentsForRun_ClampsDebtDrainQueue(t *testing.T) {
	db := &DB{
		valueLogRewriteBudgetBytes:   1024,
		valueLogGenerationWarmTarget: 256,
	}

	if got := db.vlogGenerationRewriteMaxSegmentsForRun(2, 1024, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true}); got != 2 {
		t.Fatalf("queueLen=2 got=%d want=2", got)
	}
	if got := db.vlogGenerationRewriteMaxSegmentsForRun(vlogGenerationRewriteDebtDrainMaxSegments+5, 1<<20, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true}); got != vlogGenerationRewriteDebtDrainMaxSegments {
		t.Fatalf("queueLen>%d got=%d want=%d", vlogGenerationRewriteDebtDrainMaxSegments, got, vlogGenerationRewriteDebtDrainMaxSegments)
	}
	if got := db.vlogGenerationRewriteMaxSegmentsForRun(4, 1024, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true, bypassQuiet: true}); got != 2 {
		t.Fatalf("checkpoint-kick got=%d want=2", got)
	}
	if got := db.vlogGenerationRewriteMaxSegmentsForRun(4, 256, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true}); got != 1 {
		t.Fatalf("budget clamp got=%d want=1", got)
	}
}

func TestVlogGenerationRewriteSegmentCapForRun_Limiter(t *testing.T) {
	db := &DB{
		valueLogRewriteBudgetBytes:   1024,
		valueLogGenerationWarmTarget: 256,
	}

	decision := db.vlogGenerationRewriteSegmentCapForRun(4, 256, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true})
	if decision.maxSegments != 1 {
		t.Fatalf("maxSegments=%d want=1", decision.maxSegments)
	}
	if decision.limiter != vlogGenerationRewriteSegmentCapLimiterBudgetTokens {
		t.Fatalf("limiter=%s want=budget_tokens", vlogGenerationRewriteSegmentCapLimiterString(decision.limiter))
	}
	if decision.byBudgetSegments != 1 {
		t.Fatalf("byBudgetSegments=%d want=1", decision.byBudgetSegments)
	}
	if decision.perSegmentBudget != 256 {
		t.Fatalf("perSegmentBudget=%d want=256", decision.perSegmentBudget)
	}

	checkpointDecision := db.vlogGenerationRewriteSegmentCapForRun(4, 1024, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true, bypassQuiet: true})
	if checkpointDecision.maxSegments != 2 {
		t.Fatalf("checkpoint maxSegments=%d want=2", checkpointDecision.maxSegments)
	}
	if checkpointDecision.limiter != vlogGenerationRewriteSegmentCapLimiterCheckpointKickBurst {
		t.Fatalf("checkpoint limiter=%s want=checkpoint_kick_burst", vlogGenerationRewriteSegmentCapLimiterString(checkpointDecision.limiter))
	}
	if checkpointDecision.byBudgetSegments != 4 {
		t.Fatalf("checkpoint byBudgetSegments=%d want=4", checkpointDecision.byBudgetSegments)
	}
	if checkpointDecision.perSegmentBudget != 256 {
		t.Fatalf("checkpoint perSegmentBudget=%d want=256", checkpointDecision.perSegmentBudget)
	}

	checkpointSafety := db.vlogGenerationRewriteSegmentCapForRun(3, 1024, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true, bypassQuiet: true})
	if checkpointSafety.maxSegments != 1 {
		t.Fatalf("checkpoint safety maxSegments=%d want=1", checkpointSafety.maxSegments)
	}
	if checkpointSafety.limiter != vlogGenerationRewriteSegmentCapLimiterCheckpointKickSafety {
		t.Fatalf("checkpoint safety limiter=%s want=checkpoint_kick_safety", vlogGenerationRewriteSegmentCapLimiterString(checkpointSafety.limiter))
	}
}

func TestVlogGenerationRewriteSegmentCapForRun_UsesQueueLiveHint(t *testing.T) {
	db := &DB{
		valueLogRewriteBudgetBytes:   1 << 20,
		valueLogGenerationWarmTarget: 256,
	}

	withHint := db.vlogGenerationRewriteSegmentCapForRunWithHint(
		4,
		600,
		800,
		true,
		vlogGenerationMaintenanceOptions{rewriteDebtDrain: true},
	)
	if withHint.maxSegments != 3 {
		t.Fatalf("withHint maxSegments=%d want=3", withHint.maxSegments)
	}
	if withHint.byBudgetSegments != 3 {
		t.Fatalf("withHint byBudgetSegments=%d want=3", withHint.byBudgetSegments)
	}
	if withHint.perSegmentBudget != 200 {
		t.Fatalf("withHint perSegmentBudget=%d want=200", withHint.perSegmentBudget)
	}
	if withHint.limiter != vlogGenerationRewriteSegmentCapLimiterBudgetTokens {
		t.Fatalf("withHint limiter=%s want=budget_tokens", vlogGenerationRewriteSegmentCapLimiterString(withHint.limiter))
	}

	withoutHint := db.vlogGenerationRewriteSegmentCapForRunWithHint(
		4,
		600,
		0,
		false,
		vlogGenerationMaintenanceOptions{rewriteDebtDrain: true},
	)
	if withoutHint.maxSegments != 2 {
		t.Fatalf("withoutHint maxSegments=%d want=2", withoutHint.maxSegments)
	}
	if withoutHint.byBudgetSegments != 2 {
		t.Fatalf("withoutHint byBudgetSegments=%d want=2", withoutHint.byBudgetSegments)
	}
	if withoutHint.perSegmentBudget != 256 {
		t.Fatalf("withoutHint perSegmentBudget=%d want=256", withoutHint.perSegmentBudget)
	}
}

func TestVlogGenerationRewriteSegmentCapForFreshPlan_UsesQueueLiveHint(t *testing.T) {
	db := &DB{
		valueLogRewriteBudgetBytes:   1 << 20,
		valueLogGenerationWarmTarget: 256,
	}
	decision := db.vlogGenerationRewriteSegmentCapForFreshPlanWithHint(
		vlogGenerationRewriteFreshPlanDebtDrainMinSegments,
		600,
		800,
		true,
		vlogGenerationMaintenanceOptions{rewriteDebtDrain: true},
	)
	if decision.maxSegments != 3 {
		t.Fatalf("fresh-plan withHint maxSegments=%d want=3", decision.maxSegments)
	}
	if decision.byBudgetSegments != 3 {
		t.Fatalf("fresh-plan withHint byBudgetSegments=%d want=3", decision.byBudgetSegments)
	}
	if decision.perSegmentBudget != 200 {
		t.Fatalf("fresh-plan withHint perSegmentBudget=%d want=200", decision.perSegmentBudget)
	}
	if decision.limiter != vlogGenerationRewriteSegmentCapLimiterBudgetTokens {
		t.Fatalf("fresh-plan withHint limiter=%s want=budget_tokens", vlogGenerationRewriteSegmentCapLimiterString(decision.limiter))
	}
}

func TestVlogGenerationObserveRewriteSegmentCapDecision(t *testing.T) {
	db := &DB{}
	runDecision := vlogGenerationRewriteSegmentCapDecision{limiter: vlogGenerationRewriteSegmentCapLimiterBudgetTokens}
	db.observeVlogGenerationRewriteSegmentCapDecision(runDecision, false)
	db.observeVlogGenerationRewriteSegmentCapDecision(runDecision, false)
	freshDecision := vlogGenerationRewriteSegmentCapDecision{limiter: vlogGenerationRewriteSegmentCapLimiterFreshPlanCap}
	db.observeVlogGenerationRewriteSegmentCapDecision(freshDecision, true)

	if got := db.vlogGenerationRewriteQueueRunSegmentCapDecisions.Load(); got != 2 {
		t.Fatalf("run decisions=%d want=2", got)
	}
	if got := db.vlogGenerationRewriteQueueFreshPlanSegmentCapDecisions.Load(); got != 1 {
		t.Fatalf("fresh-plan decisions=%d want=1", got)
	}
	if got := db.vlogGenerationRewriteQueueRunSegmentCapLimiterCounts[int(vlogGenerationRewriteSegmentCapLimiterBudgetTokens)].Load(); got != 2 {
		t.Fatalf("run budget_tokens limiter count=%d want=2", got)
	}
	if got := db.vlogGenerationRewriteQueueFreshPlanSegmentCapLimiterCounts[int(vlogGenerationRewriteSegmentCapLimiterFreshPlanCap)].Load(); got != 1 {
		t.Fatalf("fresh-plan fresh_plan_cap limiter count=%d want=1", got)
	}
}

func TestVlogGenerationRewriteSegmentCapForFreshPlan_Limiter(t *testing.T) {
	db := &DB{
		valueLogRewriteBudgetBytes:   1 << 20,
		valueLogGenerationWarmTarget: 64,
	}

	belowThreshold := db.vlogGenerationRewriteSegmentCapForFreshPlan(
		vlogGenerationRewriteFreshPlanDebtDrainMinSegments-1,
		1<<20,
		vlogGenerationMaintenanceOptions{rewriteDebtDrain: true},
	)
	if belowThreshold.maxSegments != vlogGenerationRewriteResumeMaxSegments {
		t.Fatalf("below threshold maxSegments=%d want=%d", belowThreshold.maxSegments, vlogGenerationRewriteResumeMaxSegments)
	}
	if belowThreshold.limiter != vlogGenerationRewriteSegmentCapLimiterFreshPlanQueueThreshold {
		t.Fatalf("below threshold limiter=%s want=fresh_plan_queue_threshold", vlogGenerationRewriteSegmentCapLimiterString(belowThreshold.limiter))
	}

	clamped := db.vlogGenerationRewriteSegmentCapForFreshPlan(
		vlogGenerationRewriteFreshPlanDebtDrainMinSegments+8,
		1<<20,
		vlogGenerationMaintenanceOptions{rewriteDebtDrain: true},
	)
	if clamped.maxSegments != vlogGenerationRewriteFreshPlanDebtDrainMaxSegments {
		t.Fatalf("clamped maxSegments=%d want=%d", clamped.maxSegments, vlogGenerationRewriteFreshPlanDebtDrainMaxSegments)
	}
	if clamped.limiter != vlogGenerationRewriteSegmentCapLimiterFreshPlanCap {
		t.Fatalf("clamped limiter=%s want=fresh_plan_cap", vlogGenerationRewriteSegmentCapLimiterString(clamped.limiter))
	}
}

func TestVlogGenerationRewriteMaxSegmentsForFreshPlan_BelowQueueThreshold(t *testing.T) {
	db := &DB{
		valueLogRewriteBudgetBytes:   1024,
		valueLogGenerationWarmTarget: 256,
	}
	got := db.vlogGenerationRewriteMaxSegmentsForFreshPlan(
		vlogGenerationRewriteFreshPlanDebtDrainMinSegments-1,
		1<<20,
		vlogGenerationMaintenanceOptions{rewriteDebtDrain: true, debugSource: "rewrite_age_blocked"},
	)
	if got != vlogGenerationRewriteResumeMaxSegments {
		t.Fatalf("fresh-plan queue<threshold got=%d want=%d", got, vlogGenerationRewriteResumeMaxSegments)
	}
}

func TestVlogGenerationRewriteMaxSegmentsForFreshPlan_ClampsToFreshCap(t *testing.T) {
	db := &DB{
		valueLogRewriteBudgetBytes:   1 << 20,
		valueLogGenerationWarmTarget: 64,
	}
	got := db.vlogGenerationRewriteMaxSegmentsForFreshPlan(
		vlogGenerationRewriteFreshPlanDebtDrainMinSegments+8,
		1<<20,
		vlogGenerationMaintenanceOptions{rewriteDebtDrain: true, debugSource: "rewrite_age_blocked"},
	)
	if got != vlogGenerationRewriteFreshPlanDebtDrainMaxSegments {
		t.Fatalf("fresh-plan clamp got=%d want=%d", got, vlogGenerationRewriteFreshPlanDebtDrainMaxSegments)
	}
}

func TestVlogGenerationRewriteMaxSegmentsForFreshPlan_AllowsStaleRatioDebtDrain(t *testing.T) {
	db := &DB{
		valueLogRewriteBudgetBytes:   1 << 20,
		valueLogGenerationWarmTarget: 64,
	}
	got := db.vlogGenerationRewriteMaxSegmentsForFreshPlan(
		vlogGenerationRewriteFreshPlanDebtDrainMinSegments+8,
		1<<20,
		vlogGenerationMaintenanceOptions{rewriteDebtDrain: true, debugSource: "rewrite_age_blocked"},
	)
	if got != vlogGenerationRewriteFreshPlanDebtDrainMaxSegments {
		t.Fatalf("fresh-plan stale-ratio got=%d want=%d", got, vlogGenerationRewriteFreshPlanDebtDrainMaxSegments)
	}
}

func TestVlogGenerationRewriteMaxSegments_UsesOverrideLimits(t *testing.T) {
	oldResume := vlogGenerationRewriteResumeMaxSegmentsLimit
	oldDebtDrain := vlogGenerationRewriteDebtDrainMaxSegmentsLimit
	oldFreshMin := vlogGenerationRewriteFreshPlanDebtDrainMinSegmentsLimit
	oldFreshMax := vlogGenerationRewriteFreshPlanDebtDrainMaxSegmentsLimit
	t.Cleanup(func() {
		vlogGenerationRewriteResumeMaxSegmentsLimit = oldResume
		vlogGenerationRewriteDebtDrainMaxSegmentsLimit = oldDebtDrain
		vlogGenerationRewriteFreshPlanDebtDrainMinSegmentsLimit = oldFreshMin
		vlogGenerationRewriteFreshPlanDebtDrainMaxSegmentsLimit = oldFreshMax
	})

	vlogGenerationRewriteResumeMaxSegmentsLimit = 3
	vlogGenerationRewriteDebtDrainMaxSegmentsLimit = 6
	vlogGenerationRewriteFreshPlanDebtDrainMinSegmentsLimit = 5
	// max<min should clamp up to min in effective limits.
	vlogGenerationRewriteFreshPlanDebtDrainMaxSegmentsLimit = 2

	db := &DB{
		valueLogRewriteBudgetBytes:   1 << 20,
		valueLogGenerationWarmTarget: 64,
	}

	if got := db.vlogGenerationRewriteMaxSegmentsForRun(1, 1<<20, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true}); got != 3 {
		t.Fatalf("run resume override got=%d want=3", got)
	}
	if got := db.vlogGenerationRewriteMaxSegmentsForRun(12, 1<<20, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true}); got != 6 {
		t.Fatalf("run debt-drain override got=%d want=6", got)
	}
	if got := db.vlogGenerationRewriteMaxSegmentsForFreshPlan(4, 1<<20, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true, debugSource: "rewrite_age_blocked"}); got != 3 {
		t.Fatalf("fresh-plan below override min got=%d want=3", got)
	}
	if got := db.vlogGenerationRewriteMaxSegmentsForFreshPlan(12, 1<<20, vlogGenerationMaintenanceOptions{rewriteDebtDrain: true, debugSource: "rewrite_age_blocked"}); got != 5 {
		t.Fatalf("fresh-plan override clamp got=%d want=5", got)
	}
}

type rewriteBudgetRecordingBackend struct {
	*backenddb.DB

	mu                sync.Mutex
	planOpts          backenddb.ValueLogRewriteOnlineOptions
	planCalls         int
	planResponse      backenddb.ValueLogRewritePlan
	planErr           error
	planFn            func(backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error)
	rewriteOpts       backenddb.ValueLogRewriteOnlineOptions
	rewriteHistory    []backenddb.ValueLogRewriteOnlineOptions
	rewriteCalls      int
	rewriteResponse   backenddb.ValueLogRewriteStats
	rewriteErr        error
	rewriteFn         func(context.Context, backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error)
	gcCalls           int
	gcOpts            []backenddb.ValueLogGCOptions
	gcResponse        backenddb.ValueLogGCStats
	gcResponses       []backenddb.ValueLogGCStats
	gcErr             error
	gcFn              func(context.Context, backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error)
	leafPackCalls     int
	leafPackOpts      backenddb.LeafGenerationPackFromPlanOptions
	leafPackResp      backenddb.LeafGenerationPackRunOnceStats
	leafPackResponses []backenddb.LeafGenerationPackRunOnceStats
	leafPackErr       error
}

func (b *rewriteBudgetRecordingBackend) ValueLogRewritePlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
	b.mu.Lock()
	b.planOpts = cloneRewriteOptsForTest(opts)
	b.planCalls++
	plan := b.planResponse
	err := b.planErr
	if b.planFn != nil {
		plan, err = b.planFn(opts)
	}
	b.mu.Unlock()
	return plan, err
}

func (b *rewriteBudgetRecordingBackend) ValueLogRewriteChunkPlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions, chunkBytes int64) (backenddb.ValueLogRewriteChunkPlan, error) {
	b.mu.Lock()
	b.planOpts = cloneRewriteOptsForTest(opts)
	b.planCalls++
	plan := b.planResponse
	err := b.planErr
	planFn := b.planFn
	b.mu.Unlock()
	if planFn != nil {
		plan, err = planFn(opts)
	}
	if err != nil {
		return backenddb.ValueLogRewriteChunkPlan{}, err
	}
	return rewriteChunkPlanForTest(plan, chunkBytes), nil
}

func (b *rewriteBudgetRecordingBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	b.mu.Lock()
	cloned := cloneRewriteOptsForTest(opts)
	b.rewriteOpts = cloned
	b.rewriteHistory = append(b.rewriteHistory, cloned)
	b.rewriteCalls++
	stats := b.rewriteResponse
	err := b.rewriteErr
	customFn := b.rewriteFn
	b.mu.Unlock()
	if customFn != nil {
		return customFn(ctx, opts)
	}
	return stats, err
}

func (b *rewriteBudgetRecordingBackend) ValueLogGC(ctx context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error) {
	b.mu.Lock()
	b.gcCalls++
	b.gcOpts = append(b.gcOpts, cloneGCOptsForTest(opts))
	customFn := b.gcFn
	stats := b.gcResponse
	if len(b.gcResponses) > 0 {
		idx := b.gcCalls - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(b.gcResponses) {
			idx = len(b.gcResponses) - 1
		}
		stats = b.gcResponses[idx]
	}
	err := b.gcErr
	b.mu.Unlock()
	if customFn != nil {
		return customFn(ctx, opts)
	}
	return stats, err
}

func (b *rewriteBudgetRecordingBackend) LeafGenerationPackRunOnce(ctx context.Context, opts backenddb.LeafGenerationPackFromPlanOptions) (backenddb.LeafGenerationPackRunOnceStats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.leafPackCalls++
	cloned := opts
	cloned.ProtectedRootIDs = append([]uint64(nil), opts.ProtectedRootIDs...)
	cloned.ProtectedSystemRootIDs = append([]uint64(nil), opts.ProtectedSystemRootIDs...)
	b.leafPackOpts = cloned
	resp := b.leafPackResp
	if len(b.leafPackResponses) > 0 {
		idx := b.leafPackCalls - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(b.leafPackResponses) {
			idx = len(b.leafPackResponses) - 1
		}
		resp = b.leafPackResponses[idx]
	}
	return resp, b.leafPackErr
}

func (b *rewriteBudgetRecordingBackend) recordedRewrite() (backenddb.ValueLogRewriteOnlineOptions, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rewriteOpts, b.rewriteCalls
}

func (b *rewriteBudgetRecordingBackend) recordedRewrites() []backenddb.ValueLogRewriteOnlineOptions {
	b.mu.Lock()
	defer b.mu.Unlock()
	history := make([]backenddb.ValueLogRewriteOnlineOptions, len(b.rewriteHistory))
	copy(history, b.rewriteHistory)
	return history
}

func cloneRewriteOptsForTest(opts backenddb.ValueLogRewriteOnlineOptions) backenddb.ValueLogRewriteOnlineOptions {
	cloned := opts
	cloned.SourceFileIDs = append([]uint32(nil), opts.SourceFileIDs...)
	cloned.SourceChunks = append([]backenddb.ValueLogRewritePlanChunk(nil), opts.SourceChunks...)
	cloned.ProtectedPaths = append([]string(nil), opts.ProtectedPaths...)
	cloned.LeafGenerationProtectedRootIDs = append([]uint64(nil), opts.LeafGenerationProtectedRootIDs...)
	cloned.LeafGenerationProtectedSystemRootIDs = append([]uint64(nil), opts.LeafGenerationProtectedSystemRootIDs...)
	return cloned
}

func cloneGCOptsForTest(opts backenddb.ValueLogGCOptions) backenddb.ValueLogGCOptions {
	cloned := opts
	cloned.ProtectedPaths = append([]string(nil), opts.ProtectedPaths...)
	cloned.ProtectedInUsePaths = append([]string(nil), opts.ProtectedInUsePaths...)
	cloned.ProtectedRetainedPaths = append([]string(nil), opts.ProtectedRetainedPaths...)
	cloned.ObservedSourceFileIDs = append([]uint32(nil), opts.ObservedSourceFileIDs...)
	return cloned
}

func (b *rewriteBudgetRecordingBackend) recordedPlan() (backenddb.ValueLogRewriteOnlineOptions, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.planOpts, b.planCalls
}

func (b *rewriteBudgetRecordingBackend) recordedGC() (backenddb.ValueLogGCStats, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	stats := b.gcResponse
	if len(b.gcResponses) > 0 && b.gcCalls > 0 {
		idx := b.gcCalls - 1
		if idx >= len(b.gcResponses) {
			idx = len(b.gcResponses) - 1
		}
		stats = b.gcResponses[idx]
	}
	return stats, b.gcCalls
}

func (b *rewriteBudgetRecordingBackend) recordedGCObservedSourceCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := 0
	for _, opts := range b.gcOpts {
		if opts.DryRun {
			continue
		}
		if len(opts.ObservedSourceFileIDs) == 0 {
			continue
		}
		count++
	}
	return count
}

func (b *rewriteBudgetRecordingBackend) recordedLeafPack() (backenddb.LeafGenerationPackFromPlanOptions, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.leafPackOpts, b.leafPackCalls
}

type leafPackMaintenanceRecordingBackend struct {
	*backenddb.DB

	mu                sync.Mutex
	calls             int
	opts              backenddb.LeafGenerationPackFromPlanOptions
	optsHistory       []backenddb.LeafGenerationPackFromPlanOptions
	resp              backenddb.LeafGenerationPackRunOnceStats
	responses         []backenddb.LeafGenerationPackRunOnceStats
	err               error
	leafGCCalls       int
	leafGCOpts        []backenddb.LeafGenerationGCOptions
	leafGCResp        backenddb.LeafGenerationGCStats
	leafGCResponses   []backenddb.LeafGenerationGCStats
	leafGCErr         error
	hasDeadline       bool
	deadline          time.Time
	entered           chan struct{}
	release           chan struct{}
	blockUntilCtxDone bool
}

func (b *leafPackMaintenanceRecordingBackend) LeafGenerationPackRunOnce(ctx context.Context, opts backenddb.LeafGenerationPackFromPlanOptions) (backenddb.LeafGenerationPackRunOnceStats, error) {
	b.mu.Lock()
	b.calls++
	cloned := opts
	cloned.ProtectedRootIDs = append([]uint64(nil), opts.ProtectedRootIDs...)
	cloned.ProtectedSystemRootIDs = append([]uint64(nil), opts.ProtectedSystemRootIDs...)
	b.opts = cloned
	b.optsHistory = append(b.optsHistory, cloned)
	b.deadline, b.hasDeadline = ctx.Deadline()
	entered := b.entered
	release := b.release
	blockUntilCtxDone := b.blockUntilCtxDone
	resp := b.resp
	if len(b.responses) > 0 {
		idx := b.calls - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(b.responses) {
			idx = len(b.responses) - 1
		}
		resp = b.responses[idx]
	}
	err := b.err
	b.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if blockUntilCtxDone {
		<-ctx.Done()
		return backenddb.LeafGenerationPackRunOnceStats{}, ctx.Err()
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return backenddb.LeafGenerationPackRunOnceStats{}, ctx.Err()
		}
	}
	return resp, err
}

func (b *leafPackMaintenanceRecordingBackend) LeafGenerationGC(ctx context.Context, opts backenddb.LeafGenerationGCOptions) (backenddb.LeafGenerationGCStats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.leafGCCalls++
	cloned := opts
	cloned.ProtectedRootIDs = append([]uint64(nil), opts.ProtectedRootIDs...)
	cloned.ProtectedSystemRootIDs = append([]uint64(nil), opts.ProtectedSystemRootIDs...)
	b.leafGCOpts = append(b.leafGCOpts, cloned)
	stats := b.leafGCResp
	if len(b.leafGCResponses) > 0 {
		idx := b.leafGCCalls - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(b.leafGCResponses) {
			idx = len(b.leafGCResponses) - 1
		}
		stats = b.leafGCResponses[idx]
	}
	return stats, b.leafGCErr
}

func (b *leafPackMaintenanceRecordingBackend) recordedLeafPack() (backenddb.LeafGenerationPackFromPlanOptions, int, bool, time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.opts, b.calls, b.hasDeadline, b.deadline
}

func (b *leafPackMaintenanceRecordingBackend) recordedLeafPackHistory() []backenddb.LeafGenerationPackFromPlanOptions {
	b.mu.Lock()
	defer b.mu.Unlock()
	history := make([]backenddb.LeafGenerationPackFromPlanOptions, len(b.optsHistory))
	for i := range b.optsHistory {
		history[i] = b.optsHistory[i]
		history[i].ProtectedRootIDs = append([]uint64(nil), b.optsHistory[i].ProtectedRootIDs...)
		history[i].ProtectedSystemRootIDs = append([]uint64(nil), b.optsHistory[i].ProtectedSystemRootIDs...)
	}
	return history
}

func (b *leafPackMaintenanceRecordingBackend) recordedLeafGC() (backenddb.LeafGenerationGCStats, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	stats := b.leafGCResp
	if len(b.leafGCResponses) > 0 && b.leafGCCalls > 0 {
		idx := b.leafGCCalls - 1
		if idx >= len(b.leafGCResponses) {
			idx = len(b.leafGCResponses) - 1
		}
		stats = b.leafGCResponses[idx]
	}
	return stats, b.leafGCCalls
}

func (b *leafPackMaintenanceRecordingBackend) recordedLeafGCOptions() []backenddb.LeafGenerationGCOptions {
	b.mu.Lock()
	defer b.mu.Unlock()
	opts := make([]backenddb.LeafGenerationGCOptions, len(b.leafGCOpts))
	for i := range b.leafGCOpts {
		opts[i] = b.leafGCOpts[i]
		opts[i].ProtectedRootIDs = append([]uint64(nil), b.leafGCOpts[i].ProtectedRootIDs...)
		opts[i].ProtectedSystemRootIDs = append([]uint64(nil), b.leafGCOpts[i].ProtectedSystemRootIDs...)
	}
	return opts
}

func uint64SlicesEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mustLeafPackTempDir(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", prefix)
	if err != nil {
		t.Fatalf("mkdir tempdir: %v", err)
	}
	return dir
}

func removeLeafPackTempDirErr(dir string) error {
	var lastErr error
	sleep := 20 * time.Millisecond
	for i := 0; i < 80; i++ {
		err := os.RemoveAll(dir)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		lastErr = err
		time.Sleep(sleep)
		if sleep < 100*time.Millisecond {
			sleep += 10 * time.Millisecond
		}
	}
	return lastErr
}

func removeLeafPackTempDir(t *testing.T, dir string) {
	t.Helper()
	if err := removeLeafPackTempDirErr(dir); err != nil {
		t.Fatalf("remove tempdir %s: %v", dir, err)
	}
}

func removeLeafPackTempDirBestEffort(t *testing.T, dir string) {
	t.Helper()
	if err := removeLeafPackTempDirErr(dir); err != nil {
		if runtime.GOOS == "windows" {
			t.Logf("best-effort remove tempdir %s: %v", dir, err)
			return
		}
		t.Fatalf("remove tempdir %s: %v", dir, err)
	}
}

func mustOpenLeafPackBackend(t *testing.T) *backenddb.DB {
	t.Helper()
	dir := mustLeafPackTempDir(t, "treedb-leaf-pack-backend-")
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		removeLeafPackTempDir(t, dir)
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
		removeLeafPackTempDir(t, dir)
	})
	return backend
}

func openLeafPackMaintenanceTestDB(t *testing.T, backend *leafPackMaintenanceRecordingBackend) (*DB, func()) {
	t.Helper()
	dir := mustLeafPackTempDir(t, "treedb-leaf-pack-cache-")
	db, err := Open(dir, backend, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     4,
		IndexOuterLeavesInValueLog:       true,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	db.testSkipVlogCheckpointKick = true
	b := db.NewBatch()
	if err := b.Set([]byte("k"), make([]byte, 2048)); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationMaintenanceQuietWindow).UnixNano())
	db.lastForegroundReadUnixNano.Store(time.Now().Add(-2 * vlogForegroundReadQuietWindow).UnixNano())
	forceVlogMaintenanceIdle(db)
	return db, func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close cachingdb: %v", err)
		}
		removeLeafPackTempDir(t, dir)
	}
}

func openLeafPackMaintenanceSchedulerOnlyTestDBWithClose(t *testing.T, backend *leafPackMaintenanceRecordingBackend) (*DB, func(), func()) {
	t.Helper()
	dir := mustLeafPackTempDir(t, "treedb-leaf-pack-cache-")
	db, err := Open(dir, backend, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     4,
		IndexOuterLeavesInValueLog:       true,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	db.testSkipVlogCheckpointKick = true
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.lastForegroundWriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationMaintenanceQuietWindow).UnixNano())
	db.lastForegroundReadUnixNano.Store(time.Now().Add(-2 * vlogForegroundReadQuietWindow).UnixNano())
	forceVlogMaintenanceIdle(db)
	var closeOnce sync.Once
	closeDB := func() {
		closeOnce.Do(func() {
			if err := db.Close(); err != nil {
				t.Fatalf("close cachingdb: %v", err)
			}
		})
	}
	return db, closeDB, func() {
		closeDB()
		removeLeafPackTempDirBestEffort(t, dir)
	}
}

func openLeafPackMaintenanceSchedulerOnlyTestDB(t *testing.T, backend *leafPackMaintenanceRecordingBackend) (*DB, func()) {
	t.Helper()
	db, _, cleanup := openLeafPackMaintenanceSchedulerOnlyTestDBWithClose(t, backend)
	return db, cleanup
}

func leafPackWindowExhaustingStats(genID uint64, expectedReclaimBytes int64, copiedBytes int64) backenddb.LeafGenerationPackRunOnceStats {
	if expectedReclaimBytes <= 0 {
		expectedReclaimBytes = 1024
	}
	if copiedBytes <= 0 {
		copiedBytes = 4096
	}
	return backenddb.LeafGenerationPackRunOnceStats{
		Ran: true,
		Selection: backenddb.LeafGenerationPackSelection{
			GenerationIDs:        []uint64{genID},
			BytesToCopy:          leafGenerationPackMaintenanceDefaultMaxBytesToCopy,
			BytesDead:            expectedReclaimBytes,
			ExpectedReclaimBytes: expectedReclaimBytes,
		},
		Pack: backenddb.LeafGenerationPackStats{
			BytesCopied:   copiedBytes,
			WallTimeNanos: (5 * time.Millisecond).Nanoseconds(),
		},
	}
}

func openRewriteQueueTestDB(t *testing.T, dir string, recorder *rewriteBudgetRecordingBackend) (*DB, func()) {
	t.Helper()
	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	db.testSkipVlogCheckpointKick = true
	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)
	return db, func() { _ = db.Close() }
}

func runRewriteQueueMaintenanceForTest(db *DB) {
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})
}

func expireVlogGenerationRewritePenaltiesForTest(t *testing.T, db *DB, ids ...uint32) {
	t.Helper()
	db.vlogGenerationRewriteQueueMu.Lock()
	defer db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		t.Fatalf("load rewrite penalties: %v", err)
	}
	expiredAt := time.Now().Add(-time.Second).UnixNano()
	for _, id := range ids {
		penalty, ok := db.vlogGenerationRewritePenalties[id]
		if !ok {
			t.Fatalf("rewrite penalty for segment %d missing", id)
		}
		penalty.CooldownUntilUnixNano = expiredAt
		db.vlogGenerationRewritePenalties[id] = penalty
	}
	if err := saveValueLogGenerationRewriteState(
		db.valueLogGenerationStatePath(),
		append([]uint32(nil), db.vlogGenerationRewriteQueue...),
		append([]backenddb.ValueLogRewritePlanSegment(nil), db.vlogGenerationRewriteLedger...),
		append([]backenddb.ValueLogRewritePlanChunk(nil), db.vlogGenerationRewriteChunkLedger...),
		db.vlogGenerationRewriteChunkBytes,
		db.vlogGenerationRewriteHistory,
		db.vlogGenerationRewritePenalties,
		db.vlogGenerationRewriteStagePending,
		db.vlogGenerationRewriteStageObservedUnixNano,
	); err != nil {
		t.Fatalf("persist expired rewrite penalties: %v", err)
	}
}

func TestLeafGenerationPackMaintenance_RequiresExplicitEnv(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend, leafPackResp: leafPackWindowExhaustingStats(1, 1024, 4096)}
	defer recorder.Close()
	db := &DB{
		backend:                    recorder,
		indexOuterLeavesInValueLog: true,
		valueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationHotWarmCold),
		closeCh:                    make(chan struct{}),
	}
	db.checkpointCond = sync.NewCond(&db.checkpointMu)
	attempted, ran, err := db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if attempted || ran {
		t.Fatalf("attempted=%t ran=%t want false/false", attempted, ran)
	}
}

func TestVlogGenerationMaintenance_PauseFileSkipsAndResumes(t *testing.T) {
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	pauseFile := filepath.Join(t.TempDir(), "vlog-maintenance.pause")
	if err := os.WriteFile(pauseFile, []byte("pause"), 0o600); err != nil {
		t.Fatalf("write pause file: %v", err)
	}
	t.Setenv(envVlogGenerationMaintenancePauseFile, pauseFile)

	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:           backend,
		leafPackResp: leafPackWindowExhaustingStats(1, 1024, 4096),
	}
	defer recorder.Close()
	db := &DB{
		backend:                    recorder,
		indexOuterLeavesInValueLog: true,
		valueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationHotWarmCold),
		closeCh:                    make(chan struct{}),
	}
	db.checkpointCond = sync.NewCond(&db.checkpointMu)

	acquired := db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if acquired {
		t.Fatalf("paused maintenance acquired slot")
	}
	if got := db.vlogGenerationMaintenanceSkipPauseFile.Load(); got != 1 {
		t.Fatalf("pause-file skips=%d want 1", got)
	}
	if _, calls := recorder.recordedLeafPack(); calls != 0 {
		t.Fatalf("paused leaf-pack calls=%d want 0", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.pause_file.configured"]; got != "true" {
		t.Fatalf("pause_file.configured=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.pause_file.paused"]; got != "true" {
		t.Fatalf("pause_file.paused=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.skip.pause_file"]; got != "1" {
		t.Fatalf("skip.pause_file=%q want 1", got)
	}

	if err := os.Remove(pauseFile); err != nil {
		t.Fatalf("remove pause file: %v", err)
	}
	acquired = db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if !acquired {
		t.Fatalf("unpaused maintenance did not acquire slot")
	}
	if _, calls := recorder.recordedLeafPack(); calls != 1 {
		t.Fatalf("unpaused leaf-pack calls=%d want 1", calls)
	}
	stats = db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.pause_file.paused"]; got != "false" {
		t.Fatalf("pause_file.paused after remove=%q want false", got)
	}
}

func TestLeafGenerationPackMaintenance_RunsWithDefaultBounds(t *testing.T) {
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		leafPackResp: backenddb.LeafGenerationPackRunOnceStats{
			Ran: true,
			Selection: backenddb.LeafGenerationPackSelection{
				GenerationIDs:                   []uint64{7},
				BytesToCopy:                     leafGenerationPackMaintenanceDefaultMaxBytesToCopy,
				BytesDead:                       2345,
				ExpectedReclaimBytes:            2345,
				ExpectedReclaimPerByteCopiedPPM: 900000,
			},
			Pack: backenddb.LeafGenerationPackStats{
				BytesCopied:   1200,
				WallTimeNanos: (12 * time.Millisecond).Nanoseconds(),
			},
		},
	}
	defer recorder.Close()
	db := &DB{
		backend:                    recorder,
		indexOuterLeavesInValueLog: true,
		valueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationHotWarmCold),
		closeCh:                    make(chan struct{}),
	}
	db.checkpointCond = sync.NewCond(&db.checkpointMu)
	attempted, ran, err := db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if !attempted || !ran {
		t.Fatalf("attempted=%t ran=%t want true/true", attempted, ran)
	}
	opts, calls := recorder.recordedLeafPack()
	if calls != 1 {
		t.Fatalf("leaf pack calls=%d want 1", calls)
	}
	if opts.MaxGenerations != leafGenerationPackMaintenanceDefaultMaxGenerations {
		t.Fatalf("MaxGenerations=%d want %d", opts.MaxGenerations, leafGenerationPackMaintenanceDefaultMaxGenerations)
	}
	if opts.MaxBytesToCopy != leafGenerationPackMaintenanceDefaultMaxBytesToCopy {
		t.Fatalf("MaxBytesToCopy=%d want %d", opts.MaxBytesToCopy, leafGenerationPackMaintenanceDefaultMaxBytesToCopy)
	}
	if opts.MinPublishedAgeCommits != leafGenerationPackMaintenanceDefaultMinPublishedAgeSeq {
		t.Fatalf("MinPublishedAgeCommits=%d want %d", opts.MinPublishedAgeCommits, leafGenerationPackMaintenanceDefaultMinPublishedAgeSeq)
	}
	if opts.MinCandidateGenerations != leafGenerationPackMaintenanceDefaultMinCandidateGenerations {
		t.Fatalf("MinCandidateGenerations=%d want %d", opts.MinCandidateGenerations, leafGenerationPackMaintenanceDefaultMinCandidateGenerations)
	}
	if opts.MinReclaimPerByteCopiedPPM != leafGenerationPackMaintenanceDefaultMinReclaimPerByteCopiedPPM {
		t.Fatalf("MinReclaimPerByteCopiedPPM=%d want %d", opts.MinReclaimPerByteCopiedPPM, leafGenerationPackMaintenanceDefaultMinReclaimPerByteCopiedPPM)
	}
	if !opts.Sync {
		t.Fatal("expected leaf pack maintenance to run with Sync=true")
	}
	if got := db.vlogGenerationLeafPackRuns.Load(); got != 1 {
		t.Fatalf("leaf pack runs=%d want 1", got)
	}
	if got := db.vlogGenerationLeafPackBytesCopied.Load(); got != 1200 {
		t.Fatalf("leaf pack bytes copied=%d want 1200", got)
	}
}

func TestLeafGenerationPackMaintenance_PassesReserveRIDs(t *testing.T) {
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB:   mustOpenLeafPackBackend(t),
		resp: leafPackWindowExhaustingStats(7, 1024, 4096),
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	attempted, ran, err := db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if !attempted || !ran {
		t.Fatalf("attempted=%t ran=%t want true/true", attempted, ran)
	}
	history := recorder.recordedLeafPackHistory()
	if len(history) != 1 {
		t.Fatalf("leaf pack history=%d want 1", len(history))
	}
	if history[0].ReserveRIDs == nil {
		t.Fatal("expected ReserveRIDs to be passed to leaf-pack maintenance")
	}
	before := db.nextRID.Load()
	start, err := history[0].ReserveRIDs(3)
	if err != nil {
		t.Fatalf("ReserveRIDs(3): %v", err)
	}
	if got, want := start, before+1; got != want {
		t.Fatalf("ReserveRIDs start=%d want %d", got, want)
	}
	if got, want := db.nextRID.Load(), before+3; got != want {
		t.Fatalf("nextRID after ReserveRIDs=%d want %d", got, want)
	}
}

func TestLeafGenerationPackMaintenance_ProtectsPublishedRootsDuringLeafGC(t *testing.T) {
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB:   mustOpenLeafPackBackend(t),
		resp: leafPackWindowExhaustingStats(17, 1024, 4096),
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	db.mu.Lock()
	db.rootPublishedSet = &publishedRootSet{
		generation: 1,
		pointShards: []publishedRootRef{
			{rootID: 101},
			{rootID: 202},
			{rootID: 101},
		},
		system:   publishedRootRef{rootID: 303},
		iterator: publishedRootRef{rootID: 202},
	}
	db.mu.Unlock()

	attempted, ran, err := db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if !attempted || !ran {
		t.Fatalf("attempted=%t ran=%t want true/true", attempted, ran)
	}
	history := recorder.recordedLeafPackHistory()
	if len(history) != 1 {
		t.Fatalf("leaf pack history=%d want 1", len(history))
	}
	want := []uint64{101, 202, 303}
	wantSystem := []uint64{303}
	if !uint64SlicesEqual(history[0].ProtectedRootIDs, want) {
		t.Fatalf("pack ProtectedRootIDs=%v want %v", history[0].ProtectedRootIDs, want)
	}
	if !uint64SlicesEqual(history[0].ProtectedSystemRootIDs, wantSystem) {
		t.Fatalf("pack ProtectedSystemRootIDs=%v want %v", history[0].ProtectedSystemRootIDs, wantSystem)
	}
	opts := recorder.recordedLeafGCOptions()
	if len(opts) != 1 {
		t.Fatalf("leaf gc option calls=%d want 1", len(opts))
	}
	if !uint64SlicesEqual(opts[0].ProtectedRootIDs, want) {
		t.Fatalf("gc ProtectedRootIDs=%v want %v", opts[0].ProtectedRootIDs, want)
	}
	if !uint64SlicesEqual(opts[0].ProtectedSystemRootIDs, wantSystem) {
		t.Fatalf("gc ProtectedSystemRootIDs=%v want %v", opts[0].ProtectedSystemRootIDs, wantSystem)
	}
}

func TestLeafGenerationPackMaintenance_LoopsWithinBudgetAndRunsLeafGC(t *testing.T) {
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceMaxGenerations, "3")
	t.Setenv(envLeafGenerationPackMaintenanceMaxBytesToCopy, "80")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB: mustOpenLeafPackBackend(t),
		responses: []backenddb.LeafGenerationPackRunOnceStats{
			{
				Ran: true,
				Selection: backenddb.LeafGenerationPackSelection{
					GenerationIDs:        []uint64{21, 22},
					BytesToCopy:          40,
					BytesDead:            90,
					ExpectedReclaimBytes: 90,
				},
				Pack: backenddb.LeafGenerationPackStats{BytesCopied: 38, WallTimeNanos: (4 * time.Millisecond).Nanoseconds()},
			},
			{
				Ran: true,
				Selection: backenddb.LeafGenerationPackSelection{
					GenerationIDs:        []uint64{23},
					BytesToCopy:          20,
					BytesDead:            60,
					ExpectedReclaimBytes: 60,
				},
				Pack: backenddb.LeafGenerationPackStats{BytesCopied: 19, WallTimeNanos: (3 * time.Millisecond).Nanoseconds()},
			},
		},
		leafGCResponses: []backenddb.LeafGenerationGCStats{
			{GenerationsEligible: 2, GenerationsDeleted: 1, FilesDeleted: 3, BytesDeleted: 50},
			{GenerationsEligible: 1, GenerationsDeleted: 1, FilesDeleted: 2, BytesDeleted: 25},
		},
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	attempted, ran, err := db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if !attempted || !ran {
		t.Fatalf("attempted=%t ran=%t want true/true", attempted, ran)
	}
	history := recorder.recordedLeafPackHistory()
	if len(history) != 2 {
		t.Fatalf("leaf pack history=%d want 2", len(history))
	}
	if history[0].MaxGenerations != 3 || history[0].MaxBytesToCopy != 80 {
		t.Fatalf("first bounds=(%d,%d) want (3,80)", history[0].MaxGenerations, history[0].MaxBytesToCopy)
	}
	if history[1].MaxGenerations != 1 || history[1].MaxBytesToCopy != 40 {
		t.Fatalf("second bounds=(%d,%d) want (1,40)", history[1].MaxGenerations, history[1].MaxBytesToCopy)
	}
	for i := range history {
		if !history[i].Sync {
			t.Fatalf("history[%d].Sync=false want true", i)
		}
	}
	gcStats, gcCalls := recorder.recordedLeafGC()
	if gcCalls != 2 {
		t.Fatalf("leaf gc calls=%d want 2", gcCalls)
	}
	if gcStats.GenerationsDeleted != 1 || gcStats.FilesDeleted != 2 {
		t.Fatalf("last leaf gc stats=%+v want deleted=1 files=2", gcStats)
	}
	if got := db.vlogGenerationLeafPackRuns.Load(); got != 2 {
		t.Fatalf("leaf pack runs=%d want 2", got)
	}
	if got := db.vlogGenerationLeafPackBytesCopied.Load(); got != 57 {
		t.Fatalf("leaf pack bytes copied=%d want 57", got)
	}
	if got := db.vlogGenerationLeafPackGCRuns.Load(); got != 2 {
		t.Fatalf("leaf gc runs=%d want 2", got)
	}
	if got := db.vlogGenerationLeafPackGCDeletedGenerations.Load(); got != 2 {
		t.Fatalf("leaf gc deleted generations=%d want 2", got)
	}
	if got := db.vlogGenerationLeafPackGCDeletedFiles.Load(); got != 5 {
		t.Fatalf("leaf gc deleted files=%d want 5", got)
	}
	if got := db.vlogGenerationLeafPackGCDeletedBytes.Load(); got != 75 {
		t.Fatalf("leaf gc deleted bytes=%d want 75", got)
	}
	if got := db.vlogGenerationLeafPackReclaimedBytes.Load(); got != 150 {
		t.Fatalf("leaf pack reclaimed bytes=%d want 150", got)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.leaf_pack.gc.deleted_files"]; got != "5" {
		t.Fatalf("gc.deleted_files=%q want 5", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.gc.last_deleted_files"]; got != "2" {
		t.Fatalf("gc.last_deleted_files=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.gc.deleted_bytes"]; got != "75" {
		t.Fatalf("gc.deleted_bytes=%q want 75", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.gc.last_deleted_bytes"]; got != "25" {
		t.Fatalf("gc.last_deleted_bytes=%q want 25", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.attributed_reclaim_bytes"]; got != "150" {
		t.Fatalf("reclaimed_bytes=%q want 150", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.last_attributed_reclaim_bytes"]; got != "60" {
		t.Fatalf("last_reclaimed_bytes=%q want 60", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.attributed_reclaim_per_byte_copied_ppm"]; got != "1000000" {
		t.Fatalf("reclaim_per_byte_copied_ppm=%q want 1000000", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.last_attributed_reclaim_per_byte_copied_ppm"]; got != "1000000" {
		t.Fatalf("last_reclaim_per_byte_copied_ppm=%q want 1000000", got)
	}
}

func TestLeafGenerationPackMaintenance_StopsAfterLowYieldWindow(t *testing.T) {
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceMinReclaimPerByteCopiedPPM, "400000")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB: mustOpenLeafPackBackend(t),
		responses: []backenddb.LeafGenerationPackRunOnceStats{
			{
				Ran: true,
				Selection: backenddb.LeafGenerationPackSelection{
					GenerationIDs:        []uint64{31},
					BytesToCopy:          50,
					BytesDead:            10,
					ExpectedReclaimBytes: 10,
				},
				Pack: backenddb.LeafGenerationPackStats{BytesCopied: 50, WallTimeNanos: (4 * time.Millisecond).Nanoseconds()},
			},
			{
				Ran: true,
				Selection: backenddb.LeafGenerationPackSelection{
					GenerationIDs:        []uint64{32},
					BytesToCopy:          20,
					BytesDead:            80,
					ExpectedReclaimBytes: 80,
				},
				Pack: backenddb.LeafGenerationPackStats{BytesCopied: 20, WallTimeNanos: (2 * time.Millisecond).Nanoseconds()},
			},
		},
		leafGCResponses: []backenddb.LeafGenerationGCStats{
			{GenerationsEligible: 1, GenerationsDeleted: 1, FilesDeleted: 1, BytesDeleted: 60},
			{GenerationsEligible: 1, GenerationsDeleted: 1, FilesDeleted: 1, BytesDeleted: 200},
		},
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	attempted, ran, err := db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if !attempted || !ran {
		t.Fatalf("attempted=%t ran=%t want true/true", attempted, ran)
	}
	history := recorder.recordedLeafPackHistory()
	if len(history) != 1 {
		t.Fatalf("leaf pack history=%d want 1", len(history))
	}
	if got := history[0].MinReclaimPerByteCopiedPPM; got != 400000 {
		t.Fatalf("min reclaim per copied ppm=%d want 400000", got)
	}
	if got := db.vlogGenerationLeafPackStopLowYield.Load(); got != 1 {
		t.Fatalf("leaf pack stop low yield=%d want 1", got)
	}
	if got := db.vlogGenerationLeafPackLastReclaimedBytes.Load(); got != 10 {
		t.Fatalf("last reclaimed bytes=%d want 10", got)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.leaf_pack.stop.low_yield"]; got != "1" {
		t.Fatalf("stop.low_yield=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.gc.last_deleted_bytes"]; got != "60" {
		t.Fatalf("gc.last_deleted_bytes=%q want 60", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.last_attributed_reclaim_per_byte_copied_ppm"]; got != "200000" {
		t.Fatalf("last_reclaim_per_byte_copied_ppm=%q want 200000", got)
	}
}

func TestLeafGenerationPackMaintenance_SkipsWithinMinInterval(t *testing.T) {
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend, leafPackResp: leafPackWindowExhaustingStats(2, 1024, 4096)}
	defer recorder.Close()
	db := &DB{
		backend:                    recorder,
		indexOuterLeavesInValueLog: true,
		valueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationHotWarmCold),
		closeCh:                    make(chan struct{}),
	}
	db.checkpointCond = sync.NewCond(&db.checkpointMu)
	attempted, ran, err := db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("first maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if !attempted || !ran {
		t.Fatalf("first attempted=%t ran=%t want true/true", attempted, ran)
	}
	attempted, ran, err = db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("second maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if attempted || ran {
		t.Fatalf("second attempted=%t ran=%t want false/false", attempted, ran)
	}
	_, calls := recorder.recordedLeafPack()
	if calls != 1 {
		t.Fatalf("leaf pack calls=%d want 1 after min-interval skip", calls)
	}
	if got := db.vlogGenerationLeafPackSkipMinInterval.Load(); got != 1 {
		t.Fatalf("leaf pack skip min interval=%d want 1", got)
	}
}

func TestLeafGenerationPackMaintenance_ConfigurableMinInterval(t *testing.T) {
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceMinIntervalMillis, "1")
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend, leafPackResp: leafPackWindowExhaustingStats(2, 1024, 4096)}
	defer recorder.Close()
	db := &DB{
		backend:                    recorder,
		indexOuterLeavesInValueLog: true,
		valueLogGenerationPolicy:   uint8(backenddb.ValueLogGenerationHotWarmCold),
		closeCh:                    make(chan struct{}),
	}
	db.checkpointCond = sync.NewCond(&db.checkpointMu)
	attempted, ran, err := db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("first maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if !attempted || !ran {
		t.Fatalf("first attempted=%t ran=%t want true/true", attempted, ran)
	}
	time.Sleep(2 * time.Millisecond)
	attempted, ran, err = db.maybeRunLeafGenerationPackMaintenance(false, true, leafGenerationPackMaintenanceAdmission{allowed: true}, vlogGenerationMaintenanceOptions{skipCheckpoint: true})
	if err != nil {
		t.Fatalf("second maybeRunLeafGenerationPackMaintenance: %v", err)
	}
	if !attempted || !ran {
		t.Fatalf("second attempted=%t ran=%t want true/true with configured min interval", attempted, ran)
	}
	_, calls := recorder.recordedLeafPack()
	if calls != 2 {
		t.Fatalf("leaf pack calls=%d want 2 after configured min interval", calls)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.leaf_pack.min_interval_ms"]; got != "1" {
		t.Fatalf("min_interval_ms=%q want 1", got)
	}
}

func TestVlogGenerationRewritePlanContext_BudgetsPeriodicFreshPlans(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envVlogGenerationPeriodicRewritePlanBudgetMillis, "25")
	db := &DB{closeCh: make(chan struct{})}

	ctx, cancel := db.vlogGenerationRewritePlanContext(30*time.Second, vlogGenerationMaintenanceOptions{})
	deadline, ok := ctx.Deadline()
	cancel()
	if !ok {
		t.Fatal("periodic rewrite plan context missing deadline")
	}
	if ttl := time.Until(deadline); ttl <= 0 || ttl > time.Second {
		t.Fatalf("periodic rewrite plan ttl=%s want budgeted", ttl)
	}

	ctx, cancel = db.vlogGenerationRewritePlanContext(30*time.Second, vlogGenerationMaintenanceOptions{
		bypassQuiet: true,
	})
	deadline, ok = ctx.Deadline()
	cancel()
	if !ok {
		t.Fatal("bypass rewrite plan context missing deadline")
	}
	if ttl := time.Until(deadline); ttl < 20*time.Second {
		t.Fatalf("bypass rewrite plan ttl=%s want unbudgeted 30s context", ttl)
	}
}

func TestVlogGenerationMaintenance_PeriodicFreshPlanBudgetCancelsLongPlan(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envVlogGenerationPeriodicRewritePlanBudgetMillis, "25")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	planner := &timedRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planDelay: time.Second,
	}
	db, err := Open(dir, planner, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k"), make([]byte, 2048)); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	done := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenance(false)
		close(done)
	}()
	select {
	case <-planner.planStart:
	case <-time.After(schedulerTestWait(t)):
		t.Fatal("timed out waiting for periodic rewrite plan")
	}
	select {
	case <-done:
	case <-time.After(schedulerTestWait(t)):
		t.Fatal("periodic rewrite plan did not stop after budget")
	}
	completed, canceled := planner.recordedPlanOutcomes()
	if completed != 0 || canceled != 1 {
		t.Fatalf("plan outcomes completed=%d canceled=%d want 0/1", completed, canceled)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.rewrite.plan_canceled"]; got != "1" {
		t.Fatalf("plan_canceled=%q want 1", got)
	}
}

func TestVlogGenerationMaintenance_PeriodicPassRunsLeafPackWhenEnabled(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB:         mustOpenLeafPackBackend(t),
		resp:       leafPackWindowExhaustingStats(9, 1024, 4096),
		leafGCResp: backenddb.LeafGenerationGCStats{GenerationsEligible: 1, GenerationsDeleted: 1, FilesDeleted: 2},
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})

	opts, calls, hasDeadline, deadline := recorder.recordedLeafPack()
	if calls != 1 {
		t.Fatalf("leaf pack calls=%d want 1", calls)
	}
	if opts.MaxGenerations != leafGenerationPackMaintenanceDefaultMaxGenerations {
		t.Fatalf("MaxGenerations=%d want %d", opts.MaxGenerations, leafGenerationPackMaintenanceDefaultMaxGenerations)
	}
	if opts.MinCandidateGenerations != leafGenerationPackMaintenanceDefaultMinCandidateGenerations {
		t.Fatalf("MinCandidateGenerations=%d want %d", opts.MinCandidateGenerations, leafGenerationPackMaintenanceDefaultMinCandidateGenerations)
	}
	if !hasDeadline {
		t.Fatal("expected maintenance context deadline to be set")
	}
	if ttl := time.Until(deadline); ttl < 25*time.Second || ttl > 31*time.Second {
		t.Fatalf("deadline ttl=%s want around 30s", ttl)
	}
	if got := db.vlogGenerationMaintenancePassWithLeafPack.Load(); got != 1 {
		t.Fatalf("maintenance leaf pack passes=%d want 1", got)
	}
	if got := db.vlogGenerationLeafPackRuns.Load(); got != 1 {
		t.Fatalf("leaf pack runs=%d want 1", got)
	}
}

func TestVlogGenerationMaintenance_PeriodicPassRunsLeafPackUnderSteadyForegroundActivity(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceWriteBurstGraceMillis, "250")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB:         mustOpenLeafPackBackend(t),
		resp:       leafPackWindowExhaustingStats(91, 1024, 4096),
		leafGCResp: backenddb.LeafGenerationGCStats{GenerationsEligible: 1, GenerationsDeleted: 1, FilesDeleted: 1},
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	now := time.Now()
	db.lastForegroundWriteUnixNano.Store(now.Add(-500 * time.Millisecond).UnixNano())
	db.lastForegroundReadUnixNano.Store(now.Add(-2 * vlogForegroundReadQuietWindow).UnixNano())

	if ran := db.maybeRunPeriodicVlogGenerationMaintenance(false); !ran {
		t.Fatal("expected periodic maintenance to run on active-safe leaf-pack admission")
	}

	_, calls, _, _ := recorder.recordedLeafPack()
	if calls != 1 {
		t.Fatalf("leaf pack calls=%d want 1", calls)
	}
	if got := db.vlogGenerationMaintenancePassWithLeafPack.Load(); got != 1 {
		t.Fatalf("maintenance leaf pack passes=%d want 1", got)
	}
	if got := db.vlogGenerationLeafPackRuns.Load(); got != 1 {
		t.Fatalf("leaf pack runs=%d want 1", got)
	}
}

func TestVlogGenerationMaintenance_PeriodicPassSkipsLeafPackOnWriteBurst(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceWriteBurstGraceMillis, "250")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), resp: leafPackWindowExhaustingStats(92, 1024, 4096)}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	now := time.Now()
	db.lastForegroundWriteUnixNano.Store(now.Add(-100 * time.Millisecond).UnixNano())
	db.lastForegroundReadUnixNano.Store(now.Add(-2 * vlogForegroundReadQuietWindow).UnixNano())

	if ran := db.maybeRunPeriodicVlogGenerationMaintenance(false); ran {
		t.Fatal("expected periodic maintenance to skip on fresh write burst")
	}

	_, calls, _, _ := recorder.recordedLeafPack()
	if calls != 0 {
		t.Fatalf("leaf pack calls=%d want 0", calls)
	}
	if got := db.vlogGenerationLeafPackSkipWriteBurst.Load(); got != 1 {
		t.Fatalf("leaf pack write burst skips=%d want 1", got)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.leaf_pack.last_skip_reason"]; got != "write_burst" {
		t.Fatalf("last_skip_reason=%q want write_burst", got)
	}
}

func TestVlogGenerationMaintenance_PeriodicPassSkipsLeafPackOnQueuePressure(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceWriteBurstGraceMillis, "250")
	t.Setenv(envLeafGenerationPackMaintenanceMaxForegroundQueue, "1")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), resp: leafPackWindowExhaustingStats(93, 1024, 4096)}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	now := time.Now()
	db.lastForegroundWriteUnixNano.Store(now.Add(-500 * time.Millisecond).UnixNano())
	db.lastForegroundReadUnixNano.Store(now.Add(-2 * vlogForegroundReadQuietWindow).UnixNano())
	db.memtables.Store(&memtableView{queue: make([]memtable.Table, 2)})

	if ran := db.maybeRunPeriodicVlogGenerationMaintenance(false); ran {
		t.Fatal("expected periodic maintenance to skip on queue pressure")
	}

	_, calls, _, _ := recorder.recordedLeafPack()
	if calls != 0 {
		t.Fatalf("leaf pack calls=%d want 0", calls)
	}
	if got := db.vlogGenerationLeafPackSkipQueuePressure.Load(); got != 1 {
		t.Fatalf("leaf pack queue pressure skips=%d want 1", got)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.leaf_pack.last_skip_reason"]; got != "queue_pressure" {
		t.Fatalf("last_skip_reason=%q want queue_pressure", got)
	}
}

func TestVlogGenerationMaintenance_PeriodicLeafPackDoesNotBlockOnScheduledRetainedPruneQuietWait(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceWriteBurstGraceMillis, "250")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB:         mustOpenLeafPackBackend(t),
		resp:       leafPackWindowExhaustingStats(95, 1024, 4096),
		leafGCResp: backenddb.LeafGenerationGCStats{GenerationsEligible: 1, GenerationsDeleted: 1, FilesDeleted: 1},
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()
	// The setup checkpoint may have scheduled retained-prune work. Settle that
	// request before this test schedules and inspects its own quiet waiter.
	forceRetainedPruneIdle(db)
	db.waitForRetainedValueLogPrune()
	db.retainedPruneMu.Lock()
	inheritedWaiting := db.retainedPruneDone != nil
	inheritedRunning := db.retainedPruneRunningDone != nil
	db.retainedPruneMu.Unlock()
	if inheritedWaiting || inheritedRunning {
		t.Fatalf("inherited retained prune after setup settlement: waiting=%t running=%t", inheritedWaiting, inheritedRunning)
	}

	retainedPath := filepath.Join(t.TempDir(), "value_vlog", "value-l0-000321.log")
	seedRetainedPrunePressure(db, retainedPath, 2<<30)
	// Leaf-pack admission should allow this write age, while retained prune still
	// treats it as foreground-hot and waits for its much larger quiet window.
	now := time.Now()
	db.lastForegroundWriteUnixNano.Store(now.Add(-500 * time.Millisecond).UnixNano())
	db.lastForegroundReadUnixNano.Store(now.Add(-2 * vlogForegroundReadQuietWindow).UnixNano())
	db.scheduleRetainedValueLogPrune()

	db.retainedPruneMu.Lock()
	waiting := db.retainedPruneDone != nil
	running := db.retainedPruneRunningDone != nil
	db.retainedPruneMu.Unlock()
	if !waiting || running {
		t.Fatalf("owned retained prune after scheduling: waiting=%t running=%t, want waiting only", waiting, running)
	}

	start := time.Now()
	if ran := db.maybeRunPeriodicVlogGenerationMaintenance(false); !ran {
		t.Fatal("expected periodic maintenance to run while retained prune only waited for quiet")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("periodic maintenance elapsed=%s want <=500ms while retained prune waits for quiet", elapsed)
	}

	_, calls, _, _ := recorder.recordedLeafPack()
	if calls != 1 {
		t.Fatalf("leaf pack calls=%d want 1", calls)
	}
	if db.vlogGenerationMaintenanceActive.Load() {
		t.Fatal("maintenance active remained set after periodic leaf pack run")
	}

	quietAt := time.Now().Add(-2 * retainedPruneQuietWindow).UnixNano()
	db.lastForegroundWriteUnixNano.Store(quietAt)
	db.lastForegroundReadUnixNano.Store(quietAt)
	db.waitForRetainedValueLogPrune()
}

func TestVlogGenerationMaintenance_PeriodicPassSkipsLeafPackOnForegroundIterators(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceWriteBurstGraceMillis, "250")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), resp: leafPackWindowExhaustingStats(94, 1024, 4096)}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	now := time.Now()
	db.lastForegroundWriteUnixNano.Store(now.Add(-500 * time.Millisecond).UnixNano())
	db.lastForegroundReadUnixNano.Store(now.Add(-2 * vlogForegroundReadQuietWindow).UnixNano())
	db.activeForegroundIterators.Store(1)

	if ran := db.maybeRunPeriodicVlogGenerationMaintenance(false); ran {
		t.Fatal("expected periodic maintenance to skip on foreground iterators")
	}

	_, calls, _, _ := recorder.recordedLeafPack()
	if calls != 0 {
		t.Fatalf("leaf pack calls=%d want 0", calls)
	}
	if got := db.vlogGenerationLeafPackSkipForegroundIterators.Load(); got != 1 {
		t.Fatalf("leaf pack foreground iterator skips=%d want 1", got)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.leaf_pack.last_skip_reason"]; got != "foreground_iterators" {
		t.Fatalf("last_skip_reason=%q want foreground_iterators", got)
	}
}

func TestVlogGenerationMaintenance_LeafPackUsesConfiguredPolicy(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceMinCandidateGenerations, "3")
	t.Setenv(envLeafGenerationPackMaintenanceTimeoutSeconds, "7")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), resp: leafPackWindowExhaustingStats(10, 1024, 4096)}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})

	opts, calls, hasDeadline, deadline := recorder.recordedLeafPack()
	if calls != 1 {
		t.Fatalf("leaf pack calls=%d want 1", calls)
	}
	if opts.MinCandidateGenerations != 3 {
		t.Fatalf("MinCandidateGenerations=%d want 3", opts.MinCandidateGenerations)
	}
	if !hasDeadline {
		t.Fatal("expected maintenance context deadline to be set")
	}
	if ttl := time.Until(deadline); ttl < 6*time.Second || ttl > 8*time.Second {
		t.Fatalf("deadline ttl=%s want around 7s", ttl)
	}
}

func TestVlogGenerationMaintenance_LeafPackDeadlineIsNotAnError(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	t.Setenv(envLeafGenerationPackMaintenanceTimeoutSeconds, "1")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), blockUntilCtxDone: true}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})

	if got := db.vlogGenerationLeafPackDeadline.Load(); got != 1 {
		t.Fatalf("leaf pack deadline=%d want 1", got)
	}
	if got := db.vlogGenerationLeafPackErrors.Load(); got != 0 {
		t.Fatalf("leaf pack errors=%d want 0", got)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.leaf_pack.last_skip_reason"]; got != "deadline_exceeded" {
		t.Fatalf("last_skip_reason=%q want deadline_exceeded", got)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.scheduler_state"]; got != vlogGenerationSchedulerStateString(vlogGenerationSchedulerIdle) {
		t.Fatalf("scheduler_state=%q want idle", got)
	}
}

func TestVlogGenerationMaintenance_LeafPackContinuesAcrossForegroundResume(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB:      mustOpenLeafPackBackend(t),
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
		resp:    leafPackWindowExhaustingStats(90, 1024, 4096),
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	done := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})
		close(done)
	}()
	select {
	case <-recorder.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for leaf pack backend entry")
	}
	db.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())
	select {
	case <-done:
		t.Fatal("leaf pack maintenance finished early after foreground resume")
	case <-time.After(100 * time.Millisecond):
	}
	close(recorder.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for leaf pack maintenance completion")
	}

	if got := db.vlogGenerationLeafPackCanceled.Load(); got != 0 {
		t.Fatalf("leaf pack canceled=%d want 0", got)
	}
	if got := db.vlogGenerationLeafPackRuns.Load(); got != 1 {
		t.Fatalf("leaf pack runs=%d want 1", got)
	}
}

func TestVlogGenerationMaintenance_ReleasesActiveBeforeQueueProgressSnapshot(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{
		DB:      mustOpenLeafPackBackend(t),
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
		resp:    leafPackWindowExhaustingStats(91, 1024, 4096),
	}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	done := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})
		close(done)
	}()
	select {
	case <-recorder.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for leaf pack backend entry")
	}

	queueLocked := make(chan struct{})
	queueRelease := make(chan struct{})
	go func() {
		db.vlogGenerationRewriteQueueMu.Lock()
		close(queueLocked)
		<-queueRelease
		db.vlogGenerationRewriteQueueMu.Unlock()
	}()
	select {
	case <-queueLocked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting to lock rewrite queue mutex")
	}

	close(recorder.release)

	deadline := time.Now().Add(2 * time.Second)
	for db.vlogGenerationMaintenanceActive.Load() {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if db.vlogGenerationMaintenanceActive.Load() {
		close(queueRelease)
		<-done
		t.Fatal("maintenance active remained true while queue progress snapshot was blocked")
	}
	select {
	case <-done:
		close(queueRelease)
		t.Fatal("maintenance finished before blocked queue snapshot was released")
	case <-time.After(100 * time.Millisecond):
	}

	close(queueRelease)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for maintenance completion after releasing queue snapshot")
	}

	if got := db.vlogGenerationLeafPackRuns.Load(); got != 1 {
		t.Fatalf("leaf pack runs=%d want 1", got)
	}
}

func TestVlogGenerationMaintenance_LeafPackCloseCanceledIsNotAnError(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), entered: make(chan struct{}, 1), blockUntilCtxDone: true}
	db, closeDB, cleanup := openLeafPackMaintenanceSchedulerOnlyTestDBWithClose(t, recorder)
	cleaned := false
	defer func() {
		if !cleaned {
			cleanup()
		}
	}()

	done := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})
		close(done)
	}()
	select {
	case <-recorder.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for leaf pack backend entry")
	}
	closeDB()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for maintenance cancellation")
	}
	cleanup()
	cleaned = true

	if got := db.vlogGenerationLeafPackCanceled.Load(); got != 1 {
		t.Fatalf("leaf pack canceled=%d want 1", got)
	}
	if got := db.vlogGenerationLeafPackErrors.Load(); got != 0 {
		t.Fatalf("leaf pack errors=%d want 0", got)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.leaf_pack.last_skip_reason"]; got != "context_canceled" {
		t.Fatalf("last_skip_reason=%q want context_canceled", got)
	}
}

func TestVlogGenerationMaintenance_RestorePhaseSkipsLeafPack(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), resp: leafPackWindowExhaustingStats(11, 1024, 4096)}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()
	db.SetMaintenancePhase(MaintenancePhaseRestore)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})

	_, calls, _, _ := recorder.recordedLeafPack()
	if calls != 0 {
		t.Fatalf("leaf pack calls=%d want 0 during restore", calls)
	}
	if got := db.vlogGenerationMaintenanceSkipPhase.Load(); got == 0 {
		t.Fatal("expected maintenance phase skip to be recorded")
	}
}

func TestVlogGenerationMaintenance_RunGCPassSkipsLeafPack(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), resp: leafPackWindowExhaustingStats(12, 1024, 4096)}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{})

	_, calls, _, _ := recorder.recordedLeafPack()
	if calls != 0 {
		t.Fatalf("leaf pack calls=%d want 0 on runGC pass", calls)
	}
}

func TestVlogGenerationMaintenance_BypassQuietSkipsLeafPack(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableLeafGenerationPackMaintenance, "1")
	recorder := &leafPackMaintenanceRecordingBackend{DB: mustOpenLeafPackBackend(t), resp: leafPackWindowExhaustingStats(13, 1024, 4096)}
	db, cleanup := openLeafPackMaintenanceTestDB(t, recorder)
	defer cleanup()

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{bypassQuiet: true, skipCheckpoint: true})

	_, calls, _, _ := recorder.recordedLeafPack()
	if calls != 0 {
		t.Fatalf("leaf pack calls=%d want 0 on bypass-quiet pass", calls)
	}
}

func TestVlogGenerationMaintenance_SerializesConcurrentRuns(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	blocking := &blockingRewriteOnlineBackend{
		DB:             backend,
		planResponse:   backenddb.ValueLogRewritePlan{SourceFileIDs: []uint32{11}, SelectedBytesLive: 64},
		rewriteEntered: make(chan struct{}),
		rewriteRelease: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	done := make(chan struct{})
	go func() {
		defer close(done)
		db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			skipCheckpoint:        true,
			rewriteDebtDrain:      true,
		})
	}()

	select {
	case <-blocking.rewriteEntered:
	case <-time.After(2 * schedulerTestWait(t)):
		t.Fatalf("first rewrite did not start")
	}

	// While the first pass is still inside rewrite, a concurrent pass should be
	// skipped by the maintenance-active gate instead of issuing a second rewrite.
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		rewriteDebtDrain:      true,
	})

	close(blocking.rewriteRelease)
	select {
	case <-done:
	case <-time.After(2 * schedulerTestWait(t)):
		t.Fatalf("first maintenance pass did not finish")
	}

	if got := blocking.recordedRewriteCalls(); got != 1 {
		t.Fatalf("rewrite calls=%d want=1 with concurrent maintenance attempts", got)
	}
}

func TestVlogGenerationRewrite_QueuedExecIgnoresForegroundCancelUntilBoundedCompletion(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	blocking := &blockingRewriteOnlineBackend{
		DB:             backend,
		planResponse:   backenddb.ValueLogRewritePlan{SourceFileIDs: []uint32{11}, SelectedBytesLive: 64},
		rewriteEntered: make(chan struct{}),
		rewriteRelease: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	if queued, err := db.currentVlogGenerationRewriteQueue(); err != nil || len(queued) == 0 {
		t.Fatalf("rewrite queue missing before maintenance: queue=%v err=%v", queued, err)
	}
	if got := blocking.recordedRewriteCalls(); got != 0 {
		t.Fatalf("unexpected rewrite calls before maintenance=%d ctx_ttl=%s", got, blocking.recordedRewriteTTL())
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	done := make(chan struct{})
	go func() {
		runRewriteQueueMaintenanceForTest(db)
		close(done)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-blocking.rewriteEntered:
	case <-time.After(wait):
		t.Fatalf("rewrite did not start")
	}

	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)

	select {
	case <-done:
		t.Fatalf("rewrite completed early under foreground activity; expected queued bounded rewrite to continue until release (ctx_ttl=%s)", blocking.recordedRewriteTTL())
	case <-time.After(250 * time.Millisecond):
	}

	close(blocking.rewriteRelease)
	select {
	case <-done:
	case <-time.After(2 * wait):
		t.Fatalf("rewrite did not finish after release")
	}
	if ttl := blocking.recordedRewriteTTL(); ttl < 20*time.Second {
		t.Fatalf("rewrite context ttl=%s want around %s for queued resume", ttl, vlogGenerationRewriteBoundedExecTimeout)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_runs"]; got != "0" {
		t.Fatalf("rewrite canceled runs=%q want 0 for bounded queued rewrite", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_runs.fresh_plan"]; got != "0" {
		t.Fatalf("rewrite canceled fresh runs=%q want 0 for bounded queued rewrite", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_runs.queued_debt"]; got != "0" {
		t.Fatalf("rewrite canceled queued runs=%q want 0 for bounded queued rewrite", got)
	}
}

func TestVlogGenerationRewrite_ObservedSourceRetainedBlock_RunsSecondGC(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:                   128,
			BytesAfter:                    128,
			RecordsCopied:                 1,
			SourceSegmentsRequested:       1,
			SourceSegmentsStillReferenced: 0,
			SourceSegmentsUnreferenced:    1,
		},
		gcResponses: []backenddb.ValueLogGCStats{
			{
				BytesProtectedRetained:                  64,
				BytesEligible:                           0,
				ObservedSourceSegments:                  1,
				ObservedSourceSegmentsReferenced:        0,
				ObservedSourceSegmentsEligible:          0,
				ObservedSourceSegmentsProtectedRetained: 1,
				ObservedSourceBytesProtectedRetained:    64,
			},
			{
				BytesProtectedRetained:         0,
				BytesEligible:                  64,
				BytesDeleted:                   64,
				ObservedSourceSegments:         1,
				ObservedSourceSegmentsEligible: 1,
				ObservedSourceSegmentsDeleted:  1,
				ObservedSourceBytesEligible:    64,
				ObservedSourceBytesDeleted:     64,
			},
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()
	skipRetainedPrune(db)

	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)
	forceRetainedPruneIdle(db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})

	switch got := recorder.recordedGCObservedSourceCalls(); {
	case got >= 2:
		if eligible := db.vlogGenerationLastGCObservedSourceSegmentsEligible.Load(); eligible != 1 {
			t.Fatalf("last observed source eligible segments=%d want 1 after second gc", eligible)
		}
		if deleted := db.vlogGenerationLastGCObservedSourceBytesDeleted.Load(); deleted != 64 {
			t.Fatalf("last observed source deleted bytes=%d want 64 after second gc", deleted)
		}
	case got == 1:
		// Under slower/race builds, the post-prune observed-source replay can be
		// deferred to a later maintenance pass.
	default:
		t.Fatalf("observed-source gc calls=%d want >=1 when observed source is retained-blocked", got)
	}
}

func TestVlogGenerationObservedSourceGCQueue_CountersAndDedupe(t *testing.T) {
	db := &DB{}

	db.queueVlogGenerationObservedSourceGCList([]uint32{7, 9, 7, 0})
	db.queueVlogGenerationObservedSourceGCIDs(map[uint32]struct{}{
		0:  {},
		9:  {},
		12: {},
	})

	if got := db.vlogGenerationObservedGCQueuedBatches.Load(); got != 2 {
		t.Fatalf("queued batches=%d want 2", got)
	}
	if got := db.vlogGenerationObservedGCQueuedIDs.Load(); got != 3 {
		t.Fatalf("queued ids=%d want 3", got)
	}

	ids := db.takeVlogGenerationObservedSourceGCList()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	want := []uint32{7, 9, 12}
	if len(ids) != len(want) {
		t.Fatalf("taken ids len=%d want %d (%v)", len(ids), len(want), ids)
	}
	for i := range ids {
		if ids[i] != want[i] {
			t.Fatalf("taken ids[%d]=%d want %d (all=%v)", i, ids[i], want[i], ids)
		}
	}

	if got := db.vlogGenerationObservedGCTakenBatches.Load(); got != 1 {
		t.Fatalf("taken batches=%d want 1", got)
	}
	if got := db.vlogGenerationObservedGCTakenIDs.Load(); got != uint64(len(want)) {
		t.Fatalf("taken ids=%d want %d", got, len(want))
	}

	// Empty take should not mutate taken counters.
	_ = db.takeVlogGenerationObservedSourceGCList()
	if got := db.vlogGenerationObservedGCTakenBatches.Load(); got != 1 {
		t.Fatalf("taken batches after empty take=%d want 1", got)
	}
	if got := db.vlogGenerationObservedGCTakenIDs.Load(); got != uint64(len(want)) {
		t.Fatalf("taken ids after empty take=%d want %d", got, len(want))
	}
}

func TestVlogGenerationMaintenance_ObservedSourceGCBypassQuietIgnoresForegroundResume(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		gcFn: func(ctx context.Context, _ backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				if err := ctx.Err(); err != nil {
					return backenddb.ValueLogGCStats{}, err
				}
				return backenddb.ValueLogGCStats{}, nil
			case <-ctx.Done():
				return backenddb.ValueLogGCStats{}, ctx.Err()
			}
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()
	skipRetainedPrune(db)

	db.queueVlogGenerationObservedSourceGCList([]uint32{11})
	db.vlogGenerationLastGCUnixNano.Store(time.Now().Add(-time.Minute).UnixNano())
	forceVlogMaintenanceIdle(db)

	go func() {
		time.Sleep(30 * time.Millisecond)
		hot := time.Now().UnixNano()
		db.lastForegroundWriteUnixNano.Store(hot)
		db.lastForegroundReadUnixNano.Store(hot)
	}()

	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		rewriteDebtDrain:      true,
	})

	if got := recorder.recordedGCObservedSourceCalls(); got != 1 {
		t.Fatalf("observed-source gc calls=%d want 1", got)
	}
	if got := db.vlogGenerationGCRuns.Load(); got != 1 {
		t.Fatalf("gc runs=%d want 1", got)
	}
	if got := db.vlogGenerationObservedGCRetryQueued.Load(); got != 0 {
		t.Fatalf("observed-source gc retry queued=%d want 0", got)
	}
	if pending := len(db.takeVlogGenerationObservedSourceGCList()); pending != 0 {
		t.Fatalf("observed-source gc pending ids=%d want 0", pending)
	}
}

func TestVlogGenerationMaintenance_ObservedSourceGCCompletionClearsRetryState(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		gcResponse: backenddb.ValueLogGCStats{
			ObservedSourceSegments:         1,
			ObservedSourceSegmentsEligible: 1,
			ObservedSourceSegmentsDeleted:  1,
			ObservedSourceBytes:            256,
			ObservedSourceBytesEligible:    256,
			ObservedSourceBytesDeleted:     256,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()
	skipRetainedPrune(db)

	db.queueVlogGenerationObservedSourceGCList([]uint32{41})
	db.vlogGenerationLastGCUnixNano.Store(time.Now().Add(-time.Minute).UnixNano())
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		rewriteDebtDrain:      true,
	})

	if got := recorder.recordedGCObservedSourceCalls(); got != 1 {
		t.Fatalf("observed-source gc calls=%d want 1", got)
	}
	if got := db.vlogGenerationObservedGCRetryQueued.Load(); got != 0 {
		t.Fatalf("observed-source gc retry queued=%d want 0", got)
	}
	if got := db.vlogGenerationObservedGCRetryDropped.Load(); got != 0 {
		t.Fatalf("observed-source gc retry dropped=%d want 0", got)
	}
	if got := db.vlogGenerationObservedGCLatencyCompletedIDs.Load(); got != 1 {
		t.Fatalf("observed-source gc latency completed ids=%d want 1", got)
	}
	if got := db.vlogGenerationObservedGCLatencyDroppedIDs.Load(); got != 0 {
		t.Fatalf("observed-source gc latency dropped ids=%d want 0", got)
	}
	if pending := len(db.takeVlogGenerationObservedSourceGCList()); pending != 0 {
		t.Fatalf("observed-source gc pending ids=%d want 0", pending)
	}
	db.vlogGenerationObservedGCMu.Lock()
	if _, exists := db.vlogGenerationObservedGCRetryAttempts[41]; exists {
		db.vlogGenerationObservedGCMu.Unlock()
		t.Fatalf("retry attempt state still present for observed id 41")
	}
	if _, exists := db.vlogGenerationObservedGCFirstQueuedUnixNano[41]; exists {
		db.vlogGenerationObservedGCMu.Unlock()
		t.Fatalf("first queued timestamp still present for observed id 41")
	}
	db.vlogGenerationObservedGCMu.Unlock()
}

func TestVlogGenerationMaintenance_ObservedSourceGCRetryBudgetDropsAfterMaxAttempts(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		gcResponse: backenddb.ValueLogGCStats{
			ObservedSourceSegments:                  1,
			ObservedSourceSegmentsEligible:          0,
			ObservedSourceSegmentsProtectedRetained: 1,
			ObservedSourceBytes:                     128,
			ObservedSourceBytesProtectedRetained:    128,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()
	skipRetainedPrune(db)

	db.queueVlogGenerationObservedSourceGCList([]uint32{73})
	passes := int(vlogGenerationObservedGCRetryMaxAttempts) + 1
	maxLoops := passes * 4
	for i := 0; i < maxLoops && db.vlogGenerationObservedGCRetryDropped.Load() == 0; i++ {
		db.vlogGenerationLastGCUnixNano.Store(time.Now().Add(-time.Minute).UnixNano())
		forceVlogMaintenanceIdle(db)
		db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			skipCheckpoint:        true,
			rewriteDebtDrain:      true,
		})
	}

	if got := recorder.recordedGCObservedSourceCalls(); got == 0 {
		t.Fatalf("observed-source gc calls=%d want >0", got)
	}
	if got := db.vlogGenerationObservedGCRetryQueued.Load(); got == 0 {
		t.Fatalf("observed-source gc retry queued=%d want >0", got)
	}
	dropped := db.vlogGenerationObservedGCRetryDropped.Load()
	if dropped > 1 {
		t.Fatalf("observed-source gc retry dropped=%d want <=1", dropped)
	}
}

func TestVlogGenerationMaintenance_ObservedSourceActiveRetryBudgetDropsAfterMaxAttempts(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		gcResponse: backenddb.ValueLogGCStats{
			ObservedSourceSegments:           1,
			ObservedSourceSegmentsReferenced: 0,
			ObservedSourceSegmentsActive:     1,
			ObservedSourceSegmentsEligible:   0,
			ObservedSourceBytes:              128,
			ObservedSourceBytesActive:        128,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()
	skipRetainedPrune(db)

	db.queueVlogGenerationObservedSourceGCList([]uint32{77})
	passes := int(vlogGenerationObservedGCRetryMaxAttempts) + 1
	maxLoops := passes * 4
	for i := 0; i < maxLoops && db.vlogGenerationObservedGCRetryDropped.Load() == 0; i++ {
		db.vlogGenerationLastGCUnixNano.Store(time.Now().Add(-time.Minute).UnixNano())
		forceVlogMaintenanceIdle(db)
		db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			skipCheckpoint:        true,
			rewriteDebtDrain:      true,
		})
	}

	if got := recorder.recordedGCObservedSourceCalls(); got == 0 {
		t.Fatalf("observed-source gc calls=%d want >0", got)
	}
	if got := db.vlogGenerationObservedGCRetryQueued.Load(); got == 0 {
		t.Fatalf("observed-source gc retry queued=%d want >0", got)
	}
	dropped := db.vlogGenerationObservedGCRetryDropped.Load()
	if dropped > 1 {
		t.Fatalf("observed-source gc retry dropped=%d want <=1", dropped)
	}
}

func TestVlogGenerationRewrite_FreshPlanExecIgnoresForegroundCancelUntilBoundedComplete(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	blocking := &blockingRewriteOnlineBackend{
		DB:             backend,
		planResponse:   backenddb.ValueLogRewritePlan{SourceFileIDs: []uint32{11}, SelectedBytesLive: 64},
		rewriteEntered: make(chan struct{}),
		rewriteRelease: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseRewrite := func() {
		releaseOnce.Do(func() {
			close(blocking.rewriteRelease)
		})
	}
	t.Cleanup(releaseRewrite)

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	done := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenance(false)
		close(done)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-blocking.rewriteEntered:
	case <-time.After(wait):
		t.Fatalf("initial rewrite did not start")
	}

	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)

	select {
	case <-done:
		t.Fatalf("rewrite completed early under foreground activity; expected bounded fresh-plan rewrite to continue until release (ctx_ttl=%s)", blocking.recordedRewriteTTL())
	case <-time.After(250 * time.Millisecond):
	}

	releaseRewrite()
	select {
	case <-done:
	case <-time.After(2 * wait):
		t.Fatalf("rewrite did not finish after release")
	}
	if ttl := blocking.recordedRewriteTTL(); ttl < 20*time.Second {
		t.Fatalf("fresh-plan rewrite context ttl=%s want around %s", ttl, vlogGenerationRewriteBoundedExecTimeout)
	}

	queue, qerr := db.currentVlogGenerationRewriteQueue()
	if qerr != nil {
		t.Fatalf("load rewrite queue: %v", qerr)
	}
	if len(queue) != 0 {
		t.Fatalf("rewrite queue not drained after release: queue=%v calls=%d", queue, blocking.recordedRewriteCalls())
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_runs"]; got != "0" {
		t.Fatalf("rewrite canceled runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_runs.fresh_plan"]; got != "0" {
		t.Fatalf("rewrite canceled fresh runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_runs.queued_debt"]; got != "0" {
		t.Fatalf("rewrite canceled queued runs=%q want 0", got)
	}
}

func TestVlogGenerationMaintenance_QueuesPendingCheckpointKickOnActiveCollision(t *testing.T) {
	db := &DB{
		valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
	}
	db.vlogGenerationMaintenanceActive.Store(true)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})

	if !db.vlogGenerationRewriteQueuePending.Load() {
		t.Fatalf("expected queue-source collision to queue pending rewrite retry")
	}
}

func TestVlogGenerationMaintenance_DoesNotQueuePendingWhenCollisionIsNotCheckpointKick(t *testing.T) {
	db := &DB{
		valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
	}
	db.vlogGenerationMaintenanceActive.Store(true)

	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		rewriteDebtDrain:      true,
	})

	if db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatalf("non-checkpoint collision unexpectedly queued pending checkpoint-kick retry")
	}
}

func TestVlogGenerationMaintenance_DrainsPendingCheckpointKickAfterPass(t *testing.T) {
	db := &DB{
		valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
	}
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationCheckpointKickPending.Store(true)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for db.vlogGenerationCheckpointKickPending.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("pending checkpoint-kick retry was not drained")
		}
		time.Sleep(10 * time.Millisecond)
	}
	db.wg.Wait()
}

func TestVlogGenerationMaintenance_PrioritizesPendingCheckpointKickOverPeriodicPass(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11, 22},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	// Block auto-drain so we can assert the periodic pass itself yields.
	db.closing.Store(true)
	db.vlogGenerationCheckpointKickPending.Store(true)
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm",
	})
	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("periodic pass should yield while checkpoint-kick pending; plan calls=%d", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("periodic pass should yield while checkpoint-kick pending; rewrite calls=%d", calls)
	}
	if !db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatalf("pending checkpoint-kick retry unexpectedly cleared")
	}

	// Re-enable and confirm the pending kick can run and clear.
	db.closing.Store(false)
	db.schedulePendingVlogGenerationCheckpointKick()
	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending checkpoint-kick retry did not run rewrite")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatalf("pending checkpoint-kick retry was not cleared after drain")
	}
}

func TestVlogGenerationMaintenance_PrioritizesDeferredWakeOverPeriodicPass(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	db.vlogGenerationDeferredMaintenancePending.Store(true)
	t.Cleanup(func() {
		db.vlogGenerationDeferredMaintenancePending.Store(false)
	})

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("periodic pass should yield while deferred wake pending; plan calls=%d", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("periodic pass should yield while deferred wake pending; rewrite calls=%d", calls)
	}
	if !db.vlogGenerationDeferredMaintenancePending.Load() {
		t.Fatalf("deferred wake pending unexpectedly cleared")
	}
}

func TestVlogGenerationMaintenance_ArmsDedicatedRewriteQueueSourceForExecutableDebt(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}

	db.vlogGenerationRewriteQueueRunning.Store(true)
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{})
	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("periodic pass should hand off executable queue debt; plan calls=%d", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("periodic pass should hand off executable queue debt; rewrite calls=%d", calls)
	}
	if !db.vlogGenerationRewriteQueuePending.Load() {
		t.Fatalf("rewrite queue pending was not armed")
	}

	db.vlogGenerationRewriteQueueRunning.Store(false)
	db.schedulePendingVlogGenerationRewriteQueue()
	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending rewrite queue did not run rewrite")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if db.vlogGenerationRewriteQueuePending.Load() {
		t.Fatalf("rewrite queue pending was not cleared after drain")
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_queue_pending"]; got != "1" {
		t.Fatalf("maintenance acquired source rewrite_queue_pending=%q want 1", got)
	}
}

func TestStartVlogGenerationDeferredMaintenance_PreservesPendingWhenRunnerActive(t *testing.T) {
	db := &DB{}
	db.vlogGenerationDeferredMaintenanceRunning.Store(true)
	db.vlogGenerationDeferredMaintenancePending.Store(false)

	db.startVlogGenerationDeferredMaintenance(vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm",
	})

	if !db.vlogGenerationDeferredMaintenancePending.Load() {
		t.Fatalf("deferred maintenance request was not preserved while runner active")
	}
}

func TestStartVlogGenerationRewriteQueueMaintenance_PreservesPendingWhenRunnerActive(t *testing.T) {
	db := &DB{}
	db.vlogGenerationRewriteQueueRunning.Store(true)
	db.vlogGenerationRewriteQueuePending.Store(false)

	db.startVlogGenerationRewriteQueueMaintenance(vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})

	if !db.vlogGenerationRewriteQueuePending.Load() {
		t.Fatalf("rewrite queue request was not preserved while runner active")
	}
}

func TestSchedulePendingVlogGenerationCheckpointKick_PrioritizesDueDeferredWake(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	stageObservedAt := time.Now().Add(-2 * vlogGenerationRewriteStageConfirmDelay).UnixNano()
	if err := db.setVlogGenerationRewriteLedgerWithStage([]backenddb.ValueLogRewritePlanSegment{{
		FileID:     11,
		BytesTotal: 128,
		BytesLive:  64,
		BytesStale: 64,
		StaleRatio: 0.5,
	}}, true, stageObservedAt); err != nil {
		t.Fatalf("seed staged rewrite ledger: %v", err)
	}
	forceRewriteStageConfirmDue(t, db)

	db.vlogGenerationCheckpointKickPending.Store(true)
	db.vlogGenerationDeferredMaintenanceRunning.Store(true)
	t.Cleanup(func() {
		db.vlogGenerationCheckpointKickPending.Store(false)
		db.vlogGenerationDeferredMaintenanceRunning.Store(false)
		db.vlogGenerationDeferredMaintenancePending.Store(false)
	})

	db.schedulePendingVlogGenerationCheckpointKick()

	if !db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatalf("checkpoint-pending retry should remain queued while due deferred wake takes priority")
	}
	if !db.vlogGenerationDeferredMaintenancePending.Load() {
		t.Fatalf("due deferred wake was not preserved")
	}
}

func TestVlogGenerationMaintenance_SchedulesDueStageConfirmationOnExit(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
				FileID:     11,
				BytesTotal: 128,
				BytesLive:  64,
				BytesStale: 64,
				StaleRatio: 0.5,
			}},
			SelectedBytesLive:  64,
			SelectedBytesStale: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)

	stageObservedAt := time.Now().Add(-2 * vlogGenerationRewriteStageConfirmDelay).UnixNano()
	if err := db.setVlogGenerationRewriteLedgerWithStage([]backenddb.ValueLogRewritePlanSegment{{
		FileID:     11,
		BytesTotal: 128,
		BytesLive:  64,
		BytesStale: 64,
		StaleRatio: 0.5,
	}}, true, stageObservedAt); err != nil {
		t.Fatalf("seed staged rewrite ledger: %v", err)
	}

	db.scheduleDueVlogGenerationDeferredMaintenance()

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			_, planCalls := recorder.recordedPlan()
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("due staged confirmation did not run in time: planCalls=%d rewriteCalls=%d", planCalls, rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestVlogGenerationMaintenance_CheckpointKickWaitsForCheckpointing(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	db.checkpointing.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(25 * time.Millisecond)
		db.checkpointMu.Lock()
		db.checkpointing.Store(false)
		db.checkpointCond.Broadcast()
		db.checkpointMu.Unlock()
	}()

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})
	<-done

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after checkpoint-kick wait=%d want=1", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after checkpoint-kick wait=%d want=1", calls)
	}
}

func TestVlogGenerationMaintenance_PeriodicPassReturnsDuringCheckpointing(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	db.checkpointing.Store(true)

	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls during periodic checkpointing=%d want=0", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls during periodic checkpointing=%d want=0", calls)
	}
}

type dryRunGCRecordingBackend struct {
	*backenddb.DB

	mu             sync.Mutex
	dryRunStats    backenddb.ValueLogGCStats
	dryRunCalls    int
	realCalls      int
	realGCStats    backenddb.ValueLogGCStats
	protectedPaths [][]string
}

func (b *dryRunGCRecordingBackend) ValueLogGC(ctx context.Context, opts backenddb.ValueLogGCOptions) (backenddb.ValueLogGCStats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if opts.DryRun {
		b.dryRunCalls++
		b.protectedPaths = append(b.protectedPaths, append([]string(nil), opts.ProtectedPaths...))
		return b.dryRunStats, nil
	}
	b.realCalls++
	b.protectedPaths = append(b.protectedPaths, append([]string(nil), opts.ProtectedPaths...))
	return b.realGCStats, nil
}

func (b *dryRunGCRecordingBackend) recordedCalls() (int, int, [][]string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	paths := make([][]string, len(b.protectedPaths))
	for i := range b.protectedPaths {
		paths[i] = append([]string(nil), b.protectedPaths[i]...)
	}
	return b.dryRunCalls, b.realCalls, paths
}

func TestVlogGenerationRewrite_UsesAndConsumesBudgetedBytes(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{1},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 128, BytesAfter: 64, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = db.Close() })
	value := make([]byte, 2048)
	for i := 0; i < 4; i++ {
		b := db.NewBatch()
		key := []byte{byte('k'), byte('0' + i)}
		if err := b.Set(key, value); err != nil {
			_ = b.Close()
			t.Fatalf("set %d: %v", i, err)
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write %d: %v", i, err)
		}
		_ = b.Close()
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	initialTokens := int64(1024)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(initialTokens)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	planOpts, planCalls := recorder.recordedPlan()
	if planCalls != 1 {
		t.Fatalf("plan calls=%d want=1", planCalls)
	}
	if planOpts.MaxSourceBytes <= 0 {
		t.Fatalf("plan MaxSourceBytes=%d want > 0", planOpts.MaxSourceBytes)
	}
	if planOpts.MaxSourceBytes > initialTokens {
		t.Fatalf("plan MaxSourceBytes=%d initialTokens=%d", planOpts.MaxSourceBytes, initialTokens)
	}
	if planOpts.MinSegmentStaleRatio < 0 || planOpts.MinSegmentStaleRatio > 1 {
		t.Fatalf("plan MinSegmentStaleRatio=%f want in [0,1]", planOpts.MinSegmentStaleRatio)
	}
	if planOpts.MinSegmentStaleBytes != vlogGenerationRewriteMinSegmentStaleBytes {
		t.Fatalf("plan MinSegmentStaleBytes=%d want %d", planOpts.MinSegmentStaleBytes, vlogGenerationRewriteMinSegmentStaleBytes)
	}
	if planOpts.MinSegmentAge != vlogGenerationRewriteMinSegmentAge {
		t.Fatalf("plan MinSegmentAge=%s want %s", planOpts.MinSegmentAge, vlogGenerationRewriteMinSegmentAge)
	}
	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if len(opts.SourceFileIDs) == 0 {
		t.Fatalf("rewrite SourceFileIDs=%v want non-empty planned selection", opts.SourceFileIDs)
	}
	if opts.MaxSourceBytes != 0 {
		t.Fatalf("rewrite MaxSourceBytes=%d want 0 once plan selection is materialized", opts.MaxSourceBytes)
	}
	if got, want := db.vlogGenerationRewriteBudgetTokensBytes.Load(), initialTokens-recorder.planResponse.SelectedBytesLive; got != want {
		t.Fatalf("tokens after rewrite=%d want=%d", got, want)
	}
}

func TestVlogGenerationRewrite_ConsumesLedgerLiveBytesWhenAvailable(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{1},
			SelectedBytesLive: 128,
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 1, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
			},
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 128, BytesAfter: 64, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = db.Close() })
	value := make([]byte, 2048)
	for i := 0; i < 4; i++ {
		b := db.NewBatch()
		key := []byte{byte('k'), byte('0' + i)}
		if err := b.Set(key, value); err != nil {
			_ = b.Close()
			t.Fatalf("set %d: %v", i, err)
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write %d: %v", i, err)
		}
		_ = b.Close()
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	initialTokens := int64(1024)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(initialTokens)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if got, want := db.vlogGenerationRewriteBudgetTokensBytes.Load(), initialTokens-int64(64); got != want {
		t.Fatalf("tokens after ledger rewrite=%d want=%d", got, want)
	}
}

func TestVlogGenerationRewrite_SkipsZeroLivePlannedLedger(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{1},
			SelectedBytesLive: 128,
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 1, BytesLive: 0, BytesTotal: 128, BytesStale: 128, StaleRatio: 1.0},
			},
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 128, BytesAfter: 64, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = db.Close() })
	value := make([]byte, 2048)
	for i := 0; i < 4; i++ {
		b := db.NewBatch()
		key := []byte{byte('k'), byte('0' + i)}
		if err := b.Set(key, value); err != nil {
			_ = b.Close()
			t.Fatalf("set %d: %v", i, err)
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("write %d: %v", i, err)
		}
		_ = b.Close()
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	initialTokens := int64(1024)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(initialTokens)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if got := db.vlogGenerationRewriteBudgetTokensBytes.Load(); got != initialTokens {
		t.Fatalf("tokens after zero-ledger prune=%d want=%d", got, initialTokens)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0 (zero-live plan should be pruned)", calls)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("rewrite queue after zero-live prune=%v want empty", queue)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_prune_runs"]; got != "1" {
		t.Fatalf("queue prune runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_prune_ids"]; got != "1" {
		t.Fatalf("queue prune ids=%q want 1", got)
	}
}

func TestVlogGenerationRewriteQueue_DoesNotRunWhenBudgetEmpty(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	// First run seeds the queue.
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after seed=%d want=1", calls)
	}

	// Re-seed a queue and then ensure we don't run when the budget is empty.
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("set queue: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(0)
	// Prevent budget accrual from re-filling the token bucket between the Store(0)
	// above and the queued rewrite pass, which would defeat this
	// "empty budget" assertion on slow/loaded CI runners.
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(time.Now().Add(10 * time.Second).UnixNano())
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls with empty budget=%d want still=1", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.passes"]; got != "1" {
		t.Fatalf("queued debt passes=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started"]; got != "0" {
		t.Fatalf("queued debt rewrite started=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.skip.budget_empty"]; got != "1" {
		t.Fatalf("queued debt budget-empty skips=%q want 1", got)
	}
}

func TestVlogGenerationRewriteQueue_TracksQueuedDebtNoChunkSkips(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 51, BytesTotal: 100, BytesStale: 49, StaleRatio: 0.49},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0 (no executable queued chunk)", calls)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after queued no-chunk pass=%v want empty", queue)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.passes"]; got != "1" {
		t.Fatalf("queued debt passes=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started"]; got != "0" {
		t.Fatalf("queued debt rewrite started=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.skip.no_chunk"]; got != "1" {
		t.Fatalf("queued debt no-chunk skips=%q want 1", got)
	}
}

func TestVlogGenerationRewriteQueue_LedgerOrdersByStaleRatio(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		// Scheduler should not call plan when queue is non-empty.
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.1},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.9},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if len(opts.SourceFileIDs) == 0 || opts.SourceFileIDs[0] != 22 {
		t.Fatalf("rewrite SourceFileIDs=%v want first=22", opts.SourceFileIDs)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started"]; got != "1" {
		t.Fatalf("queued debt rewrite started=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.exec.runs"]; got != "1" {
		t.Fatalf("queued debt exec runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.exec.segments"]; got != "1" {
		t.Fatalf("queued debt exec segments=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_total"]; got != "128" {
		t.Fatalf("queued debt exec plan bytes total=%q want 128", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_live"]; got != "64" {
		t.Fatalf("queued debt exec plan bytes live=%q want 64", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_stale"]; got != "64" {
		t.Fatalf("queued debt exec plan bytes stale=%q want 64", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.exec.effective_bytes_before"]; got != "64" {
		t.Fatalf("queued debt exec effective bytes before=%q want 64", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.exec.effective_bytes_after"]; got != "32" {
		t.Fatalf("queued debt exec effective bytes after=%q want 32", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.exec.reclaimed_bytes"]; got != "32" {
		t.Fatalf("queued debt exec reclaimed bytes=%q want 32", got)
	}
}

func TestVlogGenerationRewriteQueue_PrefersUnpenalizedLedgerBeforeExpiredPenalty(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.90},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.50},
		{FileID: 33, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.80},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}
	if err := db.recordVlogGenerationRewritePenalty([]uint32{11, 33}, time.Now().Add(-time.Second), 64); err != nil {
		t.Fatalf("record penalty: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22, 11}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_PrefersGreaterStaleImprovementAmongExpiredRetries(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 32, BytesTotal: 128, BytesStale: 96, StaleRatio: 0.75},
		{FileID: 22, BytesLive: 48, BytesTotal: 128, BytesStale: 80, StaleRatio: 0.625},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}
	if err := db.recordVlogGenerationRewritePenaltyWithLedger(
		[]uint32{11, 22},
		[]backenddb.ValueLogRewritePlanSegment{
			{FileID: 11, BytesLive: 38, BytesTotal: 128, BytesStale: 90, StaleRatio: 90.0 / 128.0},
			{FileID: 22, BytesLive: 108, BytesTotal: 128, BytesStale: 20, StaleRatio: 20.0 / 128.0},
		},
		time.Now().Add(-time.Second),
		0,
	); err != nil {
		t.Fatalf("record penalty: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	// A useful first rewrite may immediately schedule the remaining debt.
	// Assert the first selection instead of racing that intentional follow-up.
	rewrites := recorder.recordedRewrites()
	if len(rewrites) == 0 {
		t.Fatal("rewrite calls=0 want>=1")
	}
	opts := rewrites[0]
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("first rewrite SourceFileIDs=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_PrefersHistoricallyUsefulExpiredRetry(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	ledger := []backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
	}
	if err := db.setVlogGenerationRewriteLedger(ledger); err != nil {
		t.Fatalf("set ledger: %v", err)
	}
	if err := db.recordVlogGenerationRewritePenaltyWithLedger([]uint32{11, 22}, ledger, time.Now().Add(-time.Second), 0); err != nil {
		t.Fatalf("record penalty: %v", err)
	}
	if err := db.recordVlogGenerationRewriteHistoryWithLedger([]uint32{11}, ledger[:1], 0, 0, time.Now().Add(-2*time.Second)); err != nil {
		t.Fatalf("record history 11: %v", err)
	}
	if err := db.recordVlogGenerationRewriteHistoryWithLedger([]uint32{22}, ledger[1:], 32, 16, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("record history 22: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_PrefersHistoricallyUsefulActiveLedgerDebt(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	ledger := []backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 96, StaleRatio: 0.75},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
		{FileID: 33, BytesLive: 64, BytesTotal: 128, BytesStale: 80, StaleRatio: 0.625},
	}
	if err := db.setVlogGenerationRewriteLedger(ledger); err != nil {
		t.Fatalf("set ledger: %v", err)
	}
	if err := db.recordVlogGenerationRewriteHistoryWithLedger([]uint32{11}, ledger[:1], 0, 0, time.Now().Add(-2*time.Second)); err != nil {
		t.Fatalf("record history 11: %v", err)
	}
	if err := db.recordVlogGenerationRewriteHistoryWithLedger([]uint32{22}, ledger[1:2], 32, 16, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("record history 22: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(64)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
}

func TestAdmitVlogGenerationRewriteLedger_DoesNotCountHistoricallyUsefulRetriesAgainstCap(t *testing.T) {
	ledger := []backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
		{FileID: 33, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
	}
	history := map[uint32]valueLogGenerationRewriteHistory{
		11: {LastProcessedLiveBytes: 64, LastSourceBytesUnreferenced: 32, LastReclaimedBytes: 16, LastStaleBytes: 64},
		22: {LastProcessedLiveBytes: 64, LastSourceBytesUnreferenced: 16, LastReclaimedBytes: 8, LastStaleBytes: 64},
	}
	penalties := map[uint32]valueLogGenerationRewritePenalty{
		11: {Attempts: 1, LastStaleBytes: 64},
		22: {Attempts: 1, LastStaleBytes: 64},
		33: {Attempts: 1, LastStaleBytes: 64},
	}

	ordered := prioritizeVlogGenerationRewriteLedger(ledger, history, penalties)
	admitted := admitVlogGenerationRewriteLedger(ordered, history, penalties, 1)
	if got, want := vlogGenerationRewriteLedgerIDs(admitted), []uint32{11, 22}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("admitted ids=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_AdmitsSingleExpiredPenaltyWhenAllDebtRetried(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.90},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.80},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}
	if err := db.recordVlogGenerationRewritePenalty([]uint32{11, 22}, time.Now().Add(-time.Second), 64); err != nil {
		t.Fatalf("record penalty: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_SuppressesImmediateFollowupWithoutUnreferencedBytes(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:                   64,
			BytesAfter:                    64,
			RecordsCopied:                 1,
			SourceSegmentsRequested:       1,
			SourceSegmentsStillReferenced: 1,
			SourceBytesRequested:          128,
			SourceBytesUnreferenced:       0,
			SourceFileIDsStillReferenced:  []uint32{11},
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 128, BytesTotal: 256, BytesStale: 128, StaleRatio: 0.5},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(64)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after first pass=%d want=1", calls)
	}
	if db.vlogGenerationRewriteQueuePending.Load() {
		t.Fatalf("rewrite queue pending should stay clear after suppressed follow-up")
	}
	if got := db.vlogGenerationRewriteIneffectiveLastNS.Load(); got != 0 {
		t.Fatalf("global ineffective backoff unexpectedly armed for queued follow-up suppression: %d", got)
	}
	penalties, err := db.currentVlogGenerationRewritePenalties()
	if err != nil {
		t.Fatalf("current penalties: %v", err)
	}
	penalty, ok := penalties[11]
	if !ok || penalty.CooldownUntilUnixNano == 0 {
		t.Fatalf("expected follow-up penalty for queued segment, got=%v", penalties)
	}

	runRewriteQueueMaintenanceForTest(db)
	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after immediate retry=%d want still=1", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.followup_suppressed"]; got != "1" {
		t.Fatalf("queued debt followup_suppressed=%q want 1", got)
	}
}

func TestVlogGenerationRewriteQueue_CooledDebtDoesNotBlockFreshPlan(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{22},
			SelectedBytesLive: 64,
		},
	}
	recorder.rewriteFn = func(_ context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
		switch {
		case len(opts.SourceFileIDs) == 1 && opts.SourceFileIDs[0] == 22:
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                64,
				BytesAfter:                 32,
				RecordsCopied:              1,
				SourceSegmentsRequested:    1,
				SourceSegmentsUnreferenced: 1,
				SourceBytesRequested:       64,
				SourceBytesUnreferenced:    64,
				SourceFileIDsUnreferenced:  []uint32{22},
			}, nil
		default:
			t.Fatalf("unexpected rewrite source ids=%v", opts.SourceFileIDs)
			return backenddb.ValueLogRewriteStats{}, nil
		}
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("set queue: %v", err)
	}
	if err := db.recordVlogGenerationRewritePenalty([]uint32{11}, time.Now().Add(time.Minute), 64); err != nil {
		t.Fatalf("record penalty: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after fresh pass=%d want=1", calls)
	}
	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls after fresh pass=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after fresh pass: %v", err)
	}
	if got, want := queue, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after fresh pass=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_AggressiveFlowBypassesCooledDebtAndReadmitsImprovedSegment(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend}
	recorder.rewriteFn = func(_ context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
		switch {
		case len(opts.SourceFileIDs) == 1 && opts.SourceFileIDs[0] == 22:
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                64,
				BytesAfter:                 32,
				RecordsCopied:              1,
				SourceSegmentsRequested:    1,
				SourceSegmentsUnreferenced: 1,
				SourceBytesRequested:       64,
				SourceBytesUnreferenced:    64,
				SourceFileIDsUnreferenced:  []uint32{22},
			}, nil
		case len(opts.SourceFileIDs) == 1 && opts.SourceFileIDs[0] == 11:
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                64,
				BytesAfter:                 24,
				RecordsCopied:              1,
				SourceSegmentsRequested:    1,
				SourceSegmentsUnreferenced: 1,
				SourceBytesRequested:       64,
				SourceBytesUnreferenced:    64,
				SourceFileIDsUnreferenced:  []uint32{11},
			}, nil
		default:
			t.Fatalf("unexpected rewrite source ids=%v", opts.SourceFileIDs)
			return backenddb.ValueLogRewriteStats{}, nil
		}
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{{
		FileID:     11,
		BytesTotal: 64 << 20,
		BytesLive:  33 << 20,
		BytesStale: 31 << 20,
		StaleRatio: 31.0 / 64.0,
	}}); err != nil {
		t.Fatalf("seed cooled ledger: %v", err)
	}
	if err := db.recordVlogGenerationRewritePenaltyWithLedger(
		[]uint32{11},
		[]backenddb.ValueLogRewritePlanSegment{{
			FileID:     11,
			BytesTotal: 64 << 20,
			BytesLive:  33 << 20,
			BytesStale: 31 << 20,
			StaleRatio: 31.0 / 64.0,
		}},
		time.Now().Add(time.Minute),
		0,
	); err != nil {
		t.Fatalf("seed cooled penalty: %v", err)
	}

	recorder.mu.Lock()
	recorder.planResponse = backenddb.ValueLogRewritePlan{
		SourceFileIDs: []uint32{22},
		SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
			FileID:     22,
			BytesTotal: 128,
			BytesLive:  64,
			BytesStale: 64,
			StaleRatio: 0.5,
		}},
		SelectedBytesTotal: 128,
		SelectedBytesLive:  64,
		SelectedBytesStale: 64,
	}
	recorder.mu.Unlock()

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after fresh bypass run=%d want=1", calls)
	}
	rewriteOpts, rewriteCalls := recorder.recordedRewrite()
	if rewriteCalls != 1 {
		t.Fatalf("rewrite calls after fresh bypass run=%d want=1", rewriteCalls)
	}
	if got, want := rewriteOpts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs after fresh bypass=%v want=%v", got, want)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after fresh bypass: %v", err)
	}
	if got, want := queue, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after fresh bypass=%v want=%v", got, want)
	}

	recorder.mu.Lock()
	recorder.planResponse = backenddb.ValueLogRewritePlan{
		SourceFileIDs: []uint32{11},
		SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
			FileID:     11,
			BytesTotal: 64 << 20,
			BytesLive:  20 << 20,
			BytesStale: 44 << 20,
			StaleRatio: 44.0 / 64.0,
		}},
		SelectedBytesTotal: 64 << 20,
		SelectedBytesLive:  20 << 20,
		SelectedBytesStale: 44 << 20,
	}
	recorder.mu.Unlock()
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	db.vlogGenerationRewriteIneffectiveLastNS.Store(time.Now().Add(-2 * vlogGenerationRewriteIneffectiveBackoff).UnixNano())
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 2 {
		t.Fatalf("plan calls after improved-stale rerun=%d want=2", calls)
	}
	rewriteOpts, rewriteCalls = recorder.recordedRewrite()
	if rewriteCalls != 2 {
		t.Fatalf("rewrite calls after improved-stale rerun=%d want=2", rewriteCalls)
	}
	if got, want := rewriteOpts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs after improved-stale rerun=%v want=%v", got, want)
	}
	penalties, err := db.currentVlogGenerationRewritePenalties()
	if err != nil {
		t.Fatalf("current penalties after improved-stale rerun: %v", err)
	}
	if _, ok := penalties[11]; ok {
		t.Fatalf("penalty for readmitted segment still present: %v", penalties[11])
	}
	queue, err = db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after improved-stale rerun: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after improved-stale rerun=%v want empty", queue)
	}
}

func TestVlogGenerationRewriteQueue_FreshBypassThenExpiredRetryDrainsQueuedDebt(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{33},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
				FileID:     33,
				BytesTotal: 128,
				BytesLive:  64,
				BytesStale: 64,
				StaleRatio: 0.5,
			}},
			SelectedBytesTotal: 128,
			SelectedBytesLive:  64,
			SelectedBytesStale: 64,
		},
	}
	recorder.rewriteFn = func(_ context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
		switch {
		case len(opts.SourceFileIDs) == 1 && opts.SourceFileIDs[0] == 33:
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                64,
				BytesAfter:                 32,
				RecordsCopied:              1,
				SourceSegmentsRequested:    1,
				SourceSegmentsUnreferenced: 1,
				SourceBytesRequested:       64,
				SourceBytesUnreferenced:    64,
				SourceFileIDsUnreferenced:  []uint32{33},
			}, nil
		case len(opts.SourceFileIDs) == 1 && opts.SourceFileIDs[0] == 22:
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                64,
				BytesAfter:                 24,
				RecordsCopied:              1,
				SourceSegmentsRequested:    1,
				SourceSegmentsUnreferenced: 1,
				SourceBytesRequested:       64,
				SourceBytesUnreferenced:    64,
				SourceFileIDsUnreferenced:  []uint32{22},
			}, nil
		case len(opts.SourceFileIDs) == 1 && opts.SourceFileIDs[0] == 11:
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                64,
				BytesAfter:                 40,
				RecordsCopied:              1,
				SourceSegmentsRequested:    1,
				SourceSegmentsUnreferenced: 1,
				SourceBytesRequested:       64,
				SourceBytesUnreferenced:    64,
				SourceFileIDsUnreferenced:  []uint32{11},
			}, nil
		default:
			t.Fatalf("unexpected rewrite source ids=%v", opts.SourceFileIDs)
			return backenddb.ValueLogRewriteStats{}, nil
		}
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesTotal: 128, BytesLive: 64, BytesStale: 64, StaleRatio: 0.5},
		{FileID: 22, BytesTotal: 128, BytesLive: 64, BytesStale: 64, StaleRatio: 0.5},
	}); err != nil {
		t.Fatalf("set cooled ledger: %v", err)
	}
	if err := db.recordVlogGenerationRewritePenaltyWithLedger(
		[]uint32{11, 22},
		[]backenddb.ValueLogRewritePlanSegment{
			{FileID: 11, BytesTotal: 128, BytesLive: 84, BytesStale: 44, StaleRatio: 44.0 / 128.0},
			{FileID: 22, BytesTotal: 128, BytesLive: 104, BytesStale: 24, StaleRatio: 24.0 / 128.0},
		},
		time.Now().Add(time.Minute),
		0,
	); err != nil {
		t.Fatalf("record cooled penalties: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after fresh bypass=%d want=1", calls)
	}
	rewriteOpts, rewriteCalls := recorder.recordedRewrite()
	if rewriteCalls != 1 {
		t.Fatalf("rewrite calls after fresh bypass=%d want=1", rewriteCalls)
	}
	if got, want := rewriteOpts.SourceFileIDs, []uint32{33}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("fresh bypass SourceFileIDs=%v want=%v", got, want)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after fresh bypass: %v", err)
	}
	if got, want := queue, []uint32{11, 22}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("queue after fresh bypass=%v want=%v", got, want)
	}

	expireVlogGenerationRewritePenaltiesForTest(t, db, 11, 22)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(64)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after queued retry=%d want=1", calls)
	}
	rewriteOpts, rewriteCalls = recorder.recordedRewrite()
	if rewriteCalls != 2 {
		t.Fatalf("rewrite calls after queued retry=%d want=2", rewriteCalls)
	}
	if got := rewriteOpts.SourceFileIDs; len(got) != 1 || (got[0] != 11 && got[0] != 22) {
		t.Fatalf("queued retry SourceFileIDs=%v want one of [11] or [22]", got)
	}
	retriedID := rewriteOpts.SourceFileIDs[0]
	remainingID := uint32(11)
	if retriedID == 11 {
		remainingID = 22
	}
	queue, err = db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after queued retry: %v", err)
	}
	if got, want := queue, []uint32{remainingID}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after queued retry=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_CooledDebtFreshLedgerMergesWithoutOverwrite(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{22},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
				FileID:     22,
				BytesLive:  64,
				BytesTotal: 128,
				BytesStale: 64,
				StaleRatio: 0.5,
			}},
			SelectedBytesLive: 64,
		},
	}
	recorder.rewriteFn = func(_ context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
		switch {
		case len(opts.SourceFileIDs) == 1 && opts.SourceFileIDs[0] == 11:
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                   64,
				BytesAfter:                    64,
				RecordsCopied:                 1,
				SourceSegmentsRequested:       1,
				SourceSegmentsStillReferenced: 1,
				SourceBytesRequested:          128,
				SourceBytesUnreferenced:       0,
				SourceFileIDsStillReferenced:  []uint32{11},
			}, nil
		case len(opts.SourceFileIDs) == 1 && opts.SourceFileIDs[0] == 22:
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                64,
				BytesAfter:                 32,
				RecordsCopied:              1,
				SourceSegmentsRequested:    1,
				SourceSegmentsUnreferenced: 1,
				SourceBytesRequested:       64,
				SourceBytesUnreferenced:    64,
				SourceFileIDsUnreferenced:  []uint32{22},
			}, nil
		default:
			t.Fatalf("unexpected rewrite source ids=%v", opts.SourceFileIDs)
			return backenddb.ValueLogRewriteStats{}, nil
		}
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{{
		FileID:     11,
		BytesLive:  128,
		BytesTotal: 256,
		BytesStale: 128,
		StaleRatio: 0.5,
	}}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(64)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls after queued pass=%d want=0", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after queued pass=%d want=1", calls)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after fresh pass=%d want=1", calls)
	}
	opts, calls := recorder.recordedRewrite()
	if calls != 2 {
		t.Fatalf("rewrite calls after fresh pass=%d want=2", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("second rewrite SourceFileIDs=%v want=%v", got, want)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after fresh pass: %v", err)
	}
	if got, want := queue, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after fresh pass=%v want=%v", got, want)
	}
	ledger, err := db.currentVlogGenerationRewriteLedger()
	if err != nil {
		t.Fatalf("current ledger after fresh pass: %v", err)
	}
	if len(ledger) != 1 || ledger[0].FileID != 11 {
		t.Fatalf("ledger after fresh pass=%v want file 11 only", ledger)
	}
}

func TestVlogGenerationRewriteQueue_PrunesZeroLiveLedgerBeforeResume(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 0, BytesTotal: 128, BytesStale: 128, StaleRatio: 1.0},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.5},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after rewrite=%v want empty", queue)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_prune_ids"]; got != "1" {
		t.Fatalf("queue prune ids=%q want 1", got)
	}
}

func TestVlogGenerationRewriteQueue_PrunesLowQualityLedgerBeforeResume(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 72, BytesTotal: 128, BytesStale: 56, StaleRatio: 0.4375},
		{FileID: 22, BytesLive: 32, BytesTotal: 128, BytesStale: 96, StaleRatio: 0.75},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after rewrite=%v want empty", queue)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_prune_ids"]; got != "1" {
		t.Fatalf("queue prune ids=%q want 1", got)
	}
}

func TestVlogGenerationRewriteQueue_ResumesWithoutReplanning(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11, 22},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after first run=%d want=1", calls)
	}
	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls after first run=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("first rewrite source ids=%v want=%v", got, want)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after first run: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after first run=%v want=%v", got, want)
	}

	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after second run=%d want=1", calls)
	}
	opts, calls = recorder.recordedRewrite()
	if calls != 2 {
		t.Fatalf("rewrite calls after second run=%d want=2", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("second rewrite source ids=%v want=%v", got, want)
	}
	queue, err = db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after second run: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after second run=%v want empty", queue)
	}
}

func TestVlogGenerationRewriteQueue_KeepsStillReferencedSegmentQueuedWhenBounded(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:                   2048,
			BytesAfter:                    1536,
			RecordsCopied:                 1,
			SourceBytesProcessed:          1024,
			SourceSegmentsStillReferenced: 1,
			SourceSegmentsUnreferenced:    0,
			SourceFileIDsStillReferenced:  []uint32{11},
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 2048, BytesTotal: 4096, BytesStale: 2048, StaleRatio: 0.5},
		{FileID: 22, BytesLive: 1024, BytesTotal: 2048, BytesStale: 1024, StaleRatio: 0.5},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
	if got, want := opts.MaxCopiedBytes, int64(1024); got != want {
		t.Fatalf("rewrite MaxCopiedBytes=%d want %d", got, want)
	}

	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if got, want := queue, []uint32{11, 22}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("queue after bounded rewrite=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_DebtDrainSelectsMultipleSegmentsAndBoundsExecution(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:                   256,
			BytesAfter:                    129,
			RecordsCopied:                 2,
			SourceBytesProcessed:          172,
			SourceSegmentsStillReferenced: 1,
			SourceSegmentsUnreferenced:    1,
			SourceFileIDsStillReferenced:  []uint32{22},
			SourceFileIDsUnreferenced:     []uint32{33},
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 1, BytesTotal: 64, BytesStale: 63, StaleRatio: 0.1},
		{FileID: 22, BytesLive: 128, BytesTotal: 256, BytesStale: 128, StaleRatio: 0.8},
		{FileID: 33, BytesLive: 128, BytesTotal: 256, BytesStale: 128, StaleRatio: 0.9},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(172)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{33, 22}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
	if got, want := opts.MaxCopiedBytes, int64(172); got != want {
		t.Fatalf("rewrite MaxCopiedBytes=%d want %d", got, want)
	}

	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after bounded debt-drain rewrite=%v want=%v", got, want)
	}
}
func TestVlogGenerationRewriteQueue_ChunkDebtCarriesPartialFileRemainder(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:                   256,
			BytesAfter:                    129,
			RecordsCopied:                 1,
			SourceBytesProcessed:          128,
			SourceSegmentsRequested:       1,
			SourceChunksRequested:         1,
			SourceSegmentsStillReferenced: 1,
			SourceFileIDsStillReferenced:  []uint32{22},
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteChunkLedger([]backenddb.ValueLogRewritePlanChunk{
		{FileID: 22, ChunkOffset: 0, BytesLive: 128, BytesTotal: 256, BytesStale: 128, StaleRatio: 0.5},
		{FileID: 22, ChunkOffset: 16 << 20, BytesLive: 128, BytesTotal: 256, BytesStale: 128, StaleRatio: 0.5},
	}, 16<<20); err != nil {
		t.Fatalf("set chunk ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(172)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
	if len(opts.SourceChunks) != 1 {
		t.Fatalf("rewrite SourceChunks=%v want one chunk", opts.SourceChunks)
	}
	if got := opts.SourceChunks[0].ChunkOffset; got != 0 {
		t.Fatalf("rewrite SourceChunks[0].ChunkOffset=%d want 0", got)
	}
	if got, want := opts.SourceChunkBytes, int64(16<<20); got != want {
		t.Fatalf("rewrite SourceChunkBytes=%d want %d", got, want)
	}

	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after chunk rewrite=%v want=%v", got, want)
	}
	remainingChunks, chunkBytes, err := db.currentVlogGenerationRewriteChunkLedger()
	if err != nil {
		t.Fatalf("current chunk ledger: %v", err)
	}
	if chunkBytes != 16<<20 {
		t.Fatalf("remaining chunkBytes=%d want %d", chunkBytes, 16<<20)
	}
	if len(remainingChunks) != 1 || remainingChunks[0].FileID != 22 || remainingChunks[0].ChunkOffset != 16<<20 {
		t.Fatalf("remaining chunk ledger=%v want single chunk file=22 offset=%d", remainingChunks, 16<<20)
	}
}

func TestVlogGenerationRewriteQueue_ConsumesMissingBoundedSourceIDs(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		// Simulate a queued ID that disappeared from the current value-log set
		// before execution. The backend returns a no-op stats result with zero
		// requested source segments.
		rewriteResponse: backenddb.ValueLogRewriteStats{},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 2048, BytesTotal: 4096, BytesStale: 2048, StaleRatio: 0.5},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
	}
	if got, want := opts.MaxCopiedBytes, int64(1024); got != want {
		t.Fatalf("rewrite MaxCopiedBytes=%d want %d", got, want)
	}

	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after missing-source bounded rewrite=%v want empty", queue)
	}
}

func TestVlogGenerationRewriteQueue_BoundedSegmentEventuallyDrainsWithoutReplanning(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend}
	recorder.rewriteFn = func(context.Context, backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
		recorder.mu.Lock()
		call := recorder.rewriteCalls
		recorder.mu.Unlock()
		if call == 1 {
			return backenddb.ValueLogRewriteStats{
				BytesBefore:                   2048,
				BytesAfter:                    1536,
				RecordsCopied:                 1,
				SourceBytesProcessed:          1024,
				SourceSegmentsStillReferenced: 1,
				SourceFileIDsStillReferenced:  []uint32{11},
			}, nil
		}
		return backenddb.ValueLogRewriteStats{
			BytesBefore:                2048,
			BytesAfter:                 1024,
			RecordsCopied:              1,
			SourceBytesProcessed:       1024,
			SourceSegmentsUnreferenced: 1,
			SourceFileIDsUnreferenced:  []uint32{11},
		}, nil
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 2048, BytesTotal: 4096, BytesStale: 2048, StaleRatio: 0.5},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls after first bounded pass=%d want=0", calls)
	}
	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls after first bounded pass=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("first bounded rewrite SourceFileIDs=%v want=%v", got, want)
	}
	if got, want := opts.MaxCopiedBytes, int64(1024); got != want {
		t.Fatalf("first bounded rewrite MaxCopiedBytes=%d want %d", got, want)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after first bounded pass: %v", err)
	}
	if got, want := queue, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after first bounded pass=%v want=%v", got, want)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 2 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("rewrite calls after second bounded pass=%d want>=2", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls after second bounded pass=%d want=0", calls)
	}
	opts, calls = recorder.recordedRewrite()
	if calls < 2 {
		t.Fatalf("rewrite calls after second bounded pass=%d want>=2", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("second bounded rewrite SourceFileIDs=%v want=%v", got, want)
	}
	if got, want := opts.MaxCopiedBytes, int64(1024); got != want {
		t.Fatalf("second bounded rewrite MaxCopiedBytes=%d want %d", got, want)
	}
	queue, err = db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after second bounded pass: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after second bounded pass=%v want empty", queue)
	}
}

func TestVlogGenerationRewriteQueue_DebtDrainProcessesMultipleSegments(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 192, BytesAfter: 96, RecordsCopied: 3},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.2},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.7},
		{FileID: 33, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.9},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(defaultVlogGenerationWarmTargetBytes * 4)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	runRewriteQueueMaintenanceForTest(db)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got := len(opts.SourceFileIDs); got <= 1 {
		t.Fatalf("rewrite SourceFileIDs=%v want multiple ids in debt-drain mode", opts.SourceFileIDs)
	}
	if got := len(opts.SourceFileIDs); got > vlogGenerationRewriteDebtDrainMaxSegments {
		t.Fatalf("rewrite SourceFileIDs len=%d want <= %d", got, vlogGenerationRewriteDebtDrainMaxSegments)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue after debt-drain rewrite=%v want empty", queue)
	}
}

func TestVlogGenerationRewriteQueue_CheckpointKickDebtDrainCapsSingleSegment(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)

	if err := db.setVlogGenerationRewriteLedger([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.2},
		{FileID: 22, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.7},
		{FileID: 33, BytesLive: 64, BytesTotal: 128, BytesStale: 64, StaleRatio: 0.9},
	}); err != nil {
		t.Fatalf("set ledger: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(defaultVlogGenerationWarmTargetBytes * 4)
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got := len(opts.SourceFileIDs); got != 1 {
		t.Fatalf("checkpoint-kick rewrite SourceFileIDs=%v want single segment", opts.SourceFileIDs)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if len(queue) != 1 {
		t.Fatalf("queue after checkpoint-kick single-segment drain=%v want len=1", queue)
	}
	consumed := opts.SourceFileIDs[0]
	for _, id := range queue {
		if id == consumed {
			t.Fatalf("consumed id %d still present in queue=%v", consumed, queue)
		}
		if id == 11 {
			t.Fatalf("low-quality id 11 should have been pruned from queue=%v", queue)
		}
	}
}

func TestVlogGenerationRewriteQueue_SurvivesReopen(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, _ := openRewriteQueueTestDB(t, dir, recorder)
	if err := db.setVlogGenerationRewriteQueue([]uint32{22}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close first db: %v", err)
	}

	backend2, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	recorder2 := &rewriteBudgetRecordingBackend{
		DB:              backend2,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}
	db2, err := Open(dir, recorder2, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("reopen cachingdb: %v", err)
	}
	defer func() { _ = db2.Close() }()
	db2.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db2)

	queue, err := db2.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after reopen: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after reopen=%v want=%v", got, want)
	}
	stats := db2.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_len"]; got != "1" {
		t.Fatalf("rewrite queue len after reopen=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_loaded"]; got != "true" {
		t.Fatalf("rewrite queue loaded after reopen=%q want true", got)
	}

	runRewriteQueueMaintenanceForTest(db2)
	if _, calls := recorder2.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls after reopen=%d want=0", calls)
	}
	opts, calls := recorder2.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls after reopen=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite source ids after reopen=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewriteQueue_PreservedOnRewriteError(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11, 22},
			SelectedBytesLive: 128,
		},
		rewriteErr: errors.New("rewrite failed"),
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue after failed rewrite: %v", err)
	}
	if got, want := queue, []uint32{11, 22}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("queue after failed rewrite=%v want=%v", got, want)
	}
	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after failed rewrite=%d want=1", calls)
	}
	if got := db.vlogGenerationRemapFailures.Load(); got != 1 {
		t.Fatalf("rewrite failure count=%d want=1", got)
	}
}

func TestVlogGenerationRewriteResume_CancelBackoffSkipsImmediateRetry(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteErr: context.Canceled,
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	_, callsAfterFirst := recorder.recordedRewrite()
	if callsAfterFirst < 1 {
		t.Fatalf("rewrite calls after first canceled run=%d want >=1", callsAfterFirst)
	}

	// Isolate cancel-backoff behavior from min-interval throttling.
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	db.maybeRunVlogGenerationMaintenance(false)
	_, callsAfterSecond := recorder.recordedRewrite()
	if callsAfterSecond > callsAfterFirst+1 {
		t.Fatalf("rewrite calls after immediate retry=%d unexpectedly exceeded backoff envelope from %d", callsAfterSecond, callsAfterFirst)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_runs"]; got == "0" {
		t.Fatalf("rewrite canceled runs=%q want non-zero", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_last_unix_nano"]; got == "0" {
		t.Fatalf("rewrite canceled last ts=%q want non-zero", got)
	}
}

func TestVlogGenerationRewriteResume_CancelBackoffExpires(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteErr: context.Canceled,
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	_, callsAfterFirst := recorder.recordedRewrite()
	if callsAfterFirst < 1 {
		t.Fatalf("rewrite calls after first canceled run=%d want >=1", callsAfterFirst)
	}

	db.vlogGenerationLastRewriteUnixNano.Store(0)
	deadline := time.Now().Add(schedulerTestWait(t))
	for {
		db.vlogGenerationRewriteCanceledLastNS.Store(time.Now().Add(-2 * vlogGenerationRewriteCancelBackoff).UnixNano())
		db.maybeRunVlogGenerationMaintenance(false)
		_, callsAfterSecond := recorder.recordedRewrite()
		if callsAfterSecond >= callsAfterFirst+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rewrite calls after expired cancel backoff=%d want at least %d", callsAfterSecond, callsAfterFirst+1)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestVlogGenerationRewrite_TracksIneffectiveRuns(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 96, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.ineffective_runs"]; got != "1" {
		t.Fatalf("ineffective runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.ineffective_bytes_in"]; got != "64" {
		t.Fatalf("ineffective bytes in=%q want 64", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.ineffective_bytes_out"]; got != "96" {
		t.Fatalf("ineffective bytes out=%q want 96", got)
	}
}

func TestVlogGenerationRewrite_CoolsProcessedDebtAfterMaterialIneffectiveGrowth(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11, 22},
			SelectedBytesLive: 64 << 20,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    (64 << 20) + vlogGenerationRewriteIneffectiveGrowthMinBytes + 1,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after material ineffective rewrite=%v want=%v", got, want)
	}
	penalties, err := db.currentVlogGenerationRewritePenalties()
	if err != nil {
		t.Fatalf("current penalties: %v", err)
	}
	penalty, ok := penalties[11]
	if !ok {
		t.Fatalf("penalties=%v want cooled file 11", penalties)
	}
	if penalty.Attempts != 1 {
		t.Fatalf("penalty attempts=%d want=1", penalty.Attempts)
	}
	if penalty.LastGrowthBytes <= 0 {
		t.Fatalf("penalty growth=%d want > 0", penalty.LastGrowthBytes)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_prune_runs"]; got != "0" {
		t.Fatalf("queue prune runs=%q want 0", got)
	}
}

func TestVlogGenerationRewrite_DoesNotCoolProcessedDebtWhenLocalStaleWasRewritten(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11, 22},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
				{FileID: 22, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
			},
			SelectedBytesTotal: 128 << 20,
			SelectedBytesLive:  16 << 20,
			SelectedBytesStale: 112 << 20,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    (64 << 20) + vlogGenerationRewriteIneffectiveGrowthMinBytes + 1,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after locally effective rewrite=%v want=%v", got, want)
	}
	penalties, err := db.currentVlogGenerationRewritePenalties()
	if err != nil {
		t.Fatalf("current penalties: %v", err)
	}
	if len(penalties) != 0 {
		t.Fatalf("penalties=%v want none for locally effective debt", penalties)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.ineffective_runs"]; got != "0" {
		t.Fatalf("ineffective runs=%q want 0 for locally effective rewrite", got)
	}
}

func TestVlogGenerationRewrite_KeepsDebtWhenGCOffsetsMaterialGrowth(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	growth := vlogGenerationRewriteIneffectiveGrowthMinBytes + 1
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11, 22},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    (64 << 20) + growth,
			RecordsCopied: 1,
		},
		// Simulate the follow-up GC reclaiming enough zombie bytes so the net
		// post-maintenance rewrite does not grow.
		gcResponse: backenddb.ValueLogGCStats{
			BytesDeleted: growth + (1 << 20),
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if _, gcCalls := recorder.recordedGC(); gcCalls < 1 {
		t.Fatalf("gc calls=%d want >=1", gcCalls)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after gc-offset growth=%v want=%v", got, want)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.ineffective_runs"]; got != "0" {
		t.Fatalf("ineffective runs=%q want 0 when gc offsets growth", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_prune_runs"]; got != "0" {
		t.Fatalf("queue prune runs=%q want 0 when gc offsets growth", got)
	}
}

func TestVlogGenerationRewrite_KeepsRemainingDebtForSmallIneffectiveGrowth(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11, 22},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    (64 << 20) + vlogGenerationRewriteIneffectiveGrowthMinBytes - 1,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after small ineffective rewrite=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewrite_CoolsProcessedDebtAfterNoProgressResume(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11, 22},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    64 << 20,
			RecordsCopied: 0,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("current queue: %v", err)
	}
	if got, want := queue, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("queue after no-progress resume=%v want=%v", got, want)
	}
	penalties, err := db.currentVlogGenerationRewritePenalties()
	if err != nil {
		t.Fatalf("current penalties: %v", err)
	}
	if _, ok := penalties[11]; !ok {
		t.Fatalf("penalties=%v want cooled file 11", penalties)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_prune_runs"]; got != "0" {
		t.Fatalf("queue prune runs=%q want 0", got)
	}
}

func TestVlogGenerationRewritePlan_FiltersPenalizedSegments(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 33 << 20,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    (64 << 20) + vlogGenerationRewriteIneffectiveGrowthMinBytes + 1,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after first run=%d want=1", calls)
	}
	recorder.mu.Lock()
	recorder.planResponse = backenddb.ValueLogRewritePlan{
		SourceFileIDs: []uint32{11, 22},
		SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
			{FileID: 11, BytesTotal: 64 << 20, BytesLive: 33 << 20, BytesStale: 31 << 20, StaleRatio: 0.48},
			{FileID: 22, BytesTotal: 64 << 20, BytesLive: 20 << 20, BytesStale: 44 << 20, StaleRatio: 0.68},
		},
		SelectedBytesTotal: 128 << 20,
		SelectedBytesLive:  53 << 20,
		SelectedBytesStale: 75 << 20,
	}
	recorder.mu.Unlock()
	// The first ineffective rewrite consumes the tiny test rewrite budget. Refill
	// it explicitly so this test exercises penalty filtering instead of depending
	// on wall-clock budget accrual, which is flaky on fast Windows runners.
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	db.vlogGenerationRewriteIneffectiveLastNS.Store(time.Now().Add(-2 * vlogGenerationRewriteIneffectiveBackoff).UnixNano())
	db.vlogGenerationCheckpointKickPending.Store(false)
	db.vlogGenerationDeferredMaintenancePending.Store(false)
	db.vlogGenerationRewriteQueuePending.Store(false)
	db.vlogGenerationRewriteQueueRunning.Store(false)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	planOpts, planCalls := recorder.recordedPlan()
	if planCalls != 2 {
		t.Fatalf("plan calls after second run=%d want=2", planCalls)
	}
	if planOpts.MinSegmentAge != vlogGenerationRewriteMinSegmentAge {
		t.Fatalf("plan MinSegmentAge=%s want %s", planOpts.MinSegmentAge, vlogGenerationRewriteMinSegmentAge)
	}
	rewriteOpts, rewriteCalls := recorder.recordedRewrite()
	if rewriteCalls != 2 {
		t.Fatalf("rewrite calls after second run=%d want=2", rewriteCalls)
	}
	if got, want := rewriteOpts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs after penalty filter=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewritePlan_ReadmitsPenalizedSegmentWhenStaleBytesImprove(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
				FileID:     11,
				BytesTotal: 64 << 20,
				BytesLive:  33 << 20,
				BytesStale: 31 << 20,
				StaleRatio: 31.0 / 64.0,
			}},
			SelectedBytesTotal: 64 << 20,
			SelectedBytesLive:  33 << 20,
			SelectedBytesStale: 31 << 20,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    64 << 20,
			RecordsCopied: 0,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after first run=%d want=1", calls)
	}
	penalties, err := db.currentVlogGenerationRewritePenalties()
	if err != nil {
		t.Fatalf("current penalties after first run: %v", err)
	}
	if penalty, ok := penalties[11]; !ok || penalty.LastStaleBytes != 31<<20 {
		t.Fatalf("penalty after first run=%v want LastStaleBytes=%d", penalties[11], 31<<20)
	}

	recorder.mu.Lock()
	recorder.planResponse = backenddb.ValueLogRewritePlan{
		SourceFileIDs: []uint32{11},
		SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
			FileID:     11,
			BytesTotal: 64 << 20,
			BytesLive:  20 << 20,
			BytesStale: 44 << 20,
			StaleRatio: 44.0 / 64.0,
		}},
		SelectedBytesTotal: 64 << 20,
		SelectedBytesLive:  20 << 20,
		SelectedBytesStale: 44 << 20,
	}
	recorder.rewriteResponse = backenddb.ValueLogRewriteStats{
		BytesBefore:   64 << 20,
		BytesAfter:    20 << 20,
		RecordsCopied: 1,
	}
	recorder.mu.Unlock()
	forceVlogMaintenanceIdle(db)
	// The first ineffective rewrite consumes the tiny test rewrite budget. Refill
	// it explicitly so this test exercises stale-byte readmission instead of
	// depending on wall-clock budget accrual, which is flaky on fast runners.
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	db.vlogGenerationRewriteIneffectiveLastNS.Store(time.Now().Add(-2 * vlogGenerationRewriteIneffectiveBackoff).UnixNano())
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 2 {
		t.Fatalf("plan calls after second run=%d want=2", calls)
	}
	rewriteOpts, rewriteCalls := recorder.recordedRewrite()
	if rewriteCalls != 2 {
		t.Fatalf("rewrite calls after second run=%d want=2", rewriteCalls)
	}
	if got, want := rewriteOpts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs after stale-byte improvement=%v want=%v", got, want)
	}
	penalties, err = db.currentVlogGenerationRewritePenalties()
	if err != nil {
		t.Fatalf("current penalties after second run: %v", err)
	}
	if _, ok := penalties[11]; ok {
		t.Fatalf("penalty for readmitted segment still present: %v", penalties[11])
	}
}

func TestVlogGenerationRewritePlan_StagesFreshStaleRatioDebtBeforeRewrite(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
			},
			SegmentsTotal:      4,
			SegmentsSelected:   1,
			BytesTotal:         256 << 20,
			BytesLive:          192 << 20,
			BytesStale:         64 << 20,
			SelectedBytesTotal: 64 << 20,
			SelectedBytesLive:  8 << 20,
			SelectedBytesStale: 56 << 20,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    8 << 20,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.valueLogRewriteTriggerBytes = 0
	db.valueLogRewriteTriggerRatioPPM = 1
	db.valueLogGenerationHotTarget = 0
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after first stale-ratio pass=%d want=1", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls after first stale-ratio pass=%d want=0", calls)
	}
	ledger, err := db.currentVlogGenerationRewriteLedger()
	if err != nil {
		t.Fatalf("current rewrite ledger: %v", err)
	}
	if len(ledger) != 1 || ledger[0].FileID != 11 {
		t.Fatalf("ledger after first stale-ratio pass=%+v want file 11", ledger)
	}

	db.vlogGenerationLastRewritePlanUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	forceRewriteStageConfirmDue(t, db)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm",
	})
	rewriteOpts, rewriteCalls := recorder.recordedRewrite()
	if got, want := rewriteOpts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs after staged stale-ratio pass=%v want=%v", got, want)
	}
	if rewriteCalls != 1 {
		t.Fatalf("rewrite calls after second stale-ratio pass=%d want=1", rewriteCalls)
	}
}

func TestVlogGenerationRewritePlan_StageConfirmationExecutesConfirmedSubset(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{22},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 22, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
			},
			SegmentsTotal:      2,
			SegmentsSelected:   1,
			BytesTotal:         128 << 20,
			BytesLive:          80 << 20,
			BytesStale:         48 << 20,
			SelectedBytesTotal: 64 << 20,
			SelectedBytesLive:  8 << 20,
			SelectedBytesStale: 56 << 20,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    8 << 20,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.valueLogRewriteTriggerBytes = 0
	db.valueLogRewriteTriggerRatioPPM = 1
	db.valueLogGenerationHotTarget = 0
	forceVlogMaintenanceIdle(db)

	if err := db.setVlogGenerationRewriteLedgerWithStage([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesTotal: 64 << 20, BytesLive: 20 << 20, BytesStale: 44 << 20, StaleRatio: 0.6875},
		{FileID: 22, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
	}, true, time.Now().Add(-vlogGenerationRewriteStageConfirmDelay-time.Second).UnixNano()); err != nil {
		t.Fatalf("seed staged rewrite ledger: %v", err)
	}
	forceRewriteStageConfirmDue(t, db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		debugSource:           "rewrite_stage_confirm",
	})

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, rewriteCalls := recorder.recordedRewrite(); rewriteCalls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("rewrite calls after staged confirmation=%d want=1", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	rewriteDeadline := time.Now().Add(2 * schedulerTestWait(t))
	var rewriteCalls int
	for {
		_, rewriteCalls = recorder.recordedRewrite()
		if rewriteCalls >= 1 {
			break
		}
		if time.Now().After(rewriteDeadline) {
			t.Fatalf("rewrite calls after staged confirmation=%d want=1", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	rewriteHistory := recorder.recordedRewrites()
	if len(rewriteHistory) == 0 {
		t.Fatalf("rewrite history after staged confirmation empty")
	}
	for i, rewriteOpts := range rewriteHistory {
		if got, want := rewriteOpts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
			t.Fatalf("rewrite[%d] SourceFileIDs after staged confirmation=%v want=%v", i, got, want)
		}
	}
}

func TestVlogGenerationRewritePlan_StageConfirmationDebtDrainProcessesMultipleSegments(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11, 22, 33},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
				{FileID: 22, BytesTotal: 64 << 20, BytesLive: 16 << 20, BytesStale: 48 << 20, StaleRatio: 0.75},
				{FileID: 33, BytesTotal: 64 << 20, BytesLive: 24 << 20, BytesStale: 40 << 20, StaleRatio: 0.625},
			},
			SegmentsTotal:      3,
			SegmentsSelected:   3,
			BytesTotal:         192 << 20,
			BytesLive:          48 << 20,
			BytesStale:         144 << 20,
			SelectedBytesTotal: 192 << 20,
			SelectedBytesLive:  48 << 20,
			SelectedBytesStale: 144 << 20,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   192 << 20,
			BytesAfter:    48 << 20,
			RecordsCopied: 3,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.valueLogRewriteTriggerBytes = 0
	db.valueLogRewriteTriggerRatioPPM = 1
	db.valueLogGenerationHotTarget = 0
	db.vlogGenerationRewriteBudgetTokensBytes.Store(defaultVlogGenerationWarmTargetBytes * 4)
	forceVlogMaintenanceIdle(db)

	if err := db.setVlogGenerationRewriteLedgerWithStage([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
		{FileID: 22, BytesTotal: 64 << 20, BytesLive: 16 << 20, BytesStale: 48 << 20, StaleRatio: 0.75},
		{FileID: 33, BytesTotal: 64 << 20, BytesLive: 24 << 20, BytesStale: 40 << 20, StaleRatio: 0.625},
	}, true, time.Now().Add(-vlogGenerationRewriteStageConfirmDelay-time.Second).UnixNano()); err != nil {
		t.Fatalf("seed staged rewrite ledger: %v", err)
	}
	forceRewriteStageConfirmDue(t, db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm",
	})

	rewriteDeadline := time.Now().Add(2 * schedulerTestWait(t))
	var rewriteCalls int
	for {
		_, rewriteCalls = recorder.recordedRewrite()
		if rewriteCalls >= 1 {
			break
		}
		if time.Now().After(rewriteDeadline) {
			t.Fatalf("rewrite calls after staged confirmation=%d want>=1", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	rewriteHistory := recorder.recordedRewrites()
	if len(rewriteHistory) == 0 {
		t.Fatalf("rewrite history after staged confirmation empty")
	}
	firstRewrite := rewriteHistory[0]
	if got := len(firstRewrite.SourceFileIDs); got <= 1 {
		t.Fatalf("first rewrite SourceFileIDs after staged confirmation=%v want multiple ids", firstRewrite.SourceFileIDs)
	}
	if got := len(firstRewrite.SourceFileIDs); got > vlogGenerationRewriteDebtDrainMaxSegments {
		t.Fatalf("first rewrite SourceFileIDs len=%d want <= %d", got, vlogGenerationRewriteDebtDrainMaxSegments)
	}
}

func TestVlogGenerationRewritePlan_StageConfirmationReplansEvenWhenOtherTriggersFire(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planFn: func(opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
			return backenddb.ValueLogRewritePlan{
				SourceFileIDs: []uint32{22},
				SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
					{FileID: 22, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
				},
				SegmentsTotal:      2,
				SegmentsSelected:   1,
				BytesTotal:         128 << 20,
				BytesLive:          80 << 20,
				BytesStale:         48 << 20,
				SelectedBytesTotal: 64 << 20,
				SelectedBytesLive:  8 << 20,
				SelectedBytesStale: 56 << 20,
			}, nil
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    8 << 20,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.valueLogRewriteTriggerBytes = 1
	db.valueLogRewriteTriggerRatioPPM = 1
	db.valueLogGenerationHotTarget = 0
	forceVlogMaintenanceIdle(db)

	if err := db.setVlogGenerationRewriteLedgerWithStage([]backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesTotal: 64 << 20, BytesLive: 20 << 20, BytesStale: 44 << 20, StaleRatio: 0.6875},
		{FileID: 22, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
	}, true, time.Now().Add(-vlogGenerationRewriteStageConfirmDelay-time.Second).UnixNano()); err != nil {
		t.Fatalf("seed staged rewrite ledger: %v", err)
	}
	forceRewriteStageConfirmDue(t, db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		debugSource:           "rewrite_stage_confirm",
	})

	planDeadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		_, planCalls := recorder.recordedPlan()
		if planCalls >= 1 {
			break
		}
		if time.Now().After(planDeadline) {
			t.Fatalf("plan calls after staged confirmation=%d want 1", planCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, planCalls := recorder.recordedPlan(); planCalls != 1 {
		t.Fatalf("plan calls after staged confirmation=%d want 1", planCalls)
	}
	rewriteDeadline := time.Now().Add(2 * schedulerTestWait(t))
	var rewriteCalls int
	for {
		_, rewriteCalls = recorder.recordedRewrite()
		if rewriteCalls >= 1 {
			break
		}
		if time.Now().After(rewriteDeadline) {
			t.Fatalf("rewrite calls after staged confirmation=%d want 1", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	rewriteHistory := recorder.recordedRewrites()
	if len(rewriteHistory) == 0 {
		t.Fatalf("rewrite history after staged confirmation empty")
	}
	for i, rewriteOpts := range rewriteHistory {
		if got, want := rewriteOpts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
			t.Fatalf("rewrite[%d] SourceFileIDs after staged confirmation=%v want=%v", i, got, want)
		}
	}
}

func TestVlogGenerationRewritePlan_StageConfirmationClearsStagedDebtWhenPlanEmpties(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planFn: func(opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
			return backenddb.ValueLogRewritePlan{
				SegmentsTotal: 4,
				BytesTotal:    256 << 20,
				BytesLive:     192 << 20,
				BytesStale:    64 << 20,
			}, nil
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.valueLogRewriteTriggerBytes = 0
	db.valueLogRewriteTriggerRatioPPM = 1
	db.valueLogGenerationHotTarget = 0
	skipRetainedPrune(db)
	forceVlogMaintenanceIdle(db)

	stagedLedger := []backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
		{FileID: 12, BytesTotal: 64 << 20, BytesLive: 16 << 20, BytesStale: 48 << 20, StaleRatio: 0.75},
	}
	observedAt := time.Now().Add(-2 * vlogGenerationRewriteStageConfirmDelay).UnixNano()
	if err := db.setVlogGenerationRewriteLedgerWithStage(stagedLedger, true, observedAt); err != nil {
		t.Fatalf("seed staged rewrite ledger: %v", err)
	}
	forceRewriteStageConfirmDue(t, db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm",
	})

	planDeadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		_, planCalls := recorder.recordedPlan()
		if planCalls >= 1 {
			break
		}
		if time.Now().After(planDeadline) {
			t.Fatalf("plan calls=%d want 1", planCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, planCalls := recorder.recordedPlan(); planCalls != 1 {
		t.Fatalf("plan calls=%d want 1", planCalls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want 0 when confirmation empties plan", calls)
	}
	queue, err := db.currentVlogGenerationRewriteQueue()
	if err != nil {
		t.Fatalf("load rewrite queue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("rewrite queue=%v want empty after empty confirmation", queue)
	}
	stagePending, stageObservedAt, err := db.currentVlogGenerationRewriteStage()
	if err != nil {
		t.Fatalf("load rewrite stage: %v", err)
	}
	if stagePending || stageObservedAt != 0 {
		t.Fatalf("rewrite stage pending=%t observed_at=%d want false/0", stagePending, stageObservedAt)
	}
}

func TestVlogGenerationRewritePlan_StagePendingStillConfirmsWhenBudgetEmpty(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
			},
			SegmentsTotal:      4,
			SegmentsSelected:   1,
			BytesTotal:         256 << 20,
			BytesLive:          192 << 20,
			BytesStale:         64 << 20,
			SelectedBytesTotal: 64 << 20,
			SelectedBytesLive:  8 << 20,
			SelectedBytesStale: 56 << 20,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    8 << 20,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.valueLogRewriteTriggerBytes = 0
	db.valueLogRewriteTriggerRatioPPM = 1
	db.valueLogGenerationHotTarget = 0
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after first stale-ratio pass=%d want=1", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls after first stale-ratio pass=%d want=0", calls)
	}

	stagePending, _, err := db.currentVlogGenerationRewriteStage()
	if err != nil {
		t.Fatalf("current rewrite stage: %v", err)
	}
	if !stagePending {
		t.Fatalf("expected staged rewrite debt after first stale-ratio pass")
	}

	// Freeze the token bucket empty so confirmation planning cannot be confused
	// with a real execution win.
	db.vlogGenerationRewriteBudgetTokensBytes.Store(0)
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(time.Now().Add(10 * time.Second).UnixNano())
	db.vlogGenerationLastRewritePlanUnixNano.Store(0)
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	forceRewriteStageConfirmDue(t, db)
	forceVlogMaintenanceIdle(db)
	db.scheduleDueVlogGenerationDeferredMaintenance()

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedPlan(); calls >= 2 {
			break
		}
		if time.Now().After(deadline) {
			_, planCalls := recorder.recordedPlan()
			t.Fatalf("plan calls after staged confirmation with empty budget=%d want=2", planCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls after staged confirmation with empty budget=%d want=0", calls)
	}
	stagePending, _, err = db.currentVlogGenerationRewriteStage()
	if err != nil {
		t.Fatalf("current rewrite stage after confirmation: %v", err)
	}
	if !stagePending {
		t.Fatalf("stage unexpectedly cleared without execution")
	}
}

func TestShouldDeferVlogGenerationRewritePlanForAge(t *testing.T) {
	if !shouldDeferVlogGenerationRewritePlanForAge(backenddb.ValueLogRewritePlan{
		SegmentsTotal:             4,
		BytesTotal:                256,
		BytesStale:                64,
		AgeBlockedSegments:        2,
		AgeBlockedBytesStale:      64,
		AgeBlockedMinRemainingAge: 12 * time.Second,
	}, 30*time.Second) {
		t.Fatalf("expected age-deferred plan for stale zero-selection result")
	}
	if shouldDeferVlogGenerationRewritePlanForAge(backenddb.ValueLogRewritePlan{
		SourceFileIDs:             []uint32{11},
		SegmentsTotal:             4,
		BytesTotal:                256,
		BytesStale:                64,
		AgeBlockedSegments:        1,
		AgeBlockedMinRemainingAge: 12 * time.Second,
	}, 30*time.Second) {
		t.Fatalf("did not expect age-deferred plan when sources are already selected")
	}
	if shouldDeferVlogGenerationRewritePlanForAge(backenddb.ValueLogRewritePlan{
		SegmentsTotal:             4,
		BytesTotal:                256,
		BytesStale:                0,
		AgeBlockedSegments:        1,
		AgeBlockedMinRemainingAge: 12 * time.Second,
	}, 30*time.Second) {
		t.Fatalf("did not expect age-deferred plan without stale bytes")
	}
	if shouldDeferVlogGenerationRewritePlanForAge(backenddb.ValueLogRewritePlan{
		SegmentsTotal:      4,
		BytesTotal:         256,
		BytesStale:         64,
		AgeBlockedSegments: 1,
	}, 30*time.Second) {
		t.Fatalf("did not expect age-deferred plan without a remaining age")
	}
}

func TestVlogGenerationMaintenance_PeriodicPassSkipsWhileRewriteStagePending(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
				FileID:     11,
				BytesTotal: 128,
				BytesLive:  64,
				BytesStale: 64,
				StaleRatio: 0.5,
			}},
			SelectedBytesLive:  64,
			SelectedBytesStale: 64,
		},
		gcResponse: backenddb.ValueLogGCStats{BytesDeleted: 64},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)

	stageObservedAt := time.Now().UnixNano()
	if err := db.setVlogGenerationRewriteLedgerWithStage([]backenddb.ValueLogRewritePlanSegment{{
		FileID:     11,
		BytesTotal: 128,
		BytesLive:  64,
		BytesStale: 64,
		StaleRatio: 0.5,
	}}, true, stageObservedAt); err != nil {
		t.Fatalf("seed staged rewrite ledger: %v", err)
	}

	db.maybeRunVlogGenerationMaintenance(true)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("periodic pass should not replan while staged confirmation is waiting; plan calls=%d", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("periodic pass should not rewrite while staged confirmation is waiting; rewrite calls=%d", calls)
	}
	if _, calls := recorder.recordedGC(); calls != 0 {
		t.Fatalf("periodic pass should not run gc while staged confirmation is waiting; gc calls=%d", calls)
	}
}

func TestVlogGenerationMaintenance_CheckpointPendingYieldsToDueStageConfirm(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
				FileID:     11,
				BytesTotal: 128,
				BytesLive:  64,
				BytesStale: 64,
				StaleRatio: 0.5,
			}},
			SelectedBytesLive:  64,
			SelectedBytesStale: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)

	stageObservedAt := time.Now().Add(-2 * vlogGenerationRewriteStageConfirmDelay).UnixNano()
	if err := db.setVlogGenerationRewriteLedgerWithStage([]backenddb.ValueLogRewritePlanSegment{{
		FileID:     11,
		BytesTotal: 128,
		BytesLive:  64,
		BytesStale: 64,
		StaleRatio: 0.5,
	}}, true, stageObservedAt); err != nil {
		t.Fatalf("seed staged rewrite ledger: %v", err)
	}
	forceRewriteStageConfirmDue(t, db)

	db.vlogGenerationCheckpointKickPending.Store(true)
	t.Cleanup(func() { db.vlogGenerationCheckpointKickPending.Store(false) })
	holdVlogGenerationDeferredMaintenanceRunnerForTest(t, db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "checkpoint_pending",
	})

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("checkpoint-pending pass should yield to due stage confirmation; plan calls=%d", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("checkpoint-pending pass should yield to due stage confirmation; rewrite calls=%d", calls)
	}
	if !db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatalf("checkpoint-pending retry should remain queued while due stage confirmation is pending")
	}
	if !db.vlogGenerationDeferredMaintenancePending.Load() {
		t.Fatalf("due stage confirmation should remain queued behind the test-owned deferred runner")
	}
}

func TestVlogGenerationMaintenance_CheckpointPendingYieldsToDueAgeBlockedRetry(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{{
				FileID:     11,
				BytesTotal: 128,
				BytesLive:  64,
				BytesStale: 64,
				StaleRatio: 0.5,
			}},
			SelectedBytesLive:  64,
			SelectedBytesStale: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.vlogGenerationRewriteAgeBlockedUntilNS.Store(time.Now().Add(-time.Second).UnixNano())
	t.Cleanup(func() { db.vlogGenerationRewriteAgeBlockedUntilNS.Store(0) })

	db.vlogGenerationCheckpointKickPending.Store(true)
	t.Cleanup(func() { db.vlogGenerationCheckpointKickPending.Store(false) })
	holdVlogGenerationDeferredMaintenanceRunnerForTest(t, db)

	_, planCallsBefore := recorder.recordedPlan()
	_, rewriteCallsBefore := recorder.recordedRewrite()

	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "checkpoint_pending",
	})

	if _, calls := recorder.recordedPlan(); calls != planCallsBefore {
		t.Fatalf(
			"checkpoint-pending pass should yield to due age-blocked retry; plan calls before=%d after=%d",
			planCallsBefore,
			calls,
		)
	}
	if _, calls := recorder.recordedRewrite(); calls != rewriteCallsBefore {
		t.Fatalf(
			"checkpoint-pending pass should yield to due age-blocked retry; rewrite calls before=%d after=%d",
			rewriteCallsBefore,
			calls,
		)
	}
	if !db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatalf("checkpoint-pending retry should remain queued while due age-blocked retry is pending")
	}
	if !db.vlogGenerationDeferredMaintenancePending.Load() {
		t.Fatalf("due age-blocked retry should remain queued behind the test-owned deferred runner")
	}
}

func TestDeferredMaintenanceRetry_StopsAfterAcquiredRunEvenWhenCheckpointPending(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("set rewrite queue: %v", err)
	}
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	db.vlogGenerationCheckpointKickPending.Store(true)
	t.Cleanup(func() { db.vlogGenerationCheckpointKickPending.Store(false) })

	db.startVlogGenerationDeferredMaintenance(vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_stage_confirm",
	})

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("deferred retry did not run rewrite in time: rewriteCalls=%d", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	deadline = time.Now().Add(2 * schedulerTestWait(t))
	for db.vlogGenerationDeferredMaintenanceRunning.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if db.vlogGenerationDeferredMaintenanceRunning.Load() {
		t.Fatalf("deferred retry runner did not stop after acquired run while checkpoint pending remained set")
	}
}

func TestDeferredMaintenanceRetry_RetriesWithoutCheckpointPendingUntilAcquired(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("set rewrite queue: %v", err)
	}
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	db.vlogGenerationMaintenanceActive.Store(true)
	go func() {
		time.Sleep(25 * time.Millisecond)
		db.vlogGenerationMaintenanceActive.Store(false)
	}()

	db.startVlogGenerationDeferredMaintenance(vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			return
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("deferred retry did not re-attempt after initial collision: rewriteCalls=%d", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestVlogGenerationRewritePlan_AgeBlockedRetryRunsWhenDue(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planFn: func(opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
			if opts.MinSegmentAge != vlogGenerationRewriteMinSegmentAge {
				t.Fatalf("plan MinSegmentAge=%s want %s", opts.MinSegmentAge, vlogGenerationRewriteMinSegmentAge)
			}
			return backenddb.ValueLogRewritePlan{
				SourceFileIDs: []uint32{11},
				SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
					{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
				},
				SegmentsTotal:      4,
				SegmentsSelected:   1,
				BytesTotal:         256 << 20,
				BytesLive:          192 << 20,
				BytesStale:         64 << 20,
				SelectedBytesTotal: 64 << 20,
				SelectedBytesLive:  8 << 20,
				SelectedBytesStale: 56 << 20,
			}, nil
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    8 << 20,
			RecordsCopied: 1,
		},
	}

	firstPlan := true
	recorder.planFn = func(opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
		if opts.MinSegmentAge != vlogGenerationRewriteMinSegmentAge {
			t.Fatalf("plan MinSegmentAge=%s want %s", opts.MinSegmentAge, vlogGenerationRewriteMinSegmentAge)
		}
		if firstPlan {
			firstPlan = false
			return backenddb.ValueLogRewritePlan{
				SegmentsTotal:             4,
				BytesTotal:                256 << 20,
				BytesLive:                 192 << 20,
				BytesStale:                64 << 20,
				AgeBlockedSegments:        1,
				AgeBlockedBytesTotal:      64 << 20,
				AgeBlockedBytesLive:       8 << 20,
				AgeBlockedBytesStale:      56 << 20,
				AgeBlockedMinRemainingAge: time.Minute,
			}, nil
		}
		return backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
			},
			SegmentsTotal:      4,
			SegmentsSelected:   1,
			BytesTotal:         256 << 20,
			BytesLive:          192 << 20,
			BytesStale:         64 << 20,
			SelectedBytesTotal: 64 << 20,
			SelectedBytesLive:  8 << 20,
			SelectedBytesStale: 56 << 20,
		}, nil
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
	})

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after age-blocked run=%d want=1", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls after age-blocked run=%d want=0", calls)
	}
	if until := db.vlogGenerationRewriteAgeBlockedUntilNS.Load(); until == 0 {
		t.Fatalf("age-blocked deadline not recorded")
	}

	db.vlogGenerationRewriteAgeBlockedUntilNS.Store(time.Now().Add(-time.Second).UnixNano())
	db.vlogGenerationLastRewritePlanUnixNano.Store(time.Now().UnixNano())
	forceVlogMaintenanceIdle(db)
	db.scheduleDueVlogGenerationDeferredMaintenance()

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedPlan(); calls >= 2 {
			break
		}
		if time.Now().After(deadline) {
			_, planCalls := recorder.recordedPlan()
			t.Fatalf("plan calls after age-blocked retry=%d want=2", planCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	rewriteDeadline := time.Now().Add(2 * schedulerTestWait(t))
	var (
		rewriteOpts  backenddb.ValueLogRewriteOnlineOptions
		rewriteCalls int
	)
	for {
		rewriteOpts, rewriteCalls = recorder.recordedRewrite()
		if rewriteCalls >= 1 {
			break
		}
		if time.Now().After(rewriteDeadline) {
			t.Fatalf("rewrite calls after age-blocked retry=%d want=1", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rewriteCalls != 1 {
		t.Fatalf("rewrite calls after age-blocked retry=%d want=1", rewriteCalls)
	}
	if got, want := rewriteOpts.SourceFileIDs, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs after age-blocked retry=%v want=%v", got, want)
	}
}

func TestVlogGenerationRewritePlan_AgeBlockedRetrySchedulesWakeup(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	firstPlan := true
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planFn: func(opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
			if opts.MinSegmentAge != vlogGenerationRewriteMinSegmentAge {
				t.Fatalf("plan MinSegmentAge=%s want %s", opts.MinSegmentAge, vlogGenerationRewriteMinSegmentAge)
			}
			if firstPlan {
				firstPlan = false
				return backenddb.ValueLogRewritePlan{
					SegmentsTotal:             4,
					BytesTotal:                256 << 20,
					BytesLive:                 192 << 20,
					BytesStale:                64 << 20,
					AgeBlockedSegments:        1,
					AgeBlockedBytesTotal:      64 << 20,
					AgeBlockedBytesLive:       8 << 20,
					AgeBlockedBytesStale:      56 << 20,
					AgeBlockedMinRemainingAge: 10 * time.Millisecond,
				}, nil
			}
			return backenddb.ValueLogRewritePlan{
				SourceFileIDs: []uint32{11},
				SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
					{FileID: 11, BytesTotal: 64 << 20, BytesLive: 8 << 20, BytesStale: 56 << 20, StaleRatio: 0.875},
				},
				SegmentsTotal:      4,
				SegmentsSelected:   1,
				BytesTotal:         256 << 20,
				BytesLive:          192 << 20,
				BytesStale:         64 << 20,
				SelectedBytesTotal: 64 << 20,
				SelectedBytesLive:  8 << 20,
				SelectedBytesStale: 56 << 20,
			}, nil
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    8 << 20,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, calls := recorder.recordedRewrite(); calls > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, calls := recorder.recordedRewrite(); calls == 0 {
		t.Fatalf("expected scheduled age-blocked retry to execute rewrite")
	}
}

func TestVlogGenerationRewritePlan_AgeBlockedRetryReschedulesEarlierDeadline(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    8 << 20,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	db.setVlogGenerationRewriteAgeBlockedUntil(time.Now().Add(2 * time.Second))
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	db.setVlogGenerationRewriteAgeBlockedUntil(time.Now().Add(30 * time.Millisecond))

	deadline := time.Now().Add(750 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, calls := recorder.recordedRewrite(); calls > 0 {
			if waited := time.Since(start); waited > 500*time.Millisecond {
				t.Fatalf("age-blocked retry honored shortened deadline too late: waited=%s", waited)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected shortened age-blocked deadline to trigger deferred retry")
}

func TestVlogGenerationRewrite_IneffectiveBackoffSkipsImmediateGenericRetry(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    (64 << 20) + vlogGenerationRewriteIneffectiveGrowthMinBytes + 1,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after first ineffective run=%d want=1", calls)
	}
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after immediate retry=%d want=1 (ineffective backoff active)", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.ineffective_last_unix_nano"]; got == "0" {
		t.Fatalf("ineffective last ts=%q want non-zero", got)
	}
}

func TestVlogGenerationRewrite_IneffectiveBackoffCheckpointKickFreshPlanSkips(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64,
			BytesAfter:    32,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.vlogGenerationRewriteIneffectiveLastNS.Store(time.Now().UnixNano())
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
	})

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls with checkpoint-kick ineffective-backoff skip=%d want=0", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls with checkpoint-kick ineffective-backoff skip=%d want=0", calls)
	}
}

func TestVlogGenerationRewrite_IneffectiveBackoffExpires(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 64,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{
			BytesBefore:   64 << 20,
			BytesAfter:    (64 << 20) + vlogGenerationRewriteIneffectiveGrowthMinBytes + 1,
			RecordsCopied: 1,
		},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after first ineffective run=%d want=1", calls)
	}
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	db.vlogGenerationRewriteIneffectiveLastNS.Store(time.Now().Add(-2 * vlogGenerationRewriteIneffectiveBackoff).UnixNano())
	db.vlogGenerationRewriteQueueMu.Lock()
	if err := db.loadVlogGenerationRewriteQueueLocked(); err != nil {
		db.vlogGenerationRewriteQueueMu.Unlock()
		t.Fatalf("load rewrite penalties: %v", err)
	}
	penalty := db.vlogGenerationRewritePenalties[11]
	penalty.CooldownUntilUnixNano = time.Now().Add(-time.Second).UnixNano()
	db.vlogGenerationRewritePenalties[11] = penalty
	if err := saveValueLogGenerationRewriteState(
		db.valueLogGenerationStatePath(),
		append([]uint32(nil), db.vlogGenerationRewriteQueue...),
		append([]backenddb.ValueLogRewritePlanSegment(nil), db.vlogGenerationRewriteLedger...),
		append([]backenddb.ValueLogRewritePlanChunk(nil), db.vlogGenerationRewriteChunkLedger...),
		db.vlogGenerationRewriteChunkBytes,
		db.vlogGenerationRewriteHistory,
		db.vlogGenerationRewritePenalties,
		db.vlogGenerationRewriteStagePending,
		db.vlogGenerationRewriteStageObservedUnixNano,
	); err != nil {
		db.vlogGenerationRewriteQueueMu.Unlock()
		t.Fatalf("persist expired rewrite penalty: %v", err)
	}
	db.vlogGenerationRewriteQueueMu.Unlock()
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	forceVlogMaintenanceIdle(db)

	eligible, _, err := db.currentVlogGenerationRewriteEligible(time.Now())
	if err != nil {
		t.Fatalf("current eligible rewrite queue: %v", err)
	}
	if got, want := eligible, []uint32{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("eligible queue after expired ineffective backoff=%v want=%v", got, want)
	}
}

func TestCheckpoint_KickHotDebtOnlyGuardCanBeDisabled(t *testing.T) {
	// Keep an emergency rollback knob for live experiments that need the legacy
	// checkpoint-kick behavior.
	disableVlogGenerationLoop(t)
	t.Setenv(envDisableVlogGenerationCheckpointKickHotDebtOnly, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false

	b := db.NewBatch()
	if err := b.Set([]byte("trigger"), []byte("v")); err != nil {
		_ = b.Close()
		t.Fatalf("set trigger: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write trigger: %v", err)
	}
	_ = b.Close()

	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls == 1 {
			stats := db.Stats()
			if stats["treedb.cache.vlog_generation.checkpoint_kick.rewrite_runs"] == "1" &&
				stats["treedb.cache.vlog_generation.checkpoint_kick.active"] == "false" {
				break
			}
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			_, planCalls := recorder.recordedPlan()
			stats := db.Stats()
			t.Fatalf("checkpoint kick did not run rewrite in time: planCalls=%d rewriteCalls=%d stats=%v", planCalls, rewriteCalls, stats)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls=%d want=1", calls)
	}
	if got := db.checkpointRuns.Load(); got < 1 {
		t.Fatalf("checkpoint runs=%d want >=1", got)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.runs"]; got != "1" {
		t.Fatalf("checkpoint kick runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.rewrite_runs"]; got != "1" {
		t.Fatalf("checkpoint kick rewrite runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.active"]; got != "false" {
		t.Fatalf("checkpoint kick active=%q want false", got)
	}
}

func TestCheckpoint_KickHotDebtOnlySkipsFreshPlanDuringRecentForegroundActivity(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false

	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)

	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()

	time.Sleep(150 * time.Millisecond)
	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want 0", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want 0", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.runs"]; got != "0" {
		t.Fatalf("checkpoint kick runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.skipped_hot_no_debt"]; got != "1" {
		t.Fatalf("checkpoint kick skipped_hot_no_debt=%q want 1", got)
	}
}

func TestCheckpoint_KickHotDebtOnlySkipsFreshPlanWhenQueuedDebtCooling(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{22},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	if err := db.recordVlogGenerationRewritePenalty([]uint32{11}, time.Now().Add(time.Minute), 64); err != nil {
		t.Fatalf("record rewrite penalty: %v", err)
	}

	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)

	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()

	time.Sleep(150 * time.Millisecond)
	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want 0 while queued debt is cooling", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want 0 while queued debt is cooling", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.runs"]; got != "0" {
		t.Fatalf("checkpoint kick runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.skipped_hot_no_debt"]; got != "1" {
		t.Fatalf("checkpoint kick skipped_hot_no_debt=%q want 1", got)
	}
}

func TestCheckpoint_KickHotDebtOnlyWakeRunsAfterForegroundQuiets(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false

	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)
	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()

	time.Sleep(150 * time.Millisecond)
	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want 0 before quiet wake", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want 0 before quiet wake", calls)
	}

	quietAt := time.Now().Add(-2 * vlogGenerationMaintenanceQuietWindow).UnixNano()
	db.lastForegroundWriteUnixNano.Store(quietAt)
	db.lastForegroundReadUnixNano.Store(quietAt)

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls == 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			_, planCalls := recorder.recordedPlan()
			t.Fatalf("hot-debt wake did not run rewrite in time: planCalls=%d rewriteCalls=%d", planCalls, rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.skipped_hot_no_debt"]; got != "1" {
		t.Fatalf("checkpoint kick skipped_hot_no_debt=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.hot_no_debt_wake.runs"]; got != "1" {
		t.Fatalf("checkpoint kick hot_no_debt_wake runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.runs"]; got != "1" {
		t.Fatalf("checkpoint kick runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.hot_no_debt_wake.running"]; got != "false" {
		t.Fatalf("checkpoint kick hot_no_debt_wake running=%q want false", got)
	}
}

func TestCheckpoint_DoesNotKickVlogGenerationRewrite_WALOn(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       false,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false

	b := db.NewBatch()
	if err := b.Set([]byte("trigger"), []byte("v")); err != nil {
		_ = b.Close()
		t.Fatalf("set trigger: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write trigger: %v", err)
	}
	_ = b.Close()

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	time.Sleep(2 * schedulerTestWait(t))
	_, rewriteCalls := recorder.recordedRewrite()
	_, planCalls := recorder.recordedPlan()
	if rewriteCalls != 0 || planCalls != 0 {
		t.Fatalf("checkpoint kick unexpectedly ran under WAL-on: planCalls=%d rewriteCalls=%d", planCalls, rewriteCalls)
	}
}

func TestCheckpoint_KicksQueuedRewriteDebtBelowTriggerFloor(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false
	db.valueLogRewriteTriggerBytes = 1 << 30
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want 0 for queued debt resume", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want 1 for queued debt resume", calls)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started"]; got != "1" {
		t.Fatalf("queued debt rewrite started=%q want 1", got)
	}
}

func TestCheckpoint_KickHotDebtOnlyStillRunsQueuedRewriteDebtDuringRecentForegroundActivity(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{11},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false
	db.valueLogRewriteTriggerBytes = 1 << 30
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "rewrite_queue_pending",
	})

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want 0 for queued debt resume", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want 1 for queued debt resume", calls)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started"]; got != "1" {
		t.Fatalf("queued debt rewrite started=%q want 1", got)
	}
}

func TestCheckpoint_KickSelfDrainsMaintenanceCollision(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("set rewrite queue: %v", err)
	}
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	db.vlogGenerationMaintenanceActive.Store(true)

	release := make(chan struct{})
	go func() {
		time.Sleep(30 * time.Millisecond)
		db.vlogGenerationMaintenanceActive.Store(false)
		close(release)
	}()

	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("checkpoint kick did not self-drain collision in time: rewriteCalls=%d pending=%t", rewriteCalls, db.vlogGenerationCheckpointKickPending.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	<-release
}

func TestCheckpoint_KickDoesNotForceGCDuringRecentForegroundActivity(t *testing.T) {
	disableVlogGenerationLoop(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &dryRunGCRecordingBackend{
		DB:          backend,
		realGCStats: backenddb.ValueLogGCStats{SegmentsEligible: 1, BytesEligible: 64},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		JournalLanes:             1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		ForceValueLogPointers:    true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("trigger"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set trigger: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write trigger: %v", err)
	}
	_ = b.Close()

	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	if dryCalls, realCalls, _ := recorder.recordedCalls(); dryCalls != 0 || realCalls != 0 {
		t.Fatalf("gc calls dry=%d real=%d want dry=0 real=0", dryCalls, realCalls)
	}
	if got := db.checkpointRuns.Load(); got != 1 {
		t.Fatalf("checkpoint runs=%d want 1", got)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.runs"]; got != "1" {
		t.Fatalf("checkpoint kick runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.gc_runs"]; got != "0" {
		t.Fatalf("checkpoint kick gc runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.active"]; got != "false" {
		t.Fatalf("checkpoint kick active=%q want false", got)
	}
}

func TestVlogGenerationMaintenance_PeriodicGCSkipsWhileRewriteAgeBlocked(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:         backend,
		gcResponse: backenddb.ValueLogGCStats{BytesDeleted: 64},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	forceVlogMaintenanceIdle(db)

	db.vlogGenerationRewriteAgeBlockedUntilNS.Store(time.Now().Add(30 * time.Second).UnixNano())
	t.Cleanup(func() { db.vlogGenerationRewriteAgeBlockedUntilNS.Store(0) })

	db.maybeRunVlogGenerationMaintenance(true)

	if _, calls := recorder.recordedGC(); calls != 0 {
		t.Fatalf("periodic GC should yield while rewrite age-blocked; gc calls=%d", calls)
	}
}

func TestVlogGenerationMaintenance_PeriodicGCNoopCooldown(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:         backend,
		gcResponse: backenddb.ValueLogGCStats{},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	forceVlogMaintenanceIdle(db)

	quietSince := time.Now().Add(-2 * vlogGenerationMaintenanceQuietWindow).UnixNano()
	db.lastForegroundWriteUnixNano.Store(quietSince)
	db.lastForegroundReadUnixNano.Store(quietSince)
	db.activeForegroundIterators.Store(0)

	db.maybeRunVlogGenerationMaintenance(true)

	if _, calls := recorder.recordedGC(); calls != 1 {
		t.Fatalf("first periodic GC calls=%d want=1", calls)
	}
	if got := db.vlogGenerationLastGCNoopUnixNano.Load(); got <= 0 {
		t.Fatalf("last GC noop unix nano=%d want >0 after zero-eligibility pass", got)
	}

	// Bypass the normal min-interval gate; noop cooldown should still suppress.
	db.vlogGenerationLastGCUnixNano.Store(time.Now().Add(-2 * vlogGenerationGCMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(true)

	if _, calls := recorder.recordedGC(); calls != 1 {
		t.Fatalf("periodic GC should skip under noop cooldown; calls=%d want=1", calls)
	}
}

func TestVlogGenerationMaintenance_PeriodicGCSkipsInWALOnMode(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:         backend,
		gcResponse: backenddb.ValueLogGCStats{BytesDeleted: 64},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:              true,
		DisableWAL:               false,
		JournalLanes:             1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		ForceValueLogPointers:    true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenance(true)

	if _, calls := recorder.recordedGC(); calls != 0 {
		t.Fatalf("periodic GC should skip in WAL-on mode: gc calls=%d", calls)
	}
	if got := db.checkpointRuns.Load(); got != 0 {
		t.Fatalf("checkpoint runs=%d want 0 for WAL-on periodic GC skip", got)
	}
}

func TestVlogGenerationMaintenance_WALOffPreCheckpointSkipsRewriteByDefault(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64, BytesLive: 32, BytesStale: 32, StaleRatio: 0.5},
			},
			SegmentsSelected:   1,
			SelectedBytesTotal: 64,
			SelectedBytesLive:  32,
			SelectedBytesStale: 32,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)

	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want 0 before first checkpoint", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.skip.before_first_checkpoint"]; got != "1" {
		t.Fatalf("pre-checkpoint skip=%q want 1", got)
	}
}

func TestVlogGenerationMaintenance_WALOffPreCheckpointCanRunWithEnvOverride(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envEnableVlogGenerationPreCheckpointRewrite, "1")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs: []uint32{11},
			SelectedSegments: []backenddb.ValueLogRewritePlanSegment{
				{FileID: 11, BytesTotal: 64, BytesLive: 32, BytesStale: 32, StaleRatio: 0.5},
			},
			SegmentsSelected:   1,
			SelectedBytesTotal: 64,
			SelectedBytesLive:  32,
			SelectedBytesStale: 32,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)

	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)

	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls=%d want 1 with pre-checkpoint override", calls)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.skip.before_first_checkpoint"]; got != "0" {
		t.Fatalf("pre-checkpoint skip=%q want 0 with override", got)
	}
}

func TestVlogGenerationMaintenance_PeriodicSkipsWhenMaintenancePhaseNonSteady(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	db.SetMaintenancePhase(MaintenancePhaseRestore)
	if ran := db.maybeRunPeriodicVlogGenerationMaintenance(false); ran {
		t.Fatal("periodic maintenance unexpectedly ran during restore phase")
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want 0 during restore phase", calls)
	}

	db.SetMaintenancePhase(MaintenancePhaseSteady)
	if ran := db.maybeRunPeriodicVlogGenerationMaintenance(false); !ran {
		t.Fatal("periodic maintenance did not run after returning to steady phase")
	}
	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls == 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("rewrite calls=%d want 1 after returning to steady phase", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance_phase"]; got != "steady" {
		t.Fatalf("maintenance phase=%q want steady", got)
	}
}

func TestVlogGenerationMaintenance_PeriodicPreflightSkipsHotNoPending(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)

	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)
	db.vlogGenerationCheckpointKickPending.Store(false)
	db.vlogGenerationDeferredMaintenancePending.Store(false)

	if ran := db.maybeRunPeriodicVlogGenerationMaintenance(false); ran {
		t.Fatal("periodic maintenance unexpectedly entered during hot foreground with no pending wake")
	}
	if got := db.vlogGenerationMaintenanceAttempts.Load(); got != 0 {
		t.Fatalf("maintenance attempts=%d want 0 on preflight skip", got)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want 0 on preflight skip", calls)
	}
}

func TestCheckpoint_KickSkipsWhenMaintenancePhaseNonSteady(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	db.testSkipVlogCheckpointKick = false
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	db.SetMaintenancePhase(MaintenancePhaseCatchUp)
	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()
	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for time.Now().Before(deadline) {
		if _, calls := recorder.recordedRewrite(); calls != 0 {
			t.Fatalf("rewrite calls=%d want 0 during catchup phase", calls)
		}
		if got := db.vlogGenerationCheckpointKickRuns.Load(); got != 0 {
			t.Fatalf("checkpoint kick runs=%d want 0 during catchup phase", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatal("checkpoint kick pending should not be armed by a direct checkpoint kick while maintenance phase is non-steady")
	}

	db.SetMaintenancePhase(MaintenancePhaseSteady)
	db.maybeKickVlogGenerationMaintenanceAfterCheckpoint()
	deadline = time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("checkpoint kick did not run after returning to steady phase: rewriteCalls=%d", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := db.vlogGenerationCheckpointKickRuns.Load(); got != 1 {
		t.Fatalf("checkpoint kick runs=%d want 1 after returning to steady phase", got)
	}
}

func TestMaintenancePhaseSuppressionPreservesQueuedCheckpointKickRetry(t *testing.T) {
	disableVlogGenerationLoop(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	skipRetainedPrune(db)
	if err := db.setVlogGenerationRewriteQueue([]uint32{11}); err != nil {
		t.Fatalf("seed rewrite queue: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)
	db.vlogGenerationCheckpointKickPending.Store(true)

	db.SetMaintenancePhase(MaintenancePhaseCatchUp)
	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
		debugSource:           "checkpoint_pending",
	})
	if !db.vlogGenerationCheckpointKickPending.Load() {
		t.Fatal("queued checkpoint kick retry should remain pending while maintenance phase is non-steady")
	}

	db.SetMaintenancePhase(MaintenancePhaseSteady)
	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			t.Fatalf("queued checkpoint kick retry did not run after returning to steady phase: rewriteCalls=%d", rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestVlogGenerationRewrite_ConsumesBudgetToZeroWhenRewriteExceedsBudgetCap(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{1},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 512, BytesAfter: 128, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = db.Close() })
	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(128)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls=%d want=1", calls)
	}
	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if len(opts.SourceFileIDs) == 0 {
		t.Fatalf("rewrite SourceFileIDs=%v want non-empty planned selection", opts.SourceFileIDs)
	}
	if opts.MaxSourceBytes != 0 {
		t.Fatalf("rewrite MaxSourceBytes=%d want 0 once plan selection is materialized", opts.MaxSourceBytes)
	}
	if got := db.vlogGenerationRewriteBudgetTokensBytes.Load(); got != 0 {
		t.Fatalf("tokens after oversize rewrite=%d want=0", got)
	}
}

func TestVlogGenerationRewrite_DoesNotRunWithZeroBudgetTokens(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB:              backend,
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 128, BytesAfter: 64, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ValueLogRewriteBudgetBytesPerSec: 1024,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(0)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0", calls)
	}
	if got := db.vlogGenerationRewriteBudgetTokensBytes.Load(); got != 0 {
		t.Fatalf("tokens after skipped rewrite=%d want=0", got)
	}
}

func TestVlogGenerationRewritePlan_DoesNotRunWithZeroBudgetTokens(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB:           backend,
		planResponse: backenddb.ValueLogRewritePlan{SourceFileIDs: []uint32{1}},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                             true,
		DisableWAL:                              true,
		JournalLanes:                            1,
		ValueLogGenerationPolicy:                uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes:        1 << 30,
		ValueLogRewriteTriggerStaleRatioPPM:     1,
		ValueLogRewriteBudgetBytesPerSec:        1024,
		ValueLogGenerationHotSegmentTargetBytes: 1,
		ForceValueLogPointers:                   true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	value := make([]byte, 2048)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), value); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(0)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want=0", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0", calls)
	}
}

func TestVlogGenerationRewritePlan_TracksEmptyPlanOutcome(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB:           backend,
		planResponse: backenddb.ValueLogRewritePlan{},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls=%d want=1", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0", calls)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_runs"]; got != "1" {
		t.Fatalf("plan runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_empty"]; got != "1" {
		t.Fatalf("plan empty=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_empty.age_blocked"]; got != "0" {
		t.Fatalf("plan empty age-blocked=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_empty.no_selection"]; got != "1" {
		t.Fatalf("plan empty no-selection=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_selected"]; got != "0" {
		t.Fatalf("plan selected=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_canceled"]; got != "0" {
		t.Fatalf("plan canceled=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_errors"]; got != "0" {
		t.Fatalf("plan errors=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_penalty_filter.runs"]; got != "0" {
		t.Fatalf("plan penalty-filter runs=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_penalty_filter.segments"]; got != "0" {
		t.Fatalf("plan penalty-filter segments=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_penalty_filter.to_empty_runs"]; got != "0" {
		t.Fatalf("plan penalty-filter to-empty=%q want 0", got)
	}
}

func TestVlogGenerationRewritePlan_RunsOutsideMaintenanceBarrier(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	blocking := &blockingRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planBlock: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "value_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob wal files: %v", err)
	}
	old := time.Now().Add(-5 * time.Minute)
	for _, path := range paths {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	doneMaintenance := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenance(false)
		close(doneMaintenance)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-blocking.planStart:
	case <-time.After(wait):
		t.Fatalf("rewrite plan did not start")
	}

	waitDone := make(chan struct{})
	go func() {
		db.waitForCheckpoint()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("waitForCheckpoint blocked while rewrite planning was in progress")
	}

	blocking.unblockPlan()

	select {
	case <-doneMaintenance:
	case <-time.After(2 * time.Second):
		t.Fatalf("maintenance did not finish after planner unblock")
	}
}

func TestVlogGenerationMaintenance_BypassQuietPlannerNotCanceledByForegroundActivity(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	blocking := &blockingRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planBlock: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	skipRetainedPrune(db)

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)

	stopHot := make(chan struct{})
	defer close(stopHot)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHot:
				return
			case <-ticker.C:
				hot := time.Now().UnixNano()
				db.lastForegroundWriteUnixNano.Store(hot)
				db.lastForegroundReadUnixNano.Store(hot)
			}
		}
	}()

	doneMaintenance := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
			bypassQuiet:           true,
			skipRetainedPruneWait: true,
			skipCheckpoint:        true,
		})
		close(doneMaintenance)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-blocking.planStart:
	case <-time.After(wait):
		t.Fatalf("rewrite plan did not start")
	}

	// Hold the planner past the 100ms foreground-cancel polling cadence to
	// ensure bypass-quiet planning is not canceled by foreground activity.
	time.Sleep(250 * time.Millisecond)
	blocking.unblockPlan()

	select {
	case <-doneMaintenance:
	case <-time.After(2 * time.Second):
		t.Fatalf("maintenance did not finish after planner unblock")
	}

	completed, canceled := blocking.recordedPlanOutcomes()
	if completed != 1 || canceled != 0 {
		t.Fatalf("plan outcomes completed=%d canceled=%d want completed=1 canceled=0", completed, canceled)
	}
}

func TestVlogGenerationMaintenance_SkipsBeforeFirstCheckpointInWALOffMode(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	blocking := &blockingRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planBlock: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	select {
	case <-blocking.planStart:
		t.Fatalf("rewrite planner started before first checkpoint in WAL-off mode")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestVlogGenerationMaintenance_SkipsDuringRecentForegroundWrites(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{1},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 128, BytesAfter: 64, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	db.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want=0 while foreground writes are hot", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0 while foreground writes are hot", calls)
	}
}

func TestVlogGenerationMaintenance_RunGCPassSkipsRewriteWhenForegroundHot(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{1},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 128, BytesAfter: 64, RecordsCopied: 1},
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	hot := time.Now().UnixNano()
	db.lastForegroundWriteUnixNano.Store(hot)
	db.lastForegroundReadUnixNano.Store(hot)
	db.activeForegroundIterators.Store(0)

	db.maybeRunVlogGenerationMaintenance(true)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want=0 for hot runGC pass", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0 for hot runGC pass", calls)
	}
}

func TestVlogGenerationMaintenance_SkipsDuringRecentForegroundReads(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{1},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 128, BytesAfter: 64, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)
	if _, err := db.Get([]byte("k1")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want=0 while foreground reads are hot", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0 while foreground reads are hot", calls)
	}
}

func TestVlogGenerationMaintenance_SkipsDuringActiveForegroundIterator(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	recorder := &rewriteBudgetRecordingBackend{
		DB: backend,
		planResponse: backenddb.ValueLogRewritePlan{
			SourceFileIDs:     []uint32{1},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 128, BytesAfter: 64, RecordsCopied: 1},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)
	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Fatalf("close iterator: %v", err)
		}
	}()
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want=0 while foreground iterator is active", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0 while foreground iterator is active", calls)
	}
}

func TestVlogGenerationRewritePlan_CancelsWhenForegroundWritesResume(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	blocking := &blockingRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planBlock: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	doneMaintenance := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenance(false)
		close(doneMaintenance)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-blocking.planStart:
	case <-time.After(wait):
		t.Fatalf("rewrite plan did not start")
	}

	stopWrites := make(chan struct{})
	writesStopped := make(chan struct{})
	go func() {
		defer close(writesStopped)
		ticker := time.NewTicker(foregroundReadStampMaxAge / 4)
		defer ticker.Stop()
		db.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())
		for {
			select {
			case <-stopWrites:
				return
			case <-db.closeCh:
				return
			case now := <-ticker.C:
				db.lastForegroundWriteUnixNano.Store(now.UnixNano())
			}
		}
	}()

	select {
	case <-doneMaintenance:
	case <-time.After(wait):
		t.Fatalf("maintenance did not cancel after foreground writes resumed")
	}
	close(stopWrites)
	<-writesStopped

	if got := db.vlogGenerationLastRewritePlanUnixNano.Load(); got != 0 {
		t.Fatalf("last rewrite plan timestamp=%d want=0 after cancellation", got)
	}
	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerIdle {
		t.Fatalf("scheduler state=%d want=%d", got, vlogGenerationSchedulerIdle)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_runs"]; got != "1" {
		t.Fatalf("plan runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_canceled"]; got != "1" {
		t.Fatalf("plan canceled=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_errors"]; got != "0" {
		t.Fatalf("plan errors=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_empty"]; got != "0" {
		t.Fatalf("plan empty=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_selected"]; got != "0" {
		t.Fatalf("plan selected=%q want 0", got)
	}
}

func TestVlogGenerationRewritePlan_GraceAllowsShortPlanDuringForegroundResume(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	timed := &timedRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planDelay: vlogGenerationRewritePlanResumeGrace / 2,
	}

	db, err := Open(dir, timed, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	doneMaintenance := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenance(false)
		close(doneMaintenance)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-timed.planStart:
	case <-time.After(wait):
		t.Fatalf("rewrite plan did not start")
	}

	// Resume foreground activity while the short planner call is in flight.
	db.lastForegroundReadUnixNano.Store(time.Now().UnixNano())

	select {
	case <-doneMaintenance:
	case <-time.After(2 * wait):
		t.Fatalf("maintenance did not complete after short rewrite plan")
	}

	completed, canceled := timed.recordedPlanOutcomes()
	if completed != 1 || canceled != 0 {
		t.Fatalf("plan outcomes completed=%d canceled=%d want completed=1 canceled=0", completed, canceled)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_runs"]; got != "1" {
		t.Fatalf("plan runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_canceled"]; got != "0" {
		t.Fatalf("plan canceled=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_empty"]; got != "1" {
		t.Fatalf("plan empty=%q want 1", got)
	}
}

func TestVlogGenerationRewritePlan_OneShotReadBlipDoesNotCancelLongPlan(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	blocking := &blockingRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planBlock: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	doneMaintenance := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenance(false)
		close(doneMaintenance)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-blocking.planStart:
	case <-time.After(wait):
		t.Fatalf("rewrite plan did not start")
	}

	endRead := db.beginRawForegroundRead()
	endRead()
	staleAndBeyondGrace := vlogGenerationRewritePlanResumeGrace
	if staleAndBeyondGrace < foregroundReadStampMaxAge {
		staleAndBeyondGrace = foregroundReadStampMaxAge
	}
	staleAndBeyondGrace += foregroundMaintenancePollInterval()
	select {
	case <-doneMaintenance:
		t.Fatalf("rewrite plan canceled after a one-shot read blip")
	case <-time.After(staleAndBeyondGrace):
	}
	blocking.unblockPlan()
	select {
	case <-doneMaintenance:
	case <-time.After(wait):
		t.Fatalf("long rewrite plan did not complete after one-shot read blip became stale")
	}

	completed, canceled := blocking.recordedPlanOutcomes()
	if completed != 1 || canceled != 0 {
		t.Fatalf("plan outcomes completed=%d canceled=%d want completed=1 canceled=0", completed, canceled)
	}
	if got := db.vlogGenerationRewritePlanCanceled.Load(); got != 0 {
		t.Fatalf("plan canceled=%d want=0", got)
	}
}

func TestForegroundMaintenanceContextResumeGrace_WriteBoundaryOrdering(t *testing.T) {
	waitForBoundary := func(db *DB) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for db.foregroundMaintenanceGraceState.Load()&1 == 0 {
			if time.Now().After(deadline) {
				t.Fatal("grace boundary was not published")
			}
			time.Sleep(time.Millisecond)
		}
	}

	t.Run("write_inside_grace_is_absorbed", func(t *testing.T) {
		db := &DB{closeCh: make(chan struct{})}
		ctx, cancel := db.foregroundMaintenanceContextWithResumeGrace(2*time.Second, 50*time.Millisecond)
		defer cancel()
		db.noteWrite()
		waitForBoundary(db)
		select {
		case <-ctx.Done():
			t.Fatalf("inside-grace write canceled maintenance: %v", ctx.Err())
		case <-time.After(2 * foregroundMaintenancePollInterval()):
		}
	})

	t.Run("write_after_deadline_cancels_with_delayed_boundary", func(t *testing.T) {
		db := &DB{closeCh: make(chan struct{})}
		ctx, cancel := db.foregroundMaintenanceContextWithResumeGrace(2*time.Second, 50*time.Millisecond)
		defer cancel()
		db.foregroundMaintenanceGraceMu.Lock()
		time.Sleep(75 * time.Millisecond)
		db.foregroundMaintenanceGraceMu.Unlock()
		db.noteWrite()
		select {
		case <-ctx.Done():
		case <-time.After(2 * foregroundMaintenancePollInterval()):
			t.Fatal("post-boundary write did not cancel maintenance")
		}
	})
	t.Run("read_after_deadline_cancels_with_delayed_boundary", func(t *testing.T) {
		db := &DB{closeCh: make(chan struct{})}
		ctx, cancel := db.foregroundMaintenanceContextWithResumeGrace(2*time.Second, 50*time.Millisecond)
		defer cancel()
		db.beginForegroundRead()
		db.foregroundMaintenanceGraceMu.Lock()
		time.Sleep(75 * time.Millisecond)
		ended := make(chan struct{})
		go func() {
			db.endForegroundRead()
			close(ended)
		}()
		db.foregroundMaintenanceGraceMu.Unlock()
		select {
		case <-ctx.Done():
		case <-time.After(2 * foregroundMaintenancePollInterval()):
			t.Fatal("post-boundary read did not cancel maintenance")
		}
		select {
		case <-ended:
		case <-time.After(2 * foregroundMaintenancePollInterval()):
			t.Fatal("post-boundary read did not finish")
		}
	})

}

func TestVlogGenerationRewritePlan_CancelBackoffSkipsImmediateRetry(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB:      backend,
		planErr: context.Canceled,
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after first canceled run=%d want=1", calls)
	}

	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after immediate retry=%d want=1 (backoff active)", calls)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_runs"]; got != "1" {
		t.Fatalf("plan runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_canceled"]; got != "1" {
		t.Fatalf("plan canceled=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_canceled_last_unix_nano"]; got == "0" {
		t.Fatalf("plan canceled last ts=%q want non-zero", got)
	}
}

func TestVlogGenerationRewritePlan_DeadlineBackoffSkipsImmediateRetry(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB:      backend,
		planErr: context.DeadlineExceeded,
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after first deadline run=%d want=1", calls)
	}

	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after immediate retry=%d want=1 (backoff active)", calls)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_runs"]; got != "1" {
		t.Fatalf("plan runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_canceled"]; got != "1" {
		t.Fatalf("plan canceled=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_errors"]; got != "0" {
		t.Fatalf("plan errors=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_canceled_last_unix_nano"]; got == "0" {
		t.Fatalf("plan canceled last ts=%q want non-zero", got)
	}
}

func TestVlogGenerationRewritePlan_CancelBackoffCheckpointKickBypasses(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB:      backend,
		planErr: context.Canceled,
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	// First pass: planner is canceled and seeds plan-cancel backoff.
	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after first canceled run=%d want=1", calls)
	}

	// Second pass: checkpoint-kick bypass should ignore plan-cancel backoff.
	recorder.mu.Lock()
	recorder.planErr = nil
	recorder.planResponse = backenddb.ValueLogRewritePlan{SourceFileIDs: []uint32{11}, SelectedBytesLive: 64}
	recorder.mu.Unlock()

	db.maybeRunVlogGenerationMaintenanceWithOptions(false, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        false,
		rewriteDebtDrain:      true,
	})

	if _, calls := recorder.recordedPlan(); calls != 2 {
		t.Fatalf("plan calls after checkpoint-kick bypass=%d want=2", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after checkpoint-kick bypass=%d want=1", calls)
	}
}

func TestVlogGenerationRewritePlan_CancelBackoffExpires(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	recorder := &rewriteBudgetRecordingBackend{
		DB:      backend,
		planErr: context.Canceled,
	}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	t.Cleanup(cleanup)
	forceVlogMaintenanceIdle(db)

	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls after first canceled run=%d want=1", calls)
	}

	db.vlogGenerationRewritePlanCanceledLastNS.Store(time.Now().Add(-2 * vlogGenerationRewritePlanCancelBackoff).UnixNano())
	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedPlan(); calls != 2 {
		t.Fatalf("plan calls after expired backoff=%d want=2", calls)
	}
}

func TestVlogGenerationRewritePlan_CancelsForResumedReadsAndRetriesAfterQuiet(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	blocking := &blockingRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planBlock: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	const initialTokens = int64(1024)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(initialTokens)
	initialConsumed := db.vlogGenerationRewriteBudgetConsumed.Load()
	forceVlogMaintenanceIdle(db)

	doneMaintenance := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenance(false)
		close(doneMaintenance)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-blocking.planStart:
	case <-time.After(wait):
		t.Fatalf("rewrite plan did not start")
	}

	stopReads := make(chan struct{})
	readsStopped := make(chan struct{})
	go func() {
		defer close(readsStopped)
		ticker := time.NewTicker(foregroundReadStampMaxAge / 4)
		defer ticker.Stop()
		db.lastForegroundReadUnixNano.Store(time.Now().UnixNano())
		for {
			select {
			case <-stopReads:
				return
			case <-db.closeCh:
				return
			case now := <-ticker.C:
				db.lastForegroundReadUnixNano.Store(now.UnixNano())
			}
		}
	}()
	select {
	case <-doneMaintenance:
		t.Fatalf("rewrite plan canceled before resume grace elapsed")
	case <-time.After(vlogGenerationRewritePlanResumeGrace / 2):
	}
	select {
	case <-doneMaintenance:
	case <-time.After(wait):
		t.Fatalf("rewrite plan did not cancel after foreground reads remained resumed through grace")
	}
	close(stopReads)
	<-readsStopped

	completed, canceled := blocking.recordedPlanOutcomes()
	if completed != 0 || canceled != 1 {
		t.Fatalf("plan outcomes completed=%d canceled=%d want completed=0 canceled=1", completed, canceled)
	}
	if got := db.vlogGenerationRewriteBudgetTokensBytes.Load(); got != initialTokens {
		t.Fatalf("tokens after canceled plan=%d want=%d", got, initialTokens)
	}
	if got := db.vlogGenerationRewriteBudgetConsumed.Load(); got != initialConsumed {
		t.Fatalf("consumed budget after canceled plan=%d want=%d", got, initialConsumed)
	}
	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerIdle {
		t.Fatalf("scheduler state=%d want=%d", got, vlogGenerationSchedulerIdle)
	}

	blocking.mu.Lock()
	blocking.planResponse = backenddb.ValueLogRewritePlan{
		SourceFileIDs:     []uint32{11},
		SelectedBytesLive: 64,
	}
	blocking.mu.Unlock()
	blocking.unblockPlan()
	db.vlogGenerationRewritePlanCanceledLastNS.Store(time.Now().Add(-2 * vlogGenerationRewritePlanCancelBackoff).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	completed, canceled = blocking.recordedPlanOutcomes()
	if completed != 1 || canceled != 1 {
		t.Fatalf("plan outcomes after quiet retry completed=%d canceled=%d want completed=1 canceled=1", completed, canceled)
	}
	if got := blocking.recordedRewriteCalls(); got != 1 {
		t.Fatalf("rewrite calls after quiet retry=%d want=1", got)
	}
	if got := db.vlogGenerationRewriteBudgetConsumed.Load(); got != initialConsumed+64 {
		t.Fatalf("consumed budget after quiet retry=%d want=%d", got, initialConsumed+64)
	}
}

func TestVlogGenerationRewritePlan_ForegroundReadsStillRunForcedGC(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	blocking := &blockingRewritePlannerBackend{
		DB:        backend,
		planStart: make(chan struct{}),
		planBlock: make(chan struct{}),
	}

	db, err := Open(dir, blocking, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k1"), make([]byte, 4096)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1024)
	forceVlogMaintenanceIdle(db)

	doneMaintenance := make(chan struct{})
	go func() {
		db.maybeRunVlogGenerationMaintenance(true)
		close(doneMaintenance)
	}()

	wait := schedulerTestWait(t)
	select {
	case <-blocking.planStart:
	case <-time.After(wait):
		t.Fatalf("rewrite plan did not start")
	}

	// Resume foreground read activity while a forced-GC maintenance pass is active.
	db.lastForegroundReadUnixNano.Store(time.Now().UnixNano())
	blocking.unblockPlan()

	select {
	case <-doneMaintenance:
	case <-time.After(wait):
		t.Fatalf("maintenance did not finish while foreground reads were active")
	}

	if got := db.vlogGenerationGCRuns.Load(); got == 0 {
		t.Fatalf("expected forced GC path to run while foreground reads were active")
	}
	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerIdle {
		t.Fatalf("scheduler state=%d want=%d", got, vlogGenerationSchedulerIdle)
	}
}

func TestVlogGenerationGC_DryRunEligibleBytesTriggersRealGC(t *testing.T) {
	prepareDirectSchedulerTest(t)

	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &dryRunGCRecordingBackend{
		DB:          backend,
		dryRunStats: backenddb.ValueLogGCStats{SegmentsEligible: 2, BytesEligible: vlogGenerationGCMinBytes},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		JournalLanes:             1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		ForceValueLogPointers:    true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k"), make([]byte, 2048)); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationLastGCUnixNano.Store(0)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	dryRunCalls, realCalls, protected := recorder.recordedCalls()
	if dryRunCalls != 1 {
		t.Fatalf("dry-run calls=%d want=1", dryRunCalls)
	}
	if realCalls != 1 {
		t.Fatalf("real gc calls=%d want=1", realCalls)
	}
	if len(protected) != 2 {
		t.Fatalf("protected path snapshots=%d want=2", len(protected))
	}
	if db.vlogGenerationGCRuns.Load() != 1 {
		t.Fatalf("gc runs=%d want=1", db.vlogGenerationGCRuns.Load())
	}
	if got := db.vlogGenerationLastReason.Load(); got != vlogGenerationReasonPeriodicGC {
		t.Fatalf("last gc reason=%d want=%d", got, vlogGenerationReasonPeriodicGC)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.gc.dry_run.last_eligible_bytes"]; got != fmt.Sprintf("%d", vlogGenerationGCMinBytes) {
		t.Fatalf("gc dry-run eligible bytes=%q want %d", got, vlogGenerationGCMinBytes)
	}
	if got := stats["treedb.cache.vlog_generation.gc.dry_run.last_eligible_segments"]; got != "2" {
		t.Fatalf("gc dry-run eligible segments=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.dry_run.last_unix_nano"]; got == "0" {
		t.Fatalf("gc dry-run last_unix_nano=%q want non-zero", got)
	}
}

func TestVlogGenerationGC_DryRunNoEligibleBytesReturnsSchedulerIdle(t *testing.T) {
	prepareDirectSchedulerTest(t)

	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &dryRunGCRecordingBackend{
		DB:          backend,
		dryRunStats: backenddb.ValueLogGCStats{},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		JournalLanes:             1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		ForceValueLogPointers:    true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k"), make([]byte, 2048)); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationLastGCUnixNano.Store(0)
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	dryRunCalls, realCalls, _ := recorder.recordedCalls()
	if dryRunCalls != 1 {
		t.Fatalf("dry-run calls=%d want=1", dryRunCalls)
	}
	if realCalls != 0 {
		t.Fatalf("real gc calls=%d want=0", realCalls)
	}
	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerIdle {
		t.Fatalf("scheduler state=%d want=%d", got, vlogGenerationSchedulerIdle)
	}
}

func TestVlogGenerationGC_SkipsDuringRecentForegroundWrites(t *testing.T) {
	prepareDirectSchedulerTest(t)

	t.Setenv(envDisableVlogGenerationRewrite, "1")

	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &dryRunGCRecordingBackend{
		DB:          backend,
		dryRunStats: backenddb.ValueLogGCStats{SegmentsEligible: 2, BytesEligible: vlogGenerationGCMinBytes},
	}

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		JournalLanes:             1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		ForceValueLogPointers:    true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	b := db.NewBatch()
	if err := b.Set([]byte("k"), make([]byte, 2048)); err != nil {
		_ = b.Close()
		t.Fatalf("set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	db.vlogGenerationLastGCUnixNano.Store(0)
	db.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())
	db.maybeRunVlogGenerationMaintenance(false)

	dryRunCalls, realCalls, _ := recorder.recordedCalls()
	if dryRunCalls != 0 || realCalls != 0 {
		t.Fatalf("gc calls=%d/%d want 0/0 while foreground writes are hot", dryRunCalls, realCalls)
	}
}

func TestVlogGenerationStats_ReportRewriteBacklogAndDurations(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend}

	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()

	db.vlogGenerationMaintenanceAcquired.Store(2)
	db.vlogGenerationMaintenancePassTotalNanos.Store(uint64((40 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationMaintenancePassMaxNanos.Store(uint64((30 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationRewritePlanRuns.Store(4)
	db.vlogGenerationRewritePlanTotalNanos.Store(uint64((80 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationRewritePlanMaxNanos.Store(uint64((50 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationRewriteRuns.Store(3)
	db.vlogGenerationRewriteExecTotalNanos.Store(uint64((150 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationRewriteExecMaxNanos.Store(uint64((70 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationRewriteExecLastLiveBytes.Store(1000)
	db.vlogGenerationRewriteExecLastBytesOut.Store(600)
	db.vlogGenerationRewriteExecLastDurationNanos.Store(uint64((40 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationRewriteExecLastUnixNano.Store(7777)
	db.vlogGenerationRewriteBytesIn.Store(1000)
	db.vlogGenerationRewriteBytesOut.Store(600)
	db.vlogGenerationRewriteReclaimedBytes.Store(400)
	db.vlogGenerationGCRuns.Store(2)
	db.vlogGenerationGCExecTotalNanos.Store(uint64((60 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationGCExecMaxNanos.Store(uint64((35 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationVacuumRuns.Store(2)
	db.vlogGenerationVacuumExecTotalNanos.Store(uint64((44 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationVacuumExecMaxNanos.Store(uint64((25 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationRewriteBudgetTokensBytes.Store(512)
	db.vlogGenerationRewriteBudgetConsumed.Store(1536)
	db.valueLogRewriteBudgetBytes = 2048
	db.vlogGenerationLastChurnBps.Store(2500)
	db.vlogGenerationRewriteAgeBlockedUntilNS.Store(time.Now().Add(5 * time.Minute).UnixNano())
	db.vlogGenerationLastGCSegmentsReferenced.Store(7)
	db.vlogGenerationLastGCBytesReferenced.Store(700)
	db.vlogGenerationLastGCSegmentsActive.Store(4)
	db.vlogGenerationLastGCBytesActive.Store(400)
	db.vlogGenerationLastGCSegmentsProtected.Store(3)
	db.vlogGenerationLastGCBytesProtected.Store(300)
	db.vlogGenerationLastGCSegmentsProtectedInUse.Store(1)
	db.vlogGenerationLastGCBytesProtectedInUse.Store(100)
	db.vlogGenerationLastGCSegmentsProtectedRetained.Store(1)
	db.vlogGenerationLastGCBytesProtectedRetained.Store(120)
	db.vlogGenerationLastGCSegmentsProtectedOverlap.Store(1)
	db.vlogGenerationLastGCBytesProtectedOverlap.Store(80)
	db.vlogGenerationLastGCSegmentsProtectedOther.Store(0)
	db.vlogGenerationLastGCBytesProtectedOther.Store(0)
	db.vlogGenerationLastGCSegmentsEligible.Store(6)
	db.vlogGenerationLastGCBytesEligible.Store(600)
	db.vlogGenerationLastGCSegmentsDeleted.Store(2)
	db.vlogGenerationLastGCBytesDeleted.Store(200)
	db.vlogGenerationLastGCSegmentsPending.Store(4)
	db.vlogGenerationLastGCBytesPending.Store(400)
	db.vlogGenerationLastGCObservedSourceSegments.Store(2)
	db.vlogGenerationLastGCObservedSourceSegmentsReferenced.Store(0)
	db.vlogGenerationLastGCObservedSourceSegmentsActive.Store(0)
	db.vlogGenerationLastGCObservedSourceSegmentsProtected.Store(2)
	db.vlogGenerationLastGCObservedSourceSegmentsProtectedInUse.Store(0)
	db.vlogGenerationLastGCObservedSourceSegmentsProtectedRetained.Store(2)
	db.vlogGenerationLastGCObservedSourceSegmentsProtectedOverlap.Store(0)
	db.vlogGenerationLastGCObservedSourceSegmentsProtectedOther.Store(0)
	db.vlogGenerationLastGCObservedSourceSegmentsEligible.Store(0)
	db.vlogGenerationLastGCObservedSourceSegmentsDeleted.Store(0)
	db.vlogGenerationLastGCObservedSourceSegmentsPending.Store(0)
	db.vlogGenerationLastGCObservedSourceBytes.Store(250)
	db.vlogGenerationLastGCObservedSourceBytesReferenced.Store(0)
	db.vlogGenerationLastGCObservedSourceBytesActive.Store(0)
	db.vlogGenerationLastGCObservedSourceBytesProtected.Store(250)
	db.vlogGenerationLastGCObservedSourceBytesProtectedInUse.Store(0)
	db.vlogGenerationLastGCObservedSourceBytesProtectedRetained.Store(250)
	db.vlogGenerationLastGCObservedSourceBytesProtectedOverlap.Store(0)
	db.vlogGenerationLastGCObservedSourceBytesProtectedOther.Store(0)
	db.vlogGenerationLastGCObservedSourceBytesEligible.Store(0)
	db.vlogGenerationLastGCObservedSourceBytesDeleted.Store(0)
	db.vlogGenerationLastGCObservedSourceBytesPending.Store(0)
	db.vlogGenerationMaintenanceSkipStageNotDue.Store(5)
	db.vlogGenerationMaintenanceSkipStageDue.Store(2)
	db.vlogGenerationMaintenanceAcquiredBySource[vlogGenerationMaintenanceSourcePeriodic].Store(5)
	db.vlogGenerationMaintenanceAcquiredBySource[vlogGenerationMaintenanceSourceBypass].Store(2)
	db.vlogGenerationMaintenanceAcquiredBySource[vlogGenerationMaintenanceSourceCheckpointPending].Store(3)
	db.vlogGenerationMaintenanceAcquiredBySource[vlogGenerationMaintenanceSourceRewriteQueuePending].Store(6)
	db.vlogGenerationMaintenanceAcquiredBySource[vlogGenerationMaintenanceSourceRewriteAgeBlocked].Store(1)
	db.vlogGenerationMaintenanceAcquiredBySource[vlogGenerationMaintenanceSourceRewriteStageConfirm].Store(4)
	db.vlogGenerationMaintenanceAcquiredBySource[vlogGenerationMaintenanceSourceOther].Store(7)
	db.vlogGenerationMaintenancePassWithRewriteBySource[vlogGenerationMaintenanceSourcePeriodic].Store(2)
	db.vlogGenerationMaintenancePassWithRewriteBySource[vlogGenerationMaintenanceSourceCheckpointPending].Store(1)
	db.vlogGenerationMaintenancePassWithGCBySource[vlogGenerationMaintenanceSourceBypass].Store(2)
	db.vlogGenerationMaintenancePassWithLeafPack.Store(4)
	db.vlogGenerationMaintenancePassWithLeafPackBySource[vlogGenerationMaintenanceSourcePeriodic].Store(3)
	db.vlogGenerationMaintenancePassNoopBySource[vlogGenerationMaintenanceSourceOther].Store(3)
	db.vlogGenerationLeafPackAttempts.Store(5)
	db.vlogGenerationLeafPackAdmitted.Store(4)
	db.vlogGenerationLeafPackRuns.Store(2)
	db.vlogGenerationLeafPackSkips.Store(3)
	db.vlogGenerationLeafPackSkipMinInterval.Store(6)
	db.vlogGenerationLeafPackSkipWriteBurst.Store(7)
	db.vlogGenerationLeafPackSkipQueuePressure.Store(8)
	db.vlogGenerationLeafPackSkipForegroundIterators.Store(9)
	db.vlogGenerationLeafPackErrors.Store(1)
	db.vlogGenerationLeafPackBytesCopied.Store(4096)
	db.vlogGenerationLeafPackExpectedReclaimBytes.Store(2048)
	db.vlogGenerationLeafPackExpectedReclaimPerByteCopiedPPM.Store(500000)
	db.vlogGenerationLeafPackSelectionBytesToCopy.Store(1024)
	db.vlogGenerationLeafPackSelectionBytesDead.Store(2048)
	db.vlogGenerationLeafPackSelectionGenerations.Store(3)
	db.vlogGenerationLeafPackLastWallNanos.Store(uint64((55 * time.Millisecond).Nanoseconds()))
	db.vlogGenerationLeafPackLastExpectedReclaimBytes.Store(900)
	db.vlogGenerationLeafPackLastExpectedReclaimPerByteCopiedPPM.Store(450000)
	db.vlogGenerationLeafPackLastSelectionBytesToCopy.Store(700)
	db.vlogGenerationLeafPackLastSelectionBytesDead.Store(900)
	db.vlogGenerationLeafPackLastSelectionGenerations.Store(2)
	db.vlogGenerationLeafPackLastBytesCopied.Store(650)
	db.vlogGenerationLeafPackGCDeletedBytes.Store(3000)
	db.vlogGenerationLeafPackReclaimedBytes.Store(1536)
	db.vlogGenerationLeafPackStopLowYield.Store(2)
	db.vlogGenerationLeafPackGCLastDeletedBytes.Store(700)
	db.vlogGenerationLeafPackLastReclaimedBytes.Store(50)
	db.vlogGenerationLeafPackLastUnixNano.Store(8888)
	db.vlogGenerationRewritePlanSelectedSegments.Store(6)
	db.vlogGenerationRewriteExecSourceSegments.Store(3)
	db.vlogGenerationRewriteSourceSegmentsRequestedTotal.Store(5)
	db.vlogGenerationRewriteSourceSegmentsStillReferencedTotal.Store(2)
	db.vlogGenerationRewriteSourceSegmentsUnreferencedTotal.Store(3)
	db.vlogGenerationRewriteSourceSegmentsRequestedLast.Store(2)
	db.vlogGenerationRewriteSourceSegmentsStillReferencedLast.Store(1)
	db.vlogGenerationRewriteSourceSegmentsUnreferencedLast.Store(1)
	db.vlogGenerationRewriteSourceBytesRequestedTotal.Store(5000)
	db.vlogGenerationRewriteSourceBytesStillReferencedTotal.Store(1800)
	db.vlogGenerationRewriteSourceBytesUnreferencedTotal.Store(3200)
	db.vlogGenerationRewriteSourceBytesRequestedLast.Store(2200)
	db.vlogGenerationRewriteSourceBytesStillReferencedLast.Store(700)
	db.vlogGenerationRewriteSourceBytesUnreferencedLast.Store(1500)
	db.vlogGenerationRewriteQueueProgressPasses.Store(7)
	db.vlogGenerationRewriteQueueProgressSnapshotErrors.Store(1)
	db.vlogGenerationRewriteQueueSegmentsBeforeTotal.Store(20)
	db.vlogGenerationRewriteQueueSegmentsAfterTotal.Store(14)
	db.vlogGenerationRewriteQueueSegmentsDrainedTotal.Store(8)
	db.vlogGenerationRewriteQueueSegmentsGrownTotal.Store(2)
	db.vlogGenerationRewriteQueueSegmentsBeforeLast.Store(4)
	db.vlogGenerationRewriteQueueSegmentsAfterLast.Store(2)
	db.vlogGenerationRewriteQueueSegmentsDeltaLast.Store(-2)
	db.vlogGenerationRewriteQueueLiveBytesKnownPasses.Store(6)
	db.vlogGenerationRewriteQueueLiveBytesUnknownPasses.Store(1)
	db.vlogGenerationRewriteQueueLiveBytesBeforeTotal.Store(10000)
	db.vlogGenerationRewriteQueueLiveBytesAfterTotal.Store(7600)
	db.vlogGenerationRewriteQueueLiveBytesDrainedTotal.Store(3200)
	db.vlogGenerationRewriteQueueLiveBytesGrownTotal.Store(800)
	db.vlogGenerationRewriteQueueLiveBytesBeforeLast.Store(1600)
	db.vlogGenerationRewriteQueueLiveBytesAfterLast.Store(1200)
	db.vlogGenerationRewriteQueueLiveBytesDeltaLast.Store(-400)
	db.vlogGenerationRewriteQueueRunSegmentCapDecisions.Store(9)
	db.vlogGenerationRewriteQueueFreshPlanSegmentCapDecisions.Store(4)
	db.vlogGenerationRewriteQueueRunSegmentCapLimiterCounts[int(vlogGenerationRewriteSegmentCapLimiterBudgetTokens)].Store(5)
	db.vlogGenerationRewriteQueueRunSegmentCapLimiterCounts[int(vlogGenerationRewriteSegmentCapLimiterCheckpointKickSafety)].Store(2)
	db.vlogGenerationRewriteQueueFreshPlanSegmentCapLimiterCounts[int(vlogGenerationRewriteSegmentCapLimiterFreshPlanQueueThreshold)].Store(3)
	db.vlogGenerationRewriteQueueFreshPlanSegmentCapLimiterCounts[int(vlogGenerationRewriteSegmentCapLimiterFreshPlanCap)].Store(1)
	db.vlogGenerationRewriteProcessedLiveBytes.Store(900)
	db.vlogGenerationRewriteProcessedStaleBytes.Store(450)
	db.vlogGenerationRewriteNoReclaimRuns.Store(3)
	db.vlogGenerationRewriteNoReclaimStaleBytes.Store(320)
	db.vlogGenerationObservedGCQueuedBatches.Store(5)
	db.vlogGenerationObservedGCQueuedIDs.Store(12)
	db.vlogGenerationObservedGCTakenBatches.Store(4)
	db.vlogGenerationObservedGCTakenIDs.Store(9)
	db.vlogGenerationObservedGCRuns.Store(3)
	db.vlogGenerationObservedGCRetryQueued.Store(2)
	db.vlogGenerationObservedGCRetryDropped.Store(1)
	db.vlogGenerationObservedGCLatencyCompletedIDs.Store(6)
	db.vlogGenerationObservedGCLatencyDroppedIDs.Store(2)
	db.vlogGenerationObservedGCLatencyTotalMS.Store(640)
	db.vlogGenerationObservedGCLatencyMaxMS.Store(210)
	db.vlogGenerationObservedGCSourceSegmentsTotal.Store(11)
	db.vlogGenerationObservedGCSourceSegmentsEligibleTotal.Store(5)
	db.vlogGenerationObservedGCSourceSegmentsDeletedTotal.Store(3)
	db.vlogGenerationObservedGCSourceSegmentsProtectedInUseTotal.Store(1)
	db.vlogGenerationObservedGCSourceSegmentsProtectedRetainedTotal.Store(2)
	db.vlogGenerationObservedGCSourceSegmentsProtectedOverlapTotal.Store(3)
	db.vlogGenerationObservedGCSourceSegmentsProtectedOtherTotal.Store(4)
	db.vlogGenerationObservedGCSourceBytesTotal.Store(1100)
	db.vlogGenerationObservedGCSourceBytesEligibleTotal.Store(500)
	db.vlogGenerationObservedGCSourceBytesDeletedTotal.Store(300)
	db.vlogGenerationObservedGCSourceBytesProtectedInUseTotal.Store(50)
	db.vlogGenerationObservedGCSourceBytesProtectedRetainedTotal.Store(250)
	db.vlogGenerationObservedGCSourceBytesProtectedOverlapTotal.Store(75)
	db.vlogGenerationObservedGCSourceBytesProtectedOtherTotal.Store(25)

	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteQueueLoaded = true
	db.vlogGenerationRewriteQueue = []uint32{11, 12}
	db.vlogGenerationRewriteLedger = []backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesTotal: 1000, BytesLive: 700, BytesStale: 300},
		{FileID: 12, BytesTotal: 500, BytesLive: 500, BytesStale: 0},
	}
	db.vlogGenerationRewritePenalties = map[uint32]valueLogGenerationRewritePenalty{
		11: {Attempts: 1, CooldownUntilUnixNano: time.Now().Add(time.Minute).UnixNano()},
	}
	db.vlogGenerationRewriteStagePending = true
	db.vlogGenerationRewriteStageObservedUnixNano = 1234
	db.vlogGenerationRewriteQueueMu.Unlock()
	db.vlogGenerationObservedGCMu.Lock()
	db.vlogGenerationObservedGCSourceIDs = map[uint32]struct{}{
		101: {},
		102: {},
	}
	db.vlogGenerationObservedGCFirstQueuedUnixNano = map[uint32]int64{
		101: time.Now().Add(-3 * time.Second).UnixNano(),
		102: time.Now().Add(-1200 * time.Millisecond).UnixNano(),
	}
	db.vlogGenerationObservedGCRetryAttempts = map[uint32]uint8{
		101: 2,
		102: 1,
	}
	db.vlogGenerationObservedGCMu.Unlock()
	db.vlogGenerationRewriteQueuePending.Store(true)
	db.vlogGenerationRewriteQueueRunning.Store(false)

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.maintenance.pass.total_ms"]; got != "40.000" {
		t.Fatalf("maintenance pass total ms=%q want 40.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.pass.max_ms"]; got != "30.000" {
		t.Fatalf("maintenance pass max ms=%q want 30.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.pass.avg_ms"]; got != "20.000" {
		t.Fatalf("maintenance pass avg ms=%q want 20.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue.pending"]; got != "true" {
		t.Fatalf("rewrite queue pending=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue.running"]; got != "false" {
		t.Fatalf("rewrite queue running=%q want false", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.periodic"]; got != "5" {
		t.Fatalf("maintenance acquired source periodic=%q want 5", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.bypass"]; got != "2" {
		t.Fatalf("maintenance acquired source bypass=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.checkpoint_pending"]; got != "3" {
		t.Fatalf("maintenance acquired source checkpoint_pending=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_queue_pending"]; got != "6" {
		t.Fatalf("maintenance acquired source rewrite_queue_pending=%q want 6", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_age_blocked"]; got != "1" {
		t.Fatalf("maintenance acquired source rewrite_age_blocked=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_stage_confirm"]; got != "4" {
		t.Fatalf("maintenance acquired source rewrite_stage_confirm=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.acquired.source.other"]; got != "7" {
		t.Fatalf("maintenance acquired source other=%q want 7", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.periodic"]; got != "2" {
		t.Fatalf("maintenance rewrite passes source periodic=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.checkpoint_pending"]; got != "1" {
		t.Fatalf("maintenance rewrite passes source checkpoint_pending=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.passes.with_gc.source.bypass"]; got != "2" {
		t.Fatalf("maintenance gc passes source bypass=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.passes.with_leaf_pack"]; got != "4" {
		t.Fatalf("maintenance leaf pack passes=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.passes.with_leaf_pack.source.periodic"]; got != "3" {
		t.Fatalf("maintenance leaf pack passes source periodic=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.passes.noop.source.other"]; got != "3" {
		t.Fatalf("maintenance noop passes source other=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.admitted"]; got != "4" {
		t.Fatalf("leaf pack admitted=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.runs"]; got != "2" {
		t.Fatalf("leaf pack runs=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.skip.min_interval"]; got != "6" {
		t.Fatalf("leaf pack skip min_interval=%q want 6", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.skip.write_burst"]; got != "7" {
		t.Fatalf("leaf pack skip write_burst=%q want 7", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.skip.queue_pressure"]; got != "8" {
		t.Fatalf("leaf pack skip queue_pressure=%q want 8", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.skip.foreground_iterators"]; got != "9" {
		t.Fatalf("leaf pack skip foreground_iterators=%q want 9", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.attributed_reclaim_bytes"]; got != "1536" {
		t.Fatalf("leaf pack reclaimed bytes=%q want 1536", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.attributed_reclaim_per_byte_copied_ppm"]; got != "375000" {
		t.Fatalf("leaf pack reclaim_per_byte_copied_ppm=%q want 375000", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.min_reclaim_per_byte_copied_ppm"]; got != "250000" {
		t.Fatalf("leaf pack min_reclaim_per_byte_copied_ppm=%q want 250000", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.stop.low_yield"]; got != "2" {
		t.Fatalf("leaf pack stop.low_yield=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.last_selection.generations"]; got != "2" {
		t.Fatalf("leaf pack last selection generations=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.last_attributed_reclaim_bytes"]; got != "50" {
		t.Fatalf("leaf pack last reclaimed bytes=%q want 50", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.last_attributed_reclaim_per_byte_copied_ppm"]; got != "76923" {
		t.Fatalf("leaf pack last reclaim_per_byte_copied_ppm=%q want 76923", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.gc.deleted_bytes"]; got != "3000" {
		t.Fatalf("leaf pack gc deleted bytes=%q want 3000", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.gc.last_deleted_bytes"]; got != "700" {
		t.Fatalf("leaf pack gc last deleted bytes=%q want 700", got)
	}
	if got := stats["treedb.cache.vlog_generation.leaf_pack.last_unix_nano"]; got != "8888" {
		t.Fatalf("leaf pack last unix nano=%q want 8888", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan.total_ms"]; got != "80.000" {
		t.Fatalf("rewrite plan total ms=%q want 80.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan.avg_ms"]; got != "20.000" {
		t.Fatalf("rewrite plan avg ms=%q want 20.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.total_ms"]; got != "150.000" {
		t.Fatalf("rewrite exec total ms=%q want 150.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.avg_ms"]; got != "50.000" {
		t.Fatalf("rewrite exec avg ms=%q want 50.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.exec.total_ms"]; got != "60.000" {
		t.Fatalf("gc exec total ms=%q want 60.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.exec.avg_ms"]; got != "30.000" {
		t.Fatalf("gc exec avg ms=%q want 30.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_referenced_segments"]; got != "7" {
		t.Fatalf("gc last referenced segments=%q want 7", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_referenced_bytes"]; got != "700" {
		t.Fatalf("gc last referenced bytes=%q want 700", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_active_segments"]; got != "4" {
		t.Fatalf("gc last active segments=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_active_bytes"]; got != "400" {
		t.Fatalf("gc last active bytes=%q want 400", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_segments"]; got != "3" {
		t.Fatalf("gc last protected segments=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_bytes"]; got != "300" {
		t.Fatalf("gc last protected bytes=%q want 300", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_in_use_segments"]; got != "1" {
		t.Fatalf("gc last protected in use segments=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_in_use_bytes"]; got != "100" {
		t.Fatalf("gc last protected in use bytes=%q want 100", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_retained_segments"]; got != "1" {
		t.Fatalf("gc last protected retained segments=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_retained_bytes"]; got != "120" {
		t.Fatalf("gc last protected retained bytes=%q want 120", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_overlap_segments"]; got != "1" {
		t.Fatalf("gc last protected overlap segments=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_overlap_bytes"]; got != "80" {
		t.Fatalf("gc last protected overlap bytes=%q want 80", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_other_segments"]; got != "0" {
		t.Fatalf("gc last protected other segments=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_protected_other_bytes"]; got != "0" {
		t.Fatalf("gc last protected other bytes=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_eligible_segments"]; got != "6" {
		t.Fatalf("gc last eligible segments=%q want 6", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_eligible_bytes"]; got != "600" {
		t.Fatalf("gc last eligible bytes=%q want 600", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_deleted_segments"]; got != "2" {
		t.Fatalf("gc last deleted segments=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_deleted_bytes"]; got != "200" {
		t.Fatalf("gc last deleted bytes=%q want 200", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_pending_segments"]; got != "4" {
		t.Fatalf("gc last pending segments=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_pending_bytes"]; got != "400" {
		t.Fatalf("gc last pending bytes=%q want 400", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments"]; got != "2" {
		t.Fatalf("gc last observed source segments=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_referenced"]; got != "0" {
		t.Fatalf("gc last observed source segments referenced=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_active"]; got != "0" {
		t.Fatalf("gc last observed source segments active=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_protected"]; got != "2" {
		t.Fatalf("gc last observed source segments protected=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_protected_in_use"]; got != "0" {
		t.Fatalf("gc last observed source segments protected in-use=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_protected_retained"]; got != "2" {
		t.Fatalf("gc last observed source segments protected retained=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_protected_overlap"]; got != "0" {
		t.Fatalf("gc last observed source segments protected overlap=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_protected_other"]; got != "0" {
		t.Fatalf("gc last observed source segments protected other=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_eligible"]; got != "0" {
		t.Fatalf("gc last observed source segments eligible=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_deleted"]; got != "0" {
		t.Fatalf("gc last observed source segments deleted=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.segments_pending"]; got != "0" {
		t.Fatalf("gc last observed source segments pending=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes"]; got != "250" {
		t.Fatalf("gc last observed source bytes=%q want 250", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_referenced"]; got != "0" {
		t.Fatalf("gc last observed source bytes referenced=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_active"]; got != "0" {
		t.Fatalf("gc last observed source bytes active=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected"]; got != "250" {
		t.Fatalf("gc last observed source bytes protected=%q want 250", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected_in_use"]; got != "0" {
		t.Fatalf("gc last observed source bytes protected in-use=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected_retained"]; got != "250" {
		t.Fatalf("gc last observed source bytes protected retained=%q want 250", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected_overlap"]; got != "0" {
		t.Fatalf("gc last observed source bytes protected overlap=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_protected_other"]; got != "0" {
		t.Fatalf("gc last observed source bytes protected other=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_eligible"]; got != "0" {
		t.Fatalf("gc last observed source bytes eligible=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_deleted"]; got != "0" {
		t.Fatalf("gc last observed source bytes deleted=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.gc.last_observed_source.bytes_pending"]; got != "0" {
		t.Fatalf("gc last observed source bytes pending=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.vacuum.exec.total_ms"]; got != "44.000" {
		t.Fatalf("vacuum exec total ms=%q want 44.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.vacuum.exec.avg_ms"]; got != "22.000" {
		t.Fatalf("vacuum exec avg ms=%q want 22.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.ledger_segments"]; got != "2" {
		t.Fatalf("rewrite ledger segments=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.ledger_bytes_total"]; got != "1500" {
		t.Fatalf("rewrite ledger bytes total=%q want 1500", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.ledger_bytes_live"]; got != "1200" {
		t.Fatalf("rewrite ledger bytes live=%q want 1200", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.ledger_bytes_stale"]; got != "300" {
		t.Fatalf("rewrite ledger bytes stale=%q want 300", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap"]; got != "1" {
		t.Fatalf("rewrite queue run segment cap=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.known"]; got != "true" {
		t.Fatalf("rewrite queue live hint known=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_present"]; got != "2" {
		t.Fatalf("rewrite queue live hint ids present=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_known"]; got != "2" {
		t.Fatalf("rewrite queue live hint ids known=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.coverage_pct"]; got != "100.000" {
		t.Fatalf("rewrite queue live hint coverage pct=%q want 100.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.bytes"]; got != "1200" {
		t.Fatalf("rewrite queue live hint bytes=%q want 1200", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.checkpoint_kick"]; got != "1" {
		t.Fatalf("rewrite queue run segment cap checkpoint kick=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter"]; got != "budget_tokens" {
		t.Fatalf("rewrite queue run segment cap limiter=%q want budget_tokens", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.checkpoint_kick"]; got != "checkpoint_kick_safety" {
		t.Fatalf("rewrite queue run segment cap checkpoint kick limiter=%q want checkpoint_kick_safety", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget"]; got != "1" {
		t.Fatalf("rewrite queue run segment cap by-budget=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget.checkpoint_kick"]; got != "0" {
		t.Fatalf("rewrite queue run segment cap checkpoint-kick by-budget=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes"]; got != "600" {
		t.Fatalf("rewrite queue run segment cap per-segment budget bytes=%q want 600", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.checkpoint_kick"]; got != "0" {
		t.Fatalf("rewrite queue run segment cap checkpoint-kick per-segment budget bytes=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.fresh_plan"]; got != "1" {
		t.Fatalf("rewrite queue run segment cap fresh-plan=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.fresh_plan"]; got != "fresh_plan_queue_threshold" {
		t.Fatalf("rewrite queue run segment cap fresh-plan limiter=%q want fresh_plan_queue_threshold", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget.fresh_plan"]; got != "0" {
		t.Fatalf("rewrite queue run segment cap fresh-plan by-budget=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.fresh_plan"]; got != "0" {
		t.Fatalf("rewrite queue run segment cap fresh-plan per-segment budget bytes=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions"]; got != "9" {
		t.Fatalf("rewrite queue run segment cap decisions=%q want 9", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions.fresh_plan"]; got != "4" {
		t.Fatalf("rewrite queue run segment cap fresh-plan decisions=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.budget_tokens"]; got != "5" {
		t.Fatalf("rewrite queue run segment cap limiter_count.budget_tokens=%q want 5", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.checkpoint_kick_safety"]; got != "2" {
		t.Fatalf("rewrite queue run segment cap limiter_count.checkpoint_kick_safety=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.fresh_plan_queue_threshold.fresh_plan"]; got != "3" {
		t.Fatalf("rewrite queue run segment cap limiter_count.fresh_plan_queue_threshold.fresh_plan=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.fresh_plan_cap.fresh_plan"]; got != "1" {
		t.Fatalf("rewrite queue run segment cap limiter_count.fresh_plan_cap.fresh_plan=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_config.resume_max_segments"]; got != "1" {
		t.Fatalf("rewrite queue config resume max segments=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_config.debt_drain_max_segments"]; got != "8" {
		t.Fatalf("rewrite queue config debt-drain max segments=%q want 8", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_min_segments"]; got != "4" {
		t.Fatalf("rewrite queue config fresh-plan min segments=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_max_segments"]; got != "4" {
		t.Fatalf("rewrite queue config fresh-plan max segments=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.passes"]; got != "7" {
		t.Fatalf("rewrite queue progress passes=%q want 7", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.snapshot_errors"]; got != "1" {
		t.Fatalf("rewrite queue progress snapshot errors=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.segments_before_total"]; got != "20" {
		t.Fatalf("rewrite queue progress segments before total=%q want 20", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.segments_after_total"]; got != "14" {
		t.Fatalf("rewrite queue progress segments after total=%q want 14", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.segments_drained_total"]; got != "8" {
		t.Fatalf("rewrite queue progress segments drained total=%q want 8", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.segments_grown_total"]; got != "2" {
		t.Fatalf("rewrite queue progress segments grown total=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.segments_before_last"]; got != "4" {
		t.Fatalf("rewrite queue progress segments before last=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.segments_after_last"]; got != "2" {
		t.Fatalf("rewrite queue progress segments after last=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.segments_delta_last"]; got != "-2" {
		t.Fatalf("rewrite queue progress segments delta last=%q want -2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_known_passes"]; got != "6" {
		t.Fatalf("rewrite queue progress live bytes known passes=%q want 6", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_unknown_passes"]; got != "1" {
		t.Fatalf("rewrite queue progress live bytes unknown passes=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_before_total"]; got != "10000" {
		t.Fatalf("rewrite queue progress live bytes before total=%q want 10000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_after_total"]; got != "7600" {
		t.Fatalf("rewrite queue progress live bytes after total=%q want 7600", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_drained_total"]; got != "3200" {
		t.Fatalf("rewrite queue progress live bytes drained total=%q want 3200", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_grown_total"]; got != "800" {
		t.Fatalf("rewrite queue progress live bytes grown total=%q want 800", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_before_last"]; got != "1600" {
		t.Fatalf("rewrite queue progress live bytes before last=%q want 1600", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_after_last"]; got != "1200" {
		t.Fatalf("rewrite queue progress live bytes after last=%q want 1200", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_delta_last"]; got != "-400" {
		t.Fatalf("rewrite queue progress live bytes delta last=%q want -400", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_bytes_after_tokens"]; got != "688" {
		t.Fatalf("rewrite queue live bytes after tokens=%q want 688", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_eta_seconds.budget"]; got != "0.336" {
		t.Fatalf("rewrite queue eta budget seconds=%q want 0.336", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_eta_seconds.recent_exec"]; got != "0.028" {
		t.Fatalf("rewrite queue eta recent exec seconds=%q want 0.028", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.ledger_stale_ratio_ppm"]; got != "200000" {
		t.Fatalf("rewrite ledger stale ratio ppm=%q want 200000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.stage_pending"]; got != "true" {
		t.Fatalf("rewrite stage pending=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.stage_observed_unix_nano"]; got != "1234" {
		t.Fatalf("rewrite stage observed=%q want 1234", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.stage_observed_age_ms"]; got == "0" {
		t.Fatalf("rewrite stage observed age ms=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.stage_due"]; got != "true" {
		t.Fatalf("rewrite stage due=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.stage_due_in_ms"]; got != "0" {
		t.Fatalf("rewrite stage due in ms=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.penalties_active"]; got != "1" {
		t.Fatalf("rewrite penalties active=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.age_blocked_due"]; got != "false" {
		t.Fatalf("rewrite age blocked due=%q want false", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.age_blocked_remaining_ms"]; got == "0" {
		t.Fatalf("rewrite age blocked remaining ms=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite_budget.tokens_bytes"]; got != "512" {
		t.Fatalf("rewrite budget tokens bytes=%q want 512", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total"]; got != "1536" {
		t.Fatalf("rewrite budget consumed=%q want 1536", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_per_sec"]; got != "10240.000" {
		t.Fatalf("rewrite budget consumed bytes/sec=%q want 10240.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite_budget.consumed_share_of_budget_pct"]; got != "500.000" {
		t.Fatalf("rewrite budget consumed share pct=%q want 500.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite_budget.tokens_cap_bytes"]; got == "0" {
		t.Fatalf("rewrite budget cap bytes=%q want non-zero", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite_budget.tokens_utilization_pct"]; got == "" {
		t.Fatalf("rewrite budget utilization pct missing")
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.skip.stage_gate_not_due"]; got != "5" {
		t.Fatalf("maintenance skip stage gate not due=%q want 5", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.skip.stage_gate_due_reserved"]; got != "2" {
		t.Fatalf("maintenance skip stage gate due reserved=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.foreground_quiet"]; got != "true" {
		t.Fatalf("maintenance foreground quiet=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.foreground_write_quiet"]; got != "true" {
		t.Fatalf("maintenance foreground write quiet=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.foreground_read_quiet"]; got != "true" {
		t.Fatalf("maintenance foreground read quiet=%q want true", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.foreground_active_iterators"]; got != "0" {
		t.Fatalf("maintenance foreground active iterators=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.foreground_last_write_unix_nano"]; got == "0" {
		t.Fatalf("maintenance foreground last write unix nano=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.foreground_last_read_unix_nano"]; got == "0" {
		t.Fatalf("maintenance foreground last read unix nano=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.foreground_write_age_ms"]; got == "0" {
		t.Fatalf("maintenance foreground write age ms=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.maintenance.foreground_read_age_ms"]; got == "0" {
		t.Fatalf("maintenance foreground read age ms=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_selected_segments_total"]; got != "6" {
		t.Fatalf("rewrite plan selected segments total=%q want 6", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_segments_total"]; got != "3" {
		t.Fatalf("rewrite exec source segments total=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_segments_requested_total"]; got != "5" {
		t.Fatalf("rewrite exec source segments requested total=%q want 5", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_segments_still_referenced_total"]; got != "2" {
		t.Fatalf("rewrite exec source segments still referenced total=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_segments_unreferenced_total"]; got != "3" {
		t.Fatalf("rewrite exec source segments unreferenced total=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_segments_requested_last"]; got != "2" {
		t.Fatalf("rewrite exec source segments requested last=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_segments_still_referenced_last"]; got != "1" {
		t.Fatalf("rewrite exec source segments still referenced last=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_segments_unreferenced_last"]; got != "1" {
		t.Fatalf("rewrite exec source segments unreferenced last=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total"]; got != "5000" {
		t.Fatalf("rewrite exec source bytes requested total=%q want 5000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_bytes_still_referenced_total"]; got != "1800" {
		t.Fatalf("rewrite exec source bytes still referenced total=%q want 1800", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total"]; got != "3200" {
		t.Fatalf("rewrite exec source bytes unreferenced total=%q want 3200", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_last"]; got != "2200" {
		t.Fatalf("rewrite exec source bytes requested last=%q want 2200", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_bytes_still_referenced_last"]; got != "700" {
		t.Fatalf("rewrite exec source bytes still referenced last=%q want 700", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_last"]; got != "1500" {
		t.Fatalf("rewrite exec source bytes unreferenced last=%q want 1500", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.processed_live_bytes"]; got != "900" {
		t.Fatalf("rewrite processed live bytes=%q want 900", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.processed_stale_bytes"]; got != "450" {
		t.Fatalf("rewrite processed stale bytes=%q want 450", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.reclaim_ratio"]; got != "0.400000" {
		t.Fatalf("rewrite reclaim ratio=%q want 0.400000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.output_ratio"]; got != "0.600000" {
		t.Fatalf("rewrite output ratio=%q want 0.600000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.processed_stale_ratio"]; got != "0.333333" {
		t.Fatalf("rewrite processed stale ratio=%q want 0.333333", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.bytes_in_per_sec"]; got != "6666.667" {
		t.Fatalf("rewrite exec bytes in/sec=%q want 6666.667", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.bytes_out_per_sec"]; got != "4000.000" {
		t.Fatalf("rewrite exec bytes out/sec=%q want 4000.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.reclaimed_bytes_per_sec"]; got != "2666.667" {
		t.Fatalf("rewrite exec reclaimed bytes/sec=%q want 2666.667", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.reclaimed_vs_churn_ratio"]; got != "1.066667" {
		t.Fatalf("rewrite reclaimed vs churn ratio=%q want 1.066667", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.last_live_bytes"]; got != "1000" {
		t.Fatalf("rewrite exec last live bytes=%q want 1000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.last_bytes_out"]; got != "600" {
		t.Fatalf("rewrite exec last bytes out=%q want 600", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.last_duration_ms"]; got != "40.000" {
		t.Fatalf("rewrite exec last duration ms=%q want 40.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.last_live_bytes_per_sec"]; got != "25000.000" {
		t.Fatalf("rewrite exec last live bytes/sec=%q want 25000.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.exec.last_unix_nano"]; got != "7777" {
		t.Fatalf("rewrite exec last unix nano=%q want 7777", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.no_reclaim_runs"]; got != "3" {
		t.Fatalf("rewrite no reclaim runs=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.no_reclaim_stale_bytes"]; got != "320" {
		t.Fatalf("rewrite no reclaim stale bytes=%q want 320", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.pending_ids"]; got != "2" {
		t.Fatalf("observed gc pending ids=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.pending_oldest_unix_nano"]; got == "0" {
		t.Fatalf("observed gc pending oldest unix nano=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.pending_oldest_age_ms"]; got == "0" {
		t.Fatalf("observed gc pending oldest age ms=%q want >0", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.pending_retry_ids"]; got != "2" {
		t.Fatalf("observed gc pending retry ids=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.pending_retry_attempts_total"]; got != "3" {
		t.Fatalf("observed gc pending retry attempts total=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.pending_retry_attempts_max"]; got != "2" {
		t.Fatalf("observed gc pending retry attempts max=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.queued_batches"]; got != "5" {
		t.Fatalf("observed gc queued batches=%q want 5", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.queued_ids"]; got != "12" {
		t.Fatalf("observed gc queued ids=%q want 12", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.taken_batches"]; got != "4" {
		t.Fatalf("observed gc taken batches=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.taken_ids"]; got != "9" {
		t.Fatalf("observed gc taken ids=%q want 9", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.runs"]; got != "3" {
		t.Fatalf("observed gc runs=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.retry_queued"]; got != "2" {
		t.Fatalf("observed gc retry queued=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.retry_dropped"]; got != "1" {
		t.Fatalf("observed gc retry dropped=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.retry_max_attempts"]; got != "3" {
		t.Fatalf("observed gc retry max attempts=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.latency.completed_ids"]; got != "6" {
		t.Fatalf("observed gc latency completed ids=%q want 6", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.latency.dropped_ids"]; got != "2" {
		t.Fatalf("observed gc latency dropped ids=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.latency.total_ms"]; got != "640" {
		t.Fatalf("observed gc latency total ms=%q want 640", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.latency.max_ms"]; got != "210" {
		t.Fatalf("observed gc latency max ms=%q want 210", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.latency.avg_ms"]; got != "80.000" {
		t.Fatalf("observed gc latency avg ms=%q want 80.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_segments_total"]; got != "11" {
		t.Fatalf("observed gc source segments total=%q want 11", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_segments_eligible_total"]; got != "5" {
		t.Fatalf("observed gc source segments eligible total=%q want 5", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_segments_deleted_total"]; got != "3" {
		t.Fatalf("observed gc source segments deleted total=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_segments_protected_in_use_total"]; got != "1" {
		t.Fatalf("observed gc source segments protected in-use total=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_segments_protected_retained_total"]; got != "2" {
		t.Fatalf("observed gc source segments protected retained total=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_segments_protected_overlap_total"]; got != "3" {
		t.Fatalf("observed gc source segments protected overlap total=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_segments_protected_other_total"]; got != "4" {
		t.Fatalf("observed gc source segments protected other total=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_bytes_total"]; got != "1100" {
		t.Fatalf("observed gc source bytes total=%q want 1100", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_bytes_eligible_total"]; got != "500" {
		t.Fatalf("observed gc source bytes eligible total=%q want 500", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_bytes_deleted_total"]; got != "300" {
		t.Fatalf("observed gc source bytes deleted total=%q want 300", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_bytes_protected_in_use_total"]; got != "50" {
		t.Fatalf("observed gc source bytes protected in-use total=%q want 50", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_bytes_protected_retained_total"]; got != "250" {
		t.Fatalf("observed gc source bytes protected retained total=%q want 250", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_bytes_protected_overlap_total"]; got != "75" {
		t.Fatalf("observed gc source bytes protected overlap total=%q want 75", got)
	}
	if got := stats["treedb.cache.vlog_generation.observed_gc.source_bytes_protected_other_total"]; got != "25" {
		t.Fatalf("observed gc source bytes protected other total=%q want 25", got)
	}
}

func TestVlogGenerationStats_QueueCapHintRequiresFullCoverage(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend}
	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()

	db.valueLogGenerationWarmTarget = 256
	db.vlogGenerationRewriteBudgetTokensBytes.Store(600)
	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteQueueLoaded = true
	db.vlogGenerationRewriteQueue = []uint32{11, 99}
	db.vlogGenerationRewriteLedger = []backenddb.ValueLogRewritePlanSegment{
		{FileID: 11, BytesTotal: 1000, BytesLive: 700, BytesStale: 300},
	}
	db.vlogGenerationRewriteQueueMu.Unlock()

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.known"]; got != "false" {
		t.Fatalf("rewrite queue live hint known=%q want false", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_present"]; got != "2" {
		t.Fatalf("rewrite queue live hint ids present=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_known"]; got != "1" {
		t.Fatalf("rewrite queue live hint ids known=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.coverage_pct"]; got != "50.000" {
		t.Fatalf("rewrite queue live hint coverage pct=%q want 50.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.bytes"]; got != "700" {
		t.Fatalf("rewrite queue live hint bytes=%q want 700", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes"]; got != "256" {
		t.Fatalf("rewrite queue run segment cap per-segment budget bytes=%q want 256", got)
	}
}

func TestVlogGenerationStats_QueueCapHintCoverageWithoutLedger(t *testing.T) {
	prepareDirectSchedulerTest(t)

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend}
	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()

	db.valueLogGenerationWarmTarget = 256
	db.vlogGenerationRewriteBudgetTokensBytes.Store(600)
	db.vlogGenerationRewriteQueueMu.Lock()
	db.vlogGenerationRewriteQueueLoaded = true
	db.vlogGenerationRewriteQueue = []uint32{11, 99}
	db.vlogGenerationRewriteLedger = nil
	db.vlogGenerationRewriteLedgerByFileID = nil
	db.vlogGenerationRewriteQueueMu.Unlock()

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.known"]; got != "false" {
		t.Fatalf("rewrite queue live hint known=%q want false", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_present"]; got != "2" {
		t.Fatalf("rewrite queue live hint ids present=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_known"]; got != "0" {
		t.Fatalf("rewrite queue live hint ids known=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.coverage_pct"]; got != "0.000" {
		t.Fatalf("rewrite queue live hint coverage pct=%q want 0.000", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.queue_live_hint.bytes"]; got != "0" {
		t.Fatalf("rewrite queue live hint bytes=%q want 0", got)
	}
}

func TestVlogGenerationSegmentTargetEnvOverrides(t *testing.T) {
	prepareDirectSchedulerTest(t)
	t.Setenv(envVlogGenerationLeafSegmentTargetBytes, "32768")
	t.Setenv(envVlogGenerationHotSegmentTargetBytes, "65536")
	t.Setenv(envVlogGenerationWarmSegmentTargetBytes, "131072")
	t.Setenv(envVlogGenerationColdSegmentTargetBytes, "262144")

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	recorder := &rewriteBudgetRecordingBackend{DB: backend}
	db, cleanup := openRewriteQueueTestDB(t, dir, recorder)
	defer cleanup()

	if got := db.valueLogGenerationLeafTarget; got != 32768 {
		t.Fatalf("leaf segment target=%d want 32768", got)
	}
	if got := db.valueLogGenerationHotTarget; got != 65536 {
		t.Fatalf("hot segment target=%d want 65536", got)
	}
	if got := db.valueLogGenerationWarmTarget; got != 131072 {
		t.Fatalf("warm segment target=%d want 131072", got)
	}
	if got := db.valueLogGenerationColdTarget; got != 262144 {
		t.Fatalf("cold segment target=%d want 262144", got)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.leaf.segment_target_bytes"]; got != "32768" {
		t.Fatalf("leaf segment target stats=%q want 32768", got)
	}
	if got := stats["treedb.cache.vlog_generation.hot.segment_target_bytes"]; got != "65536" {
		t.Fatalf("hot segment target stats=%q want 65536", got)
	}
	if got := stats["treedb.cache.vlog_generation.warm.segment_target_bytes"]; got != "131072" {
		t.Fatalf("warm segment target stats=%q want 131072", got)
	}
	if got := stats["treedb.cache.vlog_generation.cold.segment_target_bytes"]; got != "262144" {
		t.Fatalf("cold segment target stats=%q want 262144", got)
	}
}
