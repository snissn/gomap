package caching

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type blockingRewritePlannerBackend struct {
	*backenddb.DB

	startOnce sync.Once
	planStart chan struct{}
	planBlock chan struct{}

	mu            sync.Mutex
	rewriteCalls  int
	planCompleted int
	planCanceled  int
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

func disableVlogGenerationLoop(t *testing.T) {
	t.Helper()
	t.Setenv(envDisableVlogGenerationLoop, "1")
}

func prepareDirectSchedulerTest(t *testing.T) {
	t.Helper()
	disableVlogGenerationLoop(t)
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
		b.mu.Unlock()
		return backenddb.ValueLogRewritePlan{}, nil
	case <-ctx.Done():
		b.mu.Lock()
		b.planCanceled++
		b.mu.Unlock()
		return backenddb.ValueLogRewritePlan{}, ctx.Err()
	}
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

type rewriteBudgetRecordingBackend struct {
	*backenddb.DB

	mu              sync.Mutex
	planOpts        backenddb.ValueLogRewriteOnlineOptions
	planCalls       int
	planResponse    backenddb.ValueLogRewritePlan
	planErr         error
	rewriteOpts     backenddb.ValueLogRewriteOnlineOptions
	rewriteCalls    int
	rewriteResponse backenddb.ValueLogRewriteStats
	rewriteErr      error
}

func (b *rewriteBudgetRecordingBackend) ValueLogRewritePlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
	b.mu.Lock()
	b.planOpts = cloneRewriteOptsForTest(opts)
	b.planCalls++
	plan := b.planResponse
	err := b.planErr
	b.mu.Unlock()
	return plan, err
}

func (b *rewriteBudgetRecordingBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	b.mu.Lock()
	b.rewriteOpts = cloneRewriteOptsForTest(opts)
	b.rewriteCalls++
	stats := b.rewriteResponse
	err := b.rewriteErr
	b.mu.Unlock()
	return stats, err
}

func (b *rewriteBudgetRecordingBackend) recordedRewrite() (backenddb.ValueLogRewriteOnlineOptions, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rewriteOpts, b.rewriteCalls
}

func cloneRewriteOptsForTest(opts backenddb.ValueLogRewriteOnlineOptions) backenddb.ValueLogRewriteOnlineOptions {
	cloned := opts
	cloned.SourceFileIDs = append([]uint32(nil), opts.SourceFileIDs...)
	cloned.ProtectedPaths = append([]string(nil), opts.ProtectedPaths...)
	return cloned
}

func (b *rewriteBudgetRecordingBackend) recordedPlan() (backenddb.ValueLogRewriteOnlineOptions, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.planOpts, b.planCalls
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
	// above and maybeRunVlogGenerationMaintenance(), which would defeat this
	// "empty budget" assertion on slow/loaded CI runners.
	db.vlogGenerationRewriteBudgetLastUnixNano.Store(time.Now().Add(10 * time.Second).UnixNano())
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls with empty budget=%d want still=1", calls)
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
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

	opts, calls := recorder.recordedRewrite()
	if calls != 1 {
		t.Fatalf("rewrite calls=%d want=1", calls)
	}
	if got, want := opts.SourceFileIDs, []uint32{22}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("rewrite SourceFileIDs=%v want=%v", got, want)
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
	db.vlogGenerationLastRewriteUnixNano.Store(time.Now().Add(-2 * vlogGenerationRewriteResumeMinInterval).UnixNano())
	forceVlogMaintenanceIdle(db)
	db.maybeRunVlogGenerationMaintenance(false)

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
	db.maybeRunVlogGenerationMaintenance(false)

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
	db.maybeRunVlogGenerationMaintenanceWithOptions(true, vlogGenerationMaintenanceOptions{
		bypassQuiet:           true,
		skipRetainedPruneWait: true,
		skipCheckpoint:        true,
		rewriteDebtDrain:      true,
	})

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
			SourceFileIDs:     []uint32{11, 22},
			SelectedBytesLive: 128,
		},
		rewriteResponse: backenddb.ValueLogRewriteStats{BytesBefore: 64, BytesAfter: 32, RecordsCopied: 1},
	}

	db, _ := openRewriteQueueTestDB(t, dir, recorder)
	db.maybeRunVlogGenerationMaintenance(false)
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

	db2.maybeRunVlogGenerationMaintenance(false)
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

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after first canceled run=%d want=1", calls)
	}

	// Isolate cancel-backoff behavior from min-interval throttling.
	db.vlogGenerationLastRewriteUnixNano.Store(0)
	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after immediate retry=%d want=1 (cancel backoff active)", calls)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.rewrite.canceled_runs"]; got != "1" {
		t.Fatalf("rewrite canceled runs=%q want 1", got)
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

	if _, calls := recorder.recordedRewrite(); calls != 1 {
		t.Fatalf("rewrite calls after first canceled run=%d want=1", calls)
	}

	db.vlogGenerationLastRewriteUnixNano.Store(0)
	db.vlogGenerationRewriteCanceledLastNS.Store(time.Now().Add(-2 * vlogGenerationRewriteCancelBackoff).UnixNano())
	db.maybeRunVlogGenerationMaintenance(false)
	if _, calls := recorder.recordedRewrite(); calls != 2 {
		t.Fatalf("rewrite calls after expired cancel backoff=%d want=2", calls)
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

func TestCheckpoint_KicksVlogGenerationRewriteDespiteRecentForegroundActivity(t *testing.T) {
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
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			_, planCalls := recorder.recordedPlan()
			t.Fatalf("checkpoint kick did not run rewrite in time: planCalls=%d rewriteCalls=%d", planCalls, rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, calls := recorder.recordedPlan(); calls != 1 {
		t.Fatalf("plan calls=%d want=1", calls)
	}
	if got := db.checkpointRuns.Load(); got < 2 {
		t.Fatalf("checkpoint runs=%d want >=2", got)
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

func TestCheckpoint_KicksVlogGenerationRewrite_WALOn(t *testing.T) {
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

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		if _, calls := recorder.recordedRewrite(); calls == 1 {
			break
		}
		if time.Now().After(deadline) {
			_, rewriteCalls := recorder.recordedRewrite()
			_, planCalls := recorder.recordedPlan()
			t.Fatalf("checkpoint kick did not run rewrite in time: planCalls=%d rewriteCalls=%d", planCalls, rewriteCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCheckpoint_KicksVlogGenerationGCDespiteRecentForegroundActivity(t *testing.T) {
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

	deadline := time.Now().Add(2 * schedulerTestWait(t))
	for {
		_, realCalls, _ := recorder.recordedCalls()
		if realCalls == 1 {
			break
		}
		if time.Now().After(deadline) {
			dryCalls, realCalls, _ := recorder.recordedCalls()
			t.Fatalf("checkpoint kick did not run gc in time: dryCalls=%d realCalls=%d", dryCalls, realCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if dryCalls, realCalls, _ := recorder.recordedCalls(); dryCalls != 0 || realCalls != 1 {
		t.Fatalf("gc calls dry=%d real=%d want dry=0 real=1", dryCalls, realCalls)
	}
	if got := db.checkpointRuns.Load(); got < 2 {
		t.Fatalf("checkpoint runs=%d want >=2", got)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.runs"]; got != "1" {
		t.Fatalf("checkpoint kick runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.gc_runs"]; got != "1" {
		t.Fatalf("checkpoint kick gc runs=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_generation.checkpoint_kick.active"]; got != "false" {
		t.Fatalf("checkpoint kick active=%q want false", got)
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
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_selected"]; got != "0" {
		t.Fatalf("plan selected=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_canceled"]; got != "0" {
		t.Fatalf("plan canceled=%q want 0", got)
	}
	if got := stats["treedb.cache.vlog_generation.rewrite.plan_errors"]; got != "0" {
		t.Fatalf("plan errors=%q want 0", got)
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
	paths, err := filepath.Glob(filepath.Join(dir, "wal", "value-l*.log"))
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

	close(blocking.planBlock)

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
	close(blocking.planBlock)

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

	db.lastForegroundWriteUnixNano.Store(time.Now().UnixNano())

	select {
	case <-doneMaintenance:
	case <-time.After(wait):
		t.Fatalf("maintenance did not cancel after foreground writes resumed")
	}

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

func TestVlogGenerationRewritePlan_CancelsWhenForegroundReadsResume(t *testing.T) {
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

	db.noteRead()

	select {
	case <-doneMaintenance:
	case <-time.After(wait):
		t.Fatalf("maintenance did not cancel after foreground reads resumed")
	}

	if got := db.vlogGenerationLastRewritePlanUnixNano.Load(); got != 0 {
		t.Fatalf("last rewrite plan timestamp=%d want=0 after cancellation", got)
	}
	if got := db.vlogGenerationSchedulerState.Load(); got != vlogGenerationSchedulerIdle {
		t.Fatalf("scheduler state=%d want=%d", got, vlogGenerationSchedulerIdle)
	}
}

func TestVlogGenerationRewritePlan_CancelStillRunsForcedGC(t *testing.T) {
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

	// Resume foreground activity so rewrite planning is canceled.
	db.noteRead()

	select {
	case <-doneMaintenance:
	case <-time.After(wait):
		t.Fatalf("maintenance did not finish after rewrite-plan cancellation")
	}

	if got := db.vlogGenerationGCRuns.Load(); got == 0 {
		t.Fatalf("expected forced GC path to run after rewrite-plan cancellation")
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
