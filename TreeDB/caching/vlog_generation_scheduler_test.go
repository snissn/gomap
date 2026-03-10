package caching

import (
	"context"
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
}

func forceVlogMaintenanceIdle(db *DB) {
	if db != nil {
		idleAt := time.Now().Add(-2 * vlogGenerationMaintenanceQuietWindow)
		db.lastForegroundWriteUnixNano.Store(idleAt.UnixNano())
		db.lastForegroundReadUnixNano.Store(idleAt.UnixNano())
		db.activeForegroundIterators.Store(0)
	}
}

func disableVlogGenerationLoop(t *testing.T) {
	t.Helper()
	t.Setenv(envDisableVlogGenerationLoop, "1")
}

func prepareDirectSchedulerTest(t *testing.T) {
	t.Helper()
	disableVlogGenerationLoop(t)
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
		return backenddb.ValueLogRewritePlan{}, nil
	case <-ctx.Done():
		return backenddb.ValueLogRewritePlan{}, ctx.Err()
	}
}

func (b *blockingRewritePlannerBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	return backenddb.ValueLogRewriteStats{}, nil
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
	rewriteOpts     backenddb.ValueLogRewriteOnlineOptions
	rewriteCalls    int
	rewriteResponse backenddb.ValueLogRewriteStats
}

func (b *rewriteBudgetRecordingBackend) ValueLogRewritePlan(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewritePlan, error) {
	b.mu.Lock()
	b.planOpts = opts
	b.planCalls++
	plan := b.planResponse
	b.mu.Unlock()
	return plan, nil
}

func (b *rewriteBudgetRecordingBackend) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error) {
	b.mu.Lock()
	b.rewriteOpts = opts
	b.rewriteCalls++
	stats := b.rewriteResponse
	b.mu.Unlock()
	return stats, nil
}

func (b *rewriteBudgetRecordingBackend) recordedRewrite() (backenddb.ValueLogRewriteOnlineOptions, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rewriteOpts, b.rewriteCalls
}

func (b *rewriteBudgetRecordingBackend) recordedPlan() (backenddb.ValueLogRewriteOnlineOptions, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.planOpts, b.planCalls
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
