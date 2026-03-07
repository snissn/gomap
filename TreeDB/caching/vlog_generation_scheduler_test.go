package caching

import (
	"context"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

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

func TestVlogGenerationRewrite_UsesAndConsumesBudgetedBytes(t *testing.T) {
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

	db, err := Open(dir, recorder, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		JournalLanes:                     1,
		ValueLogGenerationPolicy:         uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogRewriteTriggerTotalBytes: 1,
		ForceValueLogPointers:            true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cachingdb: %v", err)
	}
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

	initialTokens := int64(1024)
	db.vlogGenerationRewriteBudgetTokensBytes.Store(initialTokens)
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
		_ = backend.Close()
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

	db.vlogGenerationRewriteBudgetTokensBytes.Store(128)
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

	db.vlogGenerationRewriteBudgetTokensBytes.Store(0)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0", calls)
	}
	if got := db.vlogGenerationRewriteBudgetTokensBytes.Load(); got != 0 {
		t.Fatalf("tokens after skipped rewrite=%d want=0", got)
	}
}

func TestVlogGenerationRewritePlan_DoesNotRunWithZeroBudgetTokens(t *testing.T) {
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

	db.vlogGenerationRewriteBudgetTokensBytes.Store(0)
	db.maybeRunVlogGenerationMaintenance(false)

	if _, calls := recorder.recordedPlan(); calls != 0 {
		t.Fatalf("plan calls=%d want=0", calls)
	}
	if _, calls := recorder.recordedRewrite(); calls != 0 {
		t.Fatalf("rewrite calls=%d want=0", calls)
	}
}
