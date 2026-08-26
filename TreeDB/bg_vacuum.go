package treedb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func backgroundIndexVacuumShouldReport(err error) bool {
	return err != nil &&
		!errors.Is(err, backenddb.ErrVacuumConcurrentMutation) &&
		!errors.Is(err, backenddb.ErrVacuumRecoverableRootSetRequired) &&
		!errors.Is(err, backenddb.ErrRecoverableRootSetStale) &&
		!errors.Is(err, backenddb.ErrDurableWALCleanupProofStale) &&
		!errors.Is(err, rootpublication.ErrResourcePinned) &&
		!errors.Is(err, backenddb.ErrVacuumUnsupported) &&
		!errors.Is(err, context.Canceled)
}

const (
	defaultBackgroundIndexVacuumSpanRatioPPM                uint32 = 1_200_000
	defaultBackgroundIndexVacuumMaxBacklogSkips             uint32 = 3
	defaultBackgroundIndexVacuumFreelistReclaimableRatioPPM uint32 = 250_000
	defaultBackgroundIndexVacuumFreelistReclaimablePages    uint64 = 64
	defaultBackgroundIndexVacuumCollectionRootSpanRatioPPM  uint32 = 1_200_000
	defaultBackgroundIndexVacuumCollectionRootPages         uint64 = 16
	backgroundIndexVacuumDebtReasonNone                            = "none"
	backgroundIndexVacuumDebtReasonUser                            = "user"
	backgroundIndexVacuumDebtReasonFreelist                        = "freelist"
	backgroundIndexVacuumDebtReasonCollectionRoots                 = "collection_roots"
	backgroundIndexVacuumRetryReasonNone                           = "none"
	backgroundIndexVacuumRetryReasonConcurrentMutation             = "concurrent_mutation"
	backgroundIndexVacuumRetryReasonRecoverableRootSet             = "recoverable_root_set"
	backgroundIndexVacuumRetryReasonCheckpointCleanup              = "checkpoint_cleanup"
	backgroundIndexVacuumRetryReasonResourcePinned                 = "resource_pinned"
	backgroundIndexVacuumOutcomeNone                               = "none"
	backgroundIndexVacuumOutcomeBacklogSkip                        = "backlog_skip"
	backgroundIndexVacuumOutcomeForegroundSkip                     = "foreground_skip"
	backgroundIndexVacuumOutcomeUnchanged                          = "unchanged"
	backgroundIndexVacuumOutcomeNoDebt                             = "no_debt"
	backgroundIndexVacuumOutcomeRetry                              = "retry"
	backgroundIndexVacuumOutcomeUnsupported                        = "unsupported"
	backgroundIndexVacuumOutcomePermanentFailure                   = "permanent_failure"
	backgroundIndexVacuumOutcomeSuccess                            = "success"
	backgroundIndexVacuumOutcomeCanceled                           = "canceled"
)

type bgIndexVacuumConfig struct {
	Interval time.Duration

	SpanRatioPPM uint32

	MaxBacklogSkips uint32

	FreelistReclaimableRatioPPM uint32
	FreelistReclaimablePages    uint64

	CollectionRootSpanRatioPPM uint32
	CollectionRootPages        uint64
}

func normalizeBGIndexVacuumConfig(cfg bgIndexVacuumConfig) bgIndexVacuumConfig {
	if cfg.SpanRatioPPM == 0 {
		cfg.SpanRatioPPM = defaultBackgroundIndexVacuumSpanRatioPPM
	}
	if cfg.MaxBacklogSkips == 0 {
		cfg.MaxBacklogSkips = defaultBackgroundIndexVacuumMaxBacklogSkips
	}
	if cfg.FreelistReclaimableRatioPPM == 0 {
		cfg.FreelistReclaimableRatioPPM = defaultBackgroundIndexVacuumFreelistReclaimableRatioPPM
	}
	if cfg.FreelistReclaimablePages == 0 {
		cfg.FreelistReclaimablePages = defaultBackgroundIndexVacuumFreelistReclaimablePages
	}
	if cfg.CollectionRootSpanRatioPPM == 0 {
		cfg.CollectionRootSpanRatioPPM = defaultBackgroundIndexVacuumCollectionRootSpanRatioPPM
	}
	if cfg.CollectionRootPages == 0 {
		cfg.CollectionRootPages = defaultBackgroundIndexVacuumCollectionRootPages
	}
	return cfg
}

type bgIndexVacuumWorker struct {
	enabled atomic.Bool

	interval time.Duration

	spanRatioPPM uint32

	maxBacklogSkips uint32

	freelistReclaimableRatioPPM uint32
	freelistReclaimablePages    uint64

	collectionRootSpanRatioPPM uint32
	collectionRootPages        uint64

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
	kickCh   chan struct{}
	cancel   context.CancelFunc

	runMu                        sync.Mutex
	runs                         atomic.Uint64
	probes                       atomic.Uint64
	vacuums                      atomic.Uint64
	vacuumAttempts               atomic.Uint64
	probeDurationTotalNs         atomic.Uint64
	probeDurationMaxNs           atomic.Uint64
	probeDurationLastNs          atomic.Uint64
	vacuumDurationTotalNs        atomic.Uint64
	vacuumDurationMaxNs          atomic.Uint64
	vacuumDurationLastNs         atomic.Uint64
	vacuumWorkCompleted          atomic.Uint64
	lastRunUnix                  atomic.Int64
	lastVacuumUnix               atomic.Int64
	lastSpanRatio                atomic.Uint64
	lastPages                    atomic.Uint64
	lastErr                      atomic.Value // string
	lastRetryReason              atomic.Value // string
	lastOutcome                  atomic.Value // string
	retryConcurrentMutationTotal atomic.Uint64
	retryRecoverableRootSetTotal atomic.Uint64
	retryCheckpointCleanupTotal  atomic.Uint64
	retryResourcePinnedTotal     atomic.Uint64
	unsupportedTotal             atomic.Uint64
	permanentFailuresTotal       atomic.Uint64

	backlogConsecutiveSkips    atomic.Uint64
	backlogSkips               atomic.Uint64
	backlogForcedRuns          atomic.Uint64
	lastBacklogBytes           atomic.Int64
	foregroundConsecutiveSkips atomic.Uint64
	foregroundSkips            atomic.Uint64
	foregroundForcedRuns       atomic.Uint64

	lastFreelistReclaimablePages atomic.Uint64
	lastFreelistReclaimableRatio atomic.Uint64
	lastCollectionRootPages      atomic.Uint64
	lastCollectionRootSpanRatio  atomic.Uint64
	lastDebtReason               atomic.Value // string
	lastOnlineVacuum             atomic.Pointer[backenddb.VacuumOnlineStats]

	lastProbeCommitSeq              uint64
	lastProbeFreelistReclaimable    uint64
	lastProbeFreelistReclaimablePPM uint64
	lastProbeFreelistValid          bool
	lastProbeValid                  bool
	retryProbe                      bool
	unsupported                     bool
}

var bgIndexVacuumBacklogBytesHook struct {
	mu sync.RWMutex
	fn func(*DB) int64
}

var bgIndexVacuumForegroundWriteQuietHook struct {
	mu sync.RWMutex
	fn func(*DB) bool
}

var bgIndexVacuumFreelistDebtSnapshotHook struct {
	mu sync.RWMutex
	fn func(*DB) (backenddb.IndexVacuumFreelistDebtSnapshot, bool)
}

var bgIndexVacuumRunHook struct {
	mu sync.RWMutex
	fn func(*DB, context.Context) (backenddb.VacuumOnlineStats, error)
}

var bgIndexVacuumTriggerReportHook struct {
	mu sync.RWMutex
	fn func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error)
}

func setBackgroundIndexVacuumBacklogBytesHookForTest(fn func(*DB) int64) func() {
	bgIndexVacuumBacklogBytesHook.mu.Lock()
	prev := bgIndexVacuumBacklogBytesHook.fn
	bgIndexVacuumBacklogBytesHook.fn = fn
	bgIndexVacuumBacklogBytesHook.mu.Unlock()
	return func() {
		bgIndexVacuumBacklogBytesHook.mu.Lock()
		bgIndexVacuumBacklogBytesHook.fn = prev
		bgIndexVacuumBacklogBytesHook.mu.Unlock()
	}
}

func setBackgroundIndexVacuumForegroundWriteQuietHookForTest(fn func(*DB) bool) func() {
	bgIndexVacuumForegroundWriteQuietHook.mu.Lock()
	prev := bgIndexVacuumForegroundWriteQuietHook.fn
	bgIndexVacuumForegroundWriteQuietHook.fn = fn
	bgIndexVacuumForegroundWriteQuietHook.mu.Unlock()
	return func() {
		bgIndexVacuumForegroundWriteQuietHook.mu.Lock()
		bgIndexVacuumForegroundWriteQuietHook.fn = prev
		bgIndexVacuumForegroundWriteQuietHook.mu.Unlock()
	}
}

func setBackgroundIndexVacuumFreelistDebtSnapshotHookForTest(fn func(*DB) (backenddb.IndexVacuumFreelistDebtSnapshot, bool)) func() {
	bgIndexVacuumFreelistDebtSnapshotHook.mu.Lock()
	prev := bgIndexVacuumFreelistDebtSnapshotHook.fn
	bgIndexVacuumFreelistDebtSnapshotHook.fn = fn
	bgIndexVacuumFreelistDebtSnapshotHook.mu.Unlock()
	return func() {
		bgIndexVacuumFreelistDebtSnapshotHook.mu.Lock()
		bgIndexVacuumFreelistDebtSnapshotHook.fn = prev
		bgIndexVacuumFreelistDebtSnapshotHook.mu.Unlock()
	}
}

func setBackgroundIndexVacuumRunHookForTest(fn func(*DB, context.Context) (backenddb.VacuumOnlineStats, error)) func() {
	bgIndexVacuumRunHook.mu.Lock()
	prev := bgIndexVacuumRunHook.fn
	bgIndexVacuumRunHook.fn = fn
	bgIndexVacuumRunHook.mu.Unlock()
	return func() {
		bgIndexVacuumRunHook.mu.Lock()
		bgIndexVacuumRunHook.fn = prev
		bgIndexVacuumRunHook.mu.Unlock()
	}
}

func setBackgroundIndexVacuumTriggerReportHookForTest(fn func(*DB, context.Context) (backenddb.IndexVacuumTriggerReport, error)) func() {
	bgIndexVacuumTriggerReportHook.mu.Lock()
	prev := bgIndexVacuumTriggerReportHook.fn
	bgIndexVacuumTriggerReportHook.fn = fn
	bgIndexVacuumTriggerReportHook.mu.Unlock()
	return func() {
		bgIndexVacuumTriggerReportHook.mu.Lock()
		bgIndexVacuumTriggerReportHook.fn = prev
		bgIndexVacuumTriggerReportHook.mu.Unlock()
	}
}

func backgroundIndexVacuumTriggerReport(db *DB, ctx context.Context) (backenddb.IndexVacuumTriggerReport, error) {
	bgIndexVacuumTriggerReportHook.mu.RLock()
	hook := bgIndexVacuumTriggerReportHook.fn
	bgIndexVacuumTriggerReportHook.mu.RUnlock()
	if hook != nil {
		return hook(db, ctx)
	}
	return db.backend.IndexVacuumTriggerReportContext(ctx)
}

func backgroundIndexVacuumRun(db *DB, ctx context.Context) (backenddb.VacuumOnlineStats, error) {
	bgIndexVacuumRunHook.mu.RLock()
	hook := bgIndexVacuumRunHook.fn
	bgIndexVacuumRunHook.mu.RUnlock()
	if hook != nil {
		return hook(db, ctx)
	}
	return db.vacuumIndexOnlineStats(ctx)
}

func backgroundIndexVacuumBacklogBytes(db *DB) int64 {
	bgIndexVacuumBacklogBytesHook.mu.RLock()
	hook := bgIndexVacuumBacklogBytesHook.fn
	bgIndexVacuumBacklogBytesHook.mu.RUnlock()
	if hook != nil {
		return hook(db)
	}
	if db == nil || db.cached == nil {
		return 0
	}
	return db.cached.QueueBacklogBytes()
}

func backgroundIndexVacuumForegroundWriteQuiet(db *DB) bool {
	bgIndexVacuumForegroundWriteQuietHook.mu.RLock()
	hook := bgIndexVacuumForegroundWriteQuietHook.fn
	bgIndexVacuumForegroundWriteQuietHook.mu.RUnlock()
	if hook != nil {
		return hook(db)
	}
	return db == nil || db.cached == nil || db.cached.BackgroundVacuumForegroundWriteQuiet()
}

func backgroundIndexVacuumFreelistDebtSnapshot(db *DB) (backenddb.IndexVacuumFreelistDebtSnapshot, bool) {
	bgIndexVacuumFreelistDebtSnapshotHook.mu.RLock()
	hook := bgIndexVacuumFreelistDebtSnapshotHook.fn
	bgIndexVacuumFreelistDebtSnapshotHook.mu.RUnlock()
	if hook != nil {
		return hook(db)
	}
	if db == nil || db.backend == nil {
		return backenddb.IndexVacuumFreelistDebtSnapshot{}, false
	}
	return db.backend.IndexVacuumFreelistDebtSnapshot()
}

// Start launches the background index vacuum loop with the provided interval.
func (w *bgIndexVacuumWorker) Start(db *DB, cfg bgIndexVacuumConfig) {
	cfg = normalizeBGIndexVacuumConfig(cfg)
	if cfg.Interval <= 0 || db == nil {
		w.enabled.Store(false)
		return
	}

	w.enabled.Store(true)
	w.interval = cfg.Interval
	w.spanRatioPPM = cfg.SpanRatioPPM
	w.maxBacklogSkips = cfg.MaxBacklogSkips
	w.freelistReclaimableRatioPPM = cfg.FreelistReclaimableRatioPPM
	w.freelistReclaimablePages = cfg.FreelistReclaimablePages
	w.collectionRootSpanRatioPPM = cfg.CollectionRootSpanRatioPPM
	w.collectionRootPages = cfg.CollectionRootPages
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	w.kickCh = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.lastErr.Store("")
	w.lastDebtReason.Store(backgroundIndexVacuumDebtReasonNone)
	w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonNone)
	w.lastOutcome.Store(backgroundIndexVacuumOutcomeNone)

	go func() {
		defer close(w.doneCh)

		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-w.stopCh:
				return
			case <-w.kickCh:
			case <-ticker.C:
			}

			w.runOnceContext(ctx, db)
		}
	}()
}

// Enabled reports whether the background vacuum loop is running.
func (w *bgIndexVacuumWorker) Enabled() bool {
	return w.enabled.Load()
}

// Kick requests an immediate vacuum pass (best-effort).
func (w *bgIndexVacuumWorker) Kick() {
	if !w.Enabled() || w.kickCh == nil {
		return
	}
	select {
	case w.kickCh <- struct{}{}:
	default:
	}
}

// Stop terminates the background vacuum loop and waits for it to exit.
func (w *bgIndexVacuumWorker) Stop() {
	if !w.Enabled() {
		return
	}
	w.stopOnce.Do(func() {
		w.cancel()
		close(w.stopCh)
		<-w.doneCh
		w.enabled.Store(false)
	})
}

func (w *bgIndexVacuumWorker) runOnce(db *DB) {
	w.runOnceContext(context.Background(), db)
}

func (w *bgIndexVacuumWorker) runOnceContext(ctx context.Context, db *DB) {
	if db == nil || db.backend == nil {
		return
	}

	w.runMu.Lock()
	defer w.runMu.Unlock()
	if w.unsupported {
		return
	}

	now := time.Now()
	state, ok := db.backend.StateToken()
	if !ok || db.backend.IsClosing() {
		return
	}

	forcedAfterBacklog := false
	backlogBytes := backgroundIndexVacuumBacklogBytes(db)
	w.lastBacklogBytes.Store(backlogBytes)
	if backlogBytes > 0 {
		consecutive := w.backlogConsecutiveSkips.Load()
		if consecutive < uint64(w.maxBacklogSkipThreshold()) {
			consecutive++
			w.backlogConsecutiveSkips.Store(consecutive)
			w.backlogSkips.Add(1)
			w.foregroundConsecutiveSkips.Store(0)
			w.lastOutcome.Store(backgroundIndexVacuumOutcomeBacklogSkip)
			w.finishRun(now, "")
			return
		}
		forcedAfterBacklog = true
		w.backlogForcedRuns.Add(1)
		w.backlogConsecutiveSkips.Store(0)
	} else {
		w.backlogConsecutiveSkips.Store(0)
	}
	forcedAfterForeground := false
	if !forcedAfterBacklog && !backgroundIndexVacuumForegroundWriteQuiet(db) {
		consecutive := w.foregroundConsecutiveSkips.Load()
		if consecutive < uint64(w.maxBacklogSkipThreshold()) {
			w.foregroundConsecutiveSkips.Store(consecutive + 1)
			w.foregroundSkips.Add(1)
			w.lastOutcome.Store(backgroundIndexVacuumOutcomeForegroundSkip)
			w.finishRun(now, "")
			return
		}
		forcedAfterForeground = true
		w.foregroundForcedRuns.Add(1)
		w.foregroundConsecutiveSkips.Store(0)
	} else {
		w.foregroundConsecutiveSkips.Store(0)
	}

	if !forcedAfterBacklog && !forcedAfterForeground && w.lastProbeValid && !w.retryProbe && state.CommitSeq == w.lastProbeCommitSeq && !w.freelistDebtChangedSinceLastProbe(db) {
		w.lastOutcome.Store(backgroundIndexVacuumOutcomeUnchanged)
		w.finishRun(now, "")
		return
	}

	probeStarted := time.Now()
	rep, err := backgroundIndexVacuumTriggerReport(db, ctx)
	w.probes.Add(1)
	w.recordProbeDuration(time.Since(probeStarted))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			w.retryProbe = false
			w.lastOutcome.Store(backgroundIndexVacuumOutcomeCanceled)
			w.finishRun(now, err.Error())
			return
		}
		w.retryProbe = false
		w.lastProbeCommitSeq = state.CommitSeq
		if snap, ok := backgroundIndexVacuumFreelistDebtSnapshot(db); ok {
			w.lastProbeFreelistReclaimable = snap.FreelistReclaimable
			w.lastProbeFreelistReclaimablePPM = snap.FreelistReclaimablePPM
			w.lastProbeFreelistValid = snap.FreelistReclaimableValid
		}
		w.lastProbeValid = true
		w.permanentFailuresTotal.Add(1)
		w.lastOutcome.Store(backgroundIndexVacuumOutcomePermanentFailure)
		w.finishRun(now, err.Error())
		db.reportError(err)
		return
	}

	w.lastProbeCommitSeq = rep.CommitSeq
	w.lastProbeFreelistReclaimable = rep.FreelistReclaimablePages
	w.lastProbeFreelistReclaimablePPM = rep.FreelistReclaimableRatioPPM
	w.lastProbeFreelistValid = rep.FreelistReclaimableValid
	w.lastProbeValid = true
	w.recordLastTriggerReport(rep)

	reason := w.triggerReason(rep)
	w.lastDebtReason.Store(reason)
	if reason == backgroundIndexVacuumDebtReasonNone {
		w.retryProbe = false
		w.lastOutcome.Store(backgroundIndexVacuumOutcomeNoDebt)
		w.finishRun(now, "")
		return
	}

	w.vacuumAttempts.Add(1)
	vacuumStarted := time.Now()
	onlineStats, err := backgroundIndexVacuumRun(db, ctx)
	if err != nil {
		w.recordVacuumDuration(time.Since(vacuumStarted))
		if onlineStats.AttemptID != 0 {
			w.lastOnlineVacuum.Store(&onlineStats)
		}
		w.recordVacuumError(err)
		w.finishRun(now, err.Error())
		// A bounded online-vacuum pass may lose its cutover race to foreground
		// mutations. That is expected retry control flow: retain it in worker
		// diagnostics and retry the probe without poisoning the database handle.
		if backgroundIndexVacuumShouldReport(err) {
			db.reportError(err)
		}
		return
	}
	w.recordVacuumDuration(time.Since(vacuumStarted))
	if onlineStats.AttemptID != 0 {
		w.lastOnlineVacuum.Store(&onlineStats)
	}
	w.vacuumWorkCompleted.Add(1)

	w.retryProbe = false
	w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonNone)
	w.lastOutcome.Store(backgroundIndexVacuumOutcomeSuccess)
	w.lastErr.Store("")
	w.vacuums.Add(1)
	w.finishRun(now, "")
	w.lastVacuumUnix.Store(now.Unix())
}

func (w *bgIndexVacuumWorker) recordProbeDuration(d time.Duration) {
	ns := uint64(d)
	w.probeDurationLastNs.Store(ns)
	w.probeDurationTotalNs.Add(ns)
	updateBGVacuumMax(&w.probeDurationMaxNs, ns)
}

func (w *bgIndexVacuumWorker) recordVacuumDuration(d time.Duration) {
	ns := uint64(d)
	w.vacuumDurationLastNs.Store(ns)
	w.vacuumDurationTotalNs.Add(ns)
	updateBGVacuumMax(&w.vacuumDurationMaxNs, ns)
}

func updateBGVacuumMax(dst *atomic.Uint64, value uint64) {
	for current := dst.Load(); current < value && !dst.CompareAndSwap(current, value); current = dst.Load() {
	}
}

func (w *bgIndexVacuumWorker) recordVacuumError(err error) {
	switch {
	case errors.Is(err, backenddb.ErrVacuumUnsupported):
		w.retryProbe = false
		w.unsupported = true
		w.unsupportedTotal.Add(1)
		w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonNone)
		w.lastOutcome.Store(backgroundIndexVacuumOutcomeUnsupported)
	case errors.Is(err, context.Canceled):
		w.retryProbe = false
		w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonNone)
		w.lastOutcome.Store(backgroundIndexVacuumOutcomeCanceled)
	case errors.Is(err, backenddb.ErrVacuumConcurrentMutation):
		w.retryProbe = true
		w.retryConcurrentMutationTotal.Add(1)
		w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonConcurrentMutation)
		w.lastOutcome.Store(backgroundIndexVacuumOutcomeRetry)
	case errors.Is(err, backenddb.ErrRecoverableRootSetStale), errors.Is(err, backenddb.ErrVacuumRecoverableRootSetRequired):
		w.retryProbe = true
		w.retryRecoverableRootSetTotal.Add(1)
		w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonRecoverableRootSet)
		w.lastOutcome.Store(backgroundIndexVacuumOutcomeRetry)
	case errors.Is(err, backenddb.ErrDurableWALCleanupProofStale):
		w.retryProbe = true
		w.retryCheckpointCleanupTotal.Add(1)
		w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonCheckpointCleanup)
		w.lastOutcome.Store(backgroundIndexVacuumOutcomeRetry)
	case errors.Is(err, rootpublication.ErrResourcePinned):
		w.retryProbe = true
		w.retryResourcePinnedTotal.Add(1)
		w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonResourcePinned)
		w.lastOutcome.Store(backgroundIndexVacuumOutcomeRetry)
	default:
		w.retryProbe = false
		w.permanentFailuresTotal.Add(1)
		w.lastRetryReason.Store(backgroundIndexVacuumRetryReasonNone)
		w.lastOutcome.Store(backgroundIndexVacuumOutcomePermanentFailure)
	}
}

func (w *bgIndexVacuumWorker) freelistDebtChangedSinceLastProbe(db *DB) bool {
	if w.freelistReclaimablePages == 0 && w.freelistReclaimableRatioPPM == 0 {
		return false
	}
	snap, ok := backgroundIndexVacuumFreelistDebtSnapshot(db)
	if !ok {
		return true
	}
	return snap.FreelistReclaimableValid != w.lastProbeFreelistValid ||
		snap.FreelistReclaimable != w.lastProbeFreelistReclaimable ||
		snap.FreelistReclaimablePPM != w.lastProbeFreelistReclaimablePPM
}

func (w *bgIndexVacuumWorker) finishRun(now time.Time, err string) {
	w.runs.Add(1)
	w.lastRunUnix.Store(now.Unix())
	if err != "" {
		w.lastErr.Store(err)
	}
}

func (w *bgIndexVacuumWorker) recordLastTriggerReport(rep backenddb.IndexVacuumTriggerReport) {
	w.lastPages.Store(rep.UserPages)
	if rep.UserPages > 0 {
		w.lastSpanRatio.Store(rep.UserSpanRatioPPM)
	} else {
		w.lastSpanRatio.Store(0)
	}
	w.lastFreelistReclaimablePages.Store(rep.FreelistReclaimablePages)
	if rep.FreelistReclaimableValid {
		w.lastFreelistReclaimableRatio.Store(rep.FreelistReclaimableRatioPPM)
	} else {
		w.lastFreelistReclaimableRatio.Store(0)
	}
	w.lastCollectionRootPages.Store(rep.CollectionRootPages)
	if rep.CollectionRootSpanRatioValid {
		w.lastCollectionRootSpanRatio.Store(rep.CollectionRootSpanRatioPPM)
	} else {
		w.lastCollectionRootSpanRatio.Store(0)
	}
}

func (w *bgIndexVacuumWorker) triggerReason(rep backenddb.IndexVacuumTriggerReport) string {
	if rep.UserPages > 0 && rep.UserSpanRatioPPM >= uint64(w.userSpanRatioThreshold()) {
		return backgroundIndexVacuumDebtReasonUser
	}
	if rep.FreelistReclaimableValid &&
		rep.FreelistReclaimablePages >= w.freelistReclaimablePagesThreshold() &&
		rep.FreelistReclaimableRatioPPM >= uint64(w.freelistReclaimableRatioThreshold()) {
		return backgroundIndexVacuumDebtReasonFreelist
	}
	if rep.CollectionRootSpanRatioValid &&
		rep.CollectionRootPages >= w.collectionRootPagesThreshold() &&
		rep.CollectionRootSpanRatioPPM >= uint64(w.collectionRootSpanRatioThreshold()) {
		return backgroundIndexVacuumDebtReasonCollectionRoots
	}
	return backgroundIndexVacuumDebtReasonNone
}

func (w *bgIndexVacuumWorker) userSpanRatioThreshold() uint32 {
	if w.spanRatioPPM == 0 {
		return defaultBackgroundIndexVacuumSpanRatioPPM
	}
	return w.spanRatioPPM
}

func (w *bgIndexVacuumWorker) maxBacklogSkipThreshold() uint32 {
	if w.maxBacklogSkips == 0 {
		return defaultBackgroundIndexVacuumMaxBacklogSkips
	}
	return w.maxBacklogSkips
}

func (w *bgIndexVacuumWorker) freelistReclaimableRatioThreshold() uint32 {
	if w.freelistReclaimableRatioPPM == 0 {
		return defaultBackgroundIndexVacuumFreelistReclaimableRatioPPM
	}
	return w.freelistReclaimableRatioPPM
}

func (w *bgIndexVacuumWorker) freelistReclaimablePagesThreshold() uint64 {
	if w.freelistReclaimablePages == 0 {
		return defaultBackgroundIndexVacuumFreelistReclaimablePages
	}
	return w.freelistReclaimablePages
}

func (w *bgIndexVacuumWorker) collectionRootSpanRatioThreshold() uint32 {
	if w.collectionRootSpanRatioPPM == 0 {
		return defaultBackgroundIndexVacuumCollectionRootSpanRatioPPM
	}
	return w.collectionRootSpanRatioPPM
}

func (w *bgIndexVacuumWorker) collectionRootPagesThreshold() uint64 {
	if w.collectionRootPages == 0 {
		return defaultBackgroundIndexVacuumCollectionRootPages
	}
	return w.collectionRootPages
}

type bgIndexVacuumStats struct {
	Enabled  bool
	Interval time.Duration

	SpanRatioPPM uint32

	MaxBacklogSkips uint32

	FreelistReclaimableRatioPPM uint32
	FreelistReclaimablePages    uint64

	CollectionRootSpanRatioPPM uint32
	CollectionRootPages        uint64

	Runs                  uint64
	Probes                uint64
	Vacuums               uint64
	VacuumAttempts        uint64
	ProbeDurationTotalNs  uint64
	ProbeDurationMaxNs    uint64
	ProbeDurationLastNs   uint64
	VacuumDurationTotalNs uint64
	VacuumDurationMaxNs   uint64
	VacuumDurationLastNs  uint64
	VacuumWorkCompleted   uint64

	BacklogConsecutiveSkips    uint64
	BacklogSkips               uint64
	BacklogForcedRuns          uint64
	LastBacklogBytes           int64
	ForegroundConsecutiveSkips uint64
	ForegroundSkips            uint64
	ForegroundForcedRuns       uint64

	LastRunUnix     int64
	LastVacuumUnix  int64
	LastSpanRatio   uint64
	LastPages       uint64
	LastErr         string
	LastRetryReason string
	LastOutcome     string

	RetryConcurrentMutationTotal uint64
	RetryRecoverableRootSetTotal uint64
	RetryCheckpointCleanupTotal  uint64
	RetryResourcePinnedTotal     uint64
	UnsupportedTotal             uint64
	PermanentFailuresTotal       uint64

	LastFreelistReclaimablePages uint64
	LastFreelistReclaimableRatio uint64
	LastCollectionRootPages      uint64
	LastCollectionRootSpanRatio  uint64
	LastDebtReason               string
	LastOnlineVacuum             backenddb.VacuumOnlineStats
}

// Stats returns a snapshot of background index vacuum state and recent run info.
func (w *bgIndexVacuumWorker) Stats() bgIndexVacuumStats {
	out := bgIndexVacuumStats{
		Enabled:                      w.Enabled(),
		Interval:                     w.interval,
		SpanRatioPPM:                 w.userSpanRatioThreshold(),
		MaxBacklogSkips:              w.maxBacklogSkipThreshold(),
		FreelistReclaimableRatioPPM:  w.freelistReclaimableRatioThreshold(),
		FreelistReclaimablePages:     w.freelistReclaimablePagesThreshold(),
		CollectionRootSpanRatioPPM:   w.collectionRootSpanRatioThreshold(),
		CollectionRootPages:          w.collectionRootPagesThreshold(),
		Runs:                         w.runs.Load(),
		Probes:                       w.probes.Load(),
		Vacuums:                      w.vacuums.Load(),
		VacuumAttempts:               w.vacuumAttempts.Load(),
		ProbeDurationTotalNs:         w.probeDurationTotalNs.Load(),
		ProbeDurationMaxNs:           w.probeDurationMaxNs.Load(),
		ProbeDurationLastNs:          w.probeDurationLastNs.Load(),
		VacuumDurationTotalNs:        w.vacuumDurationTotalNs.Load(),
		VacuumDurationMaxNs:          w.vacuumDurationMaxNs.Load(),
		VacuumDurationLastNs:         w.vacuumDurationLastNs.Load(),
		VacuumWorkCompleted:          w.vacuumWorkCompleted.Load(),
		BacklogConsecutiveSkips:      w.backlogConsecutiveSkips.Load(),
		BacklogSkips:                 w.backlogSkips.Load(),
		BacklogForcedRuns:            w.backlogForcedRuns.Load(),
		LastBacklogBytes:             w.lastBacklogBytes.Load(),
		ForegroundConsecutiveSkips:   w.foregroundConsecutiveSkips.Load(),
		ForegroundSkips:              w.foregroundSkips.Load(),
		ForegroundForcedRuns:         w.foregroundForcedRuns.Load(),
		LastRunUnix:                  w.lastRunUnix.Load(),
		LastVacuumUnix:               w.lastVacuumUnix.Load(),
		LastSpanRatio:                w.lastSpanRatio.Load(),
		LastPages:                    w.lastPages.Load(),
		RetryConcurrentMutationTotal: w.retryConcurrentMutationTotal.Load(),
		RetryRecoverableRootSetTotal: w.retryRecoverableRootSetTotal.Load(),
		RetryCheckpointCleanupTotal:  w.retryCheckpointCleanupTotal.Load(),
		RetryResourcePinnedTotal:     w.retryResourcePinnedTotal.Load(),
		UnsupportedTotal:             w.unsupportedTotal.Load(),
		PermanentFailuresTotal:       w.permanentFailuresTotal.Load(),
		LastFreelistReclaimablePages: w.lastFreelistReclaimablePages.Load(),
		LastFreelistReclaimableRatio: w.lastFreelistReclaimableRatio.Load(),
		LastCollectionRootPages:      w.lastCollectionRootPages.Load(),
		LastCollectionRootSpanRatio:  w.lastCollectionRootSpanRatio.Load(),
	}
	if v := w.lastErr.Load(); v != nil {
		out.LastErr, _ = v.(string)
	}
	if v := w.lastDebtReason.Load(); v != nil {
		out.LastDebtReason, _ = v.(string)
	}
	if out.LastDebtReason == "" {
		out.LastDebtReason = backgroundIndexVacuumDebtReasonNone
	}
	if v := w.lastRetryReason.Load(); v != nil {
		out.LastRetryReason, _ = v.(string)
	}
	if out.LastRetryReason == "" {
		out.LastRetryReason = backgroundIndexVacuumRetryReasonNone
	}
	if v := w.lastOutcome.Load(); v != nil {
		out.LastOutcome, _ = v.(string)
	}
	if out.LastOutcome == "" {
		out.LastOutcome = backgroundIndexVacuumOutcomeNone
	}
	if last := w.lastOnlineVacuum.Load(); last != nil {
		out.LastOnlineVacuum = *last
	}
	return out
}

func bgIndexVacuumStatsInto(out map[string]string, w *bgIndexVacuumWorker) {
	stats := w.Stats()
	out["treedb.bg_vacuum.enabled"] = fmt.Sprintf("%t", stats.Enabled)
	out["treedb.bg_vacuum.interval_ms"] = fmt.Sprintf("%d", stats.Interval.Milliseconds())
	out["treedb.bg_vacuum.span_ratio_ppm_threshold"] = fmt.Sprintf("%d", stats.SpanRatioPPM)
	out["treedb.bg_vacuum.freelist_reclaimable_ratio_ppm_threshold"] = fmt.Sprintf("%d", stats.FreelistReclaimableRatioPPM)
	out["treedb.bg_vacuum.freelist_reclaimable_pages_threshold"] = fmt.Sprintf("%d", stats.FreelistReclaimablePages)
	out["treedb.bg_vacuum.collection_roots_span_ratio_ppm_threshold"] = fmt.Sprintf("%d", stats.CollectionRootSpanRatioPPM)
	out["treedb.bg_vacuum.collection_roots_pages_threshold"] = fmt.Sprintf("%d", stats.CollectionRootPages)
	out["treedb.bg_vacuum.max_backlog_skips"] = fmt.Sprintf("%d", stats.MaxBacklogSkips)
	out["treedb.bg_vacuum.runs"] = fmt.Sprintf("%d", stats.Runs)
	out["treedb.bg_vacuum.trigger_probes"] = fmt.Sprintf("%d", stats.Probes)
	out["treedb.bg_vacuum.vacuums"] = fmt.Sprintf("%d", stats.Vacuums)
	out["treedb.bg_vacuum.vacuum_attempts"] = fmt.Sprintf("%d", stats.VacuumAttempts)
	out["treedb.bg_vacuum.probe_duration_ns_total"] = fmt.Sprintf("%d", stats.ProbeDurationTotalNs)
	out["treedb.bg_vacuum.probe_duration_ns_max"] = fmt.Sprintf("%d", stats.ProbeDurationMaxNs)
	out["treedb.bg_vacuum.probe_duration_ns_last"] = fmt.Sprintf("%d", stats.ProbeDurationLastNs)
	out["treedb.bg_vacuum.vacuum_duration_ns_total"] = fmt.Sprintf("%d", stats.VacuumDurationTotalNs)
	out["treedb.bg_vacuum.vacuum_duration_ns_max"] = fmt.Sprintf("%d", stats.VacuumDurationMaxNs)
	out["treedb.bg_vacuum.vacuum_duration_ns_last"] = fmt.Sprintf("%d", stats.VacuumDurationLastNs)
	out["treedb.bg_vacuum.vacuum_work_completed"] = fmt.Sprintf("%d", stats.VacuumWorkCompleted)
	out["treedb.bg_vacuum.retry_concurrent_mutation_total"] = fmt.Sprintf("%d", stats.RetryConcurrentMutationTotal)
	out["treedb.bg_vacuum.retry_recoverable_root_set_total"] = fmt.Sprintf("%d", stats.RetryRecoverableRootSetTotal)
	out["treedb.bg_vacuum.retry_checkpoint_cleanup_total"] = fmt.Sprintf("%d", stats.RetryCheckpointCleanupTotal)
	out["treedb.bg_vacuum.retry_resource_pinned_total"] = fmt.Sprintf("%d", stats.RetryResourcePinnedTotal)
	out["treedb.bg_vacuum.unsupported_total"] = fmt.Sprintf("%d", stats.UnsupportedTotal)
	out["treedb.bg_vacuum.permanent_failures_total"] = fmt.Sprintf("%d", stats.PermanentFailuresTotal)
	out["treedb.bg_vacuum.last_retry_reason"] = stats.LastRetryReason
	out["treedb.bg_vacuum.last_outcome"] = stats.LastOutcome
	out["treedb.bg_vacuum.backlog_skips_consecutive"] = fmt.Sprintf("%d", stats.BacklogConsecutiveSkips)
	out["treedb.bg_vacuum.backlog_skips_total"] = fmt.Sprintf("%d", stats.BacklogSkips)
	out["treedb.bg_vacuum.backlog_forced_runs"] = fmt.Sprintf("%d", stats.BacklogForcedRuns)
	out["treedb.bg_vacuum.last_backlog_bytes"] = fmt.Sprintf("%d", stats.LastBacklogBytes)
	out["treedb.bg_vacuum.foreground_skips_consecutive"] = fmt.Sprintf("%d", stats.ForegroundConsecutiveSkips)
	out["treedb.bg_vacuum.foreground_skips_total"] = fmt.Sprintf("%d", stats.ForegroundSkips)
	out["treedb.bg_vacuum.foreground_forced_runs"] = fmt.Sprintf("%d", stats.ForegroundForcedRuns)
	out["treedb.bg_vacuum.last_run_unix"] = fmt.Sprintf("%d", stats.LastRunUnix)
	out["treedb.bg_vacuum.last_vacuum_unix"] = fmt.Sprintf("%d", stats.LastVacuumUnix)
	out["treedb.bg_vacuum.last_span_ratio_ppm"] = fmt.Sprintf("%d", stats.LastSpanRatio)
	out["treedb.bg_vacuum.last_pages"] = fmt.Sprintf("%d", stats.LastPages)
	out["treedb.bg_vacuum.last_freelist_reclaimable_pages"] = fmt.Sprintf("%d", stats.LastFreelistReclaimablePages)
	out["treedb.bg_vacuum.last_freelist_reclaimable_ratio_ppm"] = fmt.Sprintf("%d", stats.LastFreelistReclaimableRatio)
	out["treedb.bg_vacuum.last_collection_roots_pages"] = fmt.Sprintf("%d", stats.LastCollectionRootPages)
	out["treedb.bg_vacuum.last_collection_roots_span_ratio_ppm"] = fmt.Sprintf("%d", stats.LastCollectionRootSpanRatio)
	out["treedb.bg_vacuum.last_debt_reason"] = stats.LastDebtReason
	out["treedb.bg_vacuum.last_online.total_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.TotalDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.user_tree_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.UserTreeDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.system_reserve_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.SystemReserveDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.collection_basis_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.CollectionBasisDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.preflush_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.PreflushDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.cutover_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.CutoverDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.system_tree_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.SystemTreeDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.final_pager_sync_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.FinalPagerSyncDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.swap_publish_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.SwapPublishDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.max_writer_pause_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.MaxWriterPause.Nanoseconds())
	out["treedb.bg_vacuum.last_online.attempt_id"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.AttemptID)
	out["treedb.bg_vacuum.last_online.recoverable_set_capture_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.RecoverableSetCaptureDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.recoverable_set_capture_attempts"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.RecoverableSetCaptureAttempts)
	out["treedb.bg_vacuum.last_online.recoverable_set_captures"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.RecoverableSetCaptures)
	out["treedb.bg_vacuum.last_online.recoverable_set_recapture_attempts"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.RecoverableSetRecaptureAttempts)
	out["treedb.bg_vacuum.last_online.recoverable_set_recaptures"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.RecoverableSetRecaptures)
	out["treedb.bg_vacuum.last_online.recoverable_roots"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.RecoverableRoots)
	out["treedb.bg_vacuum.last_online.older_root_rebuild_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootRebuildDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.older_root_rebuilds"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootRebuilds)
	out["treedb.bg_vacuum.last_online.older_root_capture_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootDurableResourceCaptureDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.older_root_capture_count"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootDurableResourceCaptures)
	out["treedb.bg_vacuum.last_online.older_root_descriptors"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootDurableResourceDescriptors)
	out["treedb.bg_vacuum.last_online.older_root_bytes"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootDurableResourceBytes)
	out["treedb.bg_vacuum.last_online.older_root_exact_candidate_scans"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootExactCandidateScans)
	out["treedb.bg_vacuum.last_online.older_root_reused_non_value_log_descriptors"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootReusedNonValueLogDescriptors)
	out["treedb.bg_vacuum.last_online.older_root_unique_external_segments"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootUniqueExternalSegments)
	out["treedb.bg_vacuum.last_online.older_root_rebuilt_pages"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.OlderRootRebuiltPages)
	out["treedb.bg_vacuum.last_online.durable_resource_capture_ns"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.DurableResourceCaptureDuration.Nanoseconds())
	out["treedb.bg_vacuum.last_online.durable_resource_capture_count"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.DurableResourceCaptures)
	out["treedb.bg_vacuum.last_online.durable_resource_descriptors"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.DurableResourceDescriptors)
	out["treedb.bg_vacuum.last_online.durable_resource_bytes"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.DurableResourceBytes)
	out["treedb.bg_vacuum.last_online.replacement_pager_pages"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.ReplacementPagerPages)
	out["treedb.bg_vacuum.last_online.preclone_traversal_pages"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.PrecloneTraversalPages)
	out["treedb.bg_vacuum.last_online.reclone_traversal_pages"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.RecloneTraversalPages)
	out["treedb.bg_vacuum.last_online.cutover_clone_traversal_pages"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.CutoverCloneTraversalPages)
	out["treedb.bg_vacuum.last_online.dirty_descriptors"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.DirtyDescriptors)
	out["treedb.bg_vacuum.last_online.user_tail_mutations"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.UserTailMutations)
	out["treedb.bg_vacuum.last_online.user_tail_point_mutations"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.UserTailPointMutations)
	out["treedb.bg_vacuum.last_online.user_tail_range_mutations"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.UserTailRangeMutations)
	out["treedb.bg_vacuum.last_online.deferred_cutovers"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.DeferredCutovers)
	out["treedb.bg_vacuum.last_online.concurrent_mutation_aborts"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.ConcurrentMutationAborts)
	out["treedb.bg_vacuum.last_online.exact_candidate_scan"] = fmt.Sprintf("%t", stats.LastOnlineVacuum.ExactCandidateScan)
	out["treedb.bg_vacuum.last_online.reused_non_value_log_descriptors"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.ReusedNonValueLogDescriptors)
	out["treedb.bg_vacuum.last_online.unique_external_segments"] = fmt.Sprintf("%d", stats.LastOnlineVacuum.UniqueExternalSegments)
	out["treedb.bg_vacuum.last_online.work_completed"] = fmt.Sprintf("%t", stats.LastOnlineVacuum.WorkCompleted)
	out["treedb.bg_vacuum.last_online.canceled"] = fmt.Sprintf("%t", stats.LastOnlineVacuum.Canceled)
	if stats.LastErr != "" {
		out["treedb.bg_vacuum.last_err"] = stats.LastErr
	}
}
