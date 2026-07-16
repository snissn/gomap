package treedb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func backgroundIndexVacuumShouldReport(err error) bool {
	return err != nil &&
		!errors.Is(err, backenddb.ErrVacuumConcurrentMutation) &&
		!errors.Is(err, backenddb.ErrVacuumRecoverableRootSetRequired)
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

	runMu          sync.Mutex
	runs           atomic.Uint64
	probes         atomic.Uint64
	vacuums        atomic.Uint64
	lastRunUnix    atomic.Int64
	lastVacuumUnix atomic.Int64
	lastSpanRatio  atomic.Uint64
	lastPages      atomic.Uint64
	lastErr        atomic.Value // string

	backlogConsecutiveSkips atomic.Uint64
	backlogSkips            atomic.Uint64
	backlogForcedRuns       atomic.Uint64
	lastBacklogBytes        atomic.Int64

	lastFreelistReclaimablePages atomic.Uint64
	lastFreelistReclaimableRatio atomic.Uint64
	lastCollectionRootPages      atomic.Uint64
	lastCollectionRootSpanRatio  atomic.Uint64
	lastDebtReason               atomic.Value // string

	lastProbeCommitSeq              uint64
	lastProbeFreelistReclaimable    uint64
	lastProbeFreelistReclaimablePPM uint64
	lastProbeFreelistValid          bool
	lastProbeValid                  bool
	retryProbe                      bool
}

var bgIndexVacuumBacklogBytesHook struct {
	mu sync.RWMutex
	fn func(*DB) int64
}

var bgIndexVacuumFreelistDebtSnapshotHook struct {
	mu sync.RWMutex
	fn func(*DB) (backenddb.IndexVacuumFreelistDebtSnapshot, bool)
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
	w.lastErr.Store("")
	w.lastDebtReason.Store(backgroundIndexVacuumDebtReasonNone)

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

			w.runOnce(db)
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
		close(w.stopCh)
		<-w.doneCh
		w.enabled.Store(false)
	})
}

func (w *bgIndexVacuumWorker) runOnce(db *DB) {
	if db == nil || db.backend == nil {
		return
	}

	w.runMu.Lock()
	defer w.runMu.Unlock()

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
			w.finishRun(now, "")
			return
		}
		forcedAfterBacklog = true
		w.backlogForcedRuns.Add(1)
		w.backlogConsecutiveSkips.Store(0)
	} else {
		w.backlogConsecutiveSkips.Store(0)
	}

	if !forcedAfterBacklog && w.lastProbeValid && !w.retryProbe && state.CommitSeq == w.lastProbeCommitSeq && !w.freelistDebtChangedSinceLastProbe(db) {
		w.finishRun(now, "")
		return
	}

	rep, err := db.backend.IndexVacuumTriggerReport()
	w.probes.Add(1)
	if err != nil {
		w.retryProbe = true
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
		w.finishRun(now, "")
		return
	}

	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		w.retryProbe = true
		w.finishRun(now, err.Error())
		// A bounded online-vacuum pass may lose its cutover race to foreground
		// mutations. That is expected retry control flow: retain it in worker
		// diagnostics and retry the probe without poisoning the database handle.
		if backgroundIndexVacuumShouldReport(err) {
			db.reportError(err)
		}
		return
	}

	w.retryProbe = false
	w.vacuums.Add(1)
	w.finishRun(now, "")
	w.lastVacuumUnix.Store(now.Unix())
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
	w.lastErr.Store(err)
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

	Runs    uint64
	Probes  uint64
	Vacuums uint64

	BacklogConsecutiveSkips uint64
	BacklogSkips            uint64
	BacklogForcedRuns       uint64
	LastBacklogBytes        int64

	LastRunUnix    int64
	LastVacuumUnix int64
	LastSpanRatio  uint64
	LastPages      uint64
	LastErr        string

	LastFreelistReclaimablePages uint64
	LastFreelistReclaimableRatio uint64
	LastCollectionRootPages      uint64
	LastCollectionRootSpanRatio  uint64
	LastDebtReason               string
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
		BacklogConsecutiveSkips:      w.backlogConsecutiveSkips.Load(),
		BacklogSkips:                 w.backlogSkips.Load(),
		BacklogForcedRuns:            w.backlogForcedRuns.Load(),
		LastBacklogBytes:             w.lastBacklogBytes.Load(),
		LastRunUnix:                  w.lastRunUnix.Load(),
		LastVacuumUnix:               w.lastVacuumUnix.Load(),
		LastSpanRatio:                w.lastSpanRatio.Load(),
		LastPages:                    w.lastPages.Load(),
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
	out["treedb.bg_vacuum.backlog_skips_consecutive"] = fmt.Sprintf("%d", stats.BacklogConsecutiveSkips)
	out["treedb.bg_vacuum.backlog_skips_total"] = fmt.Sprintf("%d", stats.BacklogSkips)
	out["treedb.bg_vacuum.backlog_forced_runs"] = fmt.Sprintf("%d", stats.BacklogForcedRuns)
	out["treedb.bg_vacuum.last_backlog_bytes"] = fmt.Sprintf("%d", stats.LastBacklogBytes)
	out["treedb.bg_vacuum.last_run_unix"] = fmt.Sprintf("%d", stats.LastRunUnix)
	out["treedb.bg_vacuum.last_vacuum_unix"] = fmt.Sprintf("%d", stats.LastVacuumUnix)
	out["treedb.bg_vacuum.last_span_ratio_ppm"] = fmt.Sprintf("%d", stats.LastSpanRatio)
	out["treedb.bg_vacuum.last_pages"] = fmt.Sprintf("%d", stats.LastPages)
	out["treedb.bg_vacuum.last_freelist_reclaimable_pages"] = fmt.Sprintf("%d", stats.LastFreelistReclaimablePages)
	out["treedb.bg_vacuum.last_freelist_reclaimable_ratio_ppm"] = fmt.Sprintf("%d", stats.LastFreelistReclaimableRatio)
	out["treedb.bg_vacuum.last_collection_roots_pages"] = fmt.Sprintf("%d", stats.LastCollectionRootPages)
	out["treedb.bg_vacuum.last_collection_roots_span_ratio_ppm"] = fmt.Sprintf("%d", stats.LastCollectionRootSpanRatio)
	out["treedb.bg_vacuum.last_debt_reason"] = stats.LastDebtReason
	if stats.LastErr != "" {
		out["treedb.bg_vacuum.last_err"] = stats.LastErr
	}
}
