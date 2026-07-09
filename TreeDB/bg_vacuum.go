package treedb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const defaultBackgroundIndexVacuumSpanRatioPPM uint32 = 1_200_000

type bgIndexVacuumWorker struct {
	enabled atomic.Bool

	interval     time.Duration
	spanRatioPPM uint32

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
	kickCh   chan struct{}

	runMu          sync.Mutex
	runs           atomic.Uint64
	vacuums        atomic.Uint64
	lastRunUnix    atomic.Int64
	lastVacuumUnix atomic.Int64
	lastSpanRatio  atomic.Uint64
	lastPages      atomic.Uint64
	lastErr        atomic.Value // string

	lastProbeCommitSeq uint64
	lastProbeValid     bool
	retryProbe         bool
}

// Start launches the background index vacuum loop with the provided interval.
func (w *bgIndexVacuumWorker) Start(db *DB, interval time.Duration, spanRatioPPM uint32) {
	if interval <= 0 || db == nil {
		w.enabled.Store(false)
		return
	}
	if spanRatioPPM == 0 {
		spanRatioPPM = defaultBackgroundIndexVacuumSpanRatioPPM
	}

	w.enabled.Store(true)
	w.interval = interval
	w.spanRatioPPM = spanRatioPPM
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	w.kickCh = make(chan struct{}, 1)
	w.lastErr.Store("")

	go func() {
		defer close(w.doneCh)

		ticker := time.NewTicker(interval)
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
	state := db.backend.State()
	if state == nil || db.backend.IsClosing() {
		return
	}

	if w.lastProbeValid && !w.retryProbe && state.CommitSeq == w.lastProbeCommitSeq {
		w.runs.Add(1)
		w.lastRunUnix.Store(now.Unix())
		w.lastErr.Store("")
		return
	}

	// Avoid competing with the flush path. If there's any queued backlog, let the
	// caching layer catch up first. This is not a completed trigger probe, so keep
	// lastProbeCommitSeq unchanged.
	if db.cached != nil {
		if db.cached.QueueBacklogBytes() > 0 {
			w.runs.Add(1)
			w.lastRunUnix.Store(now.Unix())
			w.lastErr.Store("")
			return
		}
	}

	rep, err := db.backend.IndexVacuumTriggerReport()
	if err != nil {
		w.retryProbe = true
		w.runs.Add(1)
		w.lastRunUnix.Store(now.Unix())
		w.lastErr.Store(err.Error())
		db.reportError(err)
		return
	}

	w.lastProbeCommitSeq = rep.CommitSeq
	w.lastProbeValid = true
	w.lastPages.Store(rep.UserPages)
	if rep.UserPages == 0 {
		w.retryProbe = false
		w.lastSpanRatio.Store(0)
		w.runs.Add(1)
		w.lastRunUnix.Store(now.Unix())
		w.lastErr.Store("")
		return
	}

	spanRatio := rep.UserSpanRatioPPM
	w.lastSpanRatio.Store(spanRatio)

	if spanRatio < uint64(w.spanRatioPPM) {
		w.retryProbe = false
		w.runs.Add(1)
		w.lastRunUnix.Store(now.Unix())
		w.lastErr.Store("")
		return
	}

	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		w.retryProbe = true
		w.runs.Add(1)
		w.lastRunUnix.Store(now.Unix())
		w.lastErr.Store(err.Error())
		db.reportError(err)
		return
	}

	w.retryProbe = false
	w.vacuums.Add(1)
	w.runs.Add(1)
	w.lastRunUnix.Store(now.Unix())
	w.lastVacuumUnix.Store(now.Unix())
	w.lastErr.Store("")
}

// Stats returns a snapshot of background index vacuum state and recent run info.
func (w *bgIndexVacuumWorker) Stats() (enabled bool, interval time.Duration, spanRatioPPM uint32, runs uint64, vacuums uint64, lastRunUnix int64, lastVacuumUnix int64, lastSpanRatio uint64, lastPages uint64, lastErr string) {
	enabled = w.Enabled()
	interval = w.interval
	spanRatioPPM = w.spanRatioPPM
	runs = w.runs.Load()
	vacuums = w.vacuums.Load()
	lastRunUnix = w.lastRunUnix.Load()
	lastVacuumUnix = w.lastVacuumUnix.Load()
	lastSpanRatio = w.lastSpanRatio.Load()
	lastPages = w.lastPages.Load()
	if v := w.lastErr.Load(); v != nil {
		lastErr, _ = v.(string)
	}
	return
}

func bgIndexVacuumStatsInto(out map[string]string, w *bgIndexVacuumWorker) {
	enabled, interval, spanRatioPPM, runs, vacuums, lastRunUnix, lastVacuumUnix, lastSpanRatio, lastPages, lastErr := w.Stats()
	out["treedb.bg_vacuum.enabled"] = fmt.Sprintf("%t", enabled)
	out["treedb.bg_vacuum.interval_ms"] = fmt.Sprintf("%d", interval.Milliseconds())
	out["treedb.bg_vacuum.span_ratio_ppm_threshold"] = fmt.Sprintf("%d", spanRatioPPM)
	out["treedb.bg_vacuum.runs"] = fmt.Sprintf("%d", runs)
	out["treedb.bg_vacuum.vacuums"] = fmt.Sprintf("%d", vacuums)
	out["treedb.bg_vacuum.last_run_unix"] = fmt.Sprintf("%d", lastRunUnix)
	out["treedb.bg_vacuum.last_vacuum_unix"] = fmt.Sprintf("%d", lastVacuumUnix)
	out["treedb.bg_vacuum.last_span_ratio_ppm"] = fmt.Sprintf("%d", lastSpanRatio)
	out["treedb.bg_vacuum.last_pages"] = fmt.Sprintf("%d", lastPages)
	if lastErr != "" {
		out["treedb.bg_vacuum.last_err"] = lastErr
	}
}
