package treedb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/compaction"
)

type bgCompactionWorker struct {
	enabled atomic.Bool

	interval time.Duration
	opts     compaction.Options

	stopOnce sync.Once
	cancel   context.CancelFunc
	stopCh   chan struct{}
	doneCh   chan struct{}
	kickCh   chan struct{}

	runMu       sync.Mutex
	runs        atomic.Uint64
	lastRunUnix atomic.Int64
	lastErr     atomic.Value // string
}

func (w *bgCompactionWorker) Start(db *DB, interval time.Duration, opts compaction.Options) {
	if interval <= 0 || db == nil {
		w.enabled.Store(false)
		return
	}

	w.enabled.Store(true)
	w.interval = interval
	w.opts = opts
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	w.kickCh = make(chan struct{}, 1)
	w.lastErr.Store("")

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

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

			w.runOnce(ctx, db)
		}
	}()
}

func (w *bgCompactionWorker) Enabled() bool {
	return w.enabled.Load()
}

func (w *bgCompactionWorker) Kick() {
	if !w.Enabled() || w.kickCh == nil {
		return
	}
	select {
	case w.kickCh <- struct{}{}:
	default:
	}
}

func (w *bgCompactionWorker) Stop() {
	if !w.Enabled() {
		return
	}
	w.stopOnce.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
		close(w.stopCh)
		<-w.doneCh
		w.enabled.Store(false)
	})
}

func (w *bgCompactionWorker) runOnce(ctx context.Context, db *DB) {
	if db == nil || db.backend == nil {
		return
	}

	w.runMu.Lock()
	defer w.runMu.Unlock()

	// Avoid competing with the flush path. If there's any queued backlog, let the
	// caching layer catch up first.
	if db.cached != nil {
		if db.cached.QueueBacklogBytes() > 0 {
			w.runs.Add(1)
			w.lastRunUnix.Store(time.Now().Unix())
			w.lastErr.Store("")
			return
		}
	}

	opts := w.opts
	if db.cached != nil {
		userAssist := opts.Assist
		opts.Assist = func() {
			db.cached.CompactionAssist()
			if userAssist != nil {
				userAssist()
			}
		}
	}

	c := compaction.New(db.backend)
	err := c.CompactCandidatesWithContext(ctx, opts)

	w.runs.Add(1)
	w.lastRunUnix.Store(time.Now().Unix())
	if err != nil {
		w.lastErr.Store(err.Error())
	} else {
		w.lastErr.Store("")
	}
}

func (w *bgCompactionWorker) Stats() (enabled bool, interval time.Duration, runs uint64, lastRunUnix int64, lastErr string) {
	enabled = w.Enabled()
	interval = w.interval
	runs = w.runs.Load()
	lastRunUnix = w.lastRunUnix.Load()
	if v := w.lastErr.Load(); v != nil {
		lastErr, _ = v.(string)
	}
	return
}

func bgCompactionStatsInto(out map[string]string, w *bgCompactionWorker) {
	enabled, interval, runs, lastRunUnix, lastErr := w.Stats()
	out["treedb.bg_compaction.enabled"] = fmt.Sprintf("%t", enabled)
	out["treedb.bg_compaction.interval_ms"] = fmt.Sprintf("%d", interval.Milliseconds())
	out["treedb.bg_compaction.runs"] = fmt.Sprintf("%d", runs)
	out["treedb.bg_compaction.last_run_unix"] = fmt.Sprintf("%d", lastRunUnix)
	if lastErr != "" {
		out["treedb.bg_compaction.last_err"] = lastErr
	}
}
