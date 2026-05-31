package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type pruneWorkerOptions struct {
	enabled     bool
	interval    time.Duration
	maxPages    int
	maxDuration time.Duration
}

type pruneWorker struct {
	enabled atomic.Bool

	interval    time.Duration
	maxPages    int
	maxDuration time.Duration

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
	kickCh   chan struct{}

	runs        atomic.Uint64
	pagesFreed  atomic.Uint64
	lastRunUnix atomic.Int64
	lastErr     atomic.Value // string
}

func (w *pruneWorker) Start(db *DB, opts pruneWorkerOptions) {
	w.interval = opts.interval
	w.maxPages = opts.maxPages
	w.maxDuration = opts.maxDuration

	if !opts.enabled {
		w.enabled.Store(false)
		return
	}

	w.enabled.Store(true)
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	w.kickCh = make(chan struct{}, 1)
	w.lastErr.Store("")

	go func() {
		defer close(w.doneCh)

		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-w.stopCh:
				return
			case <-w.kickCh:
			case <-ticker.C:
			}

			pages, err := db.pruneSome(w.stopCh, w.maxPages, w.maxDuration)

			w.runs.Add(1)
			if pages > 0 {
				w.pagesFreed.Add(uint64(pages))
			}
			w.lastRunUnix.Store(time.Now().Unix())
			if err != nil {
				w.lastErr.Store(err.Error())
				db.reportError(err)
			} else {
				w.lastErr.Store("")
			}
		}
	}()
}

func (w *pruneWorker) Enabled() bool {
	return w.enabled.Load()
}

func (w *pruneWorker) Kick() {
	if !w.Enabled() || w.kickCh == nil {
		return
	}
	select {
	case w.kickCh <- struct{}{}:
	default:
	}
}

func (w *pruneWorker) Stop() {
	if !w.Enabled() {
		return
	}
	w.stopOnce.Do(func() {
		close(w.stopCh)
		<-w.doneCh
		w.enabled.Store(false)
	})
}

func (w *pruneWorker) Stats() (interval time.Duration, maxPages int, maxDuration time.Duration, runs uint64, pagesFreed uint64, lastRunUnix int64, lastErr string) {
	interval = w.interval
	maxPages = w.maxPages
	maxDuration = w.maxDuration
	runs = w.runs.Load()
	pagesFreed = w.pagesFreed.Load()
	lastRunUnix = w.lastRunUnix.Load()
	if v := w.lastErr.Load(); v != nil {
		lastErr, _ = v.(string)
	}
	return
}

func formatMaybeNegativeDurationMs(d time.Duration) string {
	if d < 0 {
		return "-1"
	}
	return fmt.Sprintf("%d", d.Milliseconds())
}

func (db *DB) pruneSome(stopCh <-chan struct{}, maxPages int, maxDuration time.Duration) (int, error) {
	if maxPages == 0 {
		return 0, nil
	}

	idx := db.idx.Load()
	if idx == nil {
		return 0, nil
	}
	idx.acquire()
	defer db.releaseIndex(idx)

	var deadline time.Time
	if maxDuration > 0 {
		deadline = time.Now().Add(maxDuration)
	}

	freedPages := 0
	for {
		select {
		case <-stopCh:
			return freedPages, nil
		default:
		}

		if !deadline.IsZero() && time.Now().After(deadline) {
			return freedPages, nil
		}

		state := db.state.Load()
		if state == nil {
			return freedPages, nil
		}
		currentSeq := state.CommitSeq
		minPinned := db.MinPinnedSnapshotCommitSeq()

		remaining := maxPages
		if maxPages > 0 {
			remaining = maxPages - freedPages
			if remaining <= 0 {
				return freedPages, nil
			}
		}

		// Extract a bounded amount so a single tick can't grab unbounded memory.
		batches := idx.graveyard.ExtractBatchesUpTo(minPinned, currentSeq, db.keepRecent, remaining)
		if len(batches) == 0 {
			return freedPages, nil
		}

		for bi := range batches {
			b := batches[bi]
			for i, id := range b.IDs {
				select {
				case <-stopCh:
					idx.graveyard.Reinsert(b.Seq, b.IDs[i:])
					for _, rest := range batches[bi+1:] {
						idx.graveyard.Reinsert(rest.Seq, rest.IDs)
					}
					return freedPages, nil
				default:
				}

				if !deadline.IsZero() && time.Now().After(deadline) {
					idx.graveyard.Reinsert(b.Seq, b.IDs[i:])
					for _, rest := range batches[bi+1:] {
						idx.graveyard.Reinsert(rest.Seq, rest.IDs)
					}
					return freedPages, nil
				}

				if err := idx.allocator.Free(id); err != nil {
					idx.graveyard.Reinsert(b.Seq, b.IDs[i:])
					for _, rest := range batches[bi+1:] {
						idx.graveyard.Reinsert(rest.Seq, rest.IDs)
					}
					return freedPages, err
				}
				freedPages++
				if maxPages > 0 && freedPages >= maxPages {
					return freedPages, nil
				}
			}
		}
	}
}

func pruneStatsInto(out map[string]string, w *pruneWorker) {
	interval, maxPages, maxDuration, runs, pagesFreed, lastRunUnix, lastErr := w.Stats()

	out["treedb.prune.enabled"] = fmt.Sprintf("%t", w.Enabled())
	out["treedb.prune.interval_ms"] = fmt.Sprintf("%d", interval.Milliseconds())
	out["treedb.prune.max_pages"] = fmt.Sprintf("%d", maxPages)
	out["treedb.prune.max_duration_ms"] = formatMaybeNegativeDurationMs(maxDuration)
	out["treedb.prune.runs"] = fmt.Sprintf("%d", runs)
	out["treedb.prune.pages_freed"] = fmt.Sprintf("%d", pagesFreed)
	out["treedb.prune.last_run_unix"] = fmt.Sprintf("%d", lastRunUnix)
	if lastErr != "" {
		out["treedb.prune.last_err"] = lastErr
	}
}
