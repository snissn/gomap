package treedb

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBackgroundValueLogGCInterval         = 30 * time.Second
	defaultBackgroundValueLogRewriteInterval    = 5 * time.Second
	defaultBackgroundValueLogRewriteCooldown    = 30 * time.Second
	defaultBackgroundValueLogRewriteMinTotalB   = int64(128 << 20) // 128 MiB
	defaultBackgroundValueLogRewriteMinStaleRat = 0.10
	defaultBackgroundValueLogRewriteMaxSegs     = 4
	defaultBackgroundValueLogRewriteMaxBytes    = int64(64 << 20)  // 64 MiB
	defaultBackgroundValueLogRewriteScoreTotalB = int64(128 << 20) // 128 MiB
	defaultBackgroundValueLogRewriteScoreStaleB = int64(16 << 20)  // 16 MiB
	defaultBackgroundValueLogRewriteScoreChurnB = int64(32 << 20)  // 32 MiB
	defaultBackgroundValueLogRewriteScoreTrig   = 1.0
	defaultBackgroundValueLogRewriteScoreBypass = 1.5
	defaultBackgroundValueLogColdSegmentTargetB = int64(256 << 20) // 256 MiB
)

type bgValueLogMaintenanceConfig struct {
	gcInterval         time.Duration
	rewriteInterval    time.Duration
	rewriteCooldown    time.Duration
	rewriteMinTotalB   int64
	rewriteMinStaleR   float64
	rewriteMaxSegs     int
	rewriteMaxBytes    int64
	rewriteSegTargetB  int64
	rewriteScoreTotalB int64
	rewriteScoreStaleB int64
	rewriteScoreChurnB int64
	rewriteScoreTrig   float64
	rewriteScoreBypass float64
	rewriteBudgetBps   int64
	rewriteHotTargetB  int64
	rewriteWarmTargetB int64
	rewriteColdTargetB int64
}

type bgValueLogMaintenanceWorker struct {
	enabled atomic.Bool

	cfg bgValueLogMaintenanceConfig

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	runMu sync.Mutex

	runs             atomic.Uint64
	gcRuns           atomic.Uint64
	rewriteRuns      atomic.Uint64
	lastRunUnix      atomic.Int64
	lastGCUnix       atomic.Int64
	lastRewrite      atomic.Int64
	lastErr          atomic.Value // string
	lastGCBytesT     atomic.Int64
	lastGCBytesE     atomic.Int64
	lastChurnBytes   atomic.Int64
	lastRewriteScore atomic.Uint64 // math.Float64bits
}

func (w *bgValueLogMaintenanceWorker) Start(db *DB, cfg bgValueLogMaintenanceConfig) {
	if db == nil || db.backend == nil {
		w.enabled.Store(false)
		return
	}
	if cfg.gcInterval <= 0 && cfg.rewriteInterval <= 0 {
		w.enabled.Store(false)
		return
	}
	w.cfg = cfg
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	w.lastErr.Store("")
	w.enabled.Store(true)

	go func() {
		defer close(w.doneCh)

		var gcTicker *time.Ticker
		var gcCh <-chan time.Time
		if cfg.gcInterval > 0 {
			gcTicker = time.NewTicker(cfg.gcInterval)
			gcCh = gcTicker.C
			defer gcTicker.Stop()
		}

		var rwTicker *time.Ticker
		var rwCh <-chan time.Time
		if cfg.rewriteInterval > 0 {
			rwTicker = time.NewTicker(cfg.rewriteInterval)
			rwCh = rwTicker.C
			defer rwTicker.Stop()
		}

		for {
			select {
			case <-w.stopCh:
				return
			case <-gcCh:
				w.runOnce(db, true, false)
			case <-rwCh:
				w.runOnce(db, false, true)
			}
		}
	}()
}

func (w *bgValueLogMaintenanceWorker) Stop() {
	if !w.enabled.Load() {
		return
	}
	w.stopOnce.Do(func() {
		close(w.stopCh)
		<-w.doneCh
		w.enabled.Store(false)
	})
}

func (w *bgValueLogMaintenanceWorker) runOnce(db *DB, runGC bool, maybeRewrite bool) {
	if db == nil || db.backend == nil {
		return
	}
	w.runMu.Lock()
	defer w.runMu.Unlock()

	now := time.Now()
	w.runs.Add(1)
	w.lastRunUnix.Store(now.Unix())

	if db.cached != nil {
		if db.cached.QueueBacklogBytes() > 0 {
			w.lastErr.Store("")
			return
		}
		// WAL-off mode needs regular materialization to keep read expectations
		// stable without explicit checkpoints.
		if db.writePath.redoLog == "off" {
			if err := db.cached.Drain(); err != nil {
				w.lastErr.Store(err.Error())
				db.reportError(err)
				return
			}
		}
	}

	var gcStats ValueLogGCStats
	var gcDone bool
	prevEligible := w.lastGCBytesE.Load()
	if runGC || maybeRewrite {
		stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{Mode: ValueLogGCModeOnline})
		if err != nil {
			w.lastErr.Store(err.Error())
			db.reportError(err)
			return
		}
		gcStats = stats
		gcDone = true
		w.gcRuns.Add(1)
		w.lastGCUnix.Store(now.Unix())
		w.lastGCBytesT.Store(stats.BytesTotal)
		w.lastGCBytesE.Store(stats.BytesEligible)
	}

	if maybeRewrite && w.cfg.rewriteInterval > 0 {
		if !gcDone {
			w.lastErr.Store("")
			return
		}

		total := gcStats.BytesTotal
		eligible := gcStats.BytesEligible
		if total <= 0 {
			w.lastErr.Store("")
			return
		}
		if w.cfg.rewriteMinTotalB >= 0 && total < w.cfg.rewriteMinTotalB {
			w.lastErr.Store("")
			return
		}
		staleRatio := float64(eligible) / float64(total)
		if w.cfg.rewriteMinStaleR >= 0 && staleRatio < w.cfg.rewriteMinStaleR {
			w.lastErr.Store("")
			return
		}
		churnBytes := eligible - prevEligible
		if churnBytes < 0 {
			churnBytes = -churnBytes
		}
		w.lastChurnBytes.Store(churnBytes)
		score := computeVlogRewriteScore(total, eligible, churnBytes, w.cfg.rewriteScoreTotalB, w.cfg.rewriteScoreStaleB, w.cfg.rewriteScoreChurnB)
		w.lastRewriteScore.Store(math.Float64bits(score))
		if w.cfg.rewriteScoreTrig > 0 && score < w.cfg.rewriteScoreTrig {
			w.lastErr.Store("")
			return
		}
		lastRewriteUnix := w.lastRewrite.Load()
		bypassCooldown := shouldBypassRewriteCooldown(score, w.cfg.rewriteScoreBypass)
		if !bypassCooldown && w.cfg.rewriteCooldown > 0 && lastRewriteUnix > 0 {
			last := time.Unix(lastRewriteUnix, 0)
			if now.Sub(last) < w.cfg.rewriteCooldown {
				w.lastErr.Store("")
				return
			}
		}

		maxSourceBytes := effectiveRewriteMaxSourceBytes(w.cfg.rewriteMaxBytes, w.cfg.rewriteBudgetBps, w.cfg.rewriteInterval)

		opts := ValueLogRewriteOnlineOptions{
			MaxSourceSegments: w.cfg.rewriteMaxSegs,
			MaxSourceBytes:    maxSourceBytes,
			MaxSegmentBytes:   w.cfg.rewriteSegTargetB,
			HotSegmentBytes:   w.cfg.rewriteHotTargetB,
			WarmSegmentBytes:  w.cfg.rewriteWarmTargetB,
			ColdSegmentBytes:  w.cfg.rewriteColdTargetB,
			LocalityPolicy:    ValueLogRewriteLocalityGrouped,
		}
		if _, err := db.ValueLogRewriteOnline(context.Background(), opts); err != nil {
			w.lastErr.Store(err.Error())
			db.reportError(err)
			return
		}
		w.rewriteRuns.Add(1)
		w.lastRewrite.Store(now.Unix())
		// After major pointer rewrites, request an early vacuum check.
		db.bgVac.Kick()
	}

	w.lastErr.Store("")
}

func bgValueLogMaintenanceStatsInto(out map[string]string, w *bgValueLogMaintenanceWorker) {
	out["treedb.bg_vlog_maintenance.enabled"] = fmt.Sprintf("%t", w.enabled.Load())
	out["treedb.bg_vlog_maintenance.gc_interval_ms"] = fmt.Sprintf("%d", w.cfg.gcInterval.Milliseconds())
	out["treedb.bg_vlog_maintenance.rewrite_interval_ms"] = fmt.Sprintf("%d", w.cfg.rewriteInterval.Milliseconds())
	out["treedb.bg_vlog_maintenance.rewrite_cooldown_ms"] = fmt.Sprintf("%d", w.cfg.rewriteCooldown.Milliseconds())
	out["treedb.bg_vlog_maintenance.rewrite_min_total_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteMinTotalB)
	out["treedb.bg_vlog_maintenance.rewrite_min_stale_ratio"] = fmt.Sprintf("%.4f", w.cfg.rewriteMinStaleR)
	out["treedb.bg_vlog_maintenance.rewrite_max_source_segments"] = fmt.Sprintf("%d", w.cfg.rewriteMaxSegs)
	out["treedb.bg_vlog_maintenance.rewrite_max_source_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteMaxBytes)
	out["treedb.bg_vlog_maintenance.rewrite_segment_target_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteSegTargetB)
	out["treedb.bg_vlog_maintenance.rewrite_score_target_total_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteScoreTotalB)
	out["treedb.bg_vlog_maintenance.rewrite_score_target_stale_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteScoreStaleB)
	out["treedb.bg_vlog_maintenance.rewrite_score_target_churn_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteScoreChurnB)
	out["treedb.bg_vlog_maintenance.rewrite_score_trigger"] = fmt.Sprintf("%.3f", w.cfg.rewriteScoreTrig)
	out["treedb.bg_vlog_maintenance.rewrite_score_cooldown_bypass"] = fmt.Sprintf("%.3f", w.cfg.rewriteScoreBypass)
	out["treedb.bg_vlog_maintenance.rewrite_budget_bytes_per_sec"] = fmt.Sprintf("%d", w.cfg.rewriteBudgetBps)
	out["treedb.bg_vlog_maintenance.rewrite_hot_segment_target_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteHotTargetB)
	out["treedb.bg_vlog_maintenance.rewrite_warm_segment_target_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteWarmTargetB)
	out["treedb.bg_vlog_maintenance.rewrite_cold_segment_target_bytes"] = fmt.Sprintf("%d", w.cfg.rewriteColdTargetB)
	out["treedb.bg_vlog_maintenance.last_rewrite_score"] = fmt.Sprintf("%.6f", math.Float64frombits(w.lastRewriteScore.Load()))
	out["treedb.bg_vlog_maintenance.last_churn_bytes"] = fmt.Sprintf("%d", w.lastChurnBytes.Load())
	out["treedb.bg_vlog_maintenance.runs"] = fmt.Sprintf("%d", w.runs.Load())
	out["treedb.bg_vlog_maintenance.gc_runs"] = fmt.Sprintf("%d", w.gcRuns.Load())
	out["treedb.bg_vlog_maintenance.rewrite_runs"] = fmt.Sprintf("%d", w.rewriteRuns.Load())
	out["treedb.bg_vlog_maintenance.last_run_unix"] = fmt.Sprintf("%d", w.lastRunUnix.Load())
	out["treedb.bg_vlog_maintenance.last_gc_unix"] = fmt.Sprintf("%d", w.lastGCUnix.Load())
	out["treedb.bg_vlog_maintenance.last_rewrite_unix"] = fmt.Sprintf("%d", w.lastRewrite.Load())
	out["treedb.bg_vlog_maintenance.last_gc_bytes_total"] = fmt.Sprintf("%d", w.lastGCBytesT.Load())
	out["treedb.bg_vlog_maintenance.last_gc_bytes_eligible"] = fmt.Sprintf("%d", w.lastGCBytesE.Load())
	if v := w.lastErr.Load(); v != nil {
		if s, ok := v.(string); ok {
			out["treedb.bg_vlog_maintenance.last_error"] = s
		}
	}
}

func computeVlogRewriteScore(totalBytes, staleBytes, churnBytes, targetTotalBytes, targetStaleBytes, targetChurnBytes int64) float64 {
	if totalBytes <= 0 {
		return 0
	}
	var scoreTotal float64
	if targetTotalBytes > 0 {
		scoreTotal = float64(totalBytes) / float64(targetTotalBytes)
	}
	var scoreStale float64
	if targetStaleBytes > 0 {
		scoreStale = float64(staleBytes) / float64(targetStaleBytes)
	}
	var scoreChurn float64
	if targetChurnBytes > 0 {
		scoreChurn = float64(churnBytes) / float64(targetChurnBytes)
	}
	score := scoreTotal
	if scoreStale > score {
		score = scoreStale
	}
	if scoreChurn > score {
		score = scoreChurn
	}
	return score
}

func shouldBypassRewriteCooldown(score, bypassThreshold float64) bool {
	return bypassThreshold > 0 && score >= bypassThreshold
}

func effectiveRewriteMaxSourceBytes(baseMaxBytes, budgetBytesPerSec int64, interval time.Duration) int64 {
	maxSourceBytes := baseMaxBytes
	if budgetBytesPerSec <= 0 || interval <= 0 {
		return maxSourceBytes
	}
	intervalNanos := interval.Nanoseconds()
	if intervalNanos <= 0 {
		return maxSourceBytes
	}
	budget := (budgetBytesPerSec * intervalNanos) / int64(time.Second)
	if budget <= 0 {
		budget = budgetBytesPerSec
	}
	if maxSourceBytes <= 0 || budget < maxSourceBytes {
		return budget
	}
	return maxSourceBytes
}
