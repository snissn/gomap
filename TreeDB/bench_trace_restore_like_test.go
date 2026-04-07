package treedb

import (
	"context"
	_ "embed"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

//go:embed testdata/trace_replay/restore_like.summary.json
var defaultRestoreLikeTraceSummaryJSON []byte

type restoreLikeTraceReplayConfig struct {
	scale                    float64
	flushThreshold           int
	memtableShards           int
	seed                     int64
	pointerThreshold         int
	hotTargetBytes           int64
	warmTargetBytes          int64
	coldTargetBytes          int64
	rewriteTriggerBytes      int64
	rewriteBudgetBytesPerSec int64
	rewriteMinAge            time.Duration
	sourceAgeWait            time.Duration
	checkpointGap            time.Duration
	maintenanceWait          time.Duration
	steadyChurnBytes         int64
	followupChurnBytes       int64
	steadyHotKeyCount        int
	steadyBatchOps           int
}

type restoreLikeTraceReplayMetrics struct {
	maintenanceAttempts                    int64
	maintenanceWithRewrite                 int64
	checkpointKickRuns                     int64
	rewritePlanRuns                        int64
	rewritePlanEmpty                       int64
	rewritePlanSelected                    int64
	rewriteRuns                            int64
	rewriteReclaimedBytes                  int64
	rewriteProcessedLiveBytes              int64
	rewriteProcessedStaleBytes             int64
	rewriteNoReclaimStaleBytes             int64
	queuedDebtExecRuns                     int64
	queuedDebtReclaimedBytes               int64
	rewriteQueueLen                        int64
	retainedPruneRuns                      int64
	retainedPruneForcedRuns                int64
	retainedPruneRemovedBytes              int64
	retainedPruneObservedBytes             int64
	retainedPruneClosedBytes               int64
	retainedPruneCandidateBytes            int64
	retainedPruneLiveSkippedBytes          int64
	retainedPruneZombieMarkedBytes         int64
	retainedPruneObservedZombieMarkedBytes int64
	retainedPruneScheduleForcedRequests    int64
	retainedPruneScheduleSkipMinInterval   int64
	observedGCDeletedBytes                 int64
	gcProtectedRetainedBytes               int64
	strictGCEligibleBytes                  int64
	strictGCDeletedBytes                   int64
	strictGCWalBytes                       int64
	strictGCHomeBytes                      int64
	offlineRewriteBytesBefore              int64
	offlineRewriteBytesAfter               int64
	offlineRewriteWalBytes                 int64
	offlineRewriteHomeBytes                int64
	retainedBytes                          int64
	indexBytes                             int64
	walBytes                               int64
	homeBytes                              int64
	maintenanceWaitMilliseconds            int64
}

func BenchmarkRestoreLikeTraceReplay(b *testing.B) {
	summary, err := loadRestoreLikeTraceSummary()
	if err != nil {
		b.Fatal(err)
	}
	restoreLike := restoreLikeTraceSummary(summary)
	if len(restoreLike.Phases) == 0 {
		b.Fatal("restore-like trace summary has no restore/catchup phases")
	}

	cfg := restoreLikeTraceReplayConfig{
		scale:                    parseFloatEnv("TREEDB_TRACE_SCALE", 1.0),
		flushThreshold:           parseIntEnv("TREEDB_TRACE_FLUSH_THRESHOLD", 32*1024*1024),
		memtableShards:           parseIntEnv("TREEDB_TRACE_MEMTABLE_SHARDS", 0),
		seed:                     parseInt64Env("TREEDB_TRACE_SEED", 1),
		pointerThreshold:         parseIntEnv("TREEDB_TRACE_REPLAY_POINTER_THRESHOLD", 1),
		hotTargetBytes:           parseInt64Env("TREEDB_TRACE_REPLAY_HOT_TARGET_BYTES", 256<<10),
		warmTargetBytes:          parseInt64Env("TREEDB_TRACE_REPLAY_WARM_TARGET_BYTES", 256<<10),
		coldTargetBytes:          parseInt64Env("TREEDB_TRACE_REPLAY_COLD_TARGET_BYTES", 512<<10),
		rewriteTriggerBytes:      parseInt64Env("TREEDB_TRACE_REPLAY_REWRITE_TRIGGER_BYTES", 512<<10),
		rewriteBudgetBytesPerSec: parseInt64Env("TREEDB_TRACE_REPLAY_REWRITE_BUDGET_BYTES_PER_SEC", 0),
		rewriteMinAge:            time.Duration(parseIntEnv("TREEDB_TRACE_REPLAY_REWRITE_MIN_AGE_MS", 1)) * time.Millisecond,
		sourceAgeWait:            time.Duration(parseIntEnv("TREEDB_TRACE_REPLAY_SOURCE_AGE_WAIT_MS", 3)) * time.Millisecond,
		checkpointGap:            time.Duration(parseIntEnv("TREEDB_TRACE_REPLAY_CHECKPOINT_GAP_MS", 5200)) * time.Millisecond,
		maintenanceWait:          time.Duration(parseIntEnv("TREEDB_TRACE_REPLAY_MAINTENANCE_WAIT_MS", 1500)) * time.Millisecond,
		steadyChurnBytes:         parseInt64Env("TREEDB_TRACE_REPLAY_STEADY_CHURN_BYTES", 8<<20),
		followupChurnBytes:       parseInt64Env("TREEDB_TRACE_REPLAY_FOLLOWUP_CHURN_BYTES", 256<<10),
		steadyHotKeyCount:        parseIntEnv("TREEDB_TRACE_REPLAY_STEADY_HOT_KEYS", 4096),
		steadyBatchOps:           parseIntEnv("TREEDB_TRACE_REPLAY_STEADY_BATCH_OPS", 32),
	}

	totalOps := scaledTotalOps(restoreLike, cfg.scale)
	if totalOps > 0 {
		b.ReportMetric(float64(totalOps), "ops/iter")
	}

	for _, tc := range []struct {
		name       string
		disableWAL bool
	}{
		{name: "WALOn", disableWAL: false},
		{name: "WALOff", disableWAL: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var totals restoreLikeTraceReplayMetrics
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				metrics, err := runRestoreLikeTraceReplayIteration(restoreLike, cfg, tc.disableWAL, int64(i))
				if err != nil {
					b.Fatal(err)
				}
				totals.add(metrics)
			}
			b.StopTimer()

			n := float64(b.N)
			b.ReportMetric(float64(totals.maintenanceAttempts)/n, "maint_attempts/op")
			b.ReportMetric(float64(totals.maintenanceWithRewrite)/n, "maint_with_rewrite/op")
			b.ReportMetric(float64(totals.checkpointKickRuns)/n, "checkpoint_kicks/op")
			b.ReportMetric(float64(totals.rewritePlanRuns)/n, "rewrite_plan_runs/op")
			b.ReportMetric(float64(totals.rewritePlanEmpty)/n, "rewrite_plan_empty/op")
			b.ReportMetric(float64(totals.rewritePlanSelected)/n, "rewrite_plan_selected/op")
			b.ReportMetric(float64(totals.rewriteRuns)/n, "rewrite_runs/op")
			b.ReportMetric(float64(totals.rewriteReclaimedBytes)/n, "rewrite_reclaimed_B/op")
			b.ReportMetric(float64(totals.rewriteProcessedLiveBytes)/n, "rewrite_processed_live_B/op")
			b.ReportMetric(float64(totals.rewriteProcessedStaleBytes)/n, "rewrite_processed_stale_B/op")
			b.ReportMetric(float64(totals.rewriteNoReclaimStaleBytes)/n, "rewrite_no_reclaim_stale_B/op")
			b.ReportMetric(float64(totals.queuedDebtExecRuns)/n, "queued_exec_runs/op")
			b.ReportMetric(float64(totals.queuedDebtReclaimedBytes)/n, "queued_reclaimed_B/op")
			b.ReportMetric(float64(totals.rewriteQueueLen)/n, "rewrite_queue_len/op")
			b.ReportMetric(float64(totals.retainedPruneRuns)/n, "retained_prune_runs/op")
			b.ReportMetric(float64(totals.retainedPruneForcedRuns)/n, "retained_prune_forced_runs/op")
			b.ReportMetric(float64(totals.retainedPruneRemovedBytes)/n, "retained_prune_removed_B/op")
			b.ReportMetric(float64(totals.retainedPruneObservedBytes)/n, "retained_prune_observed_removed_B/op")
			b.ReportMetric(float64(totals.retainedPruneClosedBytes)/n, "retained_prune_closed_B/op")
			b.ReportMetric(float64(totals.retainedPruneCandidateBytes)/n, "retained_prune_candidate_B/op")
			b.ReportMetric(float64(totals.retainedPruneLiveSkippedBytes)/n, "retained_prune_live_skipped_B/op")
			b.ReportMetric(float64(totals.retainedPruneZombieMarkedBytes)/n, "retained_prune_zombie_marked_B/op")
			b.ReportMetric(float64(totals.retainedPruneObservedZombieMarkedBytes)/n, "retained_prune_observed_zombie_marked_B/op")
			b.ReportMetric(float64(totals.retainedPruneScheduleForcedRequests)/n, "retained_prune_schedule_forced_requests/op")
			b.ReportMetric(float64(totals.retainedPruneScheduleSkipMinInterval)/n, "retained_prune_schedule_skip_min_interval/op")
			b.ReportMetric(float64(totals.observedGCDeletedBytes)/n, "observed_gc_deleted_B/op")
			b.ReportMetric(float64(totals.gcProtectedRetainedBytes)/n, "gc_protected_retained_B/op")
			b.ReportMetric(float64(totals.strictGCEligibleBytes)/n, "strict_gc_eligible_B/op")
			b.ReportMetric(float64(totals.strictGCDeletedBytes)/n, "strict_gc_deleted_B/op")
			b.ReportMetric(float64(totals.strictGCWalBytes)/n, "strict_gc_wal_B/op")
			b.ReportMetric(float64(totals.strictGCHomeBytes)/n, "strict_gc_home_B/op")
			b.ReportMetric(float64(totals.offlineRewriteBytesBefore)/n, "offline_floor_before_B/op")
			b.ReportMetric(float64(totals.offlineRewriteBytesAfter)/n, "offline_floor_after_B/op")
			b.ReportMetric(float64(totals.offlineRewriteWalBytes)/n, "offline_floor_wal_B/op")
			b.ReportMetric(float64(totals.offlineRewriteHomeBytes)/n, "offline_floor_home_B/op")
			b.ReportMetric(float64(totals.retainedBytes)/n, "retained_B/op")
			b.ReportMetric(float64(totals.indexBytes)/n, "index_B/op")
			b.ReportMetric(float64(totals.walBytes)/n, "wal_B/op")
			b.ReportMetric(float64(totals.homeBytes)/n, "home_B/op")
			b.ReportMetric(float64(totals.maintenanceWaitMilliseconds)/n, "maint_wait_ms/op")
		})
	}
}

func (m *restoreLikeTraceReplayMetrics) add(other restoreLikeTraceReplayMetrics) {
	m.maintenanceAttempts += other.maintenanceAttempts
	m.maintenanceWithRewrite += other.maintenanceWithRewrite
	m.checkpointKickRuns += other.checkpointKickRuns
	m.rewritePlanRuns += other.rewritePlanRuns
	m.rewritePlanEmpty += other.rewritePlanEmpty
	m.rewritePlanSelected += other.rewritePlanSelected
	m.rewriteRuns += other.rewriteRuns
	m.rewriteReclaimedBytes += other.rewriteReclaimedBytes
	m.rewriteProcessedLiveBytes += other.rewriteProcessedLiveBytes
	m.rewriteProcessedStaleBytes += other.rewriteProcessedStaleBytes
	m.rewriteNoReclaimStaleBytes += other.rewriteNoReclaimStaleBytes
	m.queuedDebtExecRuns += other.queuedDebtExecRuns
	m.queuedDebtReclaimedBytes += other.queuedDebtReclaimedBytes
	m.rewriteQueueLen += other.rewriteQueueLen
	m.retainedPruneRuns += other.retainedPruneRuns
	m.retainedPruneForcedRuns += other.retainedPruneForcedRuns
	m.retainedPruneRemovedBytes += other.retainedPruneRemovedBytes
	m.retainedPruneObservedBytes += other.retainedPruneObservedBytes
	m.retainedPruneClosedBytes += other.retainedPruneClosedBytes
	m.retainedPruneCandidateBytes += other.retainedPruneCandidateBytes
	m.retainedPruneLiveSkippedBytes += other.retainedPruneLiveSkippedBytes
	m.retainedPruneZombieMarkedBytes += other.retainedPruneZombieMarkedBytes
	m.retainedPruneObservedZombieMarkedBytes += other.retainedPruneObservedZombieMarkedBytes
	m.retainedPruneScheduleForcedRequests += other.retainedPruneScheduleForcedRequests
	m.retainedPruneScheduleSkipMinInterval += other.retainedPruneScheduleSkipMinInterval
	m.observedGCDeletedBytes += other.observedGCDeletedBytes
	m.gcProtectedRetainedBytes += other.gcProtectedRetainedBytes
	m.strictGCEligibleBytes += other.strictGCEligibleBytes
	m.strictGCDeletedBytes += other.strictGCDeletedBytes
	m.strictGCWalBytes += other.strictGCWalBytes
	m.strictGCHomeBytes += other.strictGCHomeBytes
	m.offlineRewriteBytesBefore += other.offlineRewriteBytesBefore
	m.offlineRewriteBytesAfter += other.offlineRewriteBytesAfter
	m.offlineRewriteWalBytes += other.offlineRewriteWalBytes
	m.offlineRewriteHomeBytes += other.offlineRewriteHomeBytes
	m.retainedBytes += other.retainedBytes
	m.indexBytes += other.indexBytes
	m.walBytes += other.walBytes
	m.homeBytes += other.homeBytes
	m.maintenanceWaitMilliseconds += other.maintenanceWaitMilliseconds
}

func loadRestoreLikeTraceSummary() (traceSummary, error) {
	summaryPath := strings.TrimSpace(os.Getenv("TREEDB_TRACE_SUMMARY"))
	if summaryPath != "" {
		data, err := os.ReadFile(summaryPath)
		if err != nil {
			return traceSummary{}, err
		}
		return unmarshalTraceSummary(data)
	}
	return unmarshalTraceSummary(defaultRestoreLikeTraceSummaryJSON)
}

func unmarshalTraceSummary(data []byte) (traceSummary, error) {
	var s traceSummary
	if err := json.Unmarshal(data, &s); err != nil {
		return traceSummary{}, err
	}
	return s, nil
}

func restoreLikeTraceSummary(s traceSummary) traceSummary {
	phases := make(map[string]tracePhaseSummary, 2)
	for _, phase := range []string{"restore", "catchup"} {
		if p, ok := s.Phases[phase]; ok {
			phases[phase] = p
		}
	}
	return traceSummary{
		TotalEvents: s.TotalEvents,
		Phases:      phases,
	}
}

func runRestoreLikeTraceReplayIteration(summary traceSummary, cfg restoreLikeTraceReplayConfig, disableWAL bool, iter int64) (restoreLikeTraceReplayMetrics, error) {
	dir, err := os.MkdirTemp("", "treedb-restore-trace-*")
	if err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}
	defer os.RemoveAll(dir)

	opts := Options{
		Dir:            dir,
		FlushThreshold: int64(cfg.flushThreshold),
		MemtableShards: cfg.memtableShards,
		Durability: func() DurabilityMode {
			if disableWAL {
				return DurabilityWALOffRelaxed
			}
			return DurabilityWALOnRelaxed
		}(),
		ValueLog: ValueLogOptions{
			PointerThreshold: cfg.pointerThreshold,
			ForcePointers:    cfg.pointerThreshold == 0,
			ReadIntegrity:    IntegritySkipChecksums,
			Generational: ValueLogGenerationConfig{
				Policy:                   ValueLogGenerationHotWarmCold,
				HotSegmentTargetBytes:    cfg.hotTargetBytes,
				WarmSegmentTargetBytes:   cfg.warmTargetBytes,
				ColdSegmentTargetBytes:   cfg.coldTargetBytes,
				RewriteBudgetBytesPerSec: cfg.rewriteBudgetBytesPerSec,
				RewriteTriggerTotalBytes: cfg.rewriteTriggerBytes,
				RewriteMinSegmentAge:     cfg.rewriteMinAge,
			},
		},
	}
	db, err := Open(opts)
	if err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}

	rng := rand.New(rand.NewSource(cfg.seed + iter))
	keyspace := make([][]byte, 0, 1<<20)
	keyIndex := make(map[string]struct{})

	for _, phaseName := range orderedTracePhases(summary.Phases) {
		phase, ok := summary.Phases[phaseName]
		if !ok {
			continue
		}
		db.SetMaintenancePhase(traceReplayMaintenancePhase(phaseName))
		if err := runTracePhase(db, rng, phase, cfg.scale, &keyspace, keyIndex); err != nil {
			return restoreLikeTraceReplayMetrics{}, err
		}
	}

	db.SetMaintenancePhase(MaintenancePhaseSteady)
	if err := runTraceSteadyChurn(db, rng, summary, cfg, &keyspace); err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}
	if cfg.sourceAgeWait > 0 {
		time.Sleep(cfg.sourceAgeWait)
	}
	if err := db.Checkpoint(); err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}
	if cfg.checkpointGap > 0 {
		time.Sleep(cfg.checkpointGap)
	}
	if cfg.followupChurnBytes > 0 {
		followupCfg := cfg
		followupCfg.steadyChurnBytes = cfg.followupChurnBytes
		if err := runTraceSteadyChurn(db, rng, summary, followupCfg, &keyspace); err != nil {
			return restoreLikeTraceReplayMetrics{}, err
		}
	}
	if err := db.Checkpoint(); err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}

	waited := waitForRestoreLikeMaintenance(db, cfg.maintenanceWait)
	stats := db.Stats()
	if err := db.Close(); err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}
	metrics, err := collectRestoreLikeTraceReplayMetrics(stats, dir)
	if err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}
	metrics.maintenanceWaitMilliseconds = waited.Milliseconds()
	return metrics, nil
}

func runTraceSteadyChurn(db *DB, rng *rand.Rand, summary traceSummary, cfg restoreLikeTraceReplayConfig, keyspace *[][]byte) error {
	if db == nil || cfg.steadyChurnBytes <= 0 || len(*keyspace) == 0 {
		return nil
	}
	phase := summary.Phases["catchup"]
	valueDist := phase.SetValueLens
	if valueDist.Count == 0 {
		valueDist = phase.GetValueLens
	}
	if valueDist.Count == 0 {
		valueDist = traceDistSummary{Count: 1, P50: 256, P90: 1024, P99: 4096, Max: 8192}
	}
	batchOps := cfg.steadyBatchOps
	if batchOps <= 0 {
		batchOps = 32
	}
	hotKeys := *keyspace
	if cfg.steadyHotKeyCount > 0 && cfg.steadyHotKeyCount < len(hotKeys) {
		hotKeys = hotKeys[:cfg.steadyHotKeyCount]
	}

	var churned int64
	for churned < cfg.steadyChurnBytes {
		batch := db.NewBatch()
		if batch == nil {
			return nil
		}
		for i := 0; i < batchOps && churned < cfg.steadyChurnBytes; i++ {
			key := hotKeys[rng.Intn(len(hotKeys))]
			value := randomValue(rng, valueDist)
			churned += int64(len(value))
			if err := batch.Set(key, value); err != nil {
				_ = batch.Close()
				return err
			}
		}
		if err := batch.Write(); err != nil {
			_ = batch.Close()
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
	}
	return nil
}

func traceReplayMaintenancePhase(phase string) MaintenancePhase {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "restore":
		return MaintenancePhaseRestore
	case "catchup":
		return MaintenancePhaseCatchUp
	default:
		return MaintenancePhaseSteady
	}
}

func waitForRestoreLikeMaintenance(db *DB, timeout time.Duration) time.Duration {
	if db == nil || timeout <= 0 {
		return 0
	}
	start := time.Now()
	deadline := start.Add(timeout)
	var stableSince time.Time
	last := restoreLikeMaintenanceState{}
	for time.Now().Before(deadline) {
		cur := restoreLikeMaintenanceStateFromDB(db)
		if cur != last {
			last = cur
			stableSince = time.Time{}
		}
		if !cur.activeOrPending() {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= 50*time.Millisecond {
				return time.Since(start)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return time.Since(start)
}

type restoreLikeMaintenanceState struct {
	schedulerState             string
	checkpointKickActive       bool
	checkpointKickPending      bool
	rewriteQueuePending        bool
	rewriteQueueRunning        bool
	retainedPruneActive        bool
	retainedPruneForcePending  bool
	retainedPruneObservedQueue bool
	maintenanceAttempts        int64
	rewriteRuns                int64
}

func (s restoreLikeMaintenanceState) activeOrPending() bool {
	if s.checkpointKickActive || s.checkpointKickPending || s.rewriteQueuePending || s.rewriteQueueRunning {
		return true
	}
	if s.retainedPruneActive || s.retainedPruneForcePending || s.retainedPruneObservedQueue {
		return true
	}
	switch s.schedulerState {
	case "running", "acquired":
		return true
	default:
		return false
	}
}

func restoreLikeMaintenanceStateFromDB(db *DB) restoreLikeMaintenanceState {
	stats := db.Stats()
	return restoreLikeMaintenanceState{
		schedulerState:             stats["treedb.cache.vlog_generation.scheduler_state"],
		checkpointKickActive:       parseBoolStat(stats, "treedb.cache.vlog_generation.checkpoint_kick.active"),
		checkpointKickPending:      parseBoolStat(stats, "treedb.cache.vlog_generation.checkpoint_kick.pending"),
		rewriteQueuePending:        parseBoolStat(stats, "treedb.cache.vlog_generation.rewrite.queue.pending"),
		rewriteQueueRunning:        parseBoolStat(stats, "treedb.cache.vlog_generation.rewrite.queue.running"),
		retainedPruneActive:        parseBoolStat(stats, "treedb.cache.vlog_retained_prune.active"),
		retainedPruneForcePending:  parseBoolStat(stats, "treedb.cache.vlog_retained_prune.force_pending"),
		retainedPruneObservedQueue: parseBoolStat(stats, "treedb.cache.vlog_retained_prune.observed_source.pending"),
		maintenanceAttempts:        parseInt64Stat(stats, "treedb.cache.vlog_generation.maintenance.attempts"),
		rewriteRuns:                parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.runs"),
	}
}

func collectRestoreLikeTraceReplayMetrics(stats map[string]string, dir string) (restoreLikeTraceReplayMetrics, error) {
	metrics := restoreLikeTraceReplayMetrics{
		maintenanceAttempts:                    parseInt64Stat(stats, "treedb.cache.vlog_generation.maintenance.attempts"),
		maintenanceWithRewrite:                 parseInt64Stat(stats, "treedb.cache.vlog_generation.maintenance.passes.with_rewrite"),
		checkpointKickRuns:                     parseInt64Stat(stats, "treedb.cache.vlog_generation.checkpoint_kick.runs"),
		rewritePlanRuns:                        parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.plan_runs"),
		rewritePlanEmpty:                       parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.plan_empty"),
		rewritePlanSelected:                    parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.plan_selected"),
		rewriteRuns:                            parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.runs"),
		rewriteReclaimedBytes:                  parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.reclaimed_bytes"),
		rewriteProcessedLiveBytes:              parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.processed_live_bytes"),
		rewriteProcessedStaleBytes:             parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.processed_stale_bytes"),
		rewriteNoReclaimStaleBytes:             parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.no_reclaim_stale_bytes"),
		queuedDebtExecRuns:                     parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.runs"),
		queuedDebtReclaimedBytes:               parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.reclaimed_bytes"),
		rewriteQueueLen:                        parseInt64Stat(stats, "treedb.cache.vlog_generation.rewrite.queue_len"),
		retainedPruneRuns:                      parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.runs"),
		retainedPruneForcedRuns:                parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.forced_runs"),
		retainedPruneRemovedBytes:              parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.removed_bytes"),
		retainedPruneObservedBytes:             parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_removed_total"),
		retainedPruneClosedBytes:               parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.closed_bytes"),
		retainedPruneCandidateBytes:            parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.candidate_bytes"),
		retainedPruneLiveSkippedBytes:          parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.live_skipped_bytes"),
		retainedPruneZombieMarkedBytes:         parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.zombie_marked_bytes"),
		retainedPruneObservedZombieMarkedBytes: parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_zombie_marked_total"),
		retainedPruneScheduleForcedRequests:    parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.schedule_forced_requests"),
		retainedPruneScheduleSkipMinInterval:   parseInt64Stat(stats, "treedb.cache.vlog_retained_prune.schedule_skip.min_interval"),
		observedGCDeletedBytes:                 parseInt64Stat(stats, "treedb.cache.vlog_generation.observed_gc.source_bytes_deleted_total"),
		gcProtectedRetainedBytes:               parseInt64Stat(stats, "treedb.cache.vlog_generation.gc.last_protected_retained_bytes"),
		retainedBytes:                          parseInt64Stat(stats, "treedb.cache.vlog_retained_bytes_estimate"),
		indexBytes:                             fileSizeBytes(filepath.Join(dir, "maindb", "index.db")),
		walBytes:                               dirSizeBytes(filepath.Join(dir, "maindb", "wal")),
		homeBytes:                              dirSizeBytes(dir),
	}

	backend, cleanup, err := OpenBackend(Options{Dir: dir, ReadOnly: false})
	if err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}
	gcStats, gcErr := backend.ValueLogGC(context.Background(), treedbdb.ValueLogGCOptions{})
	cleanupErr := cleanup()
	if gcErr != nil {
		return restoreLikeTraceReplayMetrics{}, gcErr
	}
	if cleanupErr != nil {
		return restoreLikeTraceReplayMetrics{}, cleanupErr
	}
	metrics.strictGCEligibleBytes = gcStats.BytesEligible
	metrics.strictGCDeletedBytes = gcStats.BytesDeleted
	metrics.strictGCWalBytes = dirSizeBytes(filepath.Join(dir, "maindb", "wal"))
	metrics.strictGCHomeBytes = dirSizeBytes(dir)

	rewriteStats, err := ValueLogRewriteOffline(Options{Dir: dir})
	if err != nil {
		return restoreLikeTraceReplayMetrics{}, err
	}
	metrics.offlineRewriteBytesBefore = rewriteStats.BytesBefore
	metrics.offlineRewriteBytesAfter = rewriteStats.BytesAfter
	metrics.offlineRewriteWalBytes = dirSizeBytes(filepath.Join(dir, "maindb", "wal"))
	metrics.offlineRewriteHomeBytes = dirSizeBytes(dir)

	return metrics, nil
}

func parseInt64Stat(stats map[string]string, key string) int64 {
	if len(stats) == 0 {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(stats[key]), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseBoolStat(stats map[string]string, key string) bool {
	return strings.EqualFold(strings.TrimSpace(stats[key]), "true")
}

func fileSizeBytes(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func dirSizeBytes(path string) int64 {
	var total int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}
