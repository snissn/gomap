package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

type valueLogMaintExitLoopPassReport struct {
	Pass           int   `json:"pass"`
	Acquired       bool  `json:"acquired"`
	BeforeBytes    int64 `json:"before_bytes"`
	AfterBytes     int64 `json:"after_bytes"`
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
	QueueBefore    int   `json:"queue_before"`
	QueueAfter     int   `json:"queue_after"`
	StageBefore    bool  `json:"stage_before"`
	StageAfter     bool  `json:"stage_after"`
	RunMillis      int64 `json:"run_millis"`
	WaitIdleMillis int64 `json:"wait_idle_millis"`
	TotalMillis    int64 `json:"total_millis"`
	Reseeded       bool  `json:"reseeded,omitempty"`
	ReseedSelected int   `json:"reseed_selected,omitempty"`
}

type valueLogMaintExitLoopReport struct {
	Dir                  string                            `json:"dir"`
	MaxPasses            int                               `json:"max_passes"`
	MinReclaimBytes      int64                             `json:"min_reclaim_bytes"`
	StopQueueNondecrease bool                              `json:"stop_queue_nondecrease"`
	InitialBytes         int64                             `json:"initial_bytes"`
	FinalBytes           int64                             `json:"final_bytes"`
	TotalReclaimedBytes  int64                             `json:"total_reclaimed_bytes"`
	StopReason           string                            `json:"stop_reason"`
	Passes               []valueLogMaintExitLoopPassReport `json:"passes"`
}

type valueLogMaintExitLoopConfig struct {
	WaitIdle               time.Duration
	MaxPasses              int
	MinReclaimBytes        int64
	StopQueueNondecrease   bool
	ReseedFromPlanWhenIdle bool
	ReseedMaxSegments      int
	ReseedMaxBytes         int64
	ReseedMinStaleRatio    float64
	ReseedMinStaleBytes    int64
	RewriteBudgetTokens    int64
	ReseedStageObservedAgo time.Duration
	MaxReseeds             int
}

func runVlogMaintExitLoop(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-maint-exit-loop", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	disableAutoDeferred := fs.Bool("disable-auto-deferred", true, "Disable automatic deferred stage/age wakes while this command is running")
	rewritePlanTimeout := fs.Duration("rewrite-plan-timeout", 0, "Override cached rewrite planner timeout for this process (debug/offline analysis only)")
	waitIdle := fs.Duration("wait-idle", 3*time.Minute, "Wait for cached value-log generation maintenance to go idle after each pass")
	maxPasses := fs.Int("max-passes", 7, "Maximum number of explicit stage-confirm-exit passes to run")
	minReclaimBytes := fs.Int64("min-reclaim-bytes", 1<<20, "Stop after a pass if reclaimed bytes are below this threshold")
	stopQueueNondecrease := fs.Bool("stop-queue-nondecrease", true, "Stop after a pass if rewrite queue size does not shrink")
	reseedFromPlanWhenIdle := fs.Bool("reseed-from-plan-when-idle", true, "When the staged/queued exit debt drains, seed one more explicit wave from a fresh backend rewrite plan")
	reseedMaxSegments := fs.Int("reseed-max-segments", 64, "Backend reseed plan selection cap in segments (0=none)")
	reseedMaxBytes := fs.Int64("reseed-max-bytes", 8<<30, "Backend reseed plan live-byte selection cap (0=none)")
	reseedMinStaleRatio := fs.Float64("reseed-min-stale-ratio", 0.50, "Backend reseed plan minimum per-segment stale ratio (0..1)")
	reseedMinStaleBytes := fs.Int64("reseed-min-stale-bytes", 1, "Backend reseed plan minimum per-segment stale bytes")
	rewriteBudgetTokens := fs.Int64("rewrite-budget-tokens", 2<<30, "Cached rewrite budget tokens to preseed for each explicit exit pass")
	reseedStageObservedAgo := fs.Duration("reseed-stage-observed-ago", 31*time.Second, "When reseeding a ledger, record the stage observation this far in the past so the next pass can spend it immediately")
	maxReseeds := fs.Int("max-reseeds", 1, "Maximum number of backend plan reseed waves to inject per loop invocation")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-maint-exit-loop requires -rw")
	}
	if *maxPasses <= 0 {
		fatalf("max-passes=%d must be > 0", *maxPasses)
	}
	if *disableAutoDeferred {
		if err := os.Setenv("TREEDB_DISABLE_VLOG_GENERATION_DEFERRED", "1"); err != nil {
			fatalf("set disable-auto-deferred env: %v", err)
		}
	}
	if *rewritePlanTimeout > 0 {
		ms := (*rewritePlanTimeout).Milliseconds()
		if ms <= 0 {
			fatalf("rewrite-plan-timeout=%s too small", *rewritePlanTimeout)
		}
		if err := os.Setenv("TREEDB_DEBUG_VLOG_GENERATION_PLAN_TIMEOUT_MS", fmt.Sprintf("%d", ms)); err != nil {
			fatalf("set rewrite-plan-timeout env: %v", err)
		}
	}
	report, err := runValueLogMaintExitLoopWithConfig(dir, valueLogMaintExitLoopConfig{
		WaitIdle:               *waitIdle,
		MaxPasses:              *maxPasses,
		MinReclaimBytes:        *minReclaimBytes,
		StopQueueNondecrease:   *stopQueueNondecrease,
		ReseedFromPlanWhenIdle: *reseedFromPlanWhenIdle,
		ReseedMaxSegments:      *reseedMaxSegments,
		ReseedMaxBytes:         *reseedMaxBytes,
		ReseedMinStaleRatio:    *reseedMinStaleRatio,
		ReseedMinStaleBytes:    *reseedMinStaleBytes,
		RewriteBudgetTokens:    *rewriteBudgetTokens,
		ReseedStageObservedAgo: *reseedStageObservedAgo,
		MaxReseeds:             *maxReseeds,
	})
	if err != nil {
		fatalf("vlog-maint-exit-loop: %v", err)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fatalf("json encode: %v", err)
		}
		return
	}

	fmt.Printf("dir=%s\n", report.Dir)
	fmt.Printf("initial_bytes=%d final_bytes=%d reclaimed_bytes=%d stop_reason=%s\n", report.InitialBytes, report.FinalBytes, report.TotalReclaimedBytes, report.StopReason)
	for _, pass := range report.Passes {
		fmt.Printf(
			"pass=%d acquired=%t reseeded=%t reseed_selected=%d timing_ms: run=%d wait_idle=%d total=%d bytes=%d->%d reclaim=%d queue=%d->%d stage=%t->%t\n",
			pass.Pass,
			pass.Acquired,
			pass.Reseeded,
			pass.ReseedSelected,
			pass.RunMillis,
			pass.WaitIdleMillis,
			pass.TotalMillis,
			pass.BeforeBytes,
			pass.AfterBytes,
			pass.ReclaimedBytes,
			pass.QueueBefore,
			pass.QueueAfter,
			pass.StageBefore,
			pass.StageAfter,
		)
	}
}

func runValueLogMaintExitLoopWithConfig(dir string, cfg valueLogMaintExitLoopConfig) (valueLogMaintExitLoopReport, error) {
	db := openTreeDB(dir, true)
	defer closeTreeDB(db)

	initialBytes, err := dirLogicalSizeBytes(dir)
	if err != nil {
		return valueLogMaintExitLoopReport{}, fmt.Errorf("dir size before loop: %w", err)
	}
	report := valueLogMaintExitLoopReport{
		Dir:                  dir,
		MaxPasses:            cfg.MaxPasses,
		MinReclaimBytes:      cfg.MinReclaimBytes,
		StopQueueNondecrease: cfg.StopQueueNondecrease,
		InitialBytes:         initialBytes,
		FinalBytes:           initialBytes,
	}

	reseedCount := 0
	for pass := 1; pass <= cfg.MaxPasses; pass++ {
		passReport, afterState, err := runOneVlogMaintExitLoopPass(db, dir, pass, cfg.WaitIdle, cfg.RewriteBudgetTokens)
		if err != nil {
			return valueLogMaintExitLoopReport{}, fmt.Errorf("exit maintenance pass %d: %w", pass, err)
		}
		idleAndDrained := len(afterState.RewriteSourceFileIDs) == 0 && !afterState.RewriteStagePending
		stopIdleNoCandidates := false
		rewriteOpts := treedb.ValueLogRewriteOnlineOptions{
			MaxSourceSegments:    cfg.ReseedMaxSegments,
			MaxSourceBytes:       cfg.ReseedMaxBytes,
			MinSegmentStaleRatio: cfg.ReseedMinStaleRatio,
			MinSegmentStaleBytes: cfg.ReseedMinStaleBytes,
		}
		if cfg.ReseedFromPlanWhenIdle && pass < cfg.MaxPasses && idleAndDrained {
			if reseedCount < cfg.MaxReseeds {
				reseeded, selected, err := reseedVlogMaintExitLoopIfIdle(db, rewriteOpts, cfg.ReseedStageObservedAgo)
				if err != nil {
					return valueLogMaintExitLoopReport{}, fmt.Errorf("exit maintenance pass %d reseed: %w", pass, err)
				}
				if reseeded {
					reseedCount++
					state, err := db.DebugValueLogGenerationState()
					if err != nil {
						return valueLogMaintExitLoopReport{}, fmt.Errorf("debug state after reseed on pass %d: %w", pass, err)
					}
					passReport.Reseeded = true
					passReport.ReseedSelected = selected
					passReport.QueueAfter = len(state.RewriteSourceFileIDs)
					passReport.StageAfter = state.RewriteStagePending
				} else {
					stopIdleNoCandidates = true
				}
			} else {
				candidateCount, err := countVlogMaintExitLoopCandidates(db, rewriteOpts)
				if err != nil {
					return valueLogMaintExitLoopReport{}, fmt.Errorf("exit maintenance pass %d probe: %w", pass, err)
				}
				stopIdleNoCandidates = candidateCount == 0
			}
		}
		report.Passes = append(report.Passes, passReport)
		report.FinalBytes = passReport.AfterBytes
		report.TotalReclaimedBytes = report.InitialBytes - report.FinalBytes
		if passReport.Reseeded {
			continue
		}
		if stopIdleNoCandidates {
			report.StopReason = "idle_no_candidates"
			break
		}
		if stop, reason := shouldStopVlogMaintExitLoop(passReport, pass, cfg.MaxPasses, cfg.MinReclaimBytes, cfg.StopQueueNondecrease); stop {
			report.StopReason = reason
			break
		}
	}
	if report.StopReason == "" {
		report.StopReason = "completed"
	}
	return report, nil
}

func runOneVlogMaintExitLoopPass(db *treedb.DB, dir string, pass int, waitIdle time.Duration, rewriteBudgetTokens int64) (valueLogMaintExitLoopPassReport, treedb.DebugValueLogGenerationState, error) {
	start := time.Now()
	beforeState, err := db.DebugValueLogGenerationState()
	if err != nil {
		return valueLogMaintExitLoopPassReport{}, treedb.DebugValueLogGenerationState{}, fmt.Errorf("debug state before pass: %w", err)
	}
	beforeBytes, err := dirLogicalSizeBytes(dir)
	if err != nil {
		return valueLogMaintExitLoopPassReport{}, treedb.DebugValueLogGenerationState{}, fmt.Errorf("dir size before pass: %w", err)
	}
	runStart := time.Now()
	acquired, err := db.DebugRunValueLogGenerationMaintenanceOnce(treedb.DebugValueLogGenerationMaintenanceOptions{
		RunGC:                 true,
		BypassQuiet:           true,
		SkipCheckpoint:        true,
		SkipRetainedPruneWait: true,
		RewriteDebtDrain:      true,
		RewriteBudgetTokens:   rewriteBudgetTokens,
		SuppressFollowOn:      true,
		Source:                "rewrite_stage_confirm_exit",
	})
	runMillis := time.Since(runStart).Milliseconds()
	if err != nil {
		return valueLogMaintExitLoopPassReport{}, treedb.DebugValueLogGenerationState{}, fmt.Errorf("run maintenance: %w", err)
	}
	waitMillis := int64(0)
	if waitIdle > 0 {
		waitStart := time.Now()
		if err := db.DebugWaitValueLogGenerationIdle(waitIdle); err != nil {
			return valueLogMaintExitLoopPassReport{}, treedb.DebugValueLogGenerationState{}, fmt.Errorf("wait idle: %w", err)
		}
		waitMillis = time.Since(waitStart).Milliseconds()
	}
	afterState, err := db.DebugValueLogGenerationState()
	if err != nil {
		return valueLogMaintExitLoopPassReport{}, treedb.DebugValueLogGenerationState{}, fmt.Errorf("debug state after pass: %w", err)
	}
	afterBytes, err := dirLogicalSizeBytes(dir)
	if err != nil {
		return valueLogMaintExitLoopPassReport{}, treedb.DebugValueLogGenerationState{}, fmt.Errorf("dir size after pass: %w", err)
	}
	return valueLogMaintExitLoopPassReport{
		Pass:           pass,
		Acquired:       acquired,
		BeforeBytes:    beforeBytes,
		AfterBytes:     afterBytes,
		ReclaimedBytes: beforeBytes - afterBytes,
		QueueBefore:    len(beforeState.RewriteSourceFileIDs),
		QueueAfter:     len(afterState.RewriteSourceFileIDs),
		StageBefore:    beforeState.RewriteStagePending,
		StageAfter:     afterState.RewriteStagePending,
		RunMillis:      runMillis,
		WaitIdleMillis: waitMillis,
		TotalMillis:    time.Since(start).Milliseconds(),
	}, afterState, nil
}

func reseedVlogMaintExitLoopIfIdle(db *treedb.DB, opts treedb.ValueLogRewriteOnlineOptions, stageObservedAgo time.Duration) (bool, int, error) {
	if db == nil {
		return false, 0, nil
	}
	plan, err := db.ValueLogRewritePlan(context.Background(), opts)
	if err != nil {
		return false, 0, err
	}
	switch {
	case len(plan.SelectedSegments) > 0:
		stageObservedAt := time.Now().Add(-stageObservedAgo).UnixNano()
		if err := db.DebugSetValueLogGenerationRewriteLedger(plan.SelectedSegments, true, stageObservedAt); err != nil {
			return false, 0, err
		}
		return true, len(plan.SelectedSegments), nil
	case len(plan.SourceFileIDs) > 0:
		if err := db.DebugSetValueLogGenerationRewriteQueue(plan.SourceFileIDs); err != nil {
			return false, 0, err
		}
		return true, len(plan.SourceFileIDs), nil
	default:
		return false, 0, nil
	}
}

func countVlogMaintExitLoopCandidates(db *treedb.DB, opts treedb.ValueLogRewriteOnlineOptions) (int, error) {
	if db == nil {
		return 0, nil
	}
	plan, err := db.ValueLogRewritePlan(context.Background(), opts)
	if err != nil {
		return 0, err
	}
	switch {
	case len(plan.SelectedSegments) > 0:
		return len(plan.SelectedSegments), nil
	case len(plan.SourceFileIDs) > 0:
		return len(plan.SourceFileIDs), nil
	default:
		return 0, nil
	}
}

func shouldStopVlogMaintExitLoop(pass valueLogMaintExitLoopPassReport, passNum int, maxPasses int, minReclaimBytes int64, stopQueueNondecrease bool) (bool, string) {
	if passNum >= maxPasses {
		return true, "max_passes"
	}
	if pass.ReclaimedBytes < minReclaimBytes {
		return true, "low_reclaim"
	}
	if stopQueueNondecrease && pass.QueueBefore > 0 && pass.QueueAfter >= pass.QueueBefore {
		return true, "queue_nondecreasing"
	}
	return false, ""
}

func dirLogicalSizeBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}
