package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	runtimepprof "runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

type valueLogMaintOnceReport struct {
	Dir            string                              `json:"dir"`
	Mode           string                              `json:"mode"`
	Acquired       bool                                `json:"acquired"`
	OpenMillis     int64                               `json:"open_millis,omitempty"`
	SeedMillis     int64                               `json:"seed_millis,omitempty"`
	RunMillis      int64                               `json:"run_millis,omitempty"`
	WaitIdleMillis int64                               `json:"wait_idle_millis,omitempty"`
	TotalMillis    int64                               `json:"total_millis,omitempty"`
	SeededFromPlan bool                                `json:"seeded_from_plan,omitempty"`
	SeedPlan       *treedbdb.ValueLogRewritePlan       `json:"seed_plan,omitempty"`
	BeforeState    treedb.DebugValueLogGenerationState `json:"before_state"`
	AfterState     treedb.DebugValueLogGenerationState `json:"after_state"`
	BeforeStats    map[string]string                   `json:"before_stats,omitempty"`
	AfterStats     map[string]string                   `json:"after_stats,omitempty"`
	BeforeVlogIO   map[string]string                   `json:"before_vlog_io,omitempty"`
	AfterVlogIO    map[string]string                   `json:"after_vlog_io,omitempty"`
}

func runVlogMaintOnce(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-maint-once", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	mode := fs.String("mode", "checkpoint", "Maintenance mode: periodic|checkpoint|stage-confirm|stage-confirm-exit|age-blocked")
	seedFromPlan := fs.Bool("seed-from-plan", false, "Seed cached rewrite debt from a fresh backend rewrite plan before running")
	clearState := fs.Bool("clear-state", false, "Clear cached rewrite debt before running")
	disableAutoDeferred := fs.Bool("disable-auto-deferred", false, "Disable automatic deferred stage/age wakes while this command is running")
	rewritePlanTimeout := fs.Duration("rewrite-plan-timeout", 0, "Override cached rewrite planner timeout for this process (debug/offline analysis only)")
	waitIdle := fs.Duration("wait-idle", 0, "Wait for cached value-log generation maintenance to go idle before collecting after-state")
	cpuprofile := fs.String("cpuprofile", "", "Write CPU profile for this maintenance run to file")
	cpuprofileTotal := fs.Bool("cpuprofile-total", false, "Extend CPU profiling through the idle-wait window instead of stopping after the initial maintenance call")
	heapprofile := fs.String("heapprofile", "", "Write in-use heap profile after this maintenance run to file")
	allocsprofile := fs.String("allocsprofile", "", "Write allocs profile after this maintenance run to file")
	seedStagePending := fs.Bool("seed-stage-pending", false, "When seeding from plan, mark the rewrite debt as stage-pending")
	seedStageObservedAgo := fs.Duration("seed-stage-observed-ago", 31*time.Second, "If -seed-stage-pending, record the stage observation this far in the past")
	maxSegments := fs.Int("rewrite-max-segments", 0, "Seed-plan selection cap in segments (0=none)")
	maxBytes := fs.Int64("rewrite-max-bytes", 0, "Seed-plan live-byte selection cap (0=none)")
	minStaleRatio := fs.Float64("rewrite-min-stale-ratio", 0, "Seed-plan minimum per-segment stale ratio (0..1)")
	minStaleBytes := fs.Int64("rewrite-min-stale-bytes", 0, "Seed-plan minimum per-segment stale bytes")
	rewriteBudgetTokens := fs.Int64("rewrite-budget-tokens", 0, "Override cached rewrite budget tokens for this explicit maintenance pass only (0 keeps runtime value)")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-maint-once requires -rw")
	}
	if *clearState && *seedFromPlan {
		fatalf("use at most one of -clear-state or -seed-from-plan")
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
		if err := os.Setenv("TREEDB_DEBUG_VLOG_GENERATION_PLAN_TIMEOUT_MS", strconv.FormatInt(ms, 10)); err != nil {
			fatalf("set rewrite-plan-timeout env: %v", err)
		}
	}
	var seedPlan *treedbdb.ValueLogRewritePlan
	if *seedFromPlan {
		audit, err := collectValueLogAudit(dir, treedbdb.ValueLogRewriteOnlineOptions{
			MaxSourceSegments:    *maxSegments,
			MaxSourceBytes:       *maxBytes,
			MinSegmentStaleRatio: *minStaleRatio,
			MinSegmentStaleBytes: *minStaleBytes,
		}, valueLogRIDAuditOptions{})
		if err != nil {
			fatalf("seed rewrite plan: %v", err)
		}
		seedPlan = &audit.RewritePlan
	}

	totalStart := time.Now()
	openStart := totalStart
	db := openTreeDB(dir, true)
	openDone := time.Now()
	defer closeTreeDB(db)

	beforeState, err := db.DebugValueLogGenerationState()
	if err != nil {
		fatalf("debug state before run: %v", err)
	}
	beforeStats := filterVlogGenerationStats(db.Stats())
	beforeVlogIO := filterVlogIOStats(db.Stats())

	report := valueLogMaintOnceReport{
		Dir:          dir,
		Mode:         *mode,
		OpenMillis:   openDone.Sub(openStart).Milliseconds(),
		BeforeState:  beforeState,
		BeforeStats:  beforeStats,
		BeforeVlogIO: beforeVlogIO,
	}

	seedStart := time.Now()
	if *clearState {
		if err := db.DebugSetValueLogGenerationRewriteQueue(nil); err != nil {
			fatalf("clear rewrite state: %v", err)
		}
	}
	if *seedFromPlan {
		report.SeededFromPlan = true
		report.SeedPlan = seedPlan
		switch {
		case len(seedPlan.SelectedSegments) > 0:
			stageObservedAt := int64(0)
			if *seedStagePending {
				stageObservedAt = time.Now().Add(-*seedStageObservedAgo).UnixNano()
			}
			if err := db.DebugSetValueLogGenerationRewriteLedger(seedPlan.SelectedSegments, *seedStagePending, stageObservedAt); err != nil {
				fatalf("seed rewrite ledger: %v", err)
			}
		case len(seedPlan.SourceFileIDs) > 0:
			if err := db.DebugSetValueLogGenerationRewriteQueue(seedPlan.SourceFileIDs); err != nil {
				fatalf("seed rewrite queue: %v", err)
			}
		default:
			if err := db.DebugSetValueLogGenerationRewriteQueue(nil); err != nil {
				fatalf("clear empty seed plan: %v", err)
			}
		}
	}
	report.SeedMillis = time.Since(seedStart).Milliseconds()

	opts, err := valueLogMaintModeOptions(*mode)
	if err != nil {
		fatalf("%v", err)
	}
	opts.RewriteBudgetTokens = *rewriteBudgetTokens

	if *allocsprofile != "" {
		runtime.MemProfileRate = 1
	}
	var (
		cpuFile           *os.File
		cpuProfileStarted bool
	)
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fatalf("cpuprofile: %v", err)
		}
		cpuFile = f
		if err := runtimepprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			fatalf("cpuprofile: %v", err)
		}
		cpuProfileStarted = true
	}
	runStart := time.Now()
	acquired, err := db.DebugRunValueLogGenerationMaintenanceOnce(opts)
	report.RunMillis = time.Since(runStart).Milliseconds()
	if cpuFile != nil && cpuProfileStarted && (!*cpuprofileTotal || *waitIdle <= 0) {
		runtimepprof.StopCPUProfile()
		_ = cpuFile.Close()
		cpuProfileStarted = false
		cpuFile = nil
	}
	if err != nil {
		fatalf("run maintenance: %v", err)
	}
	report.Acquired = acquired
	if *waitIdle > 0 {
		waitStart := time.Now()
		if err := db.DebugWaitValueLogGenerationIdle(*waitIdle); err != nil {
			fatalf("wait idle: %v", err)
		}
		report.WaitIdleMillis = time.Since(waitStart).Milliseconds()
	}
	if cpuFile != nil && cpuProfileStarted {
		runtimepprof.StopCPUProfile()
		_ = cpuFile.Close()
	}

	afterState, err := db.DebugValueLogGenerationState()
	if err != nil {
		fatalf("debug state after run: %v", err)
	}
	report.AfterState = afterState
	report.AfterStats = filterVlogGenerationStats(db.Stats())
	report.AfterVlogIO = filterVlogIOStats(db.Stats())
	report.TotalMillis = time.Since(totalStart).Milliseconds()

	if *heapprofile != "" {
		runtime.GC()
		f, err := os.Create(*heapprofile)
		if err != nil {
			fatalf("heapprofile: %v", err)
		}
		if err := runtimepprof.WriteHeapProfile(f); err != nil {
			_ = f.Close()
			fatalf("heapprofile: %v", err)
		}
		_ = f.Close()
	}
	if *allocsprofile != "" {
		f, err := os.Create(*allocsprofile)
		if err != nil {
			fatalf("allocsprofile: %v", err)
		}
		if err := runtimepprof.Lookup("allocs").WriteTo(f, 0); err != nil {
			_ = f.Close()
			fatalf("allocsprofile: %v", err)
		}
		_ = f.Close()
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
	fmt.Printf("mode=%s acquired=%t seeded_from_plan=%t\n", report.Mode, report.Acquired, report.SeededFromPlan)
	fmt.Printf("timing_ms: open=%d seed=%d run=%d wait_idle=%d total=%d\n", report.OpenMillis, report.SeedMillis, report.RunMillis, report.WaitIdleMillis, report.TotalMillis)
	if report.SeedPlan != nil {
		fmt.Printf(
			"seed_plan: selected=%d/%d selected_bytes_total=%d selected_bytes_live=%d selected_bytes_stale=%d source_file_ids=%v\n",
			report.SeedPlan.SegmentsSelected,
			report.SeedPlan.SegmentsTotal,
			report.SeedPlan.SelectedBytesTotal,
			report.SeedPlan.SelectedBytesLive,
			report.SeedPlan.SelectedBytesStale,
			report.SeedPlan.SourceFileIDs,
		)
	}
	fmt.Printf(
		"before: queue=%v ledger=%d stage_pending=%t stage_observed_unix_nano=%d\n",
		report.BeforeState.RewriteSourceFileIDs,
		len(report.BeforeState.RewriteDebtLedger),
		report.BeforeState.RewriteStagePending,
		report.BeforeState.RewriteStageObservedAtNS,
	)
	fmt.Printf(
		"after: queue=%v ledger=%d stage_pending=%t stage_observed_unix_nano=%d\n",
		report.AfterState.RewriteSourceFileIDs,
		len(report.AfterState.RewriteDebtLedger),
		report.AfterState.RewriteStagePending,
		report.AfterState.RewriteStageObservedAtNS,
	)
	printSelectedVlogGenerationStatChanges(report.BeforeStats, report.AfterStats)
	printSelectedVlogGenerationStatChanges(report.BeforeVlogIO, report.AfterVlogIO)
}

func valueLogMaintModeOptions(mode string) (treedb.DebugValueLogGenerationMaintenanceOptions, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "checkpoint":
		return treedb.DebugValueLogGenerationMaintenanceOptions{
			RunGC:                 true,
			BypassQuiet:           true,
			SkipCheckpoint:        false,
			SkipRetainedPruneWait: true,
			RewriteDebtDrain:      true,
			Source:                "checkpoint_pending",
		}, nil
	case "periodic":
		return treedb.DebugValueLogGenerationMaintenanceOptions{
			RunGC:                 true,
			BypassQuiet:           false,
			SkipCheckpoint:        false,
			SkipRetainedPruneWait: false,
			RewriteDebtDrain:      false,
			Source:                "periodic",
		}, nil
	case "stage-confirm":
		return treedb.DebugValueLogGenerationMaintenanceOptions{
			RunGC:                 true,
			BypassQuiet:           true,
			SkipCheckpoint:        false,
			SkipRetainedPruneWait: true,
			RewriteDebtDrain:      true,
			Source:                "rewrite_stage_confirm",
		}, nil
	case "stage-confirm-exit":
		return treedb.DebugValueLogGenerationMaintenanceOptions{
			RunGC:                 true,
			BypassQuiet:           true,
			SkipCheckpoint:        true,
			SkipRetainedPruneWait: true,
			RewriteDebtDrain:      true,
			SuppressFollowOn:      true,
			Source:                "rewrite_stage_confirm_exit",
		}, nil
	case "age-blocked":
		return treedb.DebugValueLogGenerationMaintenanceOptions{
			RunGC:                 true,
			BypassQuiet:           true,
			SkipCheckpoint:        false,
			SkipRetainedPruneWait: true,
			RewriteDebtDrain:      true,
			Source:                "rewrite_age_blocked",
		}, nil
	default:
		return treedb.DebugValueLogGenerationMaintenanceOptions{}, fmt.Errorf("unsupported -mode=%q (expected periodic|checkpoint|stage-confirm|stage-confirm-exit|age-blocked)", mode)
	}
}

func filterVlogGenerationStats(stats map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range stats {
		if strings.HasPrefix(k, "treedb.cache.vlog_generation.") {
			out[k] = v
		}
	}
	return out
}

func filterVlogIOStats(stats map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range stats {
		if strings.HasPrefix(k, "treedb.vlog.mmap_") ||
			strings.HasPrefix(k, "treedb.vlog.grouped_frame_cache.") ||
			strings.HasPrefix(k, "treedb.vlog.template_def_cache.") {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func printSelectedVlogGenerationStatChanges(before, after map[string]string) {
	keys := []string{
		"treedb.cache.vlog_generation.last_reason",
		"treedb.cache.vlog_generation.scheduler_state",
		"treedb.cache.vlog_generation.rewrite.runs",
		"treedb.cache.vlog_generation.rewrite.bytes_in",
		"treedb.cache.vlog_generation.rewrite.bytes_out",
		"treedb.cache.vlog_generation.rewrite.queue_prune_runs",
		"treedb.cache.vlog_generation.rewrite.queue_prune_ids",
		"treedb.cache.vlog_generation.gc.runs",
		"treedb.cache.vlog_generation.gc.deleted_bytes",
		"treedb.cache.vlog_generation.checkpoint_kick.runs",
		"treedb.cache.vlog_generation.checkpoint_kick.rewrite_runs",
		"treedb.cache.vlog_generation.checkpoint_kick.gc_runs",
	}
	sort.Strings(keys)
	fmt.Println("stats:")
	for _, k := range keys {
		beforeVal := before[k]
		afterVal := after[k]
		if beforeVal == "" && afterVal == "" {
			continue
		}
		if beforeN, err := strconv.ParseInt(beforeVal, 10, 64); err == nil {
			if afterN, err := strconv.ParseInt(afterVal, 10, 64); err == nil {
				fmt.Printf("  %s=%s -> %s (delta=%d)\n", k, beforeVal, afterVal, afterN-beforeN)
				continue
			}
		}
		fmt.Printf("  %s=%q -> %q\n", k, beforeVal, afterVal)
	}
}
