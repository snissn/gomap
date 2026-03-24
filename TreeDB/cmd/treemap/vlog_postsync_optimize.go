package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

type valueLogPostsyncOfflineReport struct {
	RunMillis      int64 `json:"run_millis"`
	SegmentsBefore int   `json:"segments_before"`
	SegmentsAfter  int   `json:"segments_after"`
	BytesBefore    int64 `json:"bytes_before"`
	BytesAfter     int64 `json:"bytes_after"`
	RecordsCopied  int   `json:"records_copied"`
}

type valueLogPostsyncOptimizeReport struct {
	Dir                string                         `json:"dir"`
	RequestedStrategy  string                         `json:"requested_strategy"`
	Strategy           string                         `json:"strategy"`
	InitialBytes       int64                          `json:"initial_bytes"`
	AfterExplicitBytes int64                          `json:"after_explicit_bytes,omitempty"`
	FinalBytes         int64                          `json:"final_bytes"`
	Explicit           *valueLogMaintExitLoopReport   `json:"explicit,omitempty"`
	Offline            *valueLogPostsyncOfflineReport `json:"offline,omitempty"`
	TotalMillis        int64                          `json:"total_millis"`
}

func runVlogPostsyncOptimize(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-postsync-optimize", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write / run maintenance (required)")
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	strategy := fs.String("strategy", "auto", "Optimization strategy: auto|offline|explicit|hybrid")
	disableAutoDeferred := fs.Bool("disable-auto-deferred", true, "Disable automatic deferred stage/age wakes during explicit maintenance")
	rewritePlanTimeout := fs.Duration("rewrite-plan-timeout", 0, "Override cached rewrite planner timeout for this process (debug/offline analysis only)")
	waitIdle := fs.Duration("wait-idle", 3*time.Minute, "Wait for cached value-log generation maintenance to go idle after each explicit pass")
	maxPasses := fs.Int("max-passes", 7, "Maximum number of explicit stage-confirm-exit passes to run")
	minReclaimBytes := fs.Int64("min-reclaim-bytes", 1<<20, "Stop explicit passes when reclaimed bytes fall below this threshold")
	stopQueueNondecrease := fs.Bool("stop-queue-nondecrease", true, "Stop explicit passes when rewrite queue does not shrink")
	reseedFromPlanWhenIdle := fs.Bool("reseed-from-plan-when-idle", true, "Inject one backend-planned reseed wave after explicit debt drains")
	reseedMaxSegments := fs.Int("reseed-max-segments", 64, "Backend reseed plan selection cap in segments (0=none)")
	reseedMaxBytes := fs.Int64("reseed-max-bytes", 8<<30, "Backend reseed plan live-byte selection cap (0=none)")
	reseedMinStaleRatio := fs.Float64("reseed-min-stale-ratio", 0.50, "Backend reseed plan minimum per-segment stale ratio (0..1)")
	reseedMinStaleBytes := fs.Int64("reseed-min-stale-bytes", 1, "Backend reseed plan minimum per-segment stale bytes")
	rewriteBudgetTokens := fs.Int64("rewrite-budget-tokens", 2<<30, "Cached rewrite budget tokens to preseed for each explicit exit pass")
	reseedStageObservedAgo := fs.Duration("reseed-stage-observed-ago", 31*time.Second, "When reseeding a ledger, record the stage observation this far in the past so the next pass can spend it immediately")
	maxReseeds := fs.Int("max-reseeds", 1, "Maximum number of backend plan reseed waves to inject per invocation")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-postsync-optimize requires -rw")
	}
	mode, err := parseValueLogPostsyncStrategy(*strategy)
	if err != nil {
		fatalf("%v", err)
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

	start := time.Now()
	initialBytes, err := dirLogicalSizeBytes(dir)
	if err != nil {
		fatalf("dir size before optimize: %v", err)
	}
	report := valueLogPostsyncOptimizeReport{
		Dir:               dir,
		RequestedStrategy: mode,
		Strategy:          mode,
		InitialBytes:      initialBytes,
		FinalBytes:        initialBytes,
	}

	runExplicit := mode == "explicit" || mode == "hybrid"
	runOffline := mode == "offline" || mode == "hybrid"
	if mode == "auto" {
		runOffline = true
	}

	if runExplicit {
		explicit, err := runValueLogMaintExitLoopWithConfig(dir, valueLogMaintExitLoopConfig{
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
			fatalf("explicit postsync optimize: %v", err)
		}
		report.Explicit = &explicit
		report.AfterExplicitBytes = explicit.FinalBytes
		report.FinalBytes = explicit.FinalBytes
	}

	if runOffline {
		offline, err := runValueLogPostsyncOffline(dir)
		if err != nil {
			if mode == "auto" && shouldFallbackPostsyncOptimizeToExplicit(err) {
				report.Strategy = "explicit"
				runExplicit = true
			} else {
				fatalf("offline postsync optimize: %v", err)
			}
		} else {
			report.Offline = offline
			finalBytes, err := dirLogicalSizeBytes(dir)
			if err != nil {
				fatalf("dir size after offline optimize: %v", err)
			}
			report.FinalBytes = finalBytes
			if mode == "auto" {
				report.Strategy = "offline"
			}
		}
	}

	if mode == "auto" && report.Offline == nil && runExplicit {
		explicit, err := runValueLogMaintExitLoopWithConfig(dir, valueLogMaintExitLoopConfig{
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
			fatalf("explicit postsync optimize fallback: %v", err)
		}
		report.Explicit = &explicit
		report.AfterExplicitBytes = explicit.FinalBytes
		report.FinalBytes = explicit.FinalBytes
	}

	report.TotalMillis = time.Since(start).Milliseconds()

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fatalf("json encode: %v", err)
		}
		return
	}

	fmt.Printf("dir=%s strategy=%s requested=%s initial_bytes=%d final_bytes=%d total_ms=%d\n", report.Dir, report.Strategy, report.RequestedStrategy, report.InitialBytes, report.FinalBytes, report.TotalMillis)
	if report.Explicit != nil {
		fmt.Printf("explicit: final_bytes=%d reclaimed=%d stop_reason=%s passes=%d\n", report.Explicit.FinalBytes, report.Explicit.TotalReclaimedBytes, report.Explicit.StopReason, len(report.Explicit.Passes))
	}
	if report.Offline != nil {
		fmt.Printf("offline: run_ms=%d segments_before=%d segments_after=%d bytes_before=%d bytes_after=%d records=%d\n",
			report.Offline.RunMillis,
			report.Offline.SegmentsBefore,
			report.Offline.SegmentsAfter,
			report.Offline.BytesBefore,
			report.Offline.BytesAfter,
			report.Offline.RecordsCopied,
		)
	}
}

func parseValueLogPostsyncStrategy(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return "auto", nil
	case "offline":
		return "offline", nil
	case "explicit":
		return "explicit", nil
	case "hybrid":
		return "hybrid", nil
	default:
		return "", fmt.Errorf("unsupported -strategy=%q (expected auto|offline|explicit|hybrid)", s)
	}
}

func runValueLogPostsyncOffline(dir string) (*valueLogPostsyncOfflineReport, error) {
	rootDir := resolveTreeDBRootDir(dir)
	opts := treedb.Options{Dir: rootDir}
	applyPersistedFormatConfig(dir, &opts)
	offlineStart := time.Now()
	stats, err := treedb.ValueLogRewriteOffline(opts)
	if err != nil {
		return nil, err
	}
	return &valueLogPostsyncOfflineReport{
		RunMillis:      time.Since(offlineStart).Milliseconds(),
		SegmentsBefore: stats.SegmentsBefore,
		SegmentsAfter:  stats.SegmentsAfter,
		BytesBefore:    stats.BytesBefore,
		BytesAfter:     stats.BytesAfter,
		RecordsCopied:  stats.RecordsCopied,
	}, nil
}

func shouldFallbackPostsyncOptimizeToExplicit(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "clean commitlog")
}
