package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

var (
	suiteGCMode      = flag.String("gc-mode", "strict", "Maintenance suites: value-log GC mode (strict|online)")
	suiteGCPeriod    = flag.Duration("gc-period", 30*time.Second, "Maintenance suites: target GC cadence")
	suiteMaintenance = flag.String("maintenance", "gc,vacuum", "Maintenance suites: comma-separated maintenance ops")
	suiteSkew        = flag.String("skew", "", "Skew model hint for skew/fairness suites (e.g. 0.90, zipf)")
	suiteRebalance   = flag.Bool("rebalance", true, "Enable rebalance path in hotspot suite")
)

const (
	maintenanceProbeBaselineWindow  = 2 * time.Second
	maintenanceProbeDuringMinWindow = 2 * time.Second
	maintenanceProbeDuringMaxWindow = 5 * time.Second
	maintenanceProbeCoordMinWindow  = 2 * time.Second
	maintenanceProbeCoordMaxWindow  = 5 * time.Second
	maintenanceProbeKeyRingSize     = 256
	maintenanceProbeValueSize       = 128
	maintenanceStallThreshold       = time.Millisecond
)

type writeSuiteRun struct {
	cfg      BenchConfig
	testName string
	ops      float64
	wall     time.Duration
	dir      string
	stats    map[string]string

	mainIndexBytes uint64
	mainWALBytes   uint64
	mainValueBytes uint64
	mainFiles      int
}

func runSingleTreeDBWriteSuite(baseCfg BenchConfig, testName string) (writeSuiteRun, func(), error) {
	cfg := baseCfg
	cfg.Progress = false
	cfg.KeepDir = true
	cfg.DBsArg = "treedb"
	cfg.DBsExcludeArg = ""
	cfg.TestsArg = testName
	if cfg.WriteWorkers <= 0 {
		cfg.WriteWorkers = max(1, runtime.GOMAXPROCS(0))
	}

	start := time.Now()
	run, err := runBenchmark(cfg)
	if err != nil {
		return writeSuiteRun{}, nil, err
	}
	cleaned := false
	cleanup := func() {
		if cleaned {
			return
		}
		_ = suiteCleanupDirs(run.Instances)
		cleaned = true
	}

	inst, err := findSuiteInstance(run.Instances, "treedb")
	if err != nil {
		cleanup()
		return writeSuiteRun{}, nil, err
	}
	dbName := inst.Wrapper.Name()

	opsByDB, ok := run.Results[testName]
	if !ok {
		cleanup()
		return writeSuiteRun{}, nil, fmt.Errorf("suite: missing result row %q", testName)
	}
	ops, ok := opsByDB[dbName]
	if !ok {
		cleanup()
		return writeSuiteRun{}, nil, fmt.Errorf("suite: missing result for %s/%s", testName, dbName)
	}

	out := writeSuiteRun{
		cfg:      cfg,
		testName: testName,
		ops:      ops,
		wall:     time.Since(start),
		dir:      inst.Dir,
	}
	if usage, ok := run.TreeDBDiskUsage[dbName]; ok {
		out.mainIndexBytes = usage.MainIndexBytes
		out.mainWALBytes = usage.MainWAL.TotalBytes
		out.mainValueBytes = usage.MainWAL.ValueBytes
		out.mainFiles = usage.MainWAL.TotalFiles
	}
	if snap, ok := run.TreeDBStats[dbName]; ok {
		out.stats = snap
	}
	return out, cleanup, nil
}

func openTreeDBAdapterFromDir(dir string) (*treedbadapter.DB, error) {
	factory, err := GetDBFactory("treedb")
	if err != nil {
		return nil, err
	}
	db, err := factory(dir)
	if err != nil {
		return nil, err
	}
	td, ok := db.(*treedbadapter.DB)
	if !ok {
		_ = db.Close()
		return nil, fmt.Errorf("suite: treedb factory returned %T", db)
	}
	return td, nil
}

func parseSuiteGCMode() (treedb.ValueLogGCMode, error) {
	switch strings.ToLower(strings.TrimSpace(*suiteGCMode)) {
	case "", "strict":
		return treedb.ValueLogGCModeStrict, nil
	case "online":
		return treedb.ValueLogGCModeOnline, nil
	default:
		return treedb.ValueLogGCModeStrict, fmt.Errorf("unsupported -gc-mode=%q (expected strict|online)", *suiteGCMode)
	}
}

func parseStatFloat(stats map[string]string, key string) (float64, bool) {
	if stats == nil {
		return 0, false
	}
	raw, ok := stats[key]
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func aggregateGCStats(dst *treedb.ValueLogGCStats, src treedb.ValueLogGCStats) {
	dst.SegmentsTotal += src.SegmentsTotal
	dst.SegmentsReferenced += src.SegmentsReferenced
	dst.SegmentsActive += src.SegmentsActive
	dst.SegmentsProtected += src.SegmentsProtected
	dst.SegmentsEligible += src.SegmentsEligible
	dst.SegmentsDeleted += src.SegmentsDeleted
	dst.BytesTotal += src.BytesTotal
	dst.BytesReferenced += src.BytesReferenced
	dst.BytesActive += src.BytesActive
	dst.BytesProtected += src.BytesProtected
	dst.BytesEligible += src.BytesEligible
	dst.BytesDeleted += src.BytesDeleted
	if src.FailClosedToDryRun {
		dst.FailClosedToDryRun = true
	}
}

type writerProbeMetrics struct {
	samples           int
	window            time.Duration
	opsPerSec         float64
	stallDutyCyclePct float64
	latencyP99        time.Duration
	latencyMax        time.Duration
}

type writerProbeResult struct {
	metrics writerProbeMetrics
	err     error
}

func makeWriterProbeKeys(prefix string, count int) [][]byte {
	if count <= 0 {
		count = maintenanceProbeKeyRingSize
	}
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		key := make([]byte, 0, len(prefix)+24)
		key = append(key, prefix...)
		key = append(key, ':')
		key = strconv.AppendInt(key, int64(i), 10)
		out = append(out, key)
	}
	return out
}

func makeWriterProbeValue(size int) []byte {
	if size <= 0 {
		size = maintenanceProbeValueSize
	}
	value := make([]byte, size)
	for i := range value {
		value[i] = byte((i % 251) + 1)
	}
	return value
}

func p99Duration(sorted []time.Duration) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (99*len(sorted) + 99) / 100
	if idx <= 0 {
		return sorted[0]
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

func writerProbeMetricsFrom(latencies []time.Duration, window, stallBudget time.Duration) writerProbeMetrics {
	if window <= 0 {
		window = time.Nanosecond
	}
	out := writerProbeMetrics{
		samples: len(latencies),
		window:  window,
	}
	if len(latencies) == 0 {
		return out
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	out.opsPerSec = float64(len(latencies)) / window.Seconds()
	out.stallDutyCyclePct = 100 * float64(stallBudget) / float64(window)
	out.latencyP99 = p99Duration(sorted)
	out.latencyMax = sorted[len(sorted)-1]
	return out
}

func runWriterProbeUntil(ctx context.Context, td *treedbadapter.DB, keyPrefix string, stallThreshold time.Duration) (writerProbeMetrics, error) {
	keys := makeWriterProbeKeys(keyPrefix, maintenanceProbeKeyRingSize)
	value := makeWriterProbeValue(maintenanceProbeValueSize)
	latencies := make([]time.Duration, 0, 2048)
	var stallBudget time.Duration
	start := time.Now()
	keyIdx := 0

	for {
		select {
		case <-ctx.Done():
			return writerProbeMetricsFrom(latencies, time.Since(start), stallBudget), nil
		default:
		}
		opStart := time.Now()
		if err := td.Set(keys[keyIdx], value); err != nil {
			return writerProbeMetrics{}, err
		}
		latency := time.Since(opStart)
		latencies = append(latencies, latency)
		if latency > stallThreshold {
			stallBudget += latency - stallThreshold
		}
		keyIdx++
		if keyIdx >= len(keys) {
			keyIdx = 0
		}
	}
}

func probeOpsRatio(baselineOps, duringOps float64) float64 {
	if baselineOps <= 0 {
		return 0
	}
	return duringOps / baselineOps
}

func formatDurationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func parseFreshnessAge(stats map[string]string, key string, now time.Time) (time.Duration, bool) {
	if stats == nil {
		return 0, false
	}
	raw, ok := stats[key]
	if !ok {
		return 0, false
	}
	ns, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || ns <= 0 {
		return 0, false
	}
	at := time.Unix(0, ns)
	age := now.Sub(at)
	if age < 0 {
		age = 0
	}
	return age, true
}

func attributableDutyCyclePct(baseline, observed float64) float64 {
	if observed <= baseline {
		return 0
	}
	return observed - baseline
}

func runVlogQueueLagSuite(baseCfg BenchConfig) (string, error) {
	s, cleanup, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup()

	lagP99, hasLagP99 := parseStatFloat(s.stats, "treedb.cache.vlog_queue.lag_p99_ms")
	lagP999, hasLagP999 := parseStatFloat(s.stats, "treedb.cache.vlog_queue.lag_p999_ms")
	lagAvg, hasLagAvg := parseStatFloat(s.stats, "treedb.cache.vlog_queue.lag_avg_ms")
	lagMax, hasLagMax := parseStatFloat(s.stats, "treedb.cache.vlog_queue.lag_max_ms")
	driftRunMax, hasDriftRunMax := parseStatFloat(s.stats, "treedb.cache.vlog_queue.positive_drift_run_max_ms")
	lagSamples := parseUint(s.stats, "treedb.cache.vlog_queue.lag_samples")
	depthMax := parseUint(s.stats, "treedb.cache.vlog_queue.depth_max")
	depthAvg, hasDepthAvg := parseStatFloat(s.stats, "treedb.cache.vlog_queue.depth_avg")
	queuedTotal := parseUint(s.stats, "treedb.cache.vlog_queue.enqueued_total")

	queueP99Pass := hasLagP99 && lagP99 <= 2.0
	queueP999Pass := hasLagP999 && lagP999 <= 10.0
	queueDriftPass := hasDriftRunMax && driftRunMax <= 60_000.0

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: vlog_queue_lag\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(s.cfg.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", s.cfg.ValueSize))
	sb.WriteString(fmt.Sprintf("- write-workers: %d\n", s.cfg.WriteWorkers))
	sb.WriteString(fmt.Sprintf("- gomaxprocs: %d\n", runtime.GOMAXPROCS(0)))
	sb.WriteString(fmt.Sprintf("- ops/sec: %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- ops/sec/worker: %s\n", formatFloat(s.ops/float64(max(1, s.cfg.WriteWorkers)))))
	sb.WriteString(fmt.Sprintf("- wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- maindb index bytes: %s\n", formatBytes(s.mainIndexBytes)))
	sb.WriteString(fmt.Sprintf("- maindb wal bytes (total): %s\n", formatBytes(s.mainWALBytes)))
	sb.WriteString(fmt.Sprintf("- maindb value-log bytes (wal dir): %s\n", formatBytes(s.mainValueBytes)))
	sb.WriteString(fmt.Sprintf("- maindb wal files: %d\n", s.mainFiles))
	sb.WriteString(fmt.Sprintf("- queue lag samples: %s\n", formatInt(int(lagSamples))))
	sb.WriteString(fmt.Sprintf("- queue enqueued total: %s\n", formatInt(int(queuedTotal))))
	if hasLagAvg {
		sb.WriteString(fmt.Sprintf("- queue lag avg: %.3fms\n", lagAvg))
	}
	if hasLagP99 {
		sb.WriteString(fmt.Sprintf("- queue lag p99: %.3fms\n", lagP99))
	}
	if hasLagP999 {
		sb.WriteString(fmt.Sprintf("- queue lag p999: %.3fms\n", lagP999))
	}
	if hasLagMax {
		sb.WriteString(fmt.Sprintf("- queue lag max: %.3fms\n", lagMax))
	}
	sb.WriteString(fmt.Sprintf("- queue depth max: %s\n", formatInt(int(depthMax))))
	if hasDepthAvg {
		sb.WriteString(fmt.Sprintf("- queue depth avg: %.3f\n", depthAvg))
	}
	if hasDriftRunMax {
		sb.WriteString(fmt.Sprintf("- max consecutive positive queue-depth drift run: %.3fms\n", driftRunMax))
	}
	sb.WriteString(fmt.Sprintf("- gate queue-lag-p99<=2ms: %s\n", passFail(queueP99Pass)))
	sb.WriteString(fmt.Sprintf("- gate queue-lag-p999<=10ms: %s\n", passFail(queueP999Pass)))
	sb.WriteString(fmt.Sprintf("- gate no-positive-drift-run>60s: %s\n", passFail(queueDriftPass)))

	laneDepthKeys := make([]string, 0)
	for k := range s.stats {
		if strings.HasPrefix(k, "treedb.cache.vlog_queue.lane.") && strings.HasSuffix(k, ".depth_max") {
			laneDepthKeys = append(laneDepthKeys, k)
		}
	}
	sort.Strings(laneDepthKeys)
	if len(laneDepthKeys) > 0 {
		sb.WriteString("- lane queue depth max:\n")
		for _, k := range laneDepthKeys {
			sb.WriteString(fmt.Sprintf("  - %s = %s\n", k, s.stats[k]))
		}
	}
	sb.WriteString("\n")
	if !hasLagP99 || !hasLagP999 {
		sb.WriteString("- note: queue-lag percentiles are missing from stats; gates above fail closed.\n")
	}
	return sb.String(), nil
}

func runBackendSaturationSuite(baseCfg BenchConfig) (string, error) {
	s, cleanup, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup()

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: backend_saturation\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(s.cfg.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", s.cfg.ValueSize))
	sb.WriteString(fmt.Sprintf("- write-workers: %d\n", s.cfg.WriteWorkers))
	sb.WriteString(fmt.Sprintf("- gomaxprocs: %d\n", runtime.GOMAXPROCS(0)))
	sb.WriteString(fmt.Sprintf("- ops/sec: %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- ops/sec/worker: %s\n", formatFloat(s.ops/float64(max(1, s.cfg.WriteWorkers)))))
	sb.WriteString(fmt.Sprintf("- wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- maindb index bytes: %s\n", formatBytes(s.mainIndexBytes)))
	sb.WriteString(fmt.Sprintf("- maindb wal bytes (total): %s\n", formatBytes(s.mainWALBytes)))
	sb.WriteString(fmt.Sprintf("- maindb wal files: %d\n", s.mainFiles))
	sb.WriteString("\n")
	return sb.String(), nil
}

func runBackendMaterializationDebtSuite(baseCfg BenchConfig) (string, error) {
	s, cleanup, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup()

	keys := max(1, s.cfg.Keys)
	indexBytesPerKey := float64(s.mainIndexBytes) / float64(keys)
	walBytesPerKey := float64(s.mainWALBytes) / float64(keys)

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: backend_materialization_debt\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(s.cfg.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", s.cfg.ValueSize))
	sb.WriteString(fmt.Sprintf("- write-workers: %d\n", s.cfg.WriteWorkers))
	sb.WriteString(fmt.Sprintf("- gomaxprocs: %d\n", runtime.GOMAXPROCS(0)))
	sb.WriteString(fmt.Sprintf("- ops/sec: %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- maindb index bytes: %s\n", formatBytes(s.mainIndexBytes)))
	sb.WriteString(fmt.Sprintf("- maindb wal bytes (total): %s\n", formatBytes(s.mainWALBytes)))
	sb.WriteString(fmt.Sprintf("- index bytes/key (proxy): %.3f\n", indexBytesPerKey))
	sb.WriteString(fmt.Sprintf("- wal bytes/key (proxy): %.3f\n", walBytesPerKey))
	sb.WriteString("\n")
	sb.WriteString("- note: debt lag percentiles/slope are not yet exported by this harness; bytes-per-key proxies are reported for baseline tracking.\n")
	return sb.String(), nil
}

func runMaintenanceGCSuite(baseCfg BenchConfig) (string, error) {
	s, cleanup, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup()

	td, err := openTreeDBAdapterFromDir(s.dir)
	if err != nil {
		return "", err
	}
	defer td.Close()

	mode, err := parseSuiteGCMode()
	if err != nil {
		return "", err
	}
	period := *suiteGCPeriod
	if period <= 0 {
		period = 30 * time.Second
	}
	runs := 1 + int(s.wall/period)
	if runs < 1 {
		runs = 1
	}
	if runs > 5 {
		runs = 5
	}

	baselineCtx, baselineCancel := context.WithTimeout(context.Background(), maintenanceProbeBaselineWindow)
	baselineProbe, err := runWriterProbeUntil(baselineCtx, td, "maintenance_gc_baseline_probe", maintenanceStallThreshold)
	baselineCancel()
	if err != nil {
		return "", err
	}

	duringProbeCtx, duringProbeCancel := context.WithTimeout(context.Background(), maintenanceProbeDuringMaxWindow)
	duringProbeCh := make(chan writerProbeResult, 1)
	go func() {
		m, probeErr := runWriterProbeUntil(duringProbeCtx, td, "maintenance_gc_during_probe", maintenanceStallThreshold)
		duringProbeCh <- writerProbeResult{metrics: m, err: probeErr}
	}()

	var total treedb.ValueLogGCStats
	var gcWall time.Duration
	gcStart := time.Now()
	executedRuns := 0
	for i := 0; i < runs; i++ {
		start := time.Now()
		stats, err := td.DB.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{Mode: mode})
		if err != nil {
			duringProbeCancel()
			<-duringProbeCh
			return "", err
		}
		gcWall += time.Since(start)
		aggregateGCStats(&total, stats)
		executedRuns++
	}
	if elapsed := time.Since(gcStart); elapsed < maintenanceProbeDuringMinWindow {
		wait := maintenanceProbeDuringMinWindow - elapsed
		if wait > 0 && wait < maintenanceProbeDuringMaxWindow {
			time.Sleep(wait)
		}
	}
	duringProbeCancel()
	duringProbeResult := <-duringProbeCh
	if duringProbeResult.err != nil {
		return "", duringProbeResult.err
	}
	duringProbe := duringProbeResult.metrics

	throughputRatio := probeOpsRatio(baselineProbe.opsPerSec, duringProbe.opsPerSec)
	attributableDuty := attributableDutyCyclePct(baselineProbe.stallDutyCyclePct, duringProbe.stallDutyCyclePct)
	ratioGate := baselineProbe.samples > 0 && throughputRatio >= 0.90
	dutyGate := duringProbe.samples > 0 && attributableDuty <= 5.0
	p99Gate := duringProbe.samples > 0 && duringProbe.latencyP99 <= 10*time.Millisecond

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: maintenance_gc\n\n")
	sb.WriteString(fmt.Sprintf("- gc-mode: %s\n", mode))
	sb.WriteString(fmt.Sprintf("- gc-period: %s\n", period))
	sb.WriteString(fmt.Sprintf("- write ops/sec: %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- write wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- gc runs: %d\n", executedRuns))
	sb.WriteString(fmt.Sprintf("- gc total wall time: %s\n", gcWall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- gc segments eligible (sum): %d\n", total.SegmentsEligible))
	sb.WriteString(fmt.Sprintf("- gc segments deleted (sum): %d\n", total.SegmentsDeleted))
	sb.WriteString(fmt.Sprintf("- gc segments protected (sum): %d\n", total.SegmentsProtected))
	sb.WriteString(fmt.Sprintf("- gc bytes eligible (sum): %s\n", formatBytes(uint64(max(0, int(total.BytesEligible))))))
	sb.WriteString(fmt.Sprintf("- gc bytes deleted (sum): %s\n", formatBytes(uint64(max(0, int(total.BytesDeleted))))))
	sb.WriteString(fmt.Sprintf("- gc bytes protected (sum): %s\n", formatBytes(uint64(max(0, int(total.BytesProtected))))))
	sb.WriteString(fmt.Sprintf("- gc fail-closed to dry-run: %t\n", total.FailClosedToDryRun))
	sb.WriteString(fmt.Sprintf("- writer probe baseline window: %s\n", baselineProbe.window.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- writer probe baseline samples: %s\n", formatInt(baselineProbe.samples)))
	sb.WriteString(fmt.Sprintf("- writer probe baseline ops/sec (no maintenance): %s\n", formatFloat(baselineProbe.opsPerSec)))
	sb.WriteString(fmt.Sprintf("- writer probe baseline stall duty cycle: %.3f%%\n", baselineProbe.stallDutyCyclePct))
	sb.WriteString(fmt.Sprintf("- writer probe during-gc window: %s\n", duringProbe.window.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- writer probe during-gc samples: %s\n", formatInt(duringProbe.samples)))
	sb.WriteString(fmt.Sprintf("- writer probe during-gc ops/sec: %s\n", formatFloat(duringProbe.opsPerSec)))
	sb.WriteString(fmt.Sprintf("- writer probe throughput ratio (during-gc/baseline): %.3f\n", throughputRatio))
	sb.WriteString(fmt.Sprintf("- writer stall duty cycle: %.3f%%\n", duringProbe.stallDutyCyclePct))
	sb.WriteString(fmt.Sprintf("- writer stall duty cycle (GC-attributable): %.3f%%\n", attributableDuty))
	sb.WriteString(fmt.Sprintf("- writer latency p99: %.3fms\n", formatDurationMS(duringProbe.latencyP99)))
	sb.WriteString(fmt.Sprintf("- writer latency max: %.3fms\n", formatDurationMS(duringProbe.latencyMax)))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("- gate probe-throughput-ratio>=0.90: %s\n", passFail(ratioGate)))
	sb.WriteString(fmt.Sprintf("- gate writer-stall-duty(gc-attributable)<=5%%: %s\n", passFail(dutyGate)))
	sb.WriteString(fmt.Sprintf("- gate writer-latency-p99<=10ms: %s\n", passFail(p99Gate)))
	return sb.String(), nil
}

func runMaintenanceCoordinationSuite(baseCfg BenchConfig) (string, error) {
	s, cleanup, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup()

	td, err := openTreeDBAdapterFromDir(s.dir)
	if err != nil {
		return "", err
	}
	defer td.Close()

	mode, err := parseSuiteGCMode()
	if err != nil {
		return "", err
	}
	ops := parseList(*suiteMaintenance)
	if len(ops) == 0 {
		ops = []string{"gc", "vacuum"}
	}
	normalizedOps := make([]string, 0, len(ops))
	needGC := false
	needVacuum := false
	for _, rawOp := range ops {
		op := strings.ToLower(strings.TrimSpace(rawOp))
		if op == "" {
			continue
		}
		switch op {
		case "gc", "vacuum":
			normalizedOps = append(normalizedOps, op)
			if op == "gc" {
				needGC = true
			}
			if op == "vacuum" {
				needVacuum = true
			}
		default:
			return "", fmt.Errorf("maintenance_coordination: unsupported maintenance op %q", op)
		}
	}
	if len(normalizedOps) == 0 {
		return "", fmt.Errorf("maintenance_coordination: no valid maintenance ops")
	}

	baselineCtx, baselineCancel := context.WithTimeout(context.Background(), maintenanceProbeBaselineWindow)
	baselineProbe, err := runWriterProbeUntil(baselineCtx, td, "maintenance_coord_baseline_probe", maintenanceStallThreshold)
	baselineCancel()
	if err != nil {
		return "", err
	}

	probeCtx, probeCancel := context.WithTimeout(context.Background(), maintenanceProbeCoordMaxWindow)
	probeCh := make(chan writerProbeResult, 1)
	go func() {
		m, probeErr := runWriterProbeUntil(probeCtx, td, "maintenance_coord_probe", maintenanceStallThreshold)
		probeCh <- writerProbeResult{metrics: m, err: probeErr}
	}()

	counts := map[string]int{}
	walls := map[string]time.Duration{}
	var mu sync.Mutex
	var firstErr error
	roundStart := time.Now()
	executedRounds := 0
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	for _, op := range normalizedOps {
		wg.Add(1)
		go func(op string) {
			defer wg.Done()
			<-startGate
			start := time.Now()
			var err error
			switch op {
			case "gc":
				_, err = td.DB.ValueLogGC(context.Background(), treedb.ValueLogGCOptions{Mode: mode})
			case "vacuum":
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				err = td.VacuumIndexOnline(ctx)
				cancel()
			}
			dur := time.Since(start)
			mu.Lock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			walls[op] += dur
			counts[op]++
			mu.Unlock()
		}(op)
	}
	close(startGate)
	wg.Wait()
	executedRounds = 1
	if elapsed := time.Since(roundStart); elapsed < maintenanceProbeCoordMinWindow {
		wait := maintenanceProbeCoordMinWindow - elapsed
		if wait > 0 && wait < maintenanceProbeCoordMaxWindow {
			time.Sleep(wait)
		}
	}
	probeCancel()
	probeResult := <-probeCh
	if probeResult.err != nil {
		return "", probeResult.err
	}
	probeMetrics := probeResult.metrics
	attributableDuty := attributableDutyCyclePct(baselineProbe.stallDutyCyclePct, probeMetrics.stallDutyCyclePct)
	if firstErr != nil {
		return "", firstErr
	}

	coordStats := td.Stats()
	fullScanActive := ""
	if coordStats != nil {
		fullScanActive = coordStats["treedb.maintenance.full_scan.active"]
	}
	deferrals := parseUint(coordStats, "treedb.maintenance.full_scan.deferrals")
	waitTotalMs, _ := parseStatFloat(coordStats, "treedb.maintenance.full_scan.wait_total_ms")
	waitMaxMs, _ := parseStatFloat(coordStats, "treedb.maintenance.full_scan.wait_max_ms")
	gcRuns := parseUint(coordStats, "treedb.maintenance.full_scan.gc_runs")
	vacuumRuns := parseUint(coordStats, "treedb.maintenance.full_scan.vacuum_runs")
	statsNow := time.Now()
	lastGCAge, hasLastGCAge := parseFreshnessAge(coordStats, "treedb.maintenance.full_scan.last_gc_unix_nano", statsNow)
	lastVacuumAge, hasLastVacuumAge := parseFreshnessAge(coordStats, "treedb.maintenance.full_scan.last_vacuum_unix_nano", statsNow)
	freshnessReported := (!needGC || hasLastGCAge) && (!needVacuum || hasLastVacuumAge)
	overlapGate := true
	if needGC && needVacuum {
		overlapGate = deferrals > 0
	}

	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: maintenance_coordination\n\n")
	sb.WriteString(fmt.Sprintf("- maintenance ops: %s\n", strings.Join(normalizedOps, ",")))
	sb.WriteString("- launch mode: concurrent\n")
	sb.WriteString(fmt.Sprintf("- write ops/sec: %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- write wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- maintenance rounds executed: %d\n", executedRounds))
	for _, k := range keys {
		sb.WriteString(fmt.Sprintf("- %s runs: %d\n", k, counts[k]))
		sb.WriteString(fmt.Sprintf("- %s total wall time: %s\n", k, walls[k].Truncate(time.Millisecond)))
	}
	sb.WriteString(fmt.Sprintf("- maintenance full-scan active (post-run): %q\n", fullScanActive))
	sb.WriteString(fmt.Sprintf("- maintenance full-scan deferrals: %s\n", formatInt(int(deferrals))))
	sb.WriteString(fmt.Sprintf("- maintenance full-scan wait total: %.3fms\n", waitTotalMs))
	sb.WriteString(fmt.Sprintf("- maintenance full-scan wait max: %.3fms\n", waitMaxMs))
	sb.WriteString(fmt.Sprintf("- maintenance full-scan gc runs (stats): %s\n", formatInt(int(gcRuns))))
	sb.WriteString(fmt.Sprintf("- maintenance full-scan vacuum runs (stats): %s\n", formatInt(int(vacuumRuns))))
	sb.WriteString(fmt.Sprintf("- writer baseline probe window: %s\n", baselineProbe.window.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- writer baseline probe samples: %s\n", formatInt(baselineProbe.samples)))
	sb.WriteString(fmt.Sprintf("- writer baseline stall duty cycle: %.3f%%\n", baselineProbe.stallDutyCyclePct))
	sb.WriteString(fmt.Sprintf("- writer probe window: %s\n", probeMetrics.window.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- writer probe samples: %s\n", formatInt(probeMetrics.samples)))
	sb.WriteString(fmt.Sprintf("- writer stall duty cycle: %.3f%%\n", probeMetrics.stallDutyCyclePct))
	sb.WriteString(fmt.Sprintf("- writer stall duty cycle (maintenance-attributable): %.3f%%\n", attributableDuty))
	sb.WriteString(fmt.Sprintf("- writer latency p99: %.3fms\n", formatDurationMS(probeMetrics.latencyP99)))
	sb.WriteString(fmt.Sprintf("- writer latency max: %.3fms\n", formatDurationMS(probeMetrics.latencyMax)))
	if hasLastGCAge {
		sb.WriteString(fmt.Sprintf("- maintenance freshness gc age: %s\n", lastGCAge.Truncate(time.Millisecond)))
	} else {
		sb.WriteString("- maintenance freshness gc age: n/a (last_gc_unix_nano missing or zero)\n")
	}
	if hasLastVacuumAge {
		sb.WriteString(fmt.Sprintf("- maintenance freshness vacuum age: %s\n", lastVacuumAge.Truncate(time.Millisecond)))
	} else {
		sb.WriteString("- maintenance freshness vacuum age: n/a (last_vacuum_unix_nano missing or zero)\n")
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("- gate overlap-token-evidence(deferrals>0): %s\n", passFail(overlapGate)))
	sb.WriteString(fmt.Sprintf("- gate writer-stall-duty(maintenance-attributable)<=10%%: %s\n", passFail(probeMetrics.samples > 0 && attributableDuty <= 10.0)))
	sb.WriteString(fmt.Sprintf("- gate freshness-reported: %s\n", passFail(freshnessReported)))
	return sb.String(), nil
}

func runBackendSkewFairnessSuite(baseCfg BenchConfig) (string, error) {
	s, cleanup, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup()

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: backend_skew_fairness\n\n")
	sb.WriteString(fmt.Sprintf("- skew: %s\n", strings.TrimSpace(*suiteSkew)))
	sb.WriteString(fmt.Sprintf("- write-workers: %d\n", s.cfg.WriteWorkers))
	sb.WriteString(fmt.Sprintf("- ops/sec: %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	sb.WriteString("\n")
	sb.WriteString("- note: this harness currently uses a uniform write workload and records skew input as metadata for gate bookkeeping.\n")
	return sb.String(), nil
}

func runBackendSyncMatrixSuite(baseCfg BenchConfig) (string, error) {
	parallelRun, cleanupParallel, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanupParallel()

	syncRun, cleanupSync, err := runSingleTreeDBWriteSuite(baseCfg, "dataset_update_fork_choice")
	if err != nil {
		return "", err
	}
	defer cleanupSync()

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: backend_sync_matrix\n\n")
	sb.WriteString(fmt.Sprintf("- random_write_parallel ops/sec: %s\n", formatFloat(parallelRun.ops)))
	sb.WriteString(fmt.Sprintf("- random_write_parallel wall: %s\n", parallelRun.wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- dataset_update_fork_choice ops/sec: %s\n", formatFloat(syncRun.ops)))
	sb.WriteString(fmt.Sprintf("- dataset_update_fork_choice wall: %s\n", syncRun.wall.Truncate(time.Millisecond)))
	sb.WriteString("\n")
	sb.WriteString("- note: this suite currently provides a compact sync-heavy vs parallel-write comparison for regression tracking.\n")
	return sb.String(), nil
}

func runPublishWatermarkSuite(baseCfg BenchConfig) (string, error) {
	s, cleanup, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup()

	td, err := openTreeDBAdapterFromDir(s.dir)
	if err != nil {
		return "", err
	}
	defer td.Close()

	stats := td.Stats()
	keys := []string{
		"treedb.cache.queue_backlog_bytes",
		"treedb.cache.flush_bps_ewma",
		"treedb.cache.auto_checkpoint.last_duration_ms",
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: publish_watermark\n\n")
	sb.WriteString(fmt.Sprintf("- write ops/sec: %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- write wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	for _, k := range keys {
		if v, ok := stats[k]; ok {
			sb.WriteString(fmt.Sprintf("- %s: %s\n", k, v))
		}
	}
	sb.WriteString("\n")
	sb.WriteString("- note: explicit publish-watermark lock-share metrics are not exported yet; cache checkpoint/backlog proxies are reported.\n")
	return sb.String(), nil
}

func runHotspotRebalanceSuite(baseCfg BenchConfig) (string, error) {
	s, cleanup, err := runSingleTreeDBWriteSuite(baseCfg, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup()

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: hotspot_rebalance\n\n")
	sb.WriteString(fmt.Sprintf("- skew: %s\n", strings.TrimSpace(*suiteSkew)))
	sb.WriteString(fmt.Sprintf("- rebalance: %t\n", *suiteRebalance))
	sb.WriteString(fmt.Sprintf("- write-workers: %d\n", s.cfg.WriteWorkers))
	sb.WriteString(fmt.Sprintf("- ops/sec: %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	sb.WriteString("\n")
	sb.WriteString("- note: this suite currently captures hotspot scenario metadata and throughput proxies.\n")
	return sb.String(), nil
}

func runFenceLagSuite(baseCfg BenchConfig) (string, error) {
	cfg := baseCfg
	if cfg.Keys > 50_000 {
		cfg.Keys = 50_000
	}
	s, cleanup, err := runSingleTreeDBWriteSuite(cfg, "update_fork_choice")
	if err != nil {
		return "", err
	}
	defer cleanup()

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: fence_lag\n\n")
	sb.WriteString(fmt.Sprintf("- requested keys: %s\n", formatInt(baseCfg.Keys)))
	sb.WriteString(fmt.Sprintf("- effective keys: %s\n", formatInt(s.cfg.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", s.cfg.ValueSize))
	sb.WriteString(fmt.Sprintf("- ops/sec (sync-heavy): %s\n", formatFloat(s.ops)))
	sb.WriteString(fmt.Sprintf("- wall time: %s\n", s.wall.Truncate(time.Millisecond)))
	sb.WriteString("\n")
	sb.WriteString("- note: explicit fence lag percentiles are not exported yet; sync-heavy throughput is reported as a proxy.\n")
	return sb.String(), nil
}

func runStorageCeilingSuite(baseCfg BenchConfig) (string, error) {
	cfg1 := baseCfg
	cfg1.WriteWorkers = 1
	w1, cleanup1, err := runSingleTreeDBWriteSuite(cfg1, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanup1()

	cfgN := baseCfg
	cfgN.WriteWorkers = max(1, runtime.GOMAXPROCS(0))
	wn, cleanupN, err := runSingleTreeDBWriteSuite(cfgN, "random_write_parallel")
	if err != nil {
		return "", err
	}
	defer cleanupN()

	ratio := 0.0
	if w1.ops > 0 {
		ratio = wn.ops / w1.ops
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: storage_ceiling\n\n")
	sb.WriteString(fmt.Sprintf("- gomaxprocs: %d\n", runtime.GOMAXPROCS(0)))
	sb.WriteString(fmt.Sprintf("- workers=1 ops/sec: %s\n", formatFloat(w1.ops)))
	sb.WriteString(fmt.Sprintf("- workers=gomaxprocs ops/sec: %s\n", formatFloat(wn.ops)))
	sb.WriteString(fmt.Sprintf("- scaling ratio (sat/1): %.3f\n", ratio))
	sb.WriteString(fmt.Sprintf("- workers=1 wall: %s\n", w1.wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- workers=gomaxprocs wall: %s\n", wn.wall.Truncate(time.Millisecond)))
	sb.WriteString("\n")
	sb.WriteString("- note: this is a high-level storage-ceiling proxy and should be paired with lock-delay and device metrics for production claims.\n")
	return sb.String(), nil
}
