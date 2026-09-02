package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

var (
	maintBudgetSweepKList    = flag.String("maint-k-sweep", "0,50000,100000,200000,400000,800000", "Comma-separated ops-per-coalesce K values for -suite maintenance_budget")
	maintBudgetSizeSlack     = flag.Float64("maint-size-slack", 0.10, "Index size slack (fraction) for recommended K in -suite maintenance_budget")
	maintBudgetCheckpointRow = "batch_delete"
)

type maintenanceSweepResult struct {
	OpsPerCoalesce   int
	Checkpoint       time.Duration
	WriteOpsPerSec   float64
	CutoverFromStats bool
	IndexBytes       int64
	Dir              string
}

type maintFlagSnapshot struct {
	opsPerCoalesce int
}

func snapshotMaintFlags() maintFlagSnapshot {
	return maintFlagSnapshot{
		opsPerCoalesce: *treedbMaintenanceOpsPerCoalesce,
	}
}

func (s maintFlagSnapshot) restore() {
	*treedbMaintenanceOpsPerCoalesce = s.opsPerCoalesce
}

func parseIntList(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("parse int %q: %w", p, err)
		}
		out = append(out, n)
	}
	return out, nil
}

func parseMaintenanceStatFloat(stats map[string]string, key string) (float64, bool) {
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

func parseMaintenanceStatInt64(stats map[string]string, key string) (int64, bool) {
	if stats == nil {
		return 0, false
	}
	raw, ok := stats[key]
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func checkpointCutoverDurationFromStats(stats map[string]string) (time.Duration, bool) {
	// `cutover_last_ms` is always emitted by cached stats, including a default
	// zero before any checkpoint cutover is measured. Require evidence that a
	// cutover sample was recorded before trusting this metric.
	cutoverMS, ok := parseMaintenanceStatFloat(stats, "treedb.cache.checkpoint.cutover_last_ms")
	if !ok || cutoverMS < 0 {
		return 0, false
	}

	if samples, ok := parseMaintenanceStatInt64(stats, "treedb.cache.checkpoint.cutover_samples"); ok && samples > 0 {
		return time.Duration(cutoverMS*float64(time.Millisecond) + 0.5), true
	}
	if lastUnixNano, ok := parseMaintenanceStatInt64(stats, "treedb.cache.checkpoint.cutover_last_unix_nano"); ok && lastUnixNano > 0 {
		return time.Duration(cutoverMS*float64(time.Millisecond) + 0.5), true
	}
	return 0, false
}

func runMaintenanceBudgetSuite(baseCfg BenchConfig) (string, error) {
	ks, err := parseIntList(*maintBudgetSweepKList)
	if err != nil {
		return "", err
	}
	if len(ks) == 0 {
		return "", fmt.Errorf("maintenance_budget: empty K sweep list")
	}
	sizeSlack := *maintBudgetSizeSlack
	if sizeSlack < 0 {
		sizeSlack = 0
	}

	orig := snapshotMaintFlags()
	defer orig.restore()

	results := make([]maintenanceSweepResult, 0, len(ks))

	for _, k := range ks {
		*treedbMaintenanceOpsPerCoalesce = k

		cfg := baseCfg
		cfg.Progress = false
		cfg.DBsArg = "treedb"
		cfg.TestsArg = "batch_write,random_write,batch_delete"
		cfg.CheckpointBetweenTests = true
		cfg.KeepDir = true

		run, err := runBenchmark(cfg)
		if err != nil {
			return "", err
		}

		var checkpoint time.Duration
		var writeOps float64
		var indexBytes int64
		usedCutoverMetric := false
		for _, inst := range run.Instances {
			row, ok := run.CheckpointDurations[maintBudgetCheckpointRow]
			if !ok {
				return "", fmt.Errorf("maintenance_budget: missing checkpoint row %q", maintBudgetCheckpointRow)
			}
			name := inst.Wrapper.Name()
			dur, ok := row[name]
			if !ok {
				return "", fmt.Errorf("maintenance_budget: missing checkpoint for %s in row %q", name, maintBudgetCheckpointRow)
			}
			checkpoint = dur
			if treeStats, ok := run.TreeDBStats[name]; ok {
				if cutoverDur, ok := checkpointCutoverDurationFromStats(treeStats); ok {
					checkpoint = cutoverDur
					usedCutoverMetric = true
				}
			}

			if wr, ok := run.Results["random_write"]; ok {
				if v, vok := wr[name]; vok {
					writeOps = v
				}
			}

			indexPath, info, err := findIndexDB(inst.Dir)
			if err != nil {
				return "", err
			}
			_ = indexPath
			indexBytes = info.Size()

			if !baseCfg.KeepDir {
				_ = os.RemoveAll(inst.Dir)
			}
		}

		results = append(results, maintenanceSweepResult{
			OpsPerCoalesce:   k,
			Checkpoint:       checkpoint,
			WriteOpsPerSec:   writeOps,
			CutoverFromStats: usedCutoverMetric,
			IndexBytes:       indexBytes,
		})
	}

	minSize := int64(0)
	for _, r := range results {
		if r.IndexBytes <= 0 {
			continue
		}
		if minSize == 0 || r.IndexBytes < minSize {
			minSize = r.IndexBytes
		}
	}
	if minSize == 0 {
		return "", fmt.Errorf("maintenance_budget: invalid index sizes")
	}

	checkpointSamples := make([]time.Duration, 0, len(results))
	dutySamplesPct := make([]float64, 0, len(results))
	for _, r := range results {
		checkpointSamples = append(checkpointSamples, r.Checkpoint)
		if r.WriteOpsPerSec > 0 && baseCfg.Keys > 0 {
			writePhase := time.Duration(float64(baseCfg.Keys)/r.WriteOpsPerSec*float64(time.Second) + 0.5)
			total := writePhase + r.Checkpoint
			if total > 0 {
				dutySamplesPct = append(dutySamplesPct, 100*float64(r.Checkpoint)/float64(total))
			}
		}
	}
	sort.Slice(checkpointSamples, func(i, j int) bool { return checkpointSamples[i] < checkpointSamples[j] })
	sort.Float64s(dutySamplesPct)

	sizeCap := int64(float64(minSize) * (1 + sizeSlack))
	best := results[0]
	bestInCap := false
	for _, r := range results {
		inCap := r.IndexBytes <= sizeCap
		if !bestInCap && inCap {
			best = r
			bestInCap = true
			continue
		}
		if inCap == bestInCap {
			if r.Checkpoint < best.Checkpoint {
				best = r
			} else if r.Checkpoint == best.Checkpoint && r.IndexBytes < best.IndexBytes {
				best = r
			}
		}
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: maintenance_budget\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(baseCfg.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", baseCfg.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", baseCfg.BatchSize))
	sb.WriteString("- tests: batch_write, random_write, batch_delete\n")
	sb.WriteString(fmt.Sprintf("- checkpoint row: %s (checkpoint after random_write)\n", maintBudgetCheckpointRow))
	sb.WriteString(fmt.Sprintf("- k sweep: %s\n", *maintBudgetSweepKList))
	sb.WriteString(fmt.Sprintf("- size slack: %.0f%%\n\n", sizeSlack*100))
	cutoverMetricRuns := 0
	for _, r := range results {
		if r.CutoverFromStats {
			cutoverMetricRuns++
		}
	}
	sb.WriteString(fmt.Sprintf("- cutover source: treedb.cache.checkpoint.cutover_last_ms used at %d/%d sweep points\n", cutoverMetricRuns, len(results)))
	if cutoverMetricRuns == 0 {
		sb.WriteString("- note: cutover source fell back to full Checkpoint() wall time (cutover metric unavailable).\n")
	}
	sb.WriteString("\n")

	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "K\tcheckpoint\tindex.db\tsize_ratio")
	for _, r := range results {
		ratio := float64(r.IndexBytes) / float64(minSize)
		fmt.Fprintf(tw, "%d\t%s\t%s\t%.2fx\n", r.OpsPerCoalesce, formatDuration(r.Checkpoint), formatBytes(uint64(r.IndexBytes)), ratio)
	}
	_ = tw.Flush()

	sb.WriteString("```text\n")
	sb.WriteString(buf.String())
	sb.WriteString("```\n\n")

	sb.WriteString(fmt.Sprintf("- recommended K: %d (checkpoint %s, index.db %s)\n",
		best.OpsPerCoalesce, formatDuration(best.Checkpoint), formatBytes(uint64(best.IndexBytes))))
	if !bestInCap {
		sb.WriteString("- note: no candidate met size slack; selected fastest overall\n")
	}

	checkpointP99 := percentileDurationFromSorted(checkpointSamples, 0.99)
	checkpointP999 := percentileDurationFromSorted(checkpointSamples, 0.999)
	checkpointMax := time.Duration(0)
	if len(checkpointSamples) > 0 {
		checkpointMax = checkpointSamples[len(checkpointSamples)-1]
	}
	dutyP99 := percentileFloat64FromSorted(dutySamplesPct, 0.99)
	dutyMax := 0.0
	if len(dutySamplesPct) > 0 {
		dutyMax = dutySamplesPct[len(dutySamplesPct)-1]
	}

	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("- cutover pause p99: %s\n", formatDuration(checkpointP99)))
	sb.WriteString(fmt.Sprintf("- cutover pause p999: %s\n", formatDuration(checkpointP999)))
	sb.WriteString(fmt.Sprintf("- cutover pause max: %s\n", formatDuration(checkpointMax)))
	if len(dutySamplesPct) > 0 {
		sb.WriteString(fmt.Sprintf("- cutover pause duty cycle p99 (checkpoint/(random_write+checkpoint)): %.3f%%\n", dutyP99))
		sb.WriteString(fmt.Sprintf("- cutover pause duty cycle max: %.3f%%\n", dutyMax))
	} else {
		sb.WriteString("- cutover pause duty cycle: n/a (missing random_write ops/sec)\n")
	}
	sb.WriteString(fmt.Sprintf("- gate cutover-pause-p99<=10ms: %s\n", passFail(len(checkpointSamples) > 0 && checkpointP99 <= 10*time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- gate cutover-pause-p999<=40ms: %s\n", passFail(len(checkpointSamples) > 0 && checkpointP999 <= 40*time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- gate cutover-pause-max<=150ms: %s\n", passFail(len(checkpointSamples) > 0 && checkpointMax <= 150*time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- gate cutover-duty-cycle<=5%%: %s\n", passFail(len(dutySamplesPct) > 0 && dutyP99 <= 5.0)))
	if len(dutySamplesPct) == 0 {
		sb.WriteString("- note: duty-cycle gate fails closed when random_write ops/sec is unavailable.\n")
	}

	return sb.String(), nil
}

func percentileDurationFromSorted(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted))*q + 0.999999)
	if idx < 1 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

func percentileFloat64FromSorted(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted))*q + 0.999999)
	if idx < 1 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

func findIndexDB(dir string) (string, os.FileInfo, error) {
	candidates := []string{
		filepath.Join(dir, "index.db"),
		filepath.Join(dir, "maindb", "index.db"),
	}
	for _, path := range candidates {
		info, err := os.Stat(path)
		if err == nil {
			return path, info, nil
		}
	}
	return "", nil, fmt.Errorf("maintenance_budget: stat index.db: no index.db in %s", dir)
}
