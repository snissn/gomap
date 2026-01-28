package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
	OpsPerCoalesce int
	Checkpoint     time.Duration
	IndexBytes     int64
	Dir            string
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
		var indexBytes int64
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

			info, err := os.Stat(filepath.Join(inst.Dir, "index.db"))
			if err != nil {
				return "", fmt.Errorf("maintenance_budget: stat index.db: %w", err)
			}
			indexBytes = info.Size()

			if !baseCfg.KeepDir {
				_ = os.RemoveAll(inst.Dir)
			}
		}

		results = append(results, maintenanceSweepResult{
			OpsPerCoalesce: k,
			Checkpoint:     checkpoint,
			IndexBytes:     indexBytes,
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

	return sb.String(), nil
}
