package main

import (
	"fmt"
	"strings"
)

func runFlushDrainSuite(baseCfg BenchConfig) (string, error) {
	// Purpose: measure a "write burst → durable boundary" drain time in a way
	// that's easy to reproduce and compare across changes.
	//
	// Implementation note: we use -checkpoint-between-tests so the suite records
	// checkpoint durations between phases (especially between random_write and
	// random_read).
	cfg := baseCfg
	cfg.Progress = false
	cfg.DBsArg = "treedb"
	cfg.TestsArg = "random_write,random_read"
	cfg.TreeDBCacheStatsBeforeReads = true
	cfg.CheckpointBetweenTests = true

	// If the caller didn't specify -keys (default is 100k), use a larger default
	// so the checkpoint has meaningful flush work to do.
	if cfg.Keys == 100_000 {
		cfg.Keys = 900_000
	}

	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}

	diag, diagErr := suiteTreeDBCacheStats(run.Instances)
	if diagErr != nil {
		return "", diagErr
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: flushdrain\n\n")
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(run.Config.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", run.Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", run.Config.BatchSize))
	sb.WriteString("- tests: random_write, random_read\n")
	sb.WriteString("- checkpoint-between-tests: true\n\n")

	sb.WriteString(renderMarkdownSingle(run))

	if diag != "" {
		sb.WriteString("\n## TreeDB cache stats (post-run)\n\n")
		sb.WriteString("```text\n")
		sb.WriteString(diag)
		sb.WriteString("```\n")
	}

	if *flushdrainCheckpointMax > 0 {
		if chk, ok := run.CheckpointDurations["random_read"]; ok {
			for dbName, dur := range chk {
				if dur > *flushdrainCheckpointMax {
					return "", fmt.Errorf("flushdrain checkpoint before random_read (%s) = %s > %s", dbName, dur, *flushdrainCheckpointMax)
				}
			}
		}
	}
	return sb.String(), nil
}
