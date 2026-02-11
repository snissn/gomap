package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

func runLaneProbeSuite(baseCfg BenchConfig) (string, error) {
	cfg := baseCfg
	cfg.Progress = false
	cfg.KeepDir = true
	cfg.DBsArg = "treedb"
	cfg.DBsExcludeArg = ""
	cfg.TestsArg = "random_write_parallel"
	if cfg.WriteWorkers <= 1 {
		// Ensure lane probes stress concurrent writers by default.
		w := runtime.GOMAXPROCS(0)
		if w < 2 {
			w = 2
		}
		if w > 16 {
			w = 16
		}
		cfg.WriteWorkers = w
	}

	start := time.Now()
	run, err := runBenchmark(cfg)
	if err != nil {
		return "", err
	}
	wall := time.Since(start)

	inst, err := findSuiteInstance(run.Instances, "treedb")
	if err != nil {
		return "", err
	}

	const testName = "random_write_parallel"
	dbName := inst.Wrapper.Name()
	opsByDB, ok := run.Results[testName]
	if !ok {
		return "", fmt.Errorf("lanes_probe: missing results for %s", testName)
	}
	ops, ok := opsByDB[dbName]
	if !ok {
		return "", fmt.Errorf("lanes_probe: missing results for %s/%s", testName, dbName)
	}

	indexBytes, err := fileSize(filepath.Join(inst.Dir, "maindb", "index.db"))
	if err != nil {
		return "", fmt.Errorf("lanes_probe: index.db size: %w", err)
	}
	walBytes, err := dirSize(filepath.Join(inst.Dir, "maindb", "wal"))
	if err != nil {
		return "", fmt.Errorf("lanes_probe: wal size: %w", err)
	}
	stats := run.TreeDBStats[dbName]

	if err := suiteCleanupDirs(run.Instances); err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: lanes_probe\n\n")
	sb.WriteString(fmt.Sprintf("- lanes requested: %d\n", *treedbJournalLanes))
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(run.Config.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", run.Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- write-workers: %d\n", run.Config.WriteWorkers))
	sb.WriteString(fmt.Sprintf("- ops/sec: %s\n", formatFloat(ops)))
	sb.WriteString(fmt.Sprintf("- wall time: %s\n", wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- index.db bytes: %s\n", formatFloat(float64(indexBytes))))
	sb.WriteString(fmt.Sprintf("- wal bytes: %s\n", formatFloat(float64(walBytes))))
	if lagP99, ok := parseStatFloat(stats, "treedb.cache.vlog_queue.lag_p99_ms"); ok {
		sb.WriteString(fmt.Sprintf("- queue lag p99: %.3fms\n", lagP99))
	}
	if lagP999, ok := parseStatFloat(stats, "treedb.cache.vlog_queue.lag_p999_ms"); ok {
		sb.WriteString(fmt.Sprintf("- queue lag p999: %.3fms\n", lagP999))
	}
	sb.WriteString(fmt.Sprintf("- queue enqueued total: %s\n", formatInt(int(parseUint(stats, "treedb.cache.vlog_queue.enqueued_total")))))
	sb.WriteString(fmt.Sprintf("- queue depth max: %s\n", formatInt(int(parseUint(stats, "treedb.cache.vlog_queue.depth_max")))))

	laneDepthKeys := make([]string, 0)
	for k := range stats {
		if strings.HasPrefix(k, "treedb.cache.vlog_queue.lane.") && strings.HasSuffix(k, ".depth_max") {
			laneDepthKeys = append(laneDepthKeys, k)
		}
	}
	sort.Strings(laneDepthKeys)
	if len(laneDepthKeys) > 0 {
		sb.WriteString("- lane queue depth max:\n")
		for _, k := range laneDepthKeys {
			sb.WriteString(fmt.Sprintf("  - %s = %s\n", k, stats[k]))
		}
	}
	sb.WriteString("\n")

	return sb.String(), nil
}

func findSuiteInstance(instances []*DBInstance, name string) (*DBInstance, error) {
	for _, inst := range instances {
		if inst != nil && inst.Name == name {
			return inst, nil
		}
	}
	return nil, fmt.Errorf("suite: %s instance not found", name)
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if info.IsDir() {
		return 0, fmt.Errorf("expected file, found dir: %s", path)
	}
	return info.Size(), nil
}

func dirSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("expected dir, found file: %s", path)
	}

	var size int64
	walkErr := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
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
		if info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	})
	if walkErr != nil {
		return 0, walkErr
	}
	return size, nil
}
