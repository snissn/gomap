package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func runLaneProbeSuite(baseCfg BenchConfig) (string, error) {
	cfg := baseCfg
	cfg.Progress = false
	cfg.KeepDir = true
	cfg.DBsArg = "treedb"
	cfg.DBsExcludeArg = ""
	cfg.TestsArg = "batch_write"

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

	const testName = "batch_write"
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

	if err := suiteCleanupDirs(run.Instances); err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("# unified_bench suite: lanes_probe\n\n")
	sb.WriteString(fmt.Sprintf("- lanes requested: %d\n", *treedbJournalLanes))
	sb.WriteString(fmt.Sprintf("- keys: %s\n", formatInt(run.Config.Keys)))
	sb.WriteString(fmt.Sprintf("- valsize: %d\n", run.Config.ValueSize))
	sb.WriteString(fmt.Sprintf("- batchsize: %d\n", run.Config.BatchSize))
	sb.WriteString(fmt.Sprintf("- ops/sec: %s\n", formatFloat(ops)))
	sb.WriteString(fmt.Sprintf("- wall time: %s\n", wall.Truncate(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- index.db bytes: %s\n", formatFloat(float64(indexBytes))))
	sb.WriteString(fmt.Sprintf("- wal bytes: %s\n", formatFloat(float64(walBytes))))
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
