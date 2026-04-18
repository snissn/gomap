package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

type leafGenerationLogicalRebuildDiskUsage struct {
	ApplicationDBBytes int64 `json:"application_db_bytes"`
	MainDBBytes        int64 `json:"maindb_bytes"`
	IndexDBBytes       int64 `json:"index_db_bytes"`
	LeafVLogBytes      int64 `json:"leaf_vlog_bytes"`
	ValueVLogBytes     int64 `json:"value_vlog_bytes"`
	DictDBBytes        int64 `json:"dictdb_bytes"`
}

type leafGenerationLogicalRebuildBenchSummary struct {
	Dir                 string                                   `json:"dir"`
	StartedAtUTC        string                                   `json:"started_at_utc"`
	ElapsedMilliseconds int64                                    `json:"elapsed_ms"`
	Before              leafGenerationLogicalRebuildDiskUsage    `json:"before"`
	After               leafGenerationLogicalRebuildDiskUsage    `json:"after"`
	LeafFloorBytes      int64                                    `json:"leaf_floor_bytes,omitempty"`
	LeafGapBeforeBytes  int64                                    `json:"leaf_gap_before_bytes,omitempty"`
	LeafGapAfterBytes   int64                                    `json:"leaf_gap_after_bytes,omitempty"`
	Stats               treedb.LeafGenerationLogicalRebuildStats `json:"stats"`
}

func runLeafGenerationLogicalRebuildBench(dir string, args []string) {
	fs := flag.NewFlagSet("leafgen-logical-rebuild-bench", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of human-readable text")
	leafFloorBytes := fs.Int64("leaf-floor-bytes", 0, "Optional offline-rewrite leaf_vlog byte floor for leaf-gap reporting")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("leafgen-logical-rebuild-bench requires -rw")
	}

	rootDir := resolveTreeDBRootDir(dir)
	before, err := leafGenerationLogicalRebuildDiskUsageForRoot(rootDir)
	if err != nil {
		fatalf("disk usage before rebuild: %v", err)
	}
	started := time.Now()
	stats, err := treedb.LeafGenerationLogicalRebuildOffline(treedb.Options{Dir: dir})
	if err != nil {
		fatalf("LeafGenerationLogicalRebuildOffline error: %v", err)
	}
	after, err := leafGenerationLogicalRebuildDiskUsageForRoot(rootDir)
	if err != nil {
		fatalf("disk usage after rebuild: %v", err)
	}

	summary := leafGenerationLogicalRebuildBenchSummary{
		Dir:                 rootDir,
		StartedAtUTC:        started.UTC().Format(time.RFC3339),
		ElapsedMilliseconds: time.Since(started).Milliseconds(),
		Before:              before,
		After:               after,
		LeafFloorBytes:      *leafFloorBytes,
		Stats:               stats,
	}
	if *leafFloorBytes > 0 {
		summary.LeafGapBeforeBytes = before.LeafVLogBytes - *leafFloorBytes
		summary.LeafGapAfterBytes = after.LeafVLogBytes - *leafFloorBytes
	}

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(summary); err != nil {
			fatalf("encode logical rebuild json: %v", err)
		}
		return
	}

	fmt.Printf(
		"leafgen-logical-rebuild-bench: app_before=%d app_after=%d leaf_before=%d leaf_after=%d index_before=%d index_after=%d elapsed_ms=%d dict_id=%d raw_pages=%t created_leaf_files=%v\n",
		summary.Before.ApplicationDBBytes,
		summary.After.ApplicationDBBytes,
		summary.Before.LeafVLogBytes,
		summary.After.LeafVLogBytes,
		summary.Before.IndexDBBytes,
		summary.After.IndexDBBytes,
		summary.ElapsedMilliseconds,
		stats.LeafDictID,
		stats.LeafDictUseRawPages,
		stats.CreatedLeafFileIDs,
	)
	if *leafFloorBytes > 0 {
		fmt.Printf(
			"leafgen-logical-rebuild-gap: floor=%d gap_before=%d gap_after=%d gap_closed=%d\n",
			*leafFloorBytes,
			summary.LeafGapBeforeBytes,
			summary.LeafGapAfterBytes,
			summary.LeafGapBeforeBytes-summary.LeafGapAfterBytes,
		)
	}
}

func leafGenerationLogicalRebuildDiskUsageForRoot(rootDir string) (leafGenerationLogicalRebuildDiskUsage, error) {
	rootDir = resolveTreeDBRootDir(rootDir)
	paths := map[string]string{
		"application": rootDir,
		"maindb":      filepath.Join(rootDir, "maindb"),
		"index":       filepath.Join(rootDir, "maindb", "index.db"),
		"leaf":        filepath.Join(rootDir, "maindb", "leaf_vlog"),
		"value":       filepath.Join(rootDir, "maindb", "value_vlog"),
		"dict":        filepath.Join(rootDir, "dictdb"),
	}
	sizes := make(map[string]int64, len(paths))
	for key, path := range paths {
		n, err := logicalRebuildRecursivePathSize(path)
		if err != nil {
			return leafGenerationLogicalRebuildDiskUsage{}, err
		}
		sizes[key] = n
	}
	return leafGenerationLogicalRebuildDiskUsage{
		ApplicationDBBytes: sizes["application"],
		MainDBBytes:        sizes["maindb"],
		IndexDBBytes:       sizes["index"],
		LeafVLogBytes:      sizes["leaf"],
		ValueVLogBytes:     sizes["value"],
		DictDBBytes:        sizes["dict"],
	}, nil
}

func logicalRebuildRecursivePathSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if !info.IsDir() {
		return info.Size(), nil
	}
	var total int64
	err = filepath.WalkDir(path, func(_ string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
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
	if err != nil {
		return 0, err
	}
	return total, nil
}
