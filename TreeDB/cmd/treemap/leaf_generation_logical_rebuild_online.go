package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

type leafGenerationLogicalRebuildRunSummary struct {
	Dir                 string                                          `json:"dir"`
	StartedAtUTC        string                                          `json:"started_at_utc"`
	ElapsedMilliseconds int64                                           `json:"elapsed_ms"`
	Before              leafGenerationLogicalRebuildDiskUsage           `json:"before"`
	After               leafGenerationLogicalRebuildDiskUsage           `json:"after"`
	Stats               treedb.LeafGenerationLogicalRebuildRunOnceStats `json:"stats"`
}

func runLeafGenerationLogicalRebuildRun(dir string, args []string) {
	fs := flag.NewFlagSet("leafgen-logical-rebuild-run", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of human-readable text")
	rawFileID := fs.Uint("raw-file-id", 0, "Optional raw sealed leaf file ID to rebuild (0=auto-select)")
	maxPublishedCommitSeq := fs.Uint64("max-published-commit-seq", 0, "Optional maximum published commit sequence to consider (0=no limit)")
	syncOut := fs.Bool("sync", true, "Sync created leaf segments before publishing")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("leafgen-logical-rebuild-run requires -rw")
	}

	rootDir := resolveTreeDBRootDir(dir)
	before, err := leafGenerationLogicalRebuildDiskUsageForRoot(rootDir)
	if err != nil {
		fatalf("disk usage before logical rebuild run: %v", err)
	}
	db := openTreeDB(dir, true)
	defer closeTreeDB(db)

	started := time.Now()
	stats, err := db.LeafGenerationLogicalRebuildRunOnce(context.Background(), treedb.LeafGenerationLogicalRebuildRunOnceOptions{
		RawFileID:             uint32(*rawFileID),
		MaxPublishedCommitSeq: *maxPublishedCommitSeq,
		Sync:                  *syncOut,
	})
	if err != nil {
		fatalf("LeafGenerationLogicalRebuildRunOnce error: %v", err)
	}
	after, err := leafGenerationLogicalRebuildDiskUsageForRoot(rootDir)
	if err != nil {
		fatalf("disk usage after logical rebuild run: %v", err)
	}

	summary := leafGenerationLogicalRebuildRunSummary{
		Dir:                 rootDir,
		StartedAtUTC:        started.UTC().Format(time.RFC3339),
		ElapsedMilliseconds: time.Since(started).Milliseconds(),
		Before:              before,
		After:               after,
		Stats:               stats,
	}

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(summary); err != nil {
			fatalf("encode logical rebuild run json: %v", err)
		}
		return
	}

	fmt.Printf(
		"leafgen-logical-rebuild-run: raw_file=%d generation=%d leaf_before=%d leaf_after=%d app_before=%d app_after=%d runs=%d source_pages=%d replacement_pages=%d created=%v retired=%v catchup_passes=%d catchup_keys=%d cutover_keys=%d elapsed_ms=%d\n",
		stats.SelectedRawFileID,
		stats.SelectedGenerationID,
		summary.Before.LeafVLogBytes,
		summary.After.LeafVLogBytes,
		summary.Before.ApplicationDBBytes,
		summary.After.ApplicationDBBytes,
		stats.SelectedRunCount,
		stats.SourceLeafPages,
		stats.ReplacementLeafPages,
		stats.CreatedFileIDs,
		stats.RetiredGenerationIDs,
		stats.CatchupPasses,
		stats.CatchupKeys,
		stats.FinalCutoverKeys,
		summary.ElapsedMilliseconds,
	)
}
