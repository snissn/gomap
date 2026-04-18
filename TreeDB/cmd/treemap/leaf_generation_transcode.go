package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

type leafGenerationTranscodeDiskUsage struct {
	ApplicationDBBytes int64 `json:"application_db_bytes"`
	MainDBBytes        int64 `json:"maindb_bytes"`
	IndexDBBytes       int64 `json:"index_db_bytes"`
	LeafVLogBytes      int64 `json:"leaf_vlog_bytes"`
	ValueVLogBytes     int64 `json:"value_vlog_bytes"`
	DictDBBytes        int64 `json:"dictdb_bytes"`
}

type leafGenerationTranscodeBenchGCStats struct {
	GenerationsEligible int   `json:"generations_eligible"`
	GenerationsDeleted  int   `json:"generations_deleted"`
	FilesDeleted        int   `json:"files_deleted"`
	BytesDeleted        int64 `json:"bytes_deleted"`
}

type leafGenerationTranscodeBenchOptions struct {
	Sync                     bool   `json:"sync"`
	Force                    bool   `json:"force"`
	MinPublishedAgeCommits   uint64 `json:"min_published_age_commits"`
	MinCandidateGenerations  int    `json:"min_candidate_generations"`
	MinExpectedSavedBytes    int64  `json:"min_expected_saved_bytes"`
	MinExpectedSavedRatioPPM int    `json:"min_expected_saved_ratio_ppm"`
	MinSavedPerByteCopiedPPM int    `json:"min_saved_per_byte_copied_ppm"`
	MaxGenerations           int    `json:"max_generations"`
	MaxBytesToCopy           int64  `json:"max_bytes_to_copy"`
	SamplePagesPerGeneration int    `json:"sample_pages_per_generation"`
}

type leafGenerationTranscodeBenchIteration struct {
	Pass                 int                                     `json:"pass"`
	StartedAtUTC         string                                  `json:"started_at_utc"`
	ElapsedMilliseconds  int64                                   `json:"elapsed_ms"`
	Before               leafGenerationTranscodeDiskUsage        `json:"before"`
	After                leafGenerationTranscodeDiskUsage        `json:"after"`
	PlanAdmission        string                                  `json:"plan_admission"`
	PlanCandidates       int                                     `json:"plan_candidates"`
	PlanExpectedSaved    int64                                   `json:"plan_expected_saved_bytes"`
	PlanExpectedSavedPPM int                                     `json:"plan_expected_saved_per_copy_ppm"`
	Ran                  bool                                    `json:"ran"`
	SkipReason           string                                  `json:"skip_reason,omitempty"`
	Selection            treedb.LeafGenerationTranscodeSelection `json:"selection"`
	Pack                 treedb.LeafGenerationPackStats          `json:"pack"`
	GC                   leafGenerationTranscodeBenchGCStats     `json:"gc"`
}

type leafGenerationTranscodeBenchSummary struct {
	Dir                 string                                  `json:"dir"`
	StartedAtUTC        string                                  `json:"started_at_utc"`
	ElapsedMilliseconds int64                                   `json:"elapsed_ms"`
	Before              leafGenerationTranscodeDiskUsage        `json:"before"`
	After               leafGenerationTranscodeDiskUsage        `json:"after"`
	StopReason          string                                  `json:"stop_reason"`
	Options             leafGenerationTranscodeBenchOptions     `json:"options"`
	Iterations          []leafGenerationTranscodeBenchIteration `json:"iterations"`
}

func runLeafGenerationTranscodePlan(dir string, args []string) {
	fs := flag.NewFlagSet("leafgen-transcode-plan", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of human-readable text")
	force := fs.Bool("force", false, "Bypass saved-bytes thresholds")
	minAgeCommits := fs.Uint64("min-age-commits", 0, "Minimum published age in commits for generation eligibility")
	minCandidateGenerations := fs.Int("min-candidate-generations", 0, "Require at least this many candidate generations unless -force")
	minExpectedSavedBytes := fs.Int64("min-expected-saved-bytes", 0, "Require at least this many estimated saved bytes unless -force")
	minExpectedSavedRatioPPM := fs.Int("min-expected-saved-ratio-ppm", 0, "Require at least this estimated saved ratio in ppm unless -force")
	minSavedPerByteCopiedPPM := fs.Int("min-saved-per-byte-copied-ppm", 10000, "Require at least this many estimated saved bytes per copied byte, in ppm, unless -force")
	samplePagesPerGeneration := fs.Int("sample-pages-per-generation", 64, "Maximum sampled live pages per generation")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("leafgen-transcode-plan requires -rw")
	}
	db := openTreeDB(dir, true)
	defer closeTreeDB(db)

	plan, err := db.LeafGenerationTranscodePlan(context.Background(), treedb.LeafGenerationTranscodeOptions{
		Force:                    *force,
		MinPublishedAgeCommits:   *minAgeCommits,
		MinCandidateGenerations:  *minCandidateGenerations,
		MinExpectedSavedBytes:    *minExpectedSavedBytes,
		MinExpectedSavedRatioPPM: *minExpectedSavedRatioPPM,
		MinSavedPerByteCopiedPPM: *minSavedPerByteCopiedPPM,
		SamplePagesPerGeneration: *samplePagesPerGeneration,
	})
	if err != nil {
		fatalf("LeafGenerationTranscodePlan error: %v", err)
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(plan); err != nil {
			fatalf("encode transcode plan json: %v", err)
		}
		return
	}
	fmt.Printf(
		"leafgen-transcode-plan: admission=%s current_commit=%d current_generation=%d candidates=%d estimated_saved_bytes=%d estimated_saved_per_copy_ppm=%d dict_id=%d raw_pages=%t\n",
		plan.Admission,
		plan.CurrentCommitSeq,
		plan.CurrentGenerationID,
		len(plan.Candidates),
		plan.ExpectedBytesSaved,
		plan.ExpectedBytesSavedPerByteCopiedPPM,
		plan.LeafDictID,
		plan.LeafDictUseRawPages,
	)
	for _, gen := range plan.Generations {
		skip := gen.SkipReason
		if skip == "" {
			skip = "-"
		}
		fmt.Printf(
			"leafgen-transcode-generation: id=%d state=%s bytes_total=%d bytes_live=%d bytes_dead=%d bytes_to_copy=%d est_after=%d est_saved=%d est_saved_per_copy_ppm=%d sample_pages=%d age_commits=%d pinned=%d eligible=%t skip=%s\n",
			gen.GenerationID,
			gen.State,
			gen.BytesTotal,
			gen.BytesLive,
			gen.BytesDead,
			gen.BytesToCopy,
			gen.EstimatedBytesAfter,
			gen.ExpectedBytesSaved,
			gen.ExpectedSavedPerByteCopiedPPM,
			gen.SamplePages,
			gen.AgeCommits,
			gen.PinnedCount,
			gen.Eligible,
			skip,
		)
	}
}

func runLeafGenerationTranscodeBench(dir string, args []string) {
	fs := flag.NewFlagSet("leafgen-transcode-bench", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of human-readable text")
	sync := fs.Bool("sync", false, "Sync each transcode pass before returning")
	force := fs.Bool("force", false, "Bypass saved-bytes thresholds")
	minAgeCommits := fs.Uint64("min-age-commits", 0, "Minimum published age in commits for generation eligibility")
	minCandidateGenerations := fs.Int("min-candidate-generations", 0, "Require at least this many candidate generations unless -force")
	minExpectedSavedBytes := fs.Int64("min-expected-saved-bytes", 0, "Require at least this many estimated saved bytes unless -force")
	minExpectedSavedRatioPPM := fs.Int("min-expected-saved-ratio-ppm", 0, "Require at least this estimated saved ratio in ppm unless -force")
	minSavedPerByteCopiedPPM := fs.Int("min-saved-per-byte-copied-ppm", 10000, "Require at least this many estimated saved bytes per copied byte, in ppm, unless -force")
	samplePagesPerGeneration := fs.Int("sample-pages-per-generation", 64, "Maximum sampled live pages per generation")
	maxGenerationsPerPass := fs.Int("max-generations-per-pass", 64, "Maximum generations to transcode in a single pass")
	maxBytesToCopyPerPass := fs.Int64("max-bytes-to-copy-per-pass", 2<<30, "Maximum bytes_to_copy for a single transcode pass")
	maxPasses := fs.Int("max-passes", 0, "Maximum transcode passes to run (0=until stop)")
	maxSeconds := fs.Int("max-seconds", 600, "Wall-clock time limit in seconds (0=unbounded)")
	gcEveryPass := fs.Bool("gc-every-pass", true, "Run leaf-generation GC after each successful transcode pass")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("leafgen-transcode-bench requires -rw")
	}
	opts := treedb.LeafGenerationTranscodeOptions{
		Sync:                     *sync,
		Force:                    *force,
		MinPublishedAgeCommits:   *minAgeCommits,
		MinCandidateGenerations:  *minCandidateGenerations,
		MinExpectedSavedBytes:    *minExpectedSavedBytes,
		MinExpectedSavedRatioPPM: *minExpectedSavedRatioPPM,
		MinSavedPerByteCopiedPPM: *minSavedPerByteCopiedPPM,
		MaxGenerations:           *maxGenerationsPerPass,
		MaxBytesToCopy:           *maxBytesToCopyPerPass,
		SamplePagesPerGeneration: *samplePagesPerGeneration,
	}

	started := time.Now()
	rootDir := resolveTreeDBRootDir(dir)
	summary := leafGenerationTranscodeBenchSummary{
		Dir:          rootDir,
		StartedAtUTC: started.UTC().Format(time.RFC3339),
		Before:       mustLeafGenerationTranscodeDiskUsage(rootDir),
		Options: leafGenerationTranscodeBenchOptions{
			Sync:                     opts.Sync,
			Force:                    opts.Force,
			MinPublishedAgeCommits:   opts.MinPublishedAgeCommits,
			MinCandidateGenerations:  opts.MinCandidateGenerations,
			MinExpectedSavedBytes:    opts.MinExpectedSavedBytes,
			MinExpectedSavedRatioPPM: opts.MinExpectedSavedRatioPPM,
			MinSavedPerByteCopiedPPM: opts.MinSavedPerByteCopiedPPM,
			MaxGenerations:           opts.MaxGenerations,
			MaxBytesToCopy:           opts.MaxBytesToCopy,
			SamplePagesPerGeneration: opts.SamplePagesPerGeneration,
		},
	}
	deadline := time.Time{}
	if *maxSeconds > 0 {
		deadline = started.Add(time.Duration(*maxSeconds) * time.Second)
	}

	for pass := 1; ; pass++ {
		if *maxPasses > 0 && pass > *maxPasses {
			summary.StopReason = "max_passes"
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			summary.StopReason = "time_limit"
			break
		}
		iterStarted := time.Now()
		before := mustLeafGenerationTranscodeDiskUsage(rootDir)
		db := openTreeDB(dir, true)
		runStats, err := db.LeafGenerationTranscodeRunOnce(context.Background(), opts)
		if err != nil {
			closeTreeDB(db)
			fatalf("LeafGenerationTranscodeRunOnce pass=%d: %v", pass, err)
		}
		iter := leafGenerationTranscodeBenchIteration{
			Pass:                 pass,
			StartedAtUTC:         iterStarted.UTC().Format(time.RFC3339),
			Before:               before,
			PlanAdmission:        runStats.Plan.Admission,
			PlanCandidates:       len(runStats.Plan.Candidates),
			PlanExpectedSaved:    runStats.Plan.ExpectedBytesSaved,
			PlanExpectedSavedPPM: runStats.Plan.ExpectedBytesSavedPerByteCopiedPPM,
			Ran:                  runStats.Ran,
			SkipReason:           runStats.SkipReason,
			Selection:            runStats.Selection,
			Pack:                 runStats.Pack,
		}
		if runStats.Ran && *gcEveryPass {
			gcStats, err := db.LeafGenerationGC(context.Background(), treedb.LeafGenerationGCOptions{})
			if err != nil {
				closeTreeDB(db)
				fatalf("LeafGenerationGC pass=%d: %v", pass, err)
			}
			iter.GC = leafGenerationTranscodeBenchGCStats{
				GenerationsEligible: gcStats.GenerationsEligible,
				GenerationsDeleted:  gcStats.GenerationsDeleted,
				FilesDeleted:        gcStats.FilesDeleted,
				BytesDeleted:        gcStats.BytesDeleted,
			}
		}
		closeTreeDB(db)
		iter.After = mustLeafGenerationTranscodeDiskUsage(rootDir)
		iter.ElapsedMilliseconds = time.Since(iterStarted).Milliseconds()
		summary.Iterations = append(summary.Iterations, iter)
		if !runStats.Ran {
			if iter.SkipReason == "" {
				summary.StopReason = "no_run"
			} else {
				summary.StopReason = iter.SkipReason
			}
			break
		}
	}
	summary.After = mustLeafGenerationTranscodeDiskUsage(rootDir)
	summary.ElapsedMilliseconds = time.Since(started).Milliseconds()

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(summary); err != nil {
			fatalf("encode transcode bench json: %v", err)
		}
		return
	}

	fmt.Printf(
		"leafgen-transcode-bench: passes=%d stop=%s app_before=%d app_after=%d leaf_before=%d leaf_after=%d value_before=%d value_after=%d elapsed_ms=%d\n",
		len(summary.Iterations),
		summary.StopReason,
		summary.Before.ApplicationDBBytes,
		summary.After.ApplicationDBBytes,
		summary.Before.LeafVLogBytes,
		summary.After.LeafVLogBytes,
		summary.Before.ValueVLogBytes,
		summary.After.ValueVLogBytes,
		summary.ElapsedMilliseconds,
	)
	for _, iter := range summary.Iterations {
		fmt.Printf(
			"leafgen-transcode-pass: pass=%d ran=%t admission=%s candidates=%d saved_bytes=%d saved_per_copy_ppm=%d selected_generations=%d selected_bytes_to_copy=%d copied_bytes=%d gc_deleted_generations=%d gc_deleted_bytes=%d app_before=%d app_after=%d leaf_before=%d leaf_after=%d value_before=%d value_after=%d elapsed_ms=%d skip=%s\n",
			iter.Pass,
			iter.Ran,
			iter.PlanAdmission,
			iter.PlanCandidates,
			iter.PlanExpectedSaved,
			iter.PlanExpectedSavedPPM,
			len(iter.Selection.GenerationIDs),
			iter.Selection.BytesToCopy,
			iter.Pack.BytesCopied,
			iter.GC.GenerationsDeleted,
			iter.GC.BytesDeleted,
			iter.Before.ApplicationDBBytes,
			iter.After.ApplicationDBBytes,
			iter.Before.LeafVLogBytes,
			iter.After.LeafVLogBytes,
			iter.Before.ValueVLogBytes,
			iter.After.ValueVLogBytes,
			iter.ElapsedMilliseconds,
			iter.SkipReason,
		)
	}
}

func mustLeafGenerationTranscodeDiskUsage(rootDir string) leafGenerationTranscodeDiskUsage {
	usage, err := leafGenerationTranscodeDiskUsageForRoot(rootDir)
	if err != nil {
		fatalf("disk usage: %v", err)
	}
	return usage
}

func leafGenerationTranscodeDiskUsageForRoot(rootDir string) (leafGenerationTranscodeDiskUsage, error) {
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
		n, err := recursivePathSize(path)
		if err != nil {
			return leafGenerationTranscodeDiskUsage{}, err
		}
		sizes[key] = n
	}
	return leafGenerationTranscodeDiskUsage{
		ApplicationDBBytes: sizes["application"],
		MainDBBytes:        sizes["maindb"],
		IndexDBBytes:       sizes["index"],
		LeafVLogBytes:      sizes["leaf"],
		ValueVLogBytes:     sizes["value"],
		DictDBBytes:        sizes["dict"],
	}, nil
}

func recursivePathSize(path string) (int64, error) {
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
	err = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}
