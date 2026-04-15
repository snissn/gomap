package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
)

func runLeafGenerationPlan(dir string, args []string) {
	fs := flag.NewFlagSet("leafgen-plan", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of human-readable text")
	force := fs.Bool("force", false, "Bypass age and reclaim thresholds")
	minAgeCommits := fs.Uint64("min-age-commits", 0, "Minimum published age in commits for candidate eligibility")
	minCandidateGenerations := fs.Int("min-candidate-generations", 0, "Require at least this many candidate generations unless -force")
	minExpectedReclaimBytes := fs.Int64("min-expected-reclaim-bytes", 0, "Require at least this many reclaimable bytes unless -force")
	minExpectedReclaimRatioPPM := fs.Int("min-expected-reclaim-ratio-ppm", 0, "Require at least this reclaim ratio in ppm unless -force")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)

	plan, err := db.LeafGenerationPlan(context.Background(), treedb.LeafGenerationPlanOptions{
		MinPublishedAgeCommits:     *minAgeCommits,
		MinCandidateGenerations:    *minCandidateGenerations,
		MinExpectedReclaimBytes:    *minExpectedReclaimBytes,
		MinExpectedReclaimRatioPPM: *minExpectedReclaimRatioPPM,
		Force:                      *force,
	})
	if err != nil {
		fatalf("LeafGenerationPlan error: %v", err)
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(plan); err != nil {
			fatalf("encode plan json: %v", err)
		}
		return
	}

	fmt.Printf(
		"leafgen-plan: admission=%s current_commit=%d current_generation=%d candidates=%d expected_reclaim_bytes=%d expected_reclaim_ratio_ppm=%d\n",
		plan.Admission,
		plan.CurrentCommitSeq,
		plan.CurrentGenerationID,
		len(plan.Candidates),
		plan.ExpectedReclaimBytes,
		plan.ExpectedReclaimRatioPPM,
	)
	for _, gen := range plan.Generations {
		skip := gen.SkipReason
		if skip == "" {
			skip = "-"
		}
		fmt.Printf(
			"leafgen-generation: id=%d state=%s files=%d bytes_total=%d bytes_live=%d bytes_dead=%d live_pages=%d age_commits=%d pinned=%d dead_ratio_ppm=%d live_ratio_ppm=%d eligible=%t skip=%s\n",
			gen.GenerationID,
			gen.State,
			gen.FileCount,
			gen.BytesTotal,
			gen.BytesLive,
			gen.BytesDead,
			gen.LivePages,
			gen.AgeCommits,
			gen.PinnedCount,
			gen.DeadRatioPPM,
			gen.LiveRatioPPM,
			gen.Eligible,
			skip,
		)
	}
}

func runLeafGenerationPack(dir string, args []string) {
	fs := flag.NewFlagSet("leafgen-pack", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of human-readable text")
	generationIDsCSV := fs.String("generation-ids", "", "Comma-separated sealed generation IDs to pack (required)")
	sync := fs.Bool("sync", true, "Sync packed leaf output before publish")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("leafgen-pack requires -rw")
	}
	generationIDs, err := parseUint64CSV(*generationIDsCSV)
	if err != nil {
		fatalf("invalid -generation-ids: %v", err)
	}
	if len(generationIDs) == 0 {
		fatalf("leafgen-pack requires at least one generation id")
	}

	db := openTreeDB(dir, true)
	defer closeTreeDB(db)

	stats, err := db.LeafGenerationPack(context.Background(), treedb.LeafGenerationPackOptions{
		GenerationIDs: generationIDs,
		Sync:          *sync,
	})
	if err != nil {
		fatalf("LeafGenerationPack error: %v", err)
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(stats); err != nil {
			fatalf("encode pack json: %v", err)
		}
		return
	}
	created := append([]uint32(nil), stats.CreatedFileIDs...)
	sort.Slice(created, func(i, j int) bool { return created[i] < created[j] })
	fmt.Printf(
		"leafgen-pack: requested=%d matched=%d source_files=%d leaf_pages_copied=%d bytes_copied=%d created_file_ids=%s\n",
		stats.GenerationsRequested,
		stats.GenerationsMatched,
		stats.SourceFilesRequested,
		stats.LeafPagesCopied,
		stats.BytesCopied,
		formatUint32List(created),
	)
}

func runLeafGenerationGC(dir string, args []string) {
	fs := flag.NewFlagSet("leafgen-gc", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of human-readable text")
	dryRun := fs.Bool("dry-run", false, "Report eligible generations without deleting them")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("leafgen-gc requires -rw")
	}

	db := openTreeDB(dir, true)
	defer closeTreeDB(db)

	stats, err := db.LeafGenerationGC(context.Background(), treedb.LeafGenerationGCOptions{DryRun: *dryRun})
	if err != nil {
		fatalf("LeafGenerationGC error: %v", err)
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(stats); err != nil {
			fatalf("encode gc json: %v", err)
		}
		return
	}
	mode := "apply"
	if *dryRun {
		mode = "dry-run"
	}
	fmt.Printf(
		"leafgen-gc: mode=%s generations_total=%d writable=%d live=%d retiring=%d eligible=%d deleted=%d files_deleted=%d\n",
		mode,
		stats.GenerationsTotal,
		stats.GenerationsWritable,
		stats.GenerationsLive,
		stats.GenerationsRetiring,
		stats.GenerationsEligible,
		stats.GenerationsDeleted,
		stats.FilesDeleted,
	)
}

func parseUint64CSV(raw string) ([]uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]uint64, 0, len(parts))
	seen := make(map[uint64]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, err
		}
		if v == 0 {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func formatUint32List(ids []uint32) string {
	if len(ids) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, strconv.FormatUint(uint64(id), 10))
	}
	return strings.Join(parts, ",")
}
