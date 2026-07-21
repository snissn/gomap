package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

const auditSummarySchemaVersion = 1

type auditSummaryCollectOptions struct {
	CompactMode        treedbdb.CompactStorageMode
	FrameStats         bool
	FrameTopLengths    int
	TopSegments        int
	TopGenerations     int
	GzipSamples        int
	GzipSampleMaxBytes int64
}

type auditSummaryReportOptions struct {
	CompactMode        string `json:"compact_mode"`
	FrameStats         bool   `json:"frame_stats"`
	FrameTopLengths    int    `json:"frame_top_lengths"`
	TopSegments        int    `json:"top_segments"`
	TopGenerations     int    `json:"top_generations"`
	GzipSamples        int    `json:"gzip_samples"`
	GzipSampleMaxBytes int64  `json:"gzip_sample_max_bytes"`
}

type auditSummaryReport struct {
	SchemaVersion int                       `json:"schema_version"`
	Command       string                    `json:"command"`
	Dir           string                    `json:"dir"`
	RootDir       string                    `json:"root_dir"`
	MainDir       string                    `json:"main_dir"`
	Options       auditSummaryReportOptions `json:"options"`

	Storage        auditSummaryStorage        `json:"storage"`
	CompactPlan    auditSummaryCompactPlan    `json:"compact_plan"`
	ValueLog       auditSummaryValueLog       `json:"value_log"`
	LeafGeneration auditSummaryLeafGeneration `json:"leaf_generation"`
	LogFamilies    []auditSummaryLogFamily    `json:"log_families"`
}

type auditSummaryStorage struct {
	Domains []auditSummaryStorageDomain `json:"domains"`
}

type auditSummaryStorageDomain struct {
	Name          string `json:"name"`
	Path          string `json:"path"`
	Exists        bool   `json:"exists"`
	Bytes         int64  `json:"bytes"`
	Files         int    `json:"files"`
	ZeroByteFiles int    `json:"zero_byte_files"`
}

type auditSummaryCompactPlan struct {
	Mode                 string                      `json:"mode"`
	DryRun               bool                        `json:"dry_run"`
	FullyCompacted       bool                        `json:"fully_compacted"`
	PolicyFullyCompacted bool                        `json:"policy_fully_compacted"`
	ByteMinimized        bool                        `json:"byte_minimized"`
	StorageBefore        []auditSummaryStorageDomain `json:"storage_before"`
	StorageAfter         []auditSummaryStorageDomain `json:"storage_after"`
	RemainingDebt        auditSummaryDebt            `json:"remaining_debt"`
}

type auditSummaryDebt struct {
	ValueLogRewriteSegments                int    `json:"value_log_rewrite_segments"`
	ValueLogRewriteBytes                   int64  `json:"value_log_rewrite_bytes"`
	ValueLogGCSegments                     int    `json:"value_log_gc_segments"`
	ValueLogGCBytes                        int64  `json:"value_log_gc_bytes"`
	LeafPackGenerations                    int    `json:"leaf_pack_generations"`
	LeafPackBytes                          int64  `json:"leaf_pack_bytes"`
	LeafGCGenerations                      int    `json:"leaf_gc_generations"`
	LeafGCBytes                            int64  `json:"leaf_gc_bytes"`
	ZeroByteValueLogFiles                  int    `json:"zero_byte_value_log_files"`
	IndexVacuumRequired                    bool   `json:"index_vacuum_required"`
	IndexVacuumReason                      string `json:"index_vacuum_reason"`
	IndexVacuumTotalPages                  uint64 `json:"index_vacuum_total_pages"`
	IndexVacuumUserPages                   uint64 `json:"index_vacuum_user_pages"`
	IndexVacuumUserSpan                    uint64 `json:"index_vacuum_user_span"`
	IndexVacuumUserSpanRatioPPM            uint64 `json:"index_vacuum_user_span_ratio_ppm"`
	IndexVacuumFreelistReclaimablePages    uint64 `json:"index_vacuum_freelist_reclaimable_pages"`
	IndexVacuumFreelistReclaimableRatioPPM uint64 `json:"index_vacuum_freelist_reclaimable_ratio_ppm"`
	IndexVacuumCollectionRootPages         uint64 `json:"index_vacuum_collection_root_pages"`
	IndexVacuumCollectionRootSpan          uint64 `json:"index_vacuum_collection_root_span"`
	IndexVacuumCollectionRootSpanRatioPPM  uint64 `json:"index_vacuum_collection_root_span_ratio_ppm"`
}

type auditSummaryValueLog struct {
	RewritePlan auditSummaryValueLogRewritePlan `json:"rewrite_plan"`
	GCDryRun    auditSummaryValueLogGC          `json:"gc_dry_run"`
}

type auditSummaryValueLogRewritePlan struct {
	SegmentsTotal      int      `json:"segments_total"`
	SegmentsSelected   int      `json:"segments_selected"`
	BytesTotal         int64    `json:"bytes_total"`
	BytesLive          int64    `json:"bytes_live"`
	BytesStale         int64    `json:"bytes_stale"`
	SelectedBytesTotal int64    `json:"selected_bytes_total"`
	SelectedBytesLive  int64    `json:"selected_bytes_live"`
	SelectedBytesStale int64    `json:"selected_bytes_stale"`
	AgeBlockedSegments int      `json:"age_blocked_segments"`
	AgeBlockedBytes    int64    `json:"age_blocked_bytes"`
	SourceFileIDs      []uint32 `json:"source_file_ids,omitempty"`
}

type auditSummaryValueLogGC struct {
	SegmentsTotal      int   `json:"segments_total"`
	SegmentsReferenced int   `json:"segments_referenced"`
	SegmentsActive     int   `json:"segments_active"`
	SegmentsProtected  int   `json:"segments_protected"`
	SegmentsEligible   int   `json:"segments_eligible"`
	BytesTotal         int64 `json:"bytes_total"`
	BytesReferenced    int64 `json:"bytes_referenced"`
	BytesActive        int64 `json:"bytes_active"`
	BytesProtected     int64 `json:"bytes_protected"`
	BytesEligible      int64 `json:"bytes_eligible"`
}

type auditSummaryLeafGeneration struct {
	Admission                       string                          `json:"admission"`
	CurrentCommitSeq                uint64                          `json:"current_commit_seq"`
	CurrentGenerationID             uint64                          `json:"current_generation_id"`
	GenerationsTotal                int                             `json:"generations_total"`
	Candidates                      int                             `json:"candidates"`
	CandidateBytesTotal             int64                           `json:"candidate_bytes_total"`
	CandidateBytesLive              int64                           `json:"candidate_bytes_live"`
	CandidateBytesDead              int64                           `json:"candidate_bytes_dead"`
	CandidateBytesToCopy            int64                           `json:"candidate_bytes_to_copy"`
	ExpectedReclaimBytes            int64                           `json:"expected_reclaim_bytes"`
	ExpectedReclaimRatioPPM         int                             `json:"expected_reclaim_ratio_ppm"`
	ExpectedReclaimPerByteCopiedPPM int                             `json:"expected_reclaim_per_byte_copied_ppm"`
	CompactSelectedGenerations      int                             `json:"compact_selected_generations"`
	CompactSelectedReclaimBytes     int64                           `json:"compact_selected_reclaim_bytes"`
	GCDryRun                        auditSummaryLeafGenerationGC    `json:"gc_dry_run"`
	TopGenerationsByDeadBytes       []auditSummaryLeafGenerationRow `json:"top_generations_by_dead_bytes,omitempty"`
}

type auditSummaryLeafGenerationGC struct {
	GenerationsTotal    int   `json:"generations_total"`
	GenerationsWritable int   `json:"generations_writable"`
	GenerationsLive     int   `json:"generations_live"`
	GenerationsRetiring int   `json:"generations_retiring"`
	GenerationsEligible int   `json:"generations_eligible"`
	BytesEligible       int64 `json:"bytes_eligible"`
}

type auditSummaryLeafGenerationRow struct {
	GenerationID              uint64 `json:"generation_id"`
	State                     string `json:"state"`
	FileCount                 int    `json:"file_count"`
	BytesTotal                int64  `json:"bytes_total"`
	BytesLive                 int64  `json:"bytes_live"`
	BytesDead                 int64  `json:"bytes_dead"`
	BytesToCopy               int64  `json:"bytes_to_copy"`
	LivePages                 int    `json:"live_pages"`
	AgeCommits                uint64 `json:"age_commits"`
	PinnedCount               uint64 `json:"pinned_count"`
	DeadRatioPPM              int    `json:"dead_ratio_ppm"`
	LiveRatioPPM              int    `json:"live_ratio_ppm"`
	WholeGenerationGCEligible bool   `json:"whole_generation_gc_eligible"`
	Eligible                  bool   `json:"eligible"`
	SkipReason                string `json:"skip_reason,omitempty"`
}

type auditSummaryLogFamily struct {
	Name                string                   `json:"name"`
	Path                string                   `json:"path"`
	Exists              bool                     `json:"exists"`
	Segments            int                      `json:"segments"`
	Bytes               int64                    `json:"bytes"`
	ZeroByteSegments    int                      `json:"zero_byte_segments"`
	LargestSegmentBytes int64                    `json:"largest_segment_bytes"`
	TopSegmentsByBytes  []auditSummarySegment    `json:"top_segments_by_bytes,omitempty"`
	FrameScan           *auditSummaryFrameScan   `json:"frame_scan,omitempty"`
	GzipSamples         []auditSummaryGzipSample `json:"gzip_samples,omitempty"`
}

type auditSummarySegment struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

type auditSummaryFrameScan struct {
	RecordsTotal               int64                   `json:"records_total"`
	UngroupedRecords           int64                   `json:"ungrouped_records"`
	UngroupedBytes             int64                   `json:"ungrouped_bytes"`
	GroupedFrames              int64                   `json:"grouped_frames"`
	GroupedSubrecords          int64                   `json:"grouped_subrecords"`
	GroupedRawPayloadBytes     int64                   `json:"grouped_raw_payload_bytes"`
	GroupedStoredPayloadBytes  int64                   `json:"grouped_stored_payload_bytes"`
	RawPayloadBytes            int64                   `json:"raw_payload_bytes"`
	StoredPayloadBytes         int64                   `json:"stored_payload_bytes"`
	StoredRawRatio             float64                 `json:"stored_raw_ratio"`
	GroupedStoredRawRatio      float64                 `json:"grouped_stored_raw_ratio"`
	PageLike4096Records        int64                   `json:"page_like_4096_records"`
	PageLike4096Bytes          int64                   `json:"page_like_4096_bytes"`
	PageLike4096StoredBytes    int64                   `json:"page_like_4096_stored_bytes"`
	PageLike4096StoredRawRatio float64                 `json:"page_like_4096_stored_raw_ratio"`
	Large40To48KRecords        int64                   `json:"large_40_to_48k_records"`
	Large40To48KBytes          int64                   `json:"large_40_to_48k_bytes"`
	Large40To48KStoredBytes    int64                   `json:"large_40_to_48k_stored_bytes"`
	Large40To48KStoredRawRatio float64                 `json:"large_40_to_48k_stored_raw_ratio"`
	TruncatedSegments          int                     `json:"truncated_segments,omitempty"`
	Modes                      []auditSummaryFrameMode `json:"modes,omitempty"`
	TopRecordLengthsByBytes    []auditSummaryRecordLen `json:"top_record_lengths_by_bytes,omitempty"`
}

type auditSummaryFrameMode struct {
	Mode               string  `json:"mode"`
	Frames             int64   `json:"frames"`
	Subrecords         int64   `json:"subrecords"`
	RawPayloadBytes    int64   `json:"raw_payload_bytes"`
	StoredPayloadBytes int64   `json:"stored_payload_bytes"`
	StoredRawRatio     float64 `json:"stored_raw_ratio"`
}

type auditSummaryRecordLen struct {
	Length         int     `json:"length"`
	Records        int64   `json:"records"`
	Bytes          int64   `json:"bytes"`
	StoredBytes    int64   `json:"stored_bytes"`
	StoredRawRatio float64 `json:"stored_raw_ratio"`
}

type auditSummaryGzipSample struct {
	Sample     string  `json:"sample"`
	Name       string  `json:"name"`
	Path       string  `json:"path"`
	FileBytes  int64   `json:"file_bytes"`
	InputBytes int64   `json:"input_bytes"`
	GzipBytes  int64   `json:"gzip_bytes"`
	GzipRatio  float64 `json:"gzip_ratio"`
	Truncated  bool    `json:"truncated,omitempty"`
}

func runAuditSummary(dir string, args []string) {
	fs := flag.NewFlagSet("audit-summary", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "Emit machine-readable JSON instead of Markdown")
	mode := fs.String("mode", "full", "Compaction plan mode: full|quick|exhaustive")
	frameStats := fs.Bool("frame-stats", false, "Scan value_vlog and leaf_vlog records and report frame compression stats")
	frameTopLengths := fs.Int("frame-top-lengths", 12, "Number of top record lengths to include when -frame-stats is set")
	topSegments := fs.Int("top-segments", 12, "Number of largest segments to include per log family (0=none)")
	topGenerations := fs.Int("top-generations", 12, "Number of leaf generations by dead bytes to include (0=none)")
	gzipSamples := fs.Int("gzip-samples", 0, "Number of deterministic gzip sample files per log family (0=disabled; first,last,largest,then next-largest)")
	gzipSampleMaxBytes := fs.Int64("gzip-sample-max-bytes", 0, "Maximum bytes to gzip from each sample file (0=entire file)")
	_ = fs.Parse(args)

	if *frameTopLengths < 0 {
		fatalf("audit-summary -frame-top-lengths must be >= 0")
	}
	if *topSegments < 0 {
		fatalf("audit-summary -top-segments must be >= 0")
	}
	if *topGenerations < 0 {
		fatalf("audit-summary -top-generations must be >= 0")
	}
	if *gzipSamples < 0 {
		fatalf("audit-summary -gzip-samples must be >= 0")
	}
	if *gzipSampleMaxBytes < 0 {
		fatalf("audit-summary -gzip-sample-max-bytes must be >= 0")
	}

	report, err := collectAuditSummary(dir, auditSummaryCollectOptions{
		CompactMode:        parseCompactStorageModeFlag("audit-summary", *mode),
		FrameStats:         *frameStats,
		FrameTopLengths:    *frameTopLengths,
		TopSegments:        *topSegments,
		TopGenerations:     *topGenerations,
		GzipSamples:        *gzipSamples,
		GzipSampleMaxBytes: *gzipSampleMaxBytes,
	})
	if err != nil {
		fatalf("Audit summary error: %v", err)
	}

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fatalf("json encode: %v", err)
		}
		return
	}
	fmt.Print(renderAuditSummaryMarkdown(report))
}

func collectAuditSummary(dir string, opts auditSummaryCollectOptions) (auditSummaryReport, error) {
	cleanDir := filepath.Clean(dir)
	mainDir, err := resolveTreemapMainDir(cleanDir)
	if err != nil {
		return auditSummaryReport{}, err
	}
	rootDir := resolveTreemapRootDir(cleanDir, mainDir)
	if opts.CompactMode == "" {
		opts.CompactMode = treedbdb.CompactStorageFull
	}
	if opts.FrameTopLengths <= 0 {
		opts.FrameTopLengths = 10
	}

	report := auditSummaryReport{
		SchemaVersion: auditSummarySchemaVersion,
		Command:       "treemap audit-summary",
		Dir:           cleanDir,
		RootDir:       rootDir,
		MainDir:       mainDir,
		Options: auditSummaryReportOptions{
			CompactMode:        string(opts.CompactMode),
			FrameStats:         opts.FrameStats,
			FrameTopLengths:    opts.FrameTopLengths,
			TopSegments:        opts.TopSegments,
			TopGenerations:     opts.TopGenerations,
			GzipSamples:        opts.GzipSamples,
			GzipSampleMaxBytes: opts.GzipSampleMaxBytes,
		},
	}

	report.Storage, err = collectAuditSummaryStorage(cleanDir, rootDir, mainDir)
	if err != nil {
		return report, err
	}

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: rootDir, ReadOnly: true})
	if err != nil {
		return report, err
	}
	defer cleanup()

	compactStats, err := backend.CompactStoragePlan(context.Background(), treedbdb.CompactStorageOptions{Mode: opts.CompactMode})
	if err != nil {
		return report, err
	}
	report.CompactPlan = summarizeAuditCompactPlan(compactStats)
	report.ValueLog = summarizeAuditValueLog(compactStats.ValueLogRewritePlan, compactStats.ValueLogGC)
	report.LeafGeneration = summarizeAuditLeafGeneration(compactStats.LeafGenerationPlan, compactStats.LeafGenerationGC, compactStats.RemainingDebt, opts.TopGenerations)

	valueFamily, err := collectAuditSummaryLogFamily("value_vlog", treedbdb.ValueLogDirPath(mainDir), opts)
	if err != nil {
		return report, err
	}
	leafFamily, err := collectAuditSummaryLogFamily("leaf_vlog", treedbdb.LeafLogDirPath(mainDir), opts)
	if err != nil {
		return report, err
	}
	report.LogFamilies = []auditSummaryLogFamily{valueFamily, leafFamily}

	return report, nil
}

func collectAuditSummaryStorage(inputDir, rootDir, mainDir string) (auditSummaryStorage, error) {
	domainPaths := []struct {
		name string
		path string
	}{
		{name: "input", path: inputDir},
		{name: "root", path: rootDir},
		{name: "ancient", path: filepath.Join(rootDir, "ancient")},
		{name: "maindb", path: mainDir},
		{name: "index_db", path: filepath.Join(mainDir, "index.db")},
		{name: "value_vlog", path: treedbdb.ValueLogDirPath(mainDir)},
		{name: "leaf_vlog", path: treedbdb.LeafLogDirPath(mainDir)},
		{name: "dictdb", path: filepath.Join(rootDir, "dictdb")},
		{name: "wal", path: filepath.Join(mainDir, "wal")},
	}
	out := auditSummaryStorage{Domains: make([]auditSummaryStorageDomain, 0, len(domainPaths))}
	for _, domain := range domainPaths {
		usage, err := auditPathUsage(domain.name, domain.path)
		if err != nil {
			return out, err
		}
		out.Domains = append(out.Domains, usage)
	}
	return out, nil
}

func auditPathUsage(name, path string) (auditSummaryStorageDomain, error) {
	out := auditSummaryStorageDomain{Name: name, Path: path}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return out, err
	}
	out.Exists = true
	if !info.IsDir() {
		out.Files = 1
		out.Bytes = info.Size()
		if info.Size() == 0 {
			out.ZeroByteFiles = 1
		}
		return out, nil
	}
	err = filepath.WalkDir(path, func(p string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		out.Files++
		out.Bytes += info.Size()
		if info.Size() == 0 {
			out.ZeroByteFiles++
		}
		return nil
	})
	return out, err
}

func collectAuditSummaryLogFamily(name, dir string, opts auditSummaryCollectOptions) (auditSummaryLogFamily, error) {
	family := auditSummaryLogFamily{Name: name, Path: dir}
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		family.Exists = true
	} else if err != nil && !os.IsNotExist(err) {
		return family, err
	}

	segments, bytesOnDisk, err := listValueLogSegments(dir)
	if err != nil {
		return family, err
	}
	family.Segments = len(segments)
	family.Bytes = bytesOnDisk
	for _, seg := range segments {
		if seg.Bytes == 0 {
			family.ZeroByteSegments++
		}
		if seg.Bytes > family.LargestSegmentBytes {
			family.LargestSegmentBytes = seg.Bytes
		}
	}
	family.TopSegmentsByBytes = summarizeAuditTopSegments(segments, opts.TopSegments)

	if opts.FrameStats && len(segments) > 0 {
		frameScan, err := scanValueLogFrames(segments, valueLogFrameScanAuditOptions{Enabled: true, TopLengths: opts.FrameTopLengths})
		if err != nil {
			return family, err
		}
		family.FrameScan = summarizeAuditFrameScan(frameScan)
	}
	if opts.GzipSamples > 0 && len(segments) > 0 {
		samples, err := gzipAuditSamples(segments, opts.GzipSamples, opts.GzipSampleMaxBytes)
		if err != nil {
			return family, err
		}
		family.GzipSamples = samples
	}
	return family, nil
}

func summarizeAuditCompactPlan(stats treedbdb.CompactStorageStats) auditSummaryCompactPlan {
	return auditSummaryCompactPlan{
		Mode:                 string(stats.Mode),
		DryRun:               stats.DryRun,
		FullyCompacted:       stats.FullyCompacted,
		PolicyFullyCompacted: stats.PolicyFullyCompacted,
		ByteMinimized:        stats.ByteMinimized,
		StorageBefore:        summarizeCompactUsages(stats.Before),
		StorageAfter:         summarizeCompactUsages(stats.After),
		RemainingDebt:        summarizeAuditDebt(stats.RemainingDebt),
	}
}

func summarizeCompactUsages(usages []treedbdb.CompactStorageUsage) []auditSummaryStorageDomain {
	out := make([]auditSummaryStorageDomain, 0, len(usages))
	for _, usage := range usages {
		out = append(out, auditSummaryStorageDomain{
			Name:          usage.Name,
			Path:          usage.Path,
			Exists:        compactUsageExists(usage),
			Bytes:         usage.Bytes,
			Files:         usage.Files,
			ZeroByteFiles: usage.ZeroByteFiles,
		})
	}
	return out
}

func compactUsageExists(usage treedbdb.CompactStorageUsage) bool {
	if usage.Bytes != 0 || usage.Files != 0 || usage.ZeroByteFiles != 0 {
		return true
	}
	if strings.TrimSpace(usage.Path) == "" {
		return false
	}
	if _, err := os.Stat(usage.Path); err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	}
	return true
}

func summarizeAuditDebt(debt treedbdb.CompactStorageDebt) auditSummaryDebt {
	return auditSummaryDebt{
		ValueLogRewriteSegments:                debt.ValueLogRewriteSegments,
		ValueLogRewriteBytes:                   debt.ValueLogRewriteBytes,
		ValueLogGCSegments:                     debt.ValueLogGCSegments,
		ValueLogGCBytes:                        debt.ValueLogGCBytes,
		LeafPackGenerations:                    debt.LeafPackGenerations,
		LeafPackBytes:                          debt.LeafPackBytes,
		LeafGCGenerations:                      debt.LeafGCGenerations,
		LeafGCBytes:                            debt.LeafGCBytes,
		ZeroByteValueLogFiles:                  debt.ZeroByteValueLogFiles,
		IndexVacuumRequired:                    debt.IndexVacuumRequired,
		IndexVacuumReason:                      debt.IndexVacuumReason,
		IndexVacuumTotalPages:                  debt.IndexVacuumTotalPages,
		IndexVacuumUserPages:                   debt.IndexVacuumUserPages,
		IndexVacuumUserSpan:                    debt.IndexVacuumUserSpan,
		IndexVacuumUserSpanRatioPPM:            debt.IndexVacuumUserSpanRatioPPM,
		IndexVacuumFreelistReclaimablePages:    debt.IndexVacuumFreelistReclaimablePages,
		IndexVacuumFreelistReclaimableRatioPPM: debt.IndexVacuumFreelistReclaimableRatioPPM,
		IndexVacuumCollectionRootPages:         debt.IndexVacuumCollectionRootPages,
		IndexVacuumCollectionRootSpan:          debt.IndexVacuumCollectionRootSpan,
		IndexVacuumCollectionRootSpanRatioPPM:  debt.IndexVacuumCollectionRootSpanRatioPPM,
	}
}

func summarizeAuditValueLog(plan treedbdb.ValueLogRewritePlan, gc treedbdb.ValueLogGCStats) auditSummaryValueLog {
	ids := append([]uint32(nil), plan.SourceFileIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return auditSummaryValueLog{
		RewritePlan: auditSummaryValueLogRewritePlan{
			SegmentsTotal:      plan.SegmentsTotal,
			SegmentsSelected:   plan.SegmentsSelected,
			BytesTotal:         plan.BytesTotal,
			BytesLive:          plan.BytesLive,
			BytesStale:         plan.BytesStale,
			SelectedBytesTotal: plan.SelectedBytesTotal,
			SelectedBytesLive:  plan.SelectedBytesLive,
			SelectedBytesStale: plan.SelectedBytesStale,
			AgeBlockedSegments: plan.AgeBlockedSegments,
			AgeBlockedBytes:    plan.AgeBlockedBytesTotal,
			SourceFileIDs:      ids,
		},
		GCDryRun: auditSummaryValueLogGC{
			SegmentsTotal:      gc.SegmentsTotal,
			SegmentsReferenced: gc.SegmentsReferenced,
			SegmentsActive:     gc.SegmentsActive,
			SegmentsProtected:  gc.SegmentsProtected,
			SegmentsEligible:   gc.SegmentsEligible,
			BytesTotal:         gc.BytesTotal,
			BytesReferenced:    gc.BytesReferenced,
			BytesActive:        gc.BytesActive,
			BytesProtected:     gc.BytesProtected,
			BytesEligible:      gc.BytesEligible,
		},
	}
}

func summarizeAuditLeafGeneration(plan treedbdb.LeafGenerationPlan, gc treedbdb.LeafGenerationGCStats, debt treedbdb.CompactStorageDebt, topN int) auditSummaryLeafGeneration {
	return auditSummaryLeafGeneration{
		Admission:                       plan.Admission,
		CurrentCommitSeq:                plan.CurrentCommitSeq,
		CurrentGenerationID:             plan.CurrentGenerationID,
		GenerationsTotal:                len(plan.Generations),
		Candidates:                      len(plan.Candidates),
		CandidateBytesTotal:             plan.CandidateBytesTotal,
		CandidateBytesLive:              plan.CandidateBytesLive,
		CandidateBytesDead:              plan.CandidateBytesDead,
		CandidateBytesToCopy:            plan.CandidateBytesToCopy,
		ExpectedReclaimBytes:            plan.ExpectedReclaimBytes,
		ExpectedReclaimRatioPPM:         plan.ExpectedReclaimRatioPPM,
		ExpectedReclaimPerByteCopiedPPM: plan.ExpectedReclaimPerByteCopiedPPM,
		CompactSelectedGenerations:      debt.LeafPackGenerations,
		CompactSelectedReclaimBytes:     debt.LeafPackBytes,
		GCDryRun: auditSummaryLeafGenerationGC{
			GenerationsTotal:    gc.GenerationsTotal,
			GenerationsWritable: gc.GenerationsWritable,
			GenerationsLive:     gc.GenerationsLive,
			GenerationsRetiring: gc.GenerationsRetiring,
			GenerationsEligible: gc.GenerationsEligible,
			BytesEligible:       gc.BytesEligible,
		},
		TopGenerationsByDeadBytes: summarizeAuditLeafGenerations(plan.Generations, topN),
	}
}

func summarizeAuditLeafGenerations(generations []treedbdb.LeafGenerationPlanGeneration, topN int) []auditSummaryLeafGenerationRow {
	if topN <= 0 || len(generations) == 0 {
		return nil
	}
	ordered := append([]treedbdb.LeafGenerationPlanGeneration(nil), generations...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].BytesDead == ordered[j].BytesDead {
			return ordered[i].GenerationID < ordered[j].GenerationID
		}
		return ordered[i].BytesDead > ordered[j].BytesDead
	})
	if len(ordered) > topN {
		ordered = ordered[:topN]
	}
	out := make([]auditSummaryLeafGenerationRow, 0, len(ordered))
	for _, gen := range ordered {
		out = append(out, auditSummaryLeafGenerationRow{
			GenerationID:              gen.GenerationID,
			State:                     gen.State,
			FileCount:                 gen.FileCount,
			BytesTotal:                gen.BytesTotal,
			BytesLive:                 gen.BytesLive,
			BytesDead:                 gen.BytesDead,
			BytesToCopy:               gen.BytesToCopy,
			LivePages:                 gen.LivePages,
			AgeCommits:                gen.AgeCommits,
			PinnedCount:               gen.PinnedCount,
			DeadRatioPPM:              gen.DeadRatioPPM,
			LiveRatioPPM:              gen.LiveRatioPPM,
			WholeGenerationGCEligible: gen.WholeGenerationGCEligible,
			Eligible:                  gen.Eligible,
			SkipReason:                gen.SkipReason,
		})
	}
	return out
}

func summarizeAuditTopSegments(segments []valueLogSegmentAudit, topN int) []auditSummarySegment {
	if topN <= 0 || len(segments) == 0 {
		return nil
	}
	ordered := append([]valueLogSegmentAudit(nil), segments...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Bytes == ordered[j].Bytes {
			return ordered[i].Path < ordered[j].Path
		}
		return ordered[i].Bytes > ordered[j].Bytes
	})
	if len(ordered) > topN {
		ordered = ordered[:topN]
	}
	out := make([]auditSummarySegment, 0, len(ordered))
	for _, seg := range ordered {
		out = append(out, auditSummarySegment{Name: seg.Name, Path: seg.Path, Bytes: seg.Bytes})
	}
	return out
}

func summarizeAuditFrameScan(scan *valueLogFrameScanAudit) *auditSummaryFrameScan {
	if scan == nil {
		return nil
	}
	rawTotal := scan.UngroupedBytes + scan.GroupedRawPayloadBytes
	storedTotal := scan.UngroupedBytes + scan.GroupedStoredPayload
	out := &auditSummaryFrameScan{
		RecordsTotal:               scan.RecordsTotal,
		UngroupedRecords:           scan.UngroupedRecords,
		UngroupedBytes:             scan.UngroupedBytes,
		GroupedFrames:              scan.GroupedFrames,
		GroupedSubrecords:          scan.GroupedSubrecords,
		GroupedRawPayloadBytes:     scan.GroupedRawPayloadBytes,
		GroupedStoredPayloadBytes:  scan.GroupedStoredPayload,
		RawPayloadBytes:            rawTotal,
		StoredPayloadBytes:         storedTotal,
		StoredRawRatio:             floatRatio(storedTotal, rawTotal),
		GroupedStoredRawRatio:      floatRatio(scan.GroupedStoredPayload, scan.GroupedRawPayloadBytes),
		PageLike4096Records:        scan.PageLike4096Records,
		PageLike4096Bytes:          scan.PageLike4096Bytes,
		PageLike4096StoredBytes:    scan.PageLike4096StoredBytes,
		PageLike4096StoredRawRatio: floatRatio(scan.PageLike4096StoredBytes, scan.PageLike4096Bytes),
		Large40To48KRecords:        scan.Large40To48KRecords,
		Large40To48KBytes:          scan.Large40To48KBytes,
		Large40To48KStoredBytes:    scan.Large40To48KStoredBytes,
		Large40To48KStoredRawRatio: floatRatio(scan.Large40To48KStoredBytes, scan.Large40To48KBytes),
		TruncatedSegments:          scan.TruncatedSegments,
	}
	if len(scan.Modes) > 0 {
		keys := make([]string, 0, len(scan.Modes))
		for key := range scan.Modes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.Modes = make([]auditSummaryFrameMode, 0, len(keys))
		for _, key := range keys {
			mode := scan.Modes[key]
			out.Modes = append(out.Modes, auditSummaryFrameMode{
				Mode:               key,
				Frames:             mode.Frames,
				Subrecords:         mode.Subrecords,
				RawPayloadBytes:    mode.RawPayloadBytes,
				StoredPayloadBytes: mode.StoredPayloadBytes,
				StoredRawRatio:     floatRatio(mode.StoredPayloadBytes, mode.RawPayloadBytes),
			})
		}
	}
	for _, row := range scan.TopRecordLengthsByBytes {
		out.TopRecordLengthsByBytes = append(out.TopRecordLengthsByBytes, auditSummaryRecordLen{
			Length:         row.Length,
			Records:        row.Records,
			Bytes:          row.Bytes,
			StoredBytes:    row.StoredBytes,
			StoredRawRatio: floatRatio(row.StoredBytes, row.Bytes),
		})
	}
	return out
}

type gzipSampleCandidate struct {
	seg   valueLogSegmentAudit
	roles []string
}

func gzipAuditSamples(segments []valueLogSegmentAudit, sampleCount int, maxBytes int64) ([]auditSummaryGzipSample, error) {
	candidates := selectGzipSampleCandidates(segments, sampleCount)
	out := make([]auditSummaryGzipSample, 0, len(candidates))
	for _, candidate := range candidates {
		inputBytes, gzipBytes, err := gzipFilePrefix(candidate.seg.Path, maxBytes)
		if err != nil {
			return nil, err
		}
		out = append(out, auditSummaryGzipSample{
			Sample:     strings.Join(candidate.roles, "+"),
			Name:       candidate.seg.Name,
			Path:       candidate.seg.Path,
			FileBytes:  candidate.seg.Bytes,
			InputBytes: inputBytes,
			GzipBytes:  gzipBytes,
			GzipRatio:  floatRatio(gzipBytes, inputBytes),
			Truncated:  maxBytes > 0 && inputBytes < candidate.seg.Bytes,
		})
	}
	return out, nil
}

func selectGzipSampleCandidates(segments []valueLogSegmentAudit, sampleCount int) []gzipSampleCandidate {
	if sampleCount <= 0 || len(segments) == 0 {
		return nil
	}
	byName := append([]valueLogSegmentAudit(nil), segments...)
	sort.Slice(byName, func(i, j int) bool {
		if byName[i].Name == byName[j].Name {
			return byName[i].Path < byName[j].Path
		}
		return byName[i].Name < byName[j].Name
	})
	bySize := append([]valueLogSegmentAudit(nil), segments...)
	sort.Slice(bySize, func(i, j int) bool {
		if bySize[i].Bytes == bySize[j].Bytes {
			if bySize[i].Name == bySize[j].Name {
				return bySize[i].Path < bySize[j].Path
			}
			return bySize[i].Name < bySize[j].Name
		}
		return bySize[i].Bytes > bySize[j].Bytes
	})

	selected := make([]gzipSampleCandidate, 0, sampleCount)
	byPath := make(map[string]int)
	add := func(role string, seg valueLogSegmentAudit) {
		if idx, ok := byPath[seg.Path]; ok {
			if !stringSliceContains(selected[idx].roles, role) {
				selected[idx].roles = append(selected[idx].roles, role)
			}
			return
		}
		if len(selected) >= sampleCount {
			return
		}
		byPath[seg.Path] = len(selected)
		selected = append(selected, gzipSampleCandidate{seg: seg, roles: []string{role}})
	}
	add("first", byName[0])
	add("last", byName[len(byName)-1])
	add("largest", bySize[0])
	for i := 1; len(selected) < sampleCount && i < len(bySize); i++ {
		add(fmt.Sprintf("largest_%d", i+1), bySize[i])
	}
	return selected
}

func gzipFilePrefix(path string, maxBytes int64) (inputBytes int64, gzipBytes int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	var counter countingWriter
	zw := gzip.NewWriter(&counter)
	defer func() {
		closeErr := zw.Close()
		if err != nil {
			return
		}
		if closeErr != nil {
			inputBytes = 0
			gzipBytes = 0
			err = closeErr
			return
		}
		gzipBytes = counter.N
	}()

	var reader io.Reader = f
	if maxBytes > 0 {
		reader = &io.LimitedReader{R: f, N: maxBytes}
	}
	inputBytes, err = io.Copy(zw, reader)
	if err != nil {
		return 0, 0, err
	}
	return inputBytes, 0, nil
}

type countingWriter struct {
	N int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.N += int64(len(p))
	return len(p), nil
}

func stringSliceContains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func renderAuditSummaryMarkdown(report auditSummaryReport) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "# TreeDB audit summary\n\n")
	fmt.Fprintf(&buf, "- command: `%s`\n", report.Command)
	fmt.Fprintf(&buf, "- dir: `%s`\n", report.Dir)
	fmt.Fprintf(&buf, "- root_dir: `%s`\n", report.RootDir)
	fmt.Fprintf(&buf, "- main_dir: `%s`\n", report.MainDir)
	fmt.Fprintf(&buf, "- compact_mode: `%s`\n", report.Options.CompactMode)
	fmt.Fprintf(&buf, "- frame_stats: `%t`\n", report.Options.FrameStats)
	fmt.Fprintf(&buf, "- gzip_samples_per_family: `%d`\n\n", report.Options.GzipSamples)

	fmt.Fprintf(&buf, "## Storage breakdown\n\n")
	fmt.Fprintf(&buf, "| domain | exists | bytes | files | zero-byte files | path |\n")
	fmt.Fprintf(&buf, "| --- | ---: | ---: | ---: | ---: | --- |\n")
	for _, domain := range report.Storage.Domains {
		fmt.Fprintf(&buf, "| `%s` | %t | %d | %d | %d | `%s` |\n",
			markdownCell(domain.Name), domain.Exists, domain.Bytes, domain.Files, domain.ZeroByteFiles, markdownCell(domain.Path))
	}
	fmt.Fprintf(&buf, "\n")

	debt := report.CompactPlan.RemainingDebt
	fmt.Fprintf(&buf, "## Compact plan / current debt\n\n")
	fmt.Fprintf(&buf, "| metric | value |\n| --- | ---: |\n")
	fmt.Fprintf(&buf, "| dry_run | %t |\n", report.CompactPlan.DryRun)
	fmt.Fprintf(&buf, "| fully_compacted | %t |\n", report.CompactPlan.FullyCompacted)
	fmt.Fprintf(&buf, "| policy_fully_compacted | %t |\n", report.CompactPlan.PolicyFullyCompacted)
	fmt.Fprintf(&buf, "| byte_minimized | %t |\n", report.CompactPlan.ByteMinimized)
	fmt.Fprintf(&buf, "| value_log_rewrite_segments | %d |\n", debt.ValueLogRewriteSegments)
	fmt.Fprintf(&buf, "| value_log_rewrite_bytes | %d |\n", debt.ValueLogRewriteBytes)
	fmt.Fprintf(&buf, "| value_log_gc_segments | %d |\n", debt.ValueLogGCSegments)
	fmt.Fprintf(&buf, "| value_log_gc_bytes | %d |\n", debt.ValueLogGCBytes)
	fmt.Fprintf(&buf, "| leaf_pack_generations | %d |\n", debt.LeafPackGenerations)
	fmt.Fprintf(&buf, "| leaf_pack_bytes | %d |\n", debt.LeafPackBytes)
	fmt.Fprintf(&buf, "| leaf_gc_generations | %d |\n", debt.LeafGCGenerations)
	fmt.Fprintf(&buf, "| leaf_gc_bytes | %d |\n", debt.LeafGCBytes)
	fmt.Fprintf(&buf, "| zero_byte_value_log_files | %d |\n\n", debt.ZeroByteValueLogFiles)

	fmt.Fprintf(&buf, "## Value-log audit summary\n\n")
	rewrite := report.ValueLog.RewritePlan
	gc := report.ValueLog.GCDryRun
	fmt.Fprintf(&buf, "| metric | value |\n| --- | ---: |\n")
	fmt.Fprintf(&buf, "| rewrite_segments_total | %d |\n", rewrite.SegmentsTotal)
	fmt.Fprintf(&buf, "| rewrite_segments_selected | %d |\n", rewrite.SegmentsSelected)
	fmt.Fprintf(&buf, "| rewrite_bytes_total | %d |\n", rewrite.BytesTotal)
	fmt.Fprintf(&buf, "| rewrite_bytes_live | %d |\n", rewrite.BytesLive)
	fmt.Fprintf(&buf, "| rewrite_bytes_stale | %d |\n", rewrite.BytesStale)
	fmt.Fprintf(&buf, "| rewrite_selected_bytes_stale | %d |\n", rewrite.SelectedBytesStale)
	fmt.Fprintf(&buf, "| gc_segments_total | %d |\n", gc.SegmentsTotal)
	fmt.Fprintf(&buf, "| gc_segments_eligible | %d |\n", gc.SegmentsEligible)
	fmt.Fprintf(&buf, "| gc_bytes_total | %d |\n", gc.BytesTotal)
	fmt.Fprintf(&buf, "| gc_bytes_eligible | %d |\n\n", gc.BytesEligible)

	leaf := report.LeafGeneration
	fmt.Fprintf(&buf, "## Leaf-generation audit summary\n\n")
	fmt.Fprintf(&buf, "| metric | value |\n| --- | ---: |\n")
	fmt.Fprintf(&buf, "| admission | %s |\n", markdownCell(leaf.Admission))
	fmt.Fprintf(&buf, "| generations_total | %d |\n", leaf.GenerationsTotal)
	fmt.Fprintf(&buf, "| candidates | %d |\n", leaf.Candidates)
	fmt.Fprintf(&buf, "| candidate_bytes_total | %d |\n", leaf.CandidateBytesTotal)
	fmt.Fprintf(&buf, "| candidate_bytes_live | %d |\n", leaf.CandidateBytesLive)
	fmt.Fprintf(&buf, "| candidate_bytes_dead | %d |\n", leaf.CandidateBytesDead)
	fmt.Fprintf(&buf, "| candidate_bytes_to_copy | %d |\n", leaf.CandidateBytesToCopy)
	fmt.Fprintf(&buf, "| expected_reclaim_bytes | %d |\n", leaf.ExpectedReclaimBytes)
	fmt.Fprintf(&buf, "| expected_reclaim_ratio_ppm | %d |\n", leaf.ExpectedReclaimRatioPPM)
	fmt.Fprintf(&buf, "| compact_selected_generations | %d |\n", leaf.CompactSelectedGenerations)
	fmt.Fprintf(&buf, "| compact_selected_reclaim_bytes | %d |\n", leaf.CompactSelectedReclaimBytes)
	fmt.Fprintf(&buf, "| gc_generations_eligible | %d |\n", leaf.GCDryRun.GenerationsEligible)
	fmt.Fprintf(&buf, "| gc_bytes_eligible | %d |\n\n", leaf.GCDryRun.BytesEligible)

	if len(leaf.TopGenerationsByDeadBytes) > 0 {
		fmt.Fprintf(&buf, "### Top leaf generations by dead bytes\n\n")
		fmt.Fprintf(&buf, "| generation | state | files | total bytes | live bytes | dead bytes | to-copy bytes | eligible | gc eligible | skip |\n")
		fmt.Fprintf(&buf, "| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |\n")
		for _, row := range leaf.TopGenerationsByDeadBytes {
			fmt.Fprintf(&buf, "| %d | `%s` | %d | %d | %d | %d | %d | %t | %t | `%s` |\n",
				row.GenerationID, markdownCell(row.State), row.FileCount, row.BytesTotal, row.BytesLive, row.BytesDead,
				row.BytesToCopy, row.Eligible, row.WholeGenerationGCEligible, markdownCell(row.SkipReason))
		}
		fmt.Fprintf(&buf, "\n")
	}

	fmt.Fprintf(&buf, "## Log families\n\n")
	for _, family := range report.LogFamilies {
		fmt.Fprintf(&buf, "### `%s`\n\n", markdownCell(family.Name))
		fmt.Fprintf(&buf, "- path: `%s`\n", markdownCell(family.Path))
		fmt.Fprintf(&buf, "- exists: `%t`\n", family.Exists)
		fmt.Fprintf(&buf, "- segments: `%d`\n", family.Segments)
		fmt.Fprintf(&buf, "- bytes: `%d`\n", family.Bytes)
		fmt.Fprintf(&buf, "- zero_byte_segments: `%d`\n", family.ZeroByteSegments)
		fmt.Fprintf(&buf, "- largest_segment_bytes: `%d`\n\n", family.LargestSegmentBytes)
		if family.FrameScan != nil {
			fmt.Fprintf(&buf, "Frame scan: records=%d raw_bytes=%d stored_bytes=%d stored/raw=%.6f grouped_frames=%d grouped_subrecords=%d\n\n",
				family.FrameScan.RecordsTotal, family.FrameScan.RawPayloadBytes, family.FrameScan.StoredPayloadBytes,
				family.FrameScan.StoredRawRatio, family.FrameScan.GroupedFrames, family.FrameScan.GroupedSubrecords)
		} else if report.Options.FrameStats {
			fmt.Fprintf(&buf, "Frame scan: no segments scanned.\n\n")
		} else {
			fmt.Fprintf(&buf, "Frame scan: omitted (pass `-frame-stats` to include).\n\n")
		}
		if len(family.TopSegmentsByBytes) > 0 {
			fmt.Fprintf(&buf, "Top segments by bytes:\n\n")
			fmt.Fprintf(&buf, "| name | bytes | path |\n| --- | ---: | --- |\n")
			for _, seg := range family.TopSegmentsByBytes {
				fmt.Fprintf(&buf, "| `%s` | %d | `%s` |\n", markdownCell(seg.Name), seg.Bytes, markdownCell(seg.Path))
			}
			fmt.Fprintf(&buf, "\n")
		}
		if len(family.GzipSamples) > 0 {
			fmt.Fprintf(&buf, "Gzip samples:\n\n")
			fmt.Fprintf(&buf, "| sample | name | file bytes | input bytes | gzip bytes | gzip/input | truncated |\n")
			fmt.Fprintf(&buf, "| --- | --- | ---: | ---: | ---: | ---: | --- |\n")
			for _, sample := range family.GzipSamples {
				fmt.Fprintf(&buf, "| `%s` | `%s` | %d | %d | %d | %.6f | %t |\n",
					markdownCell(sample.Sample), markdownCell(sample.Name), sample.FileBytes, sample.InputBytes,
					sample.GzipBytes, sample.GzipRatio, sample.Truncated)
			}
			fmt.Fprintf(&buf, "\n")
		}
	}
	return buf.String()
}

func markdownCell(s string) string {
	return strings.ReplaceAll(s, "|", "\\|")
}
