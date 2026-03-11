package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type valueLogSegmentAudit struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

type valueLogAuditReport struct {
	Dir            string                       `json:"dir"`
	MainDir        string                       `json:"main_dir"`
	ValueLogDir    string                       `json:"value_log_dir"`
	Segments       []valueLogSegmentAudit       `json:"segments"`
	SegmentsOnDisk int                          `json:"segments_on_disk"`
	BytesOnDisk    int64                        `json:"bytes_on_disk"`
	RIDScan        valueLogRIDAudit             `json:"rid_scan"`
	GCDryRun       treedbdb.ValueLogGCStats     `json:"gc_dry_run"`
	RewritePlan    treedbdb.ValueLogRewritePlan `json:"rewrite_plan"`
	RIDScanMS      float64                      `json:"rid_scan_ms,omitempty"`
	GCDryRunMS     float64                      `json:"gc_dry_run_ms,omitempty"`
	RewritePlanMS  float64                      `json:"rewrite_plan_ms,omitempty"`
	Stats          map[string]string            `json:"stats,omitempty"`
}

type valueLogRIDAudit struct {
	Records           int64  `json:"records"`
	DistinctRIDs      int    `json:"distinct_rids"`
	DuplicateRIDs     int    `json:"duplicate_rids"`
	TruncatedSegments int    `json:"truncated_segments,omitempty"`
	FirstDuplicateRID uint64 `json:"first_duplicate_rid,omitempty"`
	MaxRID            uint64 `json:"max_rid"`
}

type valueLogRIDAuditOptions struct {
	StopOnFirstDuplicate bool
	MaxTrackedRIDs       int
}

type valueLogAuditRewriteFlagOptions struct {
	maxSegments             int
	maxBytes                int64
	minStaleRatio           float64
	minStaleBytes           int64
	minAggregateStaleBytes  int64
	schedulerLike           bool
	schedulerHotTargetBytes int64
}

func buildValueLogAuditRewriteOptions(flagOpts valueLogAuditRewriteFlagOptions) treedbdb.ValueLogRewriteOnlineOptions {
	opts := treedbdb.ValueLogRewriteOnlineOptions{
		MaxSourceSegments:      flagOpts.maxSegments,
		MaxSourceBytes:         flagOpts.maxBytes,
		MinSegmentStaleRatio:   flagOpts.minStaleRatio,
		MinSegmentStaleBytes:   flagOpts.minStaleBytes,
		MinAggregateStaleBytes: flagOpts.minAggregateStaleBytes,
	}
	if !flagOpts.schedulerLike {
		return opts
	}
	if opts.MinSegmentStaleRatio <= 0 {
		opts.MinSegmentStaleRatio = 0.20
	}
	if opts.MinSegmentStaleBytes <= 0 {
		opts.MinSegmentStaleBytes = 1
	}
	if opts.MinAggregateStaleBytes <= 0 {
		threshold := flagOpts.schedulerHotTargetBytes / 8
		if opts.MaxSourceBytes > 0 {
			budgetThreshold := opts.MaxSourceBytes / 4
			if threshold <= 0 || (budgetThreshold > 0 && budgetThreshold < threshold) {
				threshold = budgetThreshold
			}
		}
		if threshold <= 0 {
			threshold = 1 << 20
		}
		opts.MinAggregateStaleBytes = threshold
	}
	return opts
}

func runVlogAudit(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-audit", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required; may replay WAL or repair files)")
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	maxSegments := fs.Int("rewrite-max-segments", 0, "Rewrite-plan selection cap in segments (0=none)")
	maxBytes := fs.Int64("rewrite-max-bytes", 0, "Rewrite-plan live-byte selection cap (0=none)")
	minStaleRatio := fs.Float64("rewrite-min-stale-ratio", 0, "Rewrite-plan minimum per-segment stale ratio (0..1)")
	minStaleBytes := fs.Int64("rewrite-min-stale-bytes", 0, "Rewrite-plan minimum per-segment stale bytes")
	minAggregateStaleBytes := fs.Int64("rewrite-min-aggregate-stale-bytes", 0, "Rewrite-plan minimum aggregate stale bytes for debt-aware fallback (0=disabled)")
	schedulerLike := fs.Bool("rewrite-scheduler-like", false, "Apply cached-maintenance-style rewrite defaults (stale ratio + aggregate debt fallback)")
	schedulerHotTargetBytes := fs.Int64("rewrite-scheduler-hot-target-bytes", 256<<20, "Hot-segment target used to derive scheduler-like aggregate debt threshold")
	stopOnFirstDuplicate := fs.Bool("rid-scan-stop-on-first-duplicate", false, "Stop the RID scan after the first duplicate is detected")
	maxTrackedRIDs := fs.Int("rid-scan-max-tracked", 0, "Maximum distinct RIDs to track in-memory during RID scan (0=unbounded exact mode; may use high memory)")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-audit requires -rw")
	}

	report, err := collectValueLogAudit(dir, buildValueLogAuditRewriteOptions(valueLogAuditRewriteFlagOptions{
		maxSegments:             *maxSegments,
		maxBytes:                *maxBytes,
		minStaleRatio:           *minStaleRatio,
		minStaleBytes:           *minStaleBytes,
		minAggregateStaleBytes:  *minAggregateStaleBytes,
		schedulerLike:           *schedulerLike,
		schedulerHotTargetBytes: *schedulerHotTargetBytes,
	}), valueLogRIDAuditOptions{
		StopOnFirstDuplicate: *stopOnFirstDuplicate,
		MaxTrackedRIDs:       *maxTrackedRIDs,
	})
	if err != nil {
		fatalf("ValueLog audit error: %v", err)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fatalf("json encode: %v", err)
		}
		return
	}

	fmt.Printf("dir=%s\n", report.Dir)
	fmt.Printf("main_dir=%s\n", report.MainDir)
	fmt.Printf("value_log_dir=%s\n", report.ValueLogDir)
	fmt.Printf("segments_on_disk=%d bytes_on_disk=%d\n", report.SegmentsOnDisk, report.BytesOnDisk)
	fmt.Printf("rid_scan: records=%d distinct=%d duplicates=%d first_duplicate_rid=%d max_rid=%d\n",
		report.RIDScan.Records,
		report.RIDScan.DistinctRIDs,
		report.RIDScan.DuplicateRIDs,
		report.RIDScan.FirstDuplicateRID,
		report.RIDScan.MaxRID,
	)
	if report.RIDScanMS > 0 {
		fmt.Printf("rid_scan_ms=%.3f\n", report.RIDScanMS)
	}
	if report.RIDScan.TruncatedSegments > 0 {
		fmt.Printf("rid_scan_truncated_segments=%d\n", report.RIDScan.TruncatedSegments)
	}
	fmt.Printf("gc_dry_run: segments_total=%d referenced=%d active=%d protected=%d eligible=%d deleted=%d bytes_total=%d bytes_referenced=%d bytes_active=%d bytes_protected=%d bytes_eligible=%d bytes_deleted=%d\n",
		report.GCDryRun.SegmentsTotal,
		report.GCDryRun.SegmentsReferenced,
		report.GCDryRun.SegmentsActive,
		report.GCDryRun.SegmentsProtected,
		report.GCDryRun.SegmentsEligible,
		report.GCDryRun.SegmentsDeleted,
		report.GCDryRun.BytesTotal,
		report.GCDryRun.BytesReferenced,
		report.GCDryRun.BytesActive,
		report.GCDryRun.BytesProtected,
		report.GCDryRun.BytesEligible,
		report.GCDryRun.BytesDeleted,
	)
	if report.GCDryRunMS > 0 {
		fmt.Printf("gc_dry_run_ms=%.3f\n", report.GCDryRunMS)
	}
	fmt.Printf("rewrite_plan: segments_total=%d selected=%d bytes_total=%d bytes_live=%d bytes_stale=%d selected_bytes_total=%d selected_bytes_live=%d selected_bytes_stale=%d source_file_ids=%v\n",
		report.RewritePlan.SegmentsTotal,
		report.RewritePlan.SegmentsSelected,
		report.RewritePlan.BytesTotal,
		report.RewritePlan.BytesLive,
		report.RewritePlan.BytesStale,
		report.RewritePlan.SelectedBytesTotal,
		report.RewritePlan.SelectedBytesLive,
		report.RewritePlan.SelectedBytesStale,
		report.RewritePlan.SourceFileIDs,
	)
	if report.RewritePlanMS > 0 {
		fmt.Printf("rewrite_plan_ms=%.3f\n", report.RewritePlanMS)
	}
	if len(report.Stats) > 0 {
		keys := make([]string, 0, len(report.Stats))
		for k := range report.Stats {
			if strings.HasPrefix(k, "treedb.") || strings.HasPrefix(k, "cosmos.db.") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		fmt.Println("stats:")
		for _, k := range keys {
			fmt.Printf("  %s=%s\n", k, report.Stats[k])
		}
	}
	if len(report.Segments) > 0 {
		fmt.Println("segments:")
		for _, seg := range report.Segments {
			fmt.Printf("  %s bytes=%d\n", seg.Path, seg.Bytes)
		}
	}
}

func collectValueLogAudit(dir string, rewriteOpts treedbdb.ValueLogRewriteOnlineOptions, ridOpts valueLogRIDAuditOptions) (report valueLogAuditReport, err error) {
	report = valueLogAuditReport{Dir: dir}
	mainDir, err := resolveTreemapMainDir(dir)
	if err != nil {
		return report, err
	}
	rootDir := resolveTreemapRootDir(filepath.Clean(dir), mainDir)
	report.MainDir = mainDir
	report.ValueLogDir = filepath.Join(mainDir, "wal")

	segs, bytesOnDisk, err := listValueLogSegments(report.ValueLogDir)
	if err != nil {
		return report, err
	}
	report.Segments = segs
	report.SegmentsOnDisk = len(segs)
	report.BytesOnDisk = bytesOnDisk
	start := time.Now()
	report.RIDScan, err = scanValueLogRIDs(segs, ridOpts)
	report.RIDScanMS = float64(time.Since(start)) / float64(time.Millisecond)
	if err != nil {
		return report, err
	}

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: rootDir, ReadOnly: false})
	if err != nil {
		return report, err
	}
	defer func() {
		if cleanup == nil {
			return
		}
		if cerr := cleanup(); cerr != nil {
			if err == nil {
				err = cerr
			} else {
				err = errors.Join(err, cerr)
			}
		}
	}()

	report.Stats = backend.Stats()
	start = time.Now()
	report.GCDryRun, err = backend.ValueLogGC(context.Background(), treedbdb.ValueLogGCOptions{DryRun: true})
	report.GCDryRunMS = float64(time.Since(start)) / float64(time.Millisecond)
	if err != nil {
		return report, err
	}
	start = time.Now()
	report.RewritePlan, err = backend.ValueLogRewritePlan(context.Background(), rewriteOpts)
	report.RewritePlanMS = float64(time.Since(start)) / float64(time.Millisecond)
	if err != nil {
		return report, err
	}
	return report, nil
}

func resolveTreemapMainDir(dir string) (string, error) {
	clean := filepath.Clean(dir)
	candidates := []string{clean}
	if filepath.Base(clean) != "maindb" {
		candidates = append(candidates, filepath.Join(clean, "maindb"))
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil || !info.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(candidate, "index.db")); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("could not resolve maindb dir under %q", dir)
}

func resolveTreemapRootDir(inputDir, mainDir string) string {
	clean := filepath.Clean(inputDir)
	if filepath.Base(clean) == "maindb" {
		return filepath.Dir(mainDir)
	}
	return clean
}

func listValueLogSegments(dir string) ([]valueLogSegmentAudit, int64, error) {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	if !info.IsDir() {
		return nil, 0, fmt.Errorf("value-log dir is not a directory: %s", dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	segs := make([]valueLogSegmentAudit, 0, len(entries))
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "value-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		path := filepath.Join(dir, name)
		stat, err := entry.Info()
		if err != nil {
			return nil, 0, err
		}
		segs = append(segs, valueLogSegmentAudit{
			Name:  name,
			Path:  path,
			Bytes: stat.Size(),
		})
		total += stat.Size()
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].Name < segs[j].Name })
	return segs, total, nil
}

func scanValueLogRIDs(segments []valueLogSegmentAudit, opts valueLogRIDAuditOptions) (valueLogRIDAudit, error) {
	var report valueLogRIDAudit
	seen := make(map[uint64]struct{})
	for _, seg := range segments {
		fileID, ok := parseValueLogAuditFileID(seg.Name)
		if !ok {
			continue
		}
		reader, err := valuelog.NewReader(seg.Path, fileID)
		if err != nil {
			return report, err
		}
		reader.DisableValueDecode()
		for {
			rid, _, _, err := reader.ReadNext()
			if err == nil {
				report.Records++
				if rid > report.MaxRID {
					report.MaxRID = rid
				}
				if _, ok := seen[rid]; ok {
					report.DuplicateRIDs++
					if report.FirstDuplicateRID == 0 {
						report.FirstDuplicateRID = rid
					}
					if opts.StopOnFirstDuplicate {
						_ = reader.Close()
						report.DistinctRIDs = len(seen)
						return report, nil
					}
				} else {
					if opts.MaxTrackedRIDs > 0 && len(seen) >= opts.MaxTrackedRIDs {
						_ = reader.Close()
						return report, fmt.Errorf("rid scan exceeded max tracked rids: max=%d", opts.MaxTrackedRIDs)
					}
					seen[rid] = struct{}{}
				}
				continue
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				report.TruncatedSegments++
				break
			}
			_ = reader.Close()
			return report, err
		}
		if err := reader.Close(); err != nil {
			return report, err
		}
	}
	report.DistinctRIDs = len(seen)
	return report, nil
}

func parseValueLogAuditFileID(name string) (uint32, bool) {
	if !strings.HasPrefix(name, "value-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(name, "value-"), ".log")
	if strings.HasPrefix(rest, "l") {
		parts := strings.SplitN(strings.TrimPrefix(rest, "l"), "-", 2)
		if len(parts) != 2 {
			return 0, false
		}
		lane, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return 0, false
		}
		seq, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return 0, false
		}
		fileID, err := valuelog.EncodeFileID(uint32(lane), uint32(seq))
		if err != nil {
			return 0, false
		}
		return fileID, true
	}
	seq, err := strconv.ParseUint(rest, 10, 32)
	if err != nil {
		return 0, false
	}
	fileID, err := valuelog.EncodeFileID(0, uint32(seq))
	if err != nil {
		return 0, false
	}
	return fileID, true
}
