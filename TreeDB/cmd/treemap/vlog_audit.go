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

func runVlogAudit(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-audit", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required; may replay WAL or repair files)")
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	maxSegments := fs.Int("rewrite-max-segments", 0, "Rewrite-plan selection cap in segments (0=none)")
	maxBytes := fs.Int64("rewrite-max-bytes", 0, "Rewrite-plan live-byte selection cap (0=none)")
	minStaleRatio := fs.Float64("rewrite-min-stale-ratio", 0, "Rewrite-plan minimum per-segment stale ratio (0..1)")
	minStaleBytes := fs.Int64("rewrite-min-stale-bytes", 0, "Rewrite-plan minimum per-segment stale bytes")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-audit requires -rw")
	}

	report, err := collectValueLogAudit(dir, treedbdb.ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    *maxSegments,
		MaxSourceBytes:       *maxBytes,
		MinSegmentStaleRatio: *minStaleRatio,
		MinSegmentStaleBytes: *minStaleBytes,
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

func collectValueLogAudit(dir string, rewriteOpts treedbdb.ValueLogRewriteOnlineOptions) (report valueLogAuditReport, err error) {
	report = valueLogAuditReport{Dir: dir}
	mainDir, err := resolveTreemapMainDir(dir)
	if err != nil {
		return report, err
	}
	report.MainDir = mainDir
	report.ValueLogDir = filepath.Join(mainDir, "wal")

	segs, bytesOnDisk, err := listValueLogSegments(report.ValueLogDir)
	if err != nil {
		return report, err
	}
	report.Segments = segs
	report.SegmentsOnDisk = len(segs)
	report.BytesOnDisk = bytesOnDisk
	report.RIDScan, err = scanValueLogRIDs(segs)
	if err != nil {
		return report, err
	}

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir, ReadOnly: false})
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
	report.GCDryRun, err = backend.ValueLogGC(context.Background(), treedbdb.ValueLogGCOptions{DryRun: true})
	if err != nil {
		return report, err
	}
	report.RewritePlan, err = backend.ValueLogRewritePlan(context.Background(), rewriteOpts)
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

func scanValueLogRIDs(segments []valueLogSegmentAudit) (valueLogRIDAudit, error) {
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
				} else {
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
