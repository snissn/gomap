package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/limits"
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
	LeafLogDir     string                       `json:"leaf_log_dir"`
	Segments       []valueLogSegmentAudit       `json:"segments"`
	SegmentsOnDisk int                          `json:"segments_on_disk"`
	BytesOnDisk    int64                        `json:"bytes_on_disk"`
	RIDScan        valueLogRIDAudit             `json:"rid_scan"`
	GCDryRun       treedbdb.ValueLogGCStats     `json:"gc_dry_run"`
	RewritePlan    treedbdb.ValueLogRewritePlan `json:"rewrite_plan"`
	FrameScan      *valueLogFrameScanAudit      `json:"frame_scan,omitempty"`
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

type valueLogFrameScanAuditOptions struct {
	Enabled    bool
	TopLengths int
}

type valueLogFrameModeAudit struct {
	Frames             int64 `json:"frames"`
	Subrecords         int64 `json:"subrecords"`
	RawPayloadBytes    int64 `json:"raw_payload_bytes"`
	StoredPayloadBytes int64 `json:"stored_payload_bytes"`
}

type valueLogRecordModeAudit struct {
	Records            int64 `json:"records"`
	RawPayloadBytes    int64 `json:"raw_payload_bytes"`
	StoredPayloadBytes int64 `json:"stored_payload_bytes"`
}

type valueLogRecordLengthAudit struct {
	Length      int   `json:"length"`
	Records     int64 `json:"records"`
	Bytes       int64 `json:"bytes"`
	StoredBytes int64 `json:"stored_bytes"`
}

type valueLogFrameScanAudit struct {
	RecordsTotal            int64                              `json:"records_total"`
	UngroupedRecords        int64                              `json:"ungrouped_records"`
	UngroupedBytes          int64                              `json:"ungrouped_bytes"`
	GroupedFrames           int64                              `json:"grouped_frames"`
	GroupedSubrecords       int64                              `json:"grouped_subrecords"`
	GroupedRawPayloadBytes  int64                              `json:"grouped_raw_payload_bytes"`
	GroupedStoredPayload    int64                              `json:"grouped_stored_payload_bytes"`
	PageLike4096Records     int64                              `json:"page_like_4096_records"`
	PageLike4096Bytes       int64                              `json:"page_like_4096_bytes"`
	PageLike4096StoredBytes int64                              `json:"page_like_4096_stored_bytes"`
	PageLike4096Modes       map[string]valueLogRecordModeAudit `json:"page_like_4096_modes,omitempty"`
	Large40To48KRecords     int64                              `json:"large_40_to_48k_records"`
	Large40To48KBytes       int64                              `json:"large_40_to_48k_bytes"`
	Large40To48KStoredBytes int64                              `json:"large_40_to_48k_stored_bytes"`
	Large40To48KModes       map[string]valueLogRecordModeAudit `json:"large_40_to_48k_modes,omitempty"`
	TruncatedSegments       int                                `json:"truncated_segments,omitempty"`
	Modes                   map[string]valueLogFrameModeAudit  `json:"modes,omitempty"`
	TopRecordLengthsByBytes []valueLogRecordLengthAudit        `json:"top_record_lengths_by_bytes,omitempty"`
}

const recordFlagGroupedAudit byte = 1 << 0

func runVlogAudit(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-audit", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write to allow recovery before auditing")
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	frameStats := fs.Bool("frame-stats", false, "Scan value-log records and report frame-mode + record-length stats")
	frameTopLengths := fs.Int("frame-top-lengths", 12, "Number of top record lengths (by retained bytes) to report with -frame-stats")
	maxSegments := fs.Int("rewrite-max-segments", 0, "Rewrite-plan selection cap in segments (0=none)")
	maxBytes := fs.Int64("rewrite-max-bytes", 0, "Rewrite-plan live-byte selection cap (0=none)")
	minStaleRatio := fs.Float64("rewrite-min-stale-ratio", 0, "Rewrite-plan minimum per-segment stale ratio (0..1)")
	minStaleBytes := fs.Int64("rewrite-min-stale-bytes", 0, "Rewrite-plan minimum per-segment stale bytes")
	stopOnFirstDuplicate := fs.Bool("rid-scan-stop-on-first-duplicate", false, "Stop the RID scan after the first duplicate is detected")
	maxTrackedRIDs := fs.Int("rid-scan-max-tracked", 0, "Maximum distinct RIDs to track in-memory during RID scan (0=unbounded exact mode; may use high memory)")
	_ = fs.Parse(args)

	report, err := collectValueLogAudit(dir, !*rw, treedbdb.ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    *maxSegments,
		MaxSourceBytes:       *maxBytes,
		MinSegmentStaleRatio: *minStaleRatio,
		MinSegmentStaleBytes: *minStaleBytes,
	}, valueLogRIDAuditOptions{
		StopOnFirstDuplicate: *stopOnFirstDuplicate,
		MaxTrackedRIDs:       *maxTrackedRIDs,
	}, valueLogFrameScanAuditOptions{
		Enabled:    *frameStats,
		TopLengths: *frameTopLengths,
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
	if strings.TrimSpace(report.LeafLogDir) != "" {
		fmt.Printf("leaf_log_dir=%s\n", report.LeafLogDir)
	}
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
	if report.FrameScan != nil {
		scan := report.FrameScan
		groupedRatio := floatRatio(scan.GroupedStoredPayload, scan.GroupedRawPayloadBytes)
		totalRaw := scan.UngroupedBytes + scan.GroupedRawPayloadBytes
		totalStored := scan.UngroupedBytes + scan.GroupedStoredPayload
		totalRatio := floatRatio(totalStored, totalRaw)
		fmt.Printf("frame_scan: records_total=%d grouped_frames=%d grouped_subrecords=%d ungrouped_records=%d truncated_segments=%d\n",
			scan.RecordsTotal,
			scan.GroupedFrames,
			scan.GroupedSubrecords,
			scan.UngroupedRecords,
			scan.TruncatedSegments,
		)
		fmt.Printf("frame_scan_bytes: grouped_raw=%d grouped_stored=%d grouped_ratio=%.6f ungrouped=%d total_ratio=%.6f\n",
			scan.GroupedRawPayloadBytes,
			scan.GroupedStoredPayload,
			groupedRatio,
			scan.UngroupedBytes,
			totalRatio,
		)
		fmt.Printf("frame_scan_focus: page_4096_records=%d page_4096_bytes=%d large_40_to_48k_records=%d large_40_to_48k_bytes=%d\n",
			scan.PageLike4096Records,
			scan.PageLike4096Bytes,
			scan.Large40To48KRecords,
			scan.Large40To48KBytes,
		)
		fmt.Printf("frame_scan_focus_bytes: page_4096_stored=%d page_4096_ratio=%.6f large_40_to_48k_stored=%d large_40_to_48k_ratio=%.6f\n",
			scan.PageLike4096StoredBytes,
			floatRatio(scan.PageLike4096StoredBytes, scan.PageLike4096Bytes),
			scan.Large40To48KStoredBytes,
			floatRatio(scan.Large40To48KStoredBytes, scan.Large40To48KBytes),
		)
		printRecordModeBreakdown("page_4096", scan.PageLike4096Modes)
		printRecordModeBreakdown("large_40_to_48k", scan.Large40To48KModes)
		if len(scan.Modes) > 0 {
			keys := make([]string, 0, len(scan.Modes))
			for k := range scan.Modes {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			fmt.Println("frame_scan_modes:")
			for _, k := range keys {
				mode := scan.Modes[k]
				fmt.Printf("  %s frames=%d subrecords=%d raw=%d stored=%d ratio=%.6f\n",
					k,
					mode.Frames,
					mode.Subrecords,
					mode.RawPayloadBytes,
					mode.StoredPayloadBytes,
					floatRatio(mode.StoredPayloadBytes, mode.RawPayloadBytes),
				)
			}
		}
		if len(scan.TopRecordLengthsByBytes) > 0 {
			fmt.Println("frame_scan_top_record_lengths:")
			for _, row := range scan.TopRecordLengthsByBytes {
				fmt.Printf("  len=%d records=%d raw=%d stored=%d ratio=%.6f\n",
					row.Length,
					row.Records,
					row.Bytes,
					row.StoredBytes,
					floatRatio(row.StoredBytes, row.Bytes),
				)
			}
		}
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

func collectValueLogAudit(dir string, readOnly bool, rewriteOpts treedbdb.ValueLogRewriteOnlineOptions, ridOpts valueLogRIDAuditOptions, frameOpts valueLogFrameScanAuditOptions) (report valueLogAuditReport, err error) {
	report = valueLogAuditReport{Dir: dir}
	mainDir, err := resolveTreemapMainDir(dir)
	if err != nil {
		return report, err
	}
	rootDir := resolveTreemapRootDir(filepath.Clean(dir), mainDir)
	report.MainDir = mainDir
	report.ValueLogDir = treedbdb.ValueLogDirPath(mainDir)
	report.LeafLogDir = treedbdb.LeafLogDirPath(mainDir)

	segs, bytesOnDisk, err := listValueLogSegments(report.ValueLogDir, report.LeafLogDir)
	if err != nil {
		return report, err
	}
	report.Segments = segs
	report.SegmentsOnDisk = len(segs)
	report.BytesOnDisk = bytesOnDisk
	report.RIDScan, err = scanValueLogRIDs(segs, ridOpts)
	if err != nil {
		return report, err
	}
	if frameOpts.Enabled {
		report.FrameScan, err = scanValueLogFrames(segs, frameOpts)
		if err != nil {
			return report, err
		}
	}

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: rootDir, ReadOnly: readOnly})
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

func scanValueLogFrames(segments []valueLogSegmentAudit, opts valueLogFrameScanAuditOptions) (*valueLogFrameScanAudit, error) {
	topN := opts.TopLengths
	if topN <= 0 {
		topN = 10
	}
	maxValueLen := int64(^uint32(0))
	if limits.MaxRecordSize > 0 {
		maxValueLen = limits.MaxRecordSize - int64(valuelog.HeaderSize)
		if maxValueLen < 0 {
			maxValueLen = 0
		}
	}
	out := &valueLogFrameScanAudit{
		Modes:             make(map[string]valueLogFrameModeAudit),
		PageLike4096Modes: make(map[string]valueLogRecordModeAudit),
		Large40To48KModes: make(map[string]valueLogRecordModeAudit),
	}
	byLength := make(map[int]valueLogRecordLengthAudit)
	addLen := func(modeKey string, rawBytes, storedBytes int64) {
		if rawBytes < 0 || rawBytes > maxValueLen {
			return
		}
		length := int(rawBytes)
		row := byLength[length]
		row.Length = length
		row.Records++
		row.Bytes += rawBytes
		row.StoredBytes += storedBytes
		byLength[length] = row
		if length == 4096 {
			out.PageLike4096Records++
			out.PageLike4096Bytes += rawBytes
			out.PageLike4096StoredBytes += storedBytes
			addRecordModeContribution(out.PageLike4096Modes, modeKey, rawBytes, storedBytes)
		}
		if length >= (40<<10) && length <= (48<<10) {
			out.Large40To48KRecords++
			out.Large40To48KBytes += rawBytes
			out.Large40To48KStoredBytes += storedBytes
			addRecordModeContribution(out.Large40To48KModes, modeKey, rawBytes, storedBytes)
		}
	}

	for _, seg := range segments {
		f, err := os.Open(seg.Path)
		if err != nil {
			return nil, err
		}
		reader := bufio.NewReaderSize(f, 1<<20)
		var payloadBuf []byte
		for {
			var header [valuelog.HeaderSize]byte
			if _, err := io.ReadFull(reader, header[:]); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				if errors.Is(err, io.ErrUnexpectedEOF) {
					out.TruncatedSegments++
					break
				}
				_ = f.Close()
				return nil, err
			}
			valueLen := int64(binary.LittleEndian.Uint32(header[16:20]))
			if valueLen > maxValueLen {
				_ = f.Close()
				return nil, valuelog.ErrRecordTooLarge
			}
			payloadLen := int(valueLen)
			if cap(payloadBuf) < payloadLen {
				payloadBuf = make([]byte, payloadLen)
			}
			payload := payloadBuf[:payloadLen]
			if _, err := io.ReadFull(reader, payload); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					out.TruncatedSegments++
					break
				}
				_ = f.Close()
				return nil, err
			}
			flags := header[5]
			if flags&recordFlagGroupedAudit == 0 {
				n := valueLen
				out.RecordsTotal++
				out.UngroupedRecords++
				out.UngroupedBytes += n
				addLen("raw_ungrouped", n, n)

				mode := out.Modes["raw_ungrouped"]
				mode.Frames++
				mode.Subrecords++
				mode.RawPayloadBytes += n
				mode.StoredPayloadBytes += n
				out.Modes["raw_ungrouped"] = mode
				continue
			}

			frameHeader, _, offsets, framePayload, err := valuelog.DecodeFrame(payload)
			if err != nil {
				_ = f.Close()
				return nil, err
			}
			if len(offsets) == 0 {
				_ = f.Close()
				return nil, valuelog.ErrCorrupt
			}
			rawPayloadBytes := int64(offsets[len(offsets)-1])
			storedPayloadBytes := int64(len(framePayload))
			subrecords := int64(len(offsets) - 1)

			out.RecordsTotal += subrecords
			out.GroupedFrames++
			out.GroupedSubrecords += subrecords
			out.GroupedRawPayloadBytes += rawPayloadBytes
			out.GroupedStoredPayload += storedPayloadBytes

			modeKey := groupedFrameModeName(frameHeader)
			mode := out.Modes[modeKey]
			mode.Frames++
			mode.Subrecords += subrecords
			mode.RawPayloadBytes += rawPayloadBytes
			mode.StoredPayloadBytes += storedPayloadBytes
			out.Modes[modeKey] = mode

			recordRawLengths := make([]int64, 0, len(offsets)-1)
			for i := 0; i+1 < len(offsets); i++ {
				recordRawLengths = append(recordRawLengths, int64(offsets[i+1]-offsets[i]))
			}
			recordStoredShares := apportionStoredBytesByRaw(recordRawLengths, storedPayloadBytes)
			for i := 0; i < len(recordRawLengths); i++ {
				addLen(modeKey, recordRawLengths[i], recordStoredShares[i])
			}
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}

	lengths := make([]valueLogRecordLengthAudit, 0, len(byLength))
	for _, row := range byLength {
		lengths = append(lengths, row)
	}
	sort.Slice(lengths, func(i, j int) bool {
		if lengths[i].Bytes == lengths[j].Bytes {
			return lengths[i].Length < lengths[j].Length
		}
		return lengths[i].Bytes > lengths[j].Bytes
	})
	if len(lengths) > topN {
		lengths = lengths[:topN]
	}
	out.TopRecordLengthsByBytes = lengths
	return out, nil
}

func groupedFrameModeName(h valuelog.FrameHeader) string {
	if h.Flags&valuelog.FrameFlagCompressed == 0 {
		return "grouped_raw"
	}
	if h.DictID != 0 {
		return "grouped_dict"
	}
	switch valuelog.BlockCodec(h.Reserved) {
	case valuelog.BlockCodecSnappy:
		return "grouped_block_snappy"
	case valuelog.BlockCodecLZ4:
		return "grouped_block_lz4"
	case valuelog.BlockCodecZSTD:
		return "grouped_block_zstd"
	case valuelog.BlockCodecNone:
		return "grouped_block_none"
	default:
		return fmt.Sprintf("grouped_block_codec_%d", h.Reserved)
	}
}

func floatRatio(num, den int64) float64 {
	if den <= 0 {
		return 0
	}
	return float64(num) / float64(den)
}

func addRecordModeContribution(dst map[string]valueLogRecordModeAudit, modeKey string, rawBytes, storedBytes int64) {
	row := dst[modeKey]
	row.Records++
	row.RawPayloadBytes += rawBytes
	row.StoredPayloadBytes += storedBytes
	dst[modeKey] = row
}

func apportionStoredBytesByRaw(rawLengths []int64, storedTotal int64) []int64 {
	if len(rawLengths) == 0 {
		return nil
	}
	shares := make([]int64, len(rawLengths))
	if storedTotal <= 0 {
		return shares
	}
	var rawTotal int64
	for _, n := range rawLengths {
		if n > 0 {
			rawTotal += n
		}
	}
	if rawTotal <= 0 {
		shares[len(shares)-1] = storedTotal
		return shares
	}
	type remainder struct {
		idx  int
		frac int64
		raw  int64
	}
	ranked := make([]remainder, 0, len(rawLengths))
	var assigned int64
	den := big.NewInt(rawTotal)
	var numerBig big.Int
	var nBig big.Int
	var baseBig big.Int
	var remBig big.Int
	maxSafeN := int64(math.MaxInt64)
	if storedTotal > 0 {
		maxSafeN = math.MaxInt64 / storedTotal
	}
	for i, n := range rawLengths {
		if n <= 0 {
			continue
		}
		var base int64
		var frac int64
		if n <= maxSafeN {
			numer := storedTotal * n
			base = numer / rawTotal
			frac = numer % rawTotal
		} else {
			numerBig.SetInt64(storedTotal)
			nBig.SetInt64(n)
			numerBig.Mul(&numerBig, &nBig)
			baseBig.QuoRem(&numerBig, den, &remBig)
			base = baseBig.Int64()
			frac = remBig.Int64()
		}
		shares[i] = base
		assigned += base
		ranked = append(ranked, remainder{
			idx:  i,
			frac: frac,
			raw:  n,
		})
	}
	leftover := storedTotal - assigned
	if leftover <= 0 || len(ranked) == 0 {
		return shares
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].frac == ranked[j].frac {
			if ranked[i].raw == ranked[j].raw {
				return ranked[i].idx < ranked[j].idx
			}
			return ranked[i].raw > ranked[j].raw
		}
		return ranked[i].frac > ranked[j].frac
	})
	if maxLeftover := int64(len(ranked)); leftover > maxLeftover {
		leftover = maxLeftover
	}
	for i := int64(0); i < leftover; i++ {
		idx := ranked[i].idx
		shares[idx]++
	}
	return shares
}

func printRecordModeBreakdown(label string, modes map[string]valueLogRecordModeAudit) {
	if len(modes) == 0 {
		return
	}
	keys := make([]string, 0, len(modes))
	for k := range modes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("frame_scan_focus_modes(%s):\n", label)
	for _, k := range keys {
		row := modes[k]
		fmt.Printf("  %s records=%d raw=%d stored=%d ratio=%.6f\n",
			k,
			row.Records,
			row.RawPayloadBytes,
			row.StoredPayloadBytes,
			floatRatio(row.StoredPayloadBytes, row.RawPayloadBytes),
		)
	}
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
	if filepath.Base(clean) != "maindb" {
		return clean
	}
	parent := filepath.Dir(mainDir)
	for _, rootSibling := range []string{"dictdb", "templatedb", "ancient"} {
		if info, err := os.Stat(filepath.Join(parent, rootSibling)); err == nil && info.IsDir() {
			return parent
		}
	}
	return clean
}

func listValueLogSegments(dirs ...string) ([]valueLogSegmentAudit, int64, error) {
	var (
		segs  []valueLogSegmentAudit
		total int64
	)
	for _, dir := range dirs {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		info, err := os.Stat(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
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
	}
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].Name == segs[j].Name {
			return segs[i].Path < segs[j].Path
		}
		return segs[i].Name < segs[j].Name
	})
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
