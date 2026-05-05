package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafLogAnalyzeReport struct {
	Dir                         string                      `json:"dir"`
	MainDir                     string                      `json:"main_dir"`
	LeafLogDir                  string                      `json:"leaf_log_dir"`
	Files                       []leafLogAnalyzeFile        `json:"files"`
	Totals                      leafLogAnalyzeTotals        `json:"totals"`
	Ratios                      leafLogAnalyzeRatios        `json:"ratios"`
	Modes                       map[string]leafLogModeStats `json:"modes,omitempty"`
	KHistogram                  []leafLogHistogramEntry     `json:"k_histogram,omitempty"`
	RecordLengths               []leafLogRecordLengthStats  `json:"record_lengths,omitempty"`
	FramePercentile             leafLogFramePercentiles     `json:"frame_percentiles"`
	LeafWeightedFramePercentile leafLogFramePercentiles     `json:"leaf_weighted_frame_percentiles"`
	TopFrames                   []leafLogFrameSummary       `json:"top_frames,omitempty"`
	TruncatedFiles              int                         `json:"truncated_files,omitempty"`
}

type leafLogAnalyzeFile struct {
	Name                  string  `json:"name"`
	Path                  string  `json:"path"`
	Bytes                 int64   `json:"bytes"`
	GzipBytes             int64   `json:"gzip_bytes,omitempty"`
	GzipRatio             float64 `json:"gzip_ratio,omitempty"`
	Frames                int64   `json:"frames"`
	Records               int64   `json:"records"`
	EncodedPayloadBytes   int64   `json:"encoded_payload_bytes"`
	StoredPayloadBytes    int64   `json:"stored_payload_bytes"`
	RecordHeaderBytes     int64   `json:"record_header_bytes"`
	FramePrefixBytes      int64   `json:"frame_prefix_bytes"`
	HeaderAndPrefixBytes  int64   `json:"header_and_prefix_bytes"`
	LogicalLeafBytes      int64   `json:"logical_leaf_bytes"`
	AvgLogBytesPerLeaf    float64 `json:"avg_log_bytes_per_leaf,omitempty"`
	AvgStoredBytesPerLeaf float64 `json:"avg_stored_bytes_per_leaf,omitempty"`
}

type leafLogAnalyzeTotals struct {
	FileCount                    int   `json:"file_count"`
	LogBytes                     int64 `json:"log_bytes"`
	GzipBytes                    int64 `json:"gzip_bytes,omitempty"`
	Frames                       int64 `json:"frames"`
	Records                      int64 `json:"records"`
	LogicalLeafBytes             int64 `json:"logical_leaf_bytes"`
	EncodedPayloadBytes          int64 `json:"encoded_payload_bytes"`
	StoredPayloadBytes           int64 `json:"stored_payload_bytes"`
	RecordHeaderBytes            int64 `json:"record_header_bytes"`
	FramePrefixBytes             int64 `json:"frame_prefix_bytes"`
	HeaderAndPrefixBytes         int64 `json:"header_and_prefix_bytes"`
	LeafPayloadLTPageRecords     int64 `json:"leaf_payload_lt_page_records"`
	LeafPayloadEQPageRecords     int64 `json:"leaf_payload_eq_page_records"`
	LeafPayloadGTPageRecords     int64 `json:"leaf_payload_gt_page_records"`
	CompressedFrames             int64 `json:"compressed_frames"`
	RawFrames                    int64 `json:"raw_frames"`
	DictFrames                   int64 `json:"dict_frames"`
	BlockFrames                  int64 `json:"block_frames"`
	MinimumK                     int   `json:"minimum_k,omitempty"`
	MaximumK                     int   `json:"maximum_k,omitempty"`
	EstimatedLeafPayloadSavings  int64 `json:"estimated_leaf_payload_savings"`
	EstimatedFrameCompressionWin int64 `json:"estimated_frame_compression_win"`
}

type leafLogAnalyzeRatios struct {
	LogicalToLog               float64 `json:"logical_to_log,omitempty"`
	LogToLogical               float64 `json:"log_to_logical,omitempty"`
	EncodedPayloadToLogical    float64 `json:"encoded_payload_to_logical,omitempty"`
	StoredPayloadToEncoded     float64 `json:"stored_payload_to_encoded,omitempty"`
	StoredPayloadToLogical     float64 `json:"stored_payload_to_logical,omitempty"`
	HeaderAndPrefixToLog       float64 `json:"header_and_prefix_to_log,omitempty"`
	GzipToLog                  float64 `json:"gzip_to_log,omitempty"`
	GzipToLogical              float64 `json:"gzip_to_logical,omitempty"`
	AverageLogBytesPerLeaf     float64 `json:"average_log_bytes_per_leaf,omitempty"`
	AverageStoredBytesPerLeaf  float64 `json:"average_stored_bytes_per_leaf,omitempty"`
	AverageEncodedBytesPerLeaf float64 `json:"average_encoded_bytes_per_leaf,omitempty"`
}

type leafLogModeStats struct {
	Frames             int64   `json:"frames"`
	Records            int64   `json:"records"`
	EncodedPayload     int64   `json:"encoded_payload_bytes"`
	StoredPayload      int64   `json:"stored_payload_bytes"`
	RecordBytes        int64   `json:"record_bytes"`
	StoredToEncoded    float64 `json:"stored_to_encoded,omitempty"`
	RecordToLogical    float64 `json:"record_to_logical,omitempty"`
	AvgLogBytesPerLeaf float64 `json:"avg_log_bytes_per_leaf,omitempty"`
}

type leafLogHistogramEntry struct {
	K      int   `json:"k"`
	Frames int64 `json:"frames"`
}

type leafLogRecordLengthStats struct {
	Length  int   `json:"length"`
	Records int64 `json:"records"`
	Bytes   int64 `json:"bytes"`
}

type leafLogFramePercentiles struct {
	EncodedBytesPerLeaf leafLogPercentileSet `json:"encoded_bytes_per_leaf"`
	StoredBytesPerLeaf  leafLogPercentileSet `json:"stored_bytes_per_leaf"`
	LogBytesPerLeaf     leafLogPercentileSet `json:"log_bytes_per_leaf"`
	StoredToEncoded     leafLogPercentileSet `json:"stored_to_encoded"`
}

type leafLogPercentileSet struct {
	P50 float64 `json:"p50,omitempty"`
	P90 float64 `json:"p90,omitempty"`
	P99 float64 `json:"p99,omitempty"`
	Max float64 `json:"max,omitempty"`
}

type leafLogFrameSummary struct {
	File                  string  `json:"file"`
	Offset                int64   `json:"offset"`
	K                     int     `json:"k"`
	Mode                  string  `json:"mode"`
	RecordBytes           int64   `json:"record_bytes"`
	EncodedPayloadBytes   int64   `json:"encoded_payload_bytes"`
	StoredPayloadBytes    int64   `json:"stored_payload_bytes"`
	LogBytesPerLeaf       float64 `json:"log_bytes_per_leaf,omitempty"`
	StoredBytesPerLeaf    float64 `json:"stored_bytes_per_leaf,omitempty"`
	EncodedBytesPerLeaf   float64 `json:"encoded_bytes_per_leaf,omitempty"`
	StoredToEncoded       float64 `json:"stored_to_encoded,omitempty"`
	HeaderAndPrefixBytes  int64   `json:"header_and_prefix_bytes"`
	Compressed            bool    `json:"compressed"`
	DictID                uint64  `json:"dict_id,omitempty"`
	BlockCodec            string  `json:"block_codec,omitempty"`
	MinRecordPayloadBytes int64   `json:"min_record_payload_bytes,omitempty"`
	MaxRecordPayloadBytes int64   `json:"max_record_payload_bytes,omitempty"`
}

type leafLogScanOptions struct {
	TopFrames        int
	TopRecordLengths int
	SkipGzip         bool
}

type leafLogFrameScanScratch struct {
	encodedPerLeaf             []float64
	storedPerLeaf              []float64
	logPerLeaf                 []float64
	storedRatio                []float64
	leafWeightedEncodedPerLeaf []float64
	leafWeightedStoredPerLeaf  []float64
	leafWeightedLogPerLeaf     []float64
	leafWeightedStoredRatio    []float64
	topFrames                  []leafLogFrameSummary
	recordLengths              map[int]leafLogRecordLengthStats
	kHistogram                 map[int]int64
}

func runLeafLogAnalyze(dir string, args []string) {
	fs := flag.NewFlagSet("leaflog-analyze", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	topFrames := fs.Int("top-frames", 12, "Number of largest per-leaf log frames to print (0 disables)")
	topRecordLengths := fs.Int("top-record-lengths", 12, "Number of top encoded leaf payload lengths by bytes (0 disables)")
	skipGzip := fs.Bool("skip-gzip", false, "Skip gzip -9 ceiling measurement")
	_ = fs.Parse(args)

	report, err := collectLeafLogAnalyze(dir, leafLogScanOptions{
		TopFrames:        *topFrames,
		TopRecordLengths: *topRecordLengths,
		SkipGzip:         *skipGzip,
	})
	if err != nil {
		fatalf("leaflog analyze error: %v", err)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fatalf("json encode: %v", err)
		}
		return
	}
	printLeafLogAnalyze(report)
}

func collectLeafLogAnalyze(dir string, opts leafLogScanOptions) (leafLogAnalyzeReport, error) {
	report := leafLogAnalyzeReport{Dir: dir}
	mainDir, err := resolveTreemapMainDir(dir)
	if err != nil {
		return report, err
	}
	report.MainDir = mainDir
	report.LeafLogDir = treedbdb.LeafLogDirPath(mainDir)
	segs, _, err := listValueLogSegments(report.LeafLogDir)
	if err != nil {
		return report, err
	}
	report.Modes = make(map[string]leafLogModeStats)
	scratch := leafLogFrameScanScratch{
		recordLengths: make(map[int]leafLogRecordLengthStats),
		kHistogram:    make(map[int]int64),
	}
	for _, seg := range segs {
		fileStats, truncated, err := scanLeafLogFile(seg, opts, &report, &scratch)
		if err != nil {
			return report, err
		}
		if truncated {
			report.TruncatedFiles++
		}
		report.Files = append(report.Files, fileStats)
		report.Totals.FileCount++
		report.Totals.LogBytes += fileStats.Bytes
		report.Totals.GzipBytes += fileStats.GzipBytes
		report.Totals.Frames += fileStats.Frames
		report.Totals.Records += fileStats.Records
		report.Totals.LogicalLeafBytes += fileStats.LogicalLeafBytes
		report.Totals.EncodedPayloadBytes += fileStats.EncodedPayloadBytes
		report.Totals.StoredPayloadBytes += fileStats.StoredPayloadBytes
		report.Totals.RecordHeaderBytes += fileStats.RecordHeaderBytes
		report.Totals.FramePrefixBytes += fileStats.FramePrefixBytes
		report.Totals.HeaderAndPrefixBytes += fileStats.HeaderAndPrefixBytes
	}
	report.Totals.EstimatedLeafPayloadSavings = report.Totals.LogicalLeafBytes - report.Totals.EncodedPayloadBytes
	report.Totals.EstimatedFrameCompressionWin = report.Totals.EncodedPayloadBytes - report.Totals.StoredPayloadBytes
	report.Ratios = leafLogAnalyzeRatios{
		LogicalToLog:               floatRatio(report.Totals.LogicalLeafBytes, report.Totals.LogBytes),
		LogToLogical:               floatRatio(report.Totals.LogBytes, report.Totals.LogicalLeafBytes),
		EncodedPayloadToLogical:    floatRatio(report.Totals.EncodedPayloadBytes, report.Totals.LogicalLeafBytes),
		StoredPayloadToEncoded:     floatRatio(report.Totals.StoredPayloadBytes, report.Totals.EncodedPayloadBytes),
		StoredPayloadToLogical:     floatRatio(report.Totals.StoredPayloadBytes, report.Totals.LogicalLeafBytes),
		HeaderAndPrefixToLog:       floatRatio(report.Totals.HeaderAndPrefixBytes, report.Totals.LogBytes),
		GzipToLog:                  floatRatio(report.Totals.GzipBytes, report.Totals.LogBytes),
		GzipToLogical:              floatRatio(report.Totals.GzipBytes, report.Totals.LogicalLeafBytes),
		AverageLogBytesPerLeaf:     floatRatio(report.Totals.LogBytes, report.Totals.Records),
		AverageStoredBytesPerLeaf:  floatRatio(report.Totals.StoredPayloadBytes, report.Totals.Records),
		AverageEncodedBytesPerLeaf: floatRatio(report.Totals.EncodedPayloadBytes, report.Totals.Records),
	}
	finalizeLeafLogModeRatios(report.Modes)
	report.KHistogram = leafLogKHistogram(scratch.kHistogram)
	if len(report.KHistogram) > 0 {
		report.Totals.MinimumK = report.KHistogram[0].K
		report.Totals.MaximumK = report.KHistogram[len(report.KHistogram)-1].K
	}
	report.RecordLengths = leafLogTopRecordLengths(scratch.recordLengths, opts.TopRecordLengths)
	report.FramePercentile = computeLeafLogFramePercentiles(scratch)
	report.LeafWeightedFramePercentile = computeLeafLogWeightedFramePercentiles(scratch)
	sort.Slice(scratch.topFrames, func(i, j int) bool {
		if scratch.topFrames[i].LogBytesPerLeaf == scratch.topFrames[j].LogBytesPerLeaf {
			if scratch.topFrames[i].File == scratch.topFrames[j].File {
				return scratch.topFrames[i].Offset < scratch.topFrames[j].Offset
			}
			return scratch.topFrames[i].File < scratch.topFrames[j].File
		}
		return scratch.topFrames[i].LogBytesPerLeaf > scratch.topFrames[j].LogBytesPerLeaf
	})
	if opts.TopFrames > 0 && len(scratch.topFrames) > opts.TopFrames {
		scratch.topFrames = scratch.topFrames[:opts.TopFrames]
	}
	report.TopFrames = scratch.topFrames
	return report, nil
}

func scanLeafLogFile(seg valueLogSegmentAudit, opts leafLogScanOptions, report *leafLogAnalyzeReport, scratch *leafLogFrameScanScratch) (leafLogAnalyzeFile, bool, error) {
	stats := leafLogAnalyzeFile{
		Name:  seg.Name,
		Path:  seg.Path,
		Bytes: seg.Bytes,
	}
	if !opts.SkipGzip {
		gzipBytes, err := gzipFileSize(seg.Path)
		if err != nil {
			return stats, false, err
		}
		stats.GzipBytes = gzipBytes
		stats.GzipRatio = floatRatio(gzipBytes, seg.Bytes)
	}
	f, err := os.Open(seg.Path)
	if err != nil {
		return stats, false, err
	}
	defer func() { _ = f.Close() }()

	maxValueLen := int64(^uint32(0))
	if limits.MaxRecordSize > 0 {
		maxValueLen = limits.MaxRecordSize - int64(valuelog.HeaderSize)
		if maxValueLen < 0 {
			maxValueLen = 0
		}
	}
	reader := bufio.NewReaderSize(f, 1<<20)
	var payloadBuf []byte
	var recordOffset int64
	for {
		var header [valuelog.HeaderSize]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return stats, true, nil
			}
			return stats, false, err
		}
		valueLen := int64(binary.LittleEndian.Uint32(header[16:20]))
		if valueLen > maxValueLen {
			return stats, false, valuelog.ErrRecordTooLarge
		}
		payloadLen := int(valueLen)
		if cap(payloadBuf) < payloadLen {
			payloadBuf = make([]byte, payloadLen)
		}
		payload := payloadBuf[:payloadLen]
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return stats, true, nil
			}
			return stats, false, err
		}
		recordBytes := int64(valuelog.HeaderSize) + valueLen
		if header[5]&recordFlagGroupedAudit == 0 {
			return stats, false, fmt.Errorf("leaf_vlog %s offset %d contains ungrouped record", seg.Path, recordOffset)
		}
		frameHeader, _, offsets, framePayload, err := valuelog.DecodeFrame(payload)
		if err != nil {
			return stats, false, err
		}
		if len(offsets) == 0 {
			return stats, false, valuelog.ErrCorrupt
		}
		k := int(frameHeader.K)
		if k <= 0 || k+1 != len(offsets) {
			return stats, false, valuelog.ErrCorrupt
		}
		framePrefixBytes := int64(valuelog.FrameHeaderSize + (k * 8) + ((k + 1) * 4))
		encodedPayloadBytes := int64(offsets[len(offsets)-1])
		storedPayloadBytes := int64(len(framePayload))
		headerAndPrefixBytes := int64(valuelog.HeaderSize) + framePrefixBytes
		logicalBytes := int64(k * page.PageSize)
		stats.Frames++
		stats.Records += int64(k)
		stats.EncodedPayloadBytes += encodedPayloadBytes
		stats.StoredPayloadBytes += storedPayloadBytes
		stats.RecordHeaderBytes += int64(valuelog.HeaderSize)
		stats.FramePrefixBytes += framePrefixBytes
		stats.HeaderAndPrefixBytes += headerAndPrefixBytes
		stats.LogicalLeafBytes += logicalBytes

		mode := groupedFrameModeName(frameHeader)
		modeStats := report.Modes[mode]
		modeStats.Frames++
		modeStats.Records += int64(k)
		modeStats.EncodedPayload += encodedPayloadBytes
		modeStats.StoredPayload += storedPayloadBytes
		modeStats.RecordBytes += recordBytes
		report.Modes[mode] = modeStats

		if frameHeader.Flags&valuelog.FrameFlagCompressed != 0 {
			report.Totals.CompressedFrames++
			if frameHeader.DictID != 0 {
				report.Totals.DictFrames++
			} else {
				report.Totals.BlockFrames++
			}
		} else {
			report.Totals.RawFrames++
		}
		scratch.kHistogram[k]++
		minRecordLen := int64(math.MaxInt64)
		maxRecordLen := int64(0)
		for i := 0; i < k; i++ {
			n := int64(offsets[i+1] - offsets[i])
			if n < minRecordLen {
				minRecordLen = n
			}
			if n > maxRecordLen {
				maxRecordLen = n
			}
			switch {
			case n < page.PageSize:
				report.Totals.LeafPayloadLTPageRecords++
			case n == page.PageSize:
				report.Totals.LeafPayloadEQPageRecords++
			default:
				report.Totals.LeafPayloadGTPageRecords++
			}
			row := scratch.recordLengths[int(n)]
			row.Length = int(n)
			row.Records++
			row.Bytes += n
			scratch.recordLengths[int(n)] = row
		}
		if minRecordLen == math.MaxInt64 {
			minRecordLen = 0
		}
		encodedPerLeaf := floatRatio(encodedPayloadBytes, int64(k))
		storedPerLeaf := floatRatio(storedPayloadBytes, int64(k))
		logPerLeaf := floatRatio(recordBytes, int64(k))
		storedToEncoded := floatRatio(storedPayloadBytes, encodedPayloadBytes)
		scratch.encodedPerLeaf = append(scratch.encodedPerLeaf, encodedPerLeaf)
		scratch.storedPerLeaf = append(scratch.storedPerLeaf, storedPerLeaf)
		scratch.logPerLeaf = append(scratch.logPerLeaf, logPerLeaf)
		scratch.storedRatio = append(scratch.storedRatio, storedToEncoded)
		scratch.leafWeightedEncodedPerLeaf = appendRepeatedFloat(scratch.leafWeightedEncodedPerLeaf, encodedPerLeaf, k)
		scratch.leafWeightedStoredPerLeaf = appendRepeatedFloat(scratch.leafWeightedStoredPerLeaf, storedPerLeaf, k)
		scratch.leafWeightedLogPerLeaf = appendRepeatedFloat(scratch.leafWeightedLogPerLeaf, logPerLeaf, k)
		scratch.leafWeightedStoredRatio = appendRepeatedFloat(scratch.leafWeightedStoredRatio, storedToEncoded, k)
		if opts.TopFrames > 0 {
			summary := leafLogFrameSummary{
				File:                  filepath.Base(seg.Path),
				Offset:                recordOffset,
				K:                     k,
				Mode:                  mode,
				RecordBytes:           recordBytes,
				EncodedPayloadBytes:   encodedPayloadBytes,
				StoredPayloadBytes:    storedPayloadBytes,
				LogBytesPerLeaf:       logPerLeaf,
				StoredBytesPerLeaf:    storedPerLeaf,
				EncodedBytesPerLeaf:   encodedPerLeaf,
				StoredToEncoded:       storedToEncoded,
				HeaderAndPrefixBytes:  headerAndPrefixBytes,
				Compressed:            frameHeader.Flags&valuelog.FrameFlagCompressed != 0,
				DictID:                frameHeader.DictID,
				MinRecordPayloadBytes: minRecordLen,
				MaxRecordPayloadBytes: maxRecordLen,
			}
			if summary.Compressed && frameHeader.DictID == 0 {
				summary.BlockCodec = blockCodecName(valuelog.BlockCodec(frameHeader.Reserved))
			}
			scratch.topFrames = append(scratch.topFrames, summary)
		}
		recordOffset += recordBytes
	}
	stats.AvgLogBytesPerLeaf = floatRatio(stats.Bytes, stats.Records)
	stats.AvgStoredBytesPerLeaf = floatRatio(stats.StoredPayloadBytes, stats.Records)
	return stats, false, nil
}

func gzipFileSize(path string) (int64, error) {
	in, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = in.Close() }()
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		gz, err := gzip.NewWriterLevel(pw, gzip.BestCompression)
		if err != nil {
			_ = pw.CloseWithError(err)
			errCh <- err
			return
		}
		_, copyErr := io.Copy(gz, in)
		closeErr := gz.Close()
		if copyErr != nil {
			_ = pw.CloseWithError(copyErr)
			errCh <- copyErr
			return
		}
		if closeErr != nil {
			_ = pw.CloseWithError(closeErr)
			errCh <- closeErr
			return
		}
		errCh <- pw.Close()
	}()
	n, copyErr := io.Copy(io.Discard, pr)
	closeErr := pr.Close()
	writeErr := <-errCh
	if copyErr != nil {
		return 0, copyErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if writeErr != nil {
		return 0, writeErr
	}
	return n, nil
}

func finalizeLeafLogModeRatios(modes map[string]leafLogModeStats) {
	for key, row := range modes {
		row.StoredToEncoded = floatRatio(row.StoredPayload, row.EncodedPayload)
		row.RecordToLogical = floatRatio(row.RecordBytes, row.Records*int64(page.PageSize))
		row.AvgLogBytesPerLeaf = floatRatio(row.RecordBytes, row.Records)
		modes[key] = row
	}
}

func leafLogKHistogram(hist map[int]int64) []leafLogHistogramEntry {
	out := make([]leafLogHistogramEntry, 0, len(hist))
	for k, frames := range hist {
		out = append(out, leafLogHistogramEntry{K: k, Frames: frames})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].K < out[j].K })
	return out
}

func leafLogTopRecordLengths(lengths map[int]leafLogRecordLengthStats, topN int) []leafLogRecordLengthStats {
	if topN <= 0 {
		return nil
	}
	out := make([]leafLogRecordLengthStats, 0, len(lengths))
	for _, row := range lengths {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Bytes == out[j].Bytes {
			return out[i].Length < out[j].Length
		}
		return out[i].Bytes > out[j].Bytes
	})
	if len(out) > topN {
		out = out[:topN]
	}
	return out
}

func computeLeafLogFramePercentiles(s leafLogFrameScanScratch) leafLogFramePercentiles {
	return leafLogFramePercentiles{
		EncodedBytesPerLeaf: percentileSet(s.encodedPerLeaf),
		StoredBytesPerLeaf:  percentileSet(s.storedPerLeaf),
		LogBytesPerLeaf:     percentileSet(s.logPerLeaf),
		StoredToEncoded:     percentileSet(s.storedRatio),
	}
}

func computeLeafLogWeightedFramePercentiles(s leafLogFrameScanScratch) leafLogFramePercentiles {
	return leafLogFramePercentiles{
		EncodedBytesPerLeaf: percentileSet(s.leafWeightedEncodedPerLeaf),
		StoredBytesPerLeaf:  percentileSet(s.leafWeightedStoredPerLeaf),
		LogBytesPerLeaf:     percentileSet(s.leafWeightedLogPerLeaf),
		StoredToEncoded:     percentileSet(s.leafWeightedStoredRatio),
	}
}

func appendRepeatedFloat(dst []float64, value float64, count int) []float64 {
	for i := 0; i < count; i++ {
		dst = append(dst, value)
	}
	return dst
}

func percentileSet(values []float64) leafLogPercentileSet {
	if len(values) == 0 {
		return leafLogPercentileSet{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	return leafLogPercentileSet{
		P50: percentile(sorted, 0.50),
		P90: percentile(sorted, 0.90),
		P99: percentile(sorted, 0.99),
		Max: sorted[len(sorted)-1],
	}
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func blockCodecName(codec valuelog.BlockCodec) string {
	switch codec {
	case valuelog.BlockCodecNone:
		return "none"
	case valuelog.BlockCodecSnappy:
		return "snappy"
	case valuelog.BlockCodecLZ4:
		return "lz4"
	default:
		return fmt.Sprintf("codec_%d", codec)
	}
}

func printLeafLogAnalyze(report leafLogAnalyzeReport) {
	fmt.Printf("dir=%s\n", report.Dir)
	fmt.Printf("main_dir=%s\n", report.MainDir)
	fmt.Printf("leaf_log_dir=%s\n", report.LeafLogDir)
	fmt.Printf("totals: files=%d frames=%d records=%d log_bytes=%d gzip_bytes=%d logical_leaf_bytes=%d\n",
		report.Totals.FileCount,
		report.Totals.Frames,
		report.Totals.Records,
		report.Totals.LogBytes,
		report.Totals.GzipBytes,
		report.Totals.LogicalLeafBytes,
	)
	fmt.Printf("payloads: encoded=%d stored=%d header_prefix=%d compact_leaf_savings=%d frame_compression_win=%d\n",
		report.Totals.EncodedPayloadBytes,
		report.Totals.StoredPayloadBytes,
		report.Totals.HeaderAndPrefixBytes,
		report.Totals.EstimatedLeafPayloadSavings,
		report.Totals.EstimatedFrameCompressionWin,
	)
	fmt.Printf("ratios: log/logical=%.6f gzip/log=%.6f gzip/logical=%.6f encoded/logical=%.6f stored/encoded=%.6f header_prefix/log=%.6f\n",
		report.Ratios.LogToLogical,
		report.Ratios.GzipToLog,
		report.Ratios.GzipToLogical,
		report.Ratios.EncodedPayloadToLogical,
		report.Ratios.StoredPayloadToEncoded,
		report.Ratios.HeaderAndPrefixToLog,
	)
	fmt.Printf("per_leaf: log_avg=%.2f stored_avg=%.2f encoded_avg=%.2f\n",
		report.Ratios.AverageLogBytesPerLeaf,
		report.Ratios.AverageStoredBytesPerLeaf,
		report.Ratios.AverageEncodedBytesPerLeaf,
	)
	fmt.Printf("leaf_payload_records: lt_page=%d eq_page=%d gt_page=%d\n",
		report.Totals.LeafPayloadLTPageRecords,
		report.Totals.LeafPayloadEQPageRecords,
		report.Totals.LeafPayloadGTPageRecords,
	)
	fmt.Printf("frames: raw=%d compressed=%d dict=%d block=%d min_k=%d max_k=%d\n",
		report.Totals.RawFrames,
		report.Totals.CompressedFrames,
		report.Totals.DictFrames,
		report.Totals.BlockFrames,
		report.Totals.MinimumK,
		report.Totals.MaximumK,
	)
	fmt.Println("frame_percentiles:")
	printLeafLogPercentiles("  encoded_bytes_per_leaf", report.FramePercentile.EncodedBytesPerLeaf)
	printLeafLogPercentiles("  stored_bytes_per_leaf", report.FramePercentile.StoredBytesPerLeaf)
	printLeafLogPercentiles("  log_bytes_per_leaf", report.FramePercentile.LogBytesPerLeaf)
	printLeafLogPercentiles("  stored_to_encoded", report.FramePercentile.StoredToEncoded)
	fmt.Println("leaf_weighted_frame_percentiles:")
	printLeafLogPercentiles("  encoded_bytes_per_leaf", report.LeafWeightedFramePercentile.EncodedBytesPerLeaf)
	printLeafLogPercentiles("  stored_bytes_per_leaf", report.LeafWeightedFramePercentile.StoredBytesPerLeaf)
	printLeafLogPercentiles("  log_bytes_per_leaf", report.LeafWeightedFramePercentile.LogBytesPerLeaf)
	printLeafLogPercentiles("  stored_to_encoded", report.LeafWeightedFramePercentile.StoredToEncoded)
	if len(report.KHistogram) > 0 {
		fmt.Println("k_histogram:")
		for _, row := range report.KHistogram {
			fmt.Printf("  k=%d frames=%d\n", row.K, row.Frames)
		}
	}
	if len(report.Modes) > 0 {
		keys := make([]string, 0, len(report.Modes))
		for key := range report.Modes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Println("modes:")
		for _, key := range keys {
			row := report.Modes[key]
			fmt.Printf("  %s frames=%d records=%d record_bytes=%d encoded=%d stored=%d stored/encoded=%.6f log/leaf=%.2f\n",
				key,
				row.Frames,
				row.Records,
				row.RecordBytes,
				row.EncodedPayload,
				row.StoredPayload,
				row.StoredToEncoded,
				row.AvgLogBytesPerLeaf,
			)
		}
	}
	if len(report.Files) > 0 {
		fmt.Println("files:")
		for _, file := range report.Files {
			fmt.Printf("  %s bytes=%d gzip=%d gzip/log=%.6f frames=%d records=%d log/leaf=%.2f stored/leaf=%.2f\n",
				file.Path,
				file.Bytes,
				file.GzipBytes,
				file.GzipRatio,
				file.Frames,
				file.Records,
				file.AvgLogBytesPerLeaf,
				file.AvgStoredBytesPerLeaf,
			)
		}
	}
	if len(report.RecordLengths) > 0 {
		fmt.Println("top_encoded_leaf_payload_lengths:")
		for _, row := range report.RecordLengths {
			fmt.Printf("  len=%d records=%d bytes=%d\n", row.Length, row.Records, row.Bytes)
		}
	}
	if len(report.TopFrames) > 0 {
		fmt.Println("top_frames_by_log_bytes_per_leaf:")
		for _, frame := range report.TopFrames {
			extra := ""
			if strings.TrimSpace(frame.BlockCodec) != "" {
				extra = " block=" + frame.BlockCodec
			}
			if frame.DictID != 0 {
				extra = fmt.Sprintf(" dict=%d", frame.DictID)
			}
			fmt.Printf("  %s:%d k=%d mode=%s%s log/leaf=%.2f stored/leaf=%.2f encoded/leaf=%.2f stored/encoded=%.6f min_payload=%d max_payload=%d\n",
				frame.File,
				frame.Offset,
				frame.K,
				frame.Mode,
				extra,
				frame.LogBytesPerLeaf,
				frame.StoredBytesPerLeaf,
				frame.EncodedBytesPerLeaf,
				frame.StoredToEncoded,
				frame.MinRecordPayloadBytes,
				frame.MaxRecordPayloadBytes,
			)
		}
	}
}

func printLeafLogPercentiles(label string, set leafLogPercentileSet) {
	fmt.Printf("%s: p50=%.2f p90=%.2f p99=%.2f max=%.2f\n", label, set.P50, set.P90, set.P99, set.Max)
}
