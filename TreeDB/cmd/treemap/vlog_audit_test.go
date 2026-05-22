package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCollectValueLogAudit_WiresDictLookupFromRoot(t *testing.T) {
	dir := t.TempDir()
	buildDictCompressedDBForAudit(t, dir)

	report, err := collectValueLogAudit(dir, true, treedbdb.ValueLogRewriteOnlineOptions{}, valueLogRIDAuditOptions{}, valueLogFrameScanAuditOptions{})
	if err != nil {
		t.Fatalf("collectValueLogAudit(root): %v", err)
	}
	if report.MainDir != filepath.Join(dir, "maindb") {
		t.Fatalf("unexpected main dir: got=%q want=%q", report.MainDir, filepath.Join(dir, "maindb"))
	}
	if report.SegmentsOnDisk == 0 {
		t.Fatalf("expected on-disk value-log segments")
	}
	if report.GCDryRun.SegmentsTotal == 0 {
		t.Fatalf("expected GC dry-run to observe value-log segments: %+v", report.GCDryRun)
	}
	if report.RIDScan.Records == 0 || report.RIDScan.MaxRID == 0 {
		t.Fatalf("expected RID scan to observe value-log records: %+v", report.RIDScan)
	}
	if report.RewritePlan.SegmentsTotal == 0 {
		t.Fatalf("expected rewrite plan to observe value-log segments: %+v", report.RewritePlan)
	}
	if got := report.Stats["cosmos.db.type"]; got != "treedb" {
		t.Fatalf("unexpected stats db type: %q", got)
	}
}

func TestCollectValueLogAudit_AcceptsMainDBDir(t *testing.T) {
	dir := t.TempDir()
	buildDictCompressedDBForAudit(t, dir)

	report, err := collectValueLogAudit(filepath.Join(dir, "maindb"), true, treedbdb.ValueLogRewriteOnlineOptions{}, valueLogRIDAuditOptions{}, valueLogFrameScanAuditOptions{})
	if err != nil {
		t.Fatalf("collectValueLogAudit(maindb): %v", err)
	}
	if report.MainDir != filepath.Join(dir, "maindb") {
		t.Fatalf("unexpected main dir: got=%q want=%q", report.MainDir, filepath.Join(dir, "maindb"))
	}
	if report.SegmentsOnDisk == 0 || report.BytesOnDisk == 0 {
		t.Fatalf("expected segment inventory, got segments=%d bytes=%d", report.SegmentsOnDisk, report.BytesOnDisk)
	}
	if report.GCDryRun.SegmentsTotal == 0 {
		t.Fatalf("expected GC dry-run to observe value-log segments from maindb path: %+v", report.GCDryRun)
	}
	if report.RewritePlan.SegmentsTotal == 0 {
		t.Fatalf("expected rewrite plan to observe value-log segments from maindb path: %+v", report.RewritePlan)
	}
	if got := report.Stats["cosmos.db.type"]; got != "treedb" {
		t.Fatalf("unexpected stats db type from maindb path: %q", got)
	}
}

func TestCollectValueLogAuditDoesNotCreateEmptyValueLogLaneFiles(t *testing.T) {
	dir := t.TempDir()
	buildDictCompressedDBForAudit(t, dir)
	valueLogDir := filepath.Join(dir, "maindb", "value_vlog")
	beforeFiles, beforeZero := countValueLogFilesForTest(t, valueLogDir)

	if _, err := collectValueLogAudit(dir, true, treedbdb.ValueLogRewriteOnlineOptions{}, valueLogRIDAuditOptions{}, valueLogFrameScanAuditOptions{}); err != nil {
		t.Fatalf("collectValueLogAudit: %v", err)
	}

	afterFiles, afterZero := countValueLogFilesForTest(t, valueLogDir)
	if afterFiles != beforeFiles || afterZero != beforeZero {
		t.Fatalf("value-log file counts changed after audit: before files=%d zero=%d after files=%d zero=%d", beforeFiles, beforeZero, afterFiles, afterZero)
	}
}

func TestCollectValueLogAudit_IncludesLeafLogSegments(t *testing.T) {
	dir := t.TempDir()
	buildDictCompressedDBForAudit(t, dir)

	leafDir := treedbdb.LeafLogDirPath(filepath.Join(dir, "maindb"))
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(leaf log): %v", err)
	}
	fileID := mustEncodeAuditFileID(t, 255, 1)
	w, err := valuelog.NewWriter(filepath.Join(leafDir, "value-l255-000001.log"), fileID)
	if err != nil {
		t.Fatalf("NewWriter(leaf log): %v", err)
	}
	if _, err := w.Append(0, nil, 9001, bytes.Repeat([]byte("leaf"), 1024)); err != nil {
		_ = w.Close()
		t.Fatalf("Append(leaf log): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close(leaf log): %v", err)
	}

	report, err := collectValueLogAudit(dir, true, treedbdb.ValueLogRewriteOnlineOptions{}, valueLogRIDAuditOptions{}, valueLogFrameScanAuditOptions{})
	if err != nil {
		t.Fatalf("collectValueLogAudit(with leaf_vlog): %v", err)
	}
	if report.LeafLogDir != leafDir {
		t.Fatalf("LeafLogDir=%q want %q", report.LeafLogDir, leafDir)
	}
	if report.SegmentsOnDisk < 2 {
		t.Fatalf("expected combined value+leaf segments, got segments=%d", report.SegmentsOnDisk)
	}
	if report.RIDScan.MaxRID < 9001 {
		t.Fatalf("expected RID scan to include leaf_vlog record, got %+v", report.RIDScan)
	}
}

func TestParseValueLogAuditFileID_AcceptsLegacyAndLaneNames(t *testing.T) {
	tests := []struct {
		name   string
		want   uint32
		wantOK bool
	}{
		{
			name:   "value-42.log",
			want:   mustEncodeAuditFileID(t, 0, 42),
			wantOK: true,
		},
		{
			name:   "value-l7-42.log",
			want:   mustEncodeAuditFileID(t, 7, 42),
			wantOK: true,
		},
		{
			name:   "value-lbad-42.log",
			wantOK: false,
		},
	}
	for _, tc := range tests {
		got, ok := parseValueLogAuditFileID(tc.name)
		if ok != tc.wantOK {
			t.Fatalf("parseValueLogAuditFileID(%q) ok=%v want %v", tc.name, ok, tc.wantOK)
		}
		if got != tc.want {
			t.Fatalf("parseValueLogAuditFileID(%q)=%d want %d", tc.name, got, tc.want)
		}
	}
}

func TestScanValueLogRIDs_ReportsTruncatedSegments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	if err := os.WriteFile(path, []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	report, err := scanValueLogRIDs([]valueLogSegmentAudit{{Name: filepath.Base(path), Path: path, Bytes: 3}}, valueLogRIDAuditOptions{})
	if err != nil {
		t.Fatalf("scanValueLogRIDs: %v", err)
	}
	if report.TruncatedSegments != 1 {
		t.Fatalf("TruncatedSegments=%d want 1", report.TruncatedSegments)
	}
}

func TestScanValueLogFrames_RejectsOversizedRecordLength(t *testing.T) {
	if limits.MaxRecordSize <= 0 {
		t.Skip("record size cap disabled")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	header := make([]byte, valuelog.HeaderSize)
	binary.LittleEndian.PutUint32(header[16:20], ^uint32(0))
	if err := os.WriteFile(path, header, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	report, err := scanValueLogFrames([]valueLogSegmentAudit{{Name: filepath.Base(path), Path: path, Bytes: int64(len(header))}}, valueLogFrameScanAuditOptions{Enabled: true})
	if !errors.Is(err, valuelog.ErrRecordTooLarge) {
		t.Fatalf("scanValueLogFrames error=%v want %v", err, valuelog.ErrRecordTooLarge)
	}
	if report != nil {
		t.Fatalf("scanValueLogFrames report=%+v want nil on oversized record", report)
	}
}

func TestScanValueLogRIDs_StopOnFirstDuplicate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	fileID := mustEncodeAuditFileID(t, 0, 1)
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, []byte("a")); err != nil {
		_ = w.Close()
		t.Fatalf("Append rid=1 #1: %v", err)
	}
	if _, err := w.Append(0, nil, 1, []byte("b")); err != nil {
		_ = w.Close()
		t.Fatalf("Append rid=1 #2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	report, err := scanValueLogRIDs([]valueLogSegmentAudit{{Name: filepath.Base(path), Path: path, Bytes: 0}}, valueLogRIDAuditOptions{
		StopOnFirstDuplicate: true,
	})
	if err != nil {
		t.Fatalf("scanValueLogRIDs(stop on duplicate): %v", err)
	}
	if report.DuplicateRIDs != 1 {
		t.Fatalf("DuplicateRIDs=%d want 1", report.DuplicateRIDs)
	}
	if report.FirstDuplicateRID != 1 {
		t.Fatalf("FirstDuplicateRID=%d want 1", report.FirstDuplicateRID)
	}
}

func TestScanValueLogRIDs_MaxTrackedRIDs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	fileID := mustEncodeAuditFileID(t, 0, 1)
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, []byte("a")); err != nil {
		_ = w.Close()
		t.Fatalf("Append rid=1: %v", err)
	}
	if _, err := w.Append(0, nil, 2, []byte("b")); err != nil {
		_ = w.Close()
		t.Fatalf("Append rid=2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := scanValueLogRIDs([]valueLogSegmentAudit{{Name: filepath.Base(path), Path: path, Bytes: 0}}, valueLogRIDAuditOptions{
		MaxTrackedRIDs: 1,
	}); err == nil {
		t.Fatal("expected max tracked RIDs error")
	}
}

func TestCollectValueLogAudit_FrameScanIncludesModeAndLengthBreakdown(t *testing.T) {
	dir := t.TempDir()
	buildDictCompressedDBForAudit(t, dir)

	report, err := collectValueLogAudit(dir, true, treedbdb.ValueLogRewriteOnlineOptions{}, valueLogRIDAuditOptions{}, valueLogFrameScanAuditOptions{
		Enabled:    true,
		TopLengths: 8,
	})
	if err != nil {
		t.Fatalf("collectValueLogAudit(frame scan): %v", err)
	}
	if report.FrameScan == nil {
		t.Fatalf("expected frame scan report")
	}
	if report.FrameScan.RecordsTotal == 0 {
		t.Fatalf("expected non-zero frame scan records")
	}
	dictMode, ok := report.FrameScan.Modes["grouped_dict"]
	if !ok || dictMode.Frames == 0 || dictMode.Subrecords == 0 {
		t.Fatalf("expected grouped_dict mode stats, got=%+v", report.FrameScan.Modes)
	}
	found16K := false
	for _, row := range report.FrameScan.TopRecordLengthsByBytes {
		if row.Length == 16<<10 && row.Records > 0 && row.Bytes > 0 && row.StoredBytes > 0 {
			found16K = true
			break
		}
	}
	if !found16K {
		t.Fatalf("expected 16KiB record length in top lengths, got=%+v", report.FrameScan.TopRecordLengthsByBytes)
	}
}

func TestScanValueLogFrames_FocusModeBreakdownByRecordLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	fileID := mustEncodeAuditFileID(t, 0, 1)
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte{0x11}, 4096)); err != nil {
		_ = w.Close()
		t.Fatalf("Append(4KiB): %v", err)
	}
	if _, err := w.Append(0, nil, 2, bytes.Repeat([]byte{0x22}, 43008)); err != nil {
		_ = w.Close()
		t.Fatalf("Append(43KiB): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	scan, err := scanValueLogFrames([]valueLogSegmentAudit{{
		Name:  filepath.Base(path),
		Path:  path,
		Bytes: 0,
	}}, valueLogFrameScanAuditOptions{Enabled: true, TopLengths: 8})
	if err != nil {
		t.Fatalf("scanValueLogFrames: %v", err)
	}
	if scan.PageLike4096Records != 1 || scan.PageLike4096Bytes != 4096 || scan.PageLike4096StoredBytes != 4096 {
		t.Fatalf("unexpected 4KiB focus totals: records=%d bytes=%d stored=%d", scan.PageLike4096Records, scan.PageLike4096Bytes, scan.PageLike4096StoredBytes)
	}
	pageMode, ok := scan.PageLike4096Modes["raw_ungrouped"]
	if !ok {
		pageMode, ok = scan.PageLike4096Modes["grouped_raw"]
	}
	if !ok {
		t.Fatalf("expected raw_ungrouped or grouped_raw mode for 4KiB focus, got=%+v", scan.PageLike4096Modes)
	}
	if pageMode.Records != 1 || pageMode.RawPayloadBytes != 4096 || pageMode.StoredPayloadBytes != 4096 {
		t.Fatalf("unexpected 4KiB mode totals: %+v", pageMode)
	}
	if scan.Large40To48KRecords != 1 || scan.Large40To48KBytes != 43008 || scan.Large40To48KStoredBytes != 43008 {
		t.Fatalf("unexpected 40-48KiB focus totals: records=%d bytes=%d stored=%d", scan.Large40To48KRecords, scan.Large40To48KBytes, scan.Large40To48KStoredBytes)
	}
	largeMode, ok := scan.Large40To48KModes["raw_ungrouped"]
	if !ok {
		largeMode, ok = scan.Large40To48KModes["grouped_raw"]
	}
	if !ok {
		t.Fatalf("expected raw_ungrouped or grouped_raw mode for 40-48KiB focus, got=%+v", scan.Large40To48KModes)
	}
	if largeMode.Records != 1 || largeMode.RawPayloadBytes != 43008 || largeMode.StoredPayloadBytes != 43008 {
		t.Fatalf("unexpected 40-48KiB mode totals: %+v", largeMode)
	}
}

func TestApportionStoredBytesByRaw_ConservesTotals(t *testing.T) {
	raw := []int64{43008, 43008, 4096}
	shares := apportionStoredBytesByRaw(raw, 1000)
	if len(shares) != len(raw) {
		t.Fatalf("shares len=%d want=%d", len(shares), len(raw))
	}
	var sum int64
	for _, n := range shares {
		sum += n
	}
	if sum != 1000 {
		t.Fatalf("sum(shares)=%d want=1000", sum)
	}
	if shares[0] != shares[1] {
		t.Fatalf("expected equal shares for equal raw lengths, got %d vs %d", shares[0], shares[1])
	}
	if shares[2] >= shares[0] {
		t.Fatalf("expected smaller share for 4KiB value, got shares=%v", shares)
	}
	// Allocation should be independent of record order.
	ordered := apportionStoredBytesByRaw([]int64{100, 1, 1}, 1)
	if ordered[0] != 1 {
		t.Fatalf("expected largest record to receive leftover byte, got shares=%v", ordered)
	}
	reordered := apportionStoredBytesByRaw([]int64{1, 1, 100}, 1)
	if reordered[2] != 1 {
		t.Fatalf("expected largest reordered record to receive leftover byte, got shares=%v", reordered)
	}

	zeroRaw := apportionStoredBytesByRaw([]int64{0, 0}, 7)
	if len(zeroRaw) != 2 || zeroRaw[0] != 0 || zeroRaw[1] != 7 {
		t.Fatalf("zero-raw apportion unexpected: %v", zeroRaw)
	}

	// Large-value apportioning must avoid integer overflow and conserve totals.
	largeRaw := []int64{math.MaxInt32, math.MaxInt32 - 1, 1}
	largeStored := int64(math.MaxInt64 - 12345)
	largeShares := apportionStoredBytesByRaw(largeRaw, largeStored)
	if len(largeShares) != len(largeRaw) {
		t.Fatalf("large shares len=%d want=%d", len(largeShares), len(largeRaw))
	}
	var largeSum int64
	for _, n := range largeShares {
		if n < 0 {
			t.Fatalf("expected non-negative large share, got %d in %v", n, largeShares)
		}
		largeSum += n
	}
	if largeSum != largeStored {
		t.Fatalf("sum(large shares)=%d want=%d shares=%v", largeSum, largeStored, largeShares)
	}
}

func mustEncodeAuditFileID(t *testing.T, lane, seq uint32) uint32 {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
	}
	return fileID
}

func buildDictCompressedDBForAudit(t *testing.T, dir string) {
	t.Helper()

	bgErrCh := make(chan error, 16)
	opts := treedb.Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
			Compression:      treedb.ValueLogCompressionAuto,
			AutoPolicy:       treedb.ValueLogAutoSize,
			CompressionAutotune: treedb.AutotuneOptions{
				Mode: treedb.AutotuneOff,
			},
			DictAdaptiveRatio: -1,
		},
		NotifyError: func(err error) {
			select {
			case bgErrCh <- err:
			default:
			}
		},
	}
	treedb.EnableValueLogDictCompression(&opts)

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const valueSize = 16 << 10
	base := bytes.Repeat([]byte("compressible-"), valueSize/len("compressible-")+1)[:valueSize]
	writeBatch := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			key := []byte(prefix + strconv.Itoa(i))
			val := make([]byte, valueSize)
			copy(val, base)
			binary.LittleEndian.PutUint32(val[valueSize-4:], uint32(i))
			if err := db.Set(key, val); err != nil {
				t.Fatalf("Set(%q): %v", key, err)
			}
		}
	}

	writeBatch("phase1-", 128)
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint(phase1): %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-bgErrCh:
			_ = db.Close()
			t.Fatalf("background error: %v", err)
		default:
		}
		stats := db.Stats()
		if stats != nil && stats["treedb.cache.vlog_dict.last_applied_dict_id"] != "0" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	writeBatch("phase2-", 512)
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint(phase2): %v", err)
	}
	stats := db.Stats()
	if stats == nil {
		_ = db.Close()
		t.Fatalf("Stats: nil")
	}
	if stats["treedb.cache.vlog_auto.frames.dict"] == "0" {
		_ = db.Close()
		t.Fatalf("expected dict frames, got stats=%v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func countValueLogFilesForTest(t *testing.T, dir string) (files int, zero int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "value-") || (!strings.HasSuffix(name, ".log") && !strings.HasSuffix(name, ".log.gz")) {
			continue
		}
		files++
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("Info(%s): %v", name, err)
		}
		if info.Size() == 0 {
			zero++
		}
	}
	return files, zero
}
