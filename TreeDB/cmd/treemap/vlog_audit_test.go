package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestBuildValueLogAuditRewriteOptions_Defaults(t *testing.T) {
	opts := buildValueLogAuditRewriteOptions(valueLogAuditRewriteFlagOptions{})
	if opts.MaxSourceSegments != 0 || opts.MaxSourceBytes != 0 {
		t.Fatalf("unexpected source caps: %+v", opts)
	}
	if opts.MinSegmentStaleRatio != 0 || opts.MinSegmentStaleBytes != 0 || opts.MinAggregateStaleBytes != 0 {
		t.Fatalf("unexpected stale defaults: %+v", opts)
	}
}

func TestBuildValueLogAuditRewriteOptions_SchedulerLikeDefaults(t *testing.T) {
	opts := buildValueLogAuditRewriteOptions(valueLogAuditRewriteFlagOptions{
		schedulerLike:           true,
		maxBytes:                128 << 20,
		schedulerHotTargetBytes: 256 << 20,
	})
	if got, want := opts.MinSegmentStaleRatio, 0.20; got != want {
		t.Fatalf("MinSegmentStaleRatio=%f want=%f", got, want)
	}
	if got, want := opts.MinSegmentStaleBytes, int64(1); got != want {
		t.Fatalf("MinSegmentStaleBytes=%d want=%d", got, want)
	}
	if got, want := opts.MinAggregateStaleBytes, int64(32<<20); got != want {
		t.Fatalf("MinAggregateStaleBytes=%d want=%d", got, want)
	}
}

func TestBuildValueLogAuditRewriteOptions_SchedulerLikeHonorsExplicitOverrides(t *testing.T) {
	opts := buildValueLogAuditRewriteOptions(valueLogAuditRewriteFlagOptions{
		schedulerLike:           true,
		maxBytes:                64 << 20,
		minStaleRatio:           0.35,
		minStaleBytes:           9,
		minAggregateStaleBytes:  1234,
		schedulerHotTargetBytes: 256 << 20,
	})
	if got, want := opts.MinSegmentStaleRatio, 0.35; got != want {
		t.Fatalf("MinSegmentStaleRatio=%f want=%f", got, want)
	}
	if got, want := opts.MinSegmentStaleBytes, int64(9); got != want {
		t.Fatalf("MinSegmentStaleBytes=%d want=%d", got, want)
	}
	if got, want := opts.MinAggregateStaleBytes, int64(1234); got != want {
		t.Fatalf("MinAggregateStaleBytes=%d want=%d", got, want)
	}
}

func TestCollectValueLogAudit_WiresDictLookupFromRoot(t *testing.T) {
	dir := t.TempDir()
	buildDictCompressedDBForAudit(t, dir)

	report, err := collectValueLogAudit(dir, treedbdb.ValueLogRewriteOnlineOptions{}, valueLogRIDAuditOptions{})
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
	if report.RIDScanMS <= 0 {
		t.Fatalf("expected positive rid scan timing, got=%f", report.RIDScanMS)
	}
	if report.RewritePlan.SegmentsTotal == 0 {
		t.Fatalf("expected rewrite plan to observe value-log segments: %+v", report.RewritePlan)
	}
	if report.GCDryRunMS <= 0 {
		t.Fatalf("expected positive gc dry-run timing, got=%f", report.GCDryRunMS)
	}
	if report.RewritePlanMS <= 0 {
		t.Fatalf("expected positive rewrite-plan timing, got=%f", report.RewritePlanMS)
	}
	if got := report.Stats["cosmos.db.type"]; got != "treedb" {
		t.Fatalf("unexpected stats db type: %q", got)
	}
}

func TestCollectValueLogAudit_AcceptsMainDBDir(t *testing.T) {
	dir := t.TempDir()
	buildDictCompressedDBForAudit(t, dir)

	report, err := collectValueLogAudit(filepath.Join(dir, "maindb"), treedbdb.ValueLogRewriteOnlineOptions{}, valueLogRIDAuditOptions{})
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
	if report.GCDryRunMS <= 0 {
		t.Fatalf("expected positive gc dry-run timing, got=%f", report.GCDryRunMS)
	}
	if report.RewritePlan.SegmentsTotal == 0 {
		t.Fatalf("expected rewrite plan to observe value-log segments from maindb path: %+v", report.RewritePlan)
	}
	if report.RewritePlanMS <= 0 {
		t.Fatalf("expected positive rewrite-plan timing, got=%f", report.RewritePlanMS)
	}
	if got := report.Stats["cosmos.db.type"]; got != "treedb" {
		t.Fatalf("unexpected stats db type from maindb path: %q", got)
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
			ForcePointers:    true,
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
