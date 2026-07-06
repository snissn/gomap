package db

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func commandWALTestStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %s", key)
	}
	got, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return got
}

func TestCommandWALStatsProveModeAndFrames(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true, WALMaxSegmentBytes: 1024})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			_ = db.Close()
		}
	})
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.command_wal.enabled"]; got != "true" {
		t.Fatalf("command WAL enabled stat=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.required_feature"]; got != "true" {
		t.Fatalf("command WAL required feature stat=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.applied_command_lsn"]; got != "1" {
		t.Fatalf("applied command LSN stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.frames"]; got != "1" {
		t.Fatalf("command WAL frame stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.live_accepted_frames"]; got != "1" {
		t.Fatalf("live accepted frame stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.live_covered_frames"]; got != "1" {
		t.Fatalf("live covered frame stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.append.count_total"); got != 1 {
		t.Fatalf("append.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.append.entry_scan.count_total"); got != 1 {
		t.Fatalf("append.entry_scan.count_total=%d, want 1", got)
	}
	_ = commandWALTestStatUint64(t, stats, "treedb.command_wal.append.ns_total")
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.sync.count_total"); got != 1 {
		t.Fatalf("sync.count_total=%d, want 1 (stats=%#v)", got, stats)
	}
	_ = commandWALTestStatUint64(t, stats, "treedb.command_wal.sync.ns_total")
	if got := stats["treedb.command_wal.typed_segments"]; got != "1" {
		t.Fatalf("command WAL typed segment stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.stats_error"]; got != "" {
		t.Fatalf("unexpected command WAL stats error %q (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.writer.command_buffer.limit_bytes"]; got != "67108864" {
		t.Fatalf("command WAL command buffer limit=%q, want 67108864 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.writer.command_buffer.retain_limit_bytes"]; got != "4194304" {
		t.Fatalf("command WAL command buffer retain limit=%q, want 4194304 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.writer.buffer_size_bytes"]; got != "16777216" {
		t.Fatalf("command WAL writer buffer size=%q, want 16777216 (stats=%#v)", got, stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dbClosed = true
}

func TestCommandWALRuntimeStatsExposeAppendPaths(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.Stats()
	for _, key := range []string{
		"treedb.command_wal.append.count_total",
		"treedb.command_wal.append.point.count_total",
		"treedb.command_wal.append.payload.count_total",
		"treedb.command_wal.append.entry_scan.count_total",
		"treedb.command_wal.flush.count_total",
		"treedb.command_wal.sync.count_total",
	} {
		if got := commandWALTestStatUint64(t, before, key); got != 0 {
			t.Fatalf("%s before writes=%d, want 0 (stats=%#v)", key, got, before)
		}
	}

	if _, err := db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("point"), []byte("one"), false); err != nil {
		t.Fatalf("AppendRawKVPointCommandWALTrusted: %v", err)
	}
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:    commitlog.RawKVOpSet,
		Key:   []byte("payload"),
		Value: []byte("two"),
	}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	if _, err := db.AppendRawKVBatchPayloadCommandWALTrusted(payload, false); err != nil {
		t.Fatalf("AppendRawKVBatchPayloadCommandWALTrusted: %v", err)
	}
	if _, err := db.AppendRawKVCommandWALOrderedEntries([]batchpkg.Entry{{
		Type:  batchpkg.OpPut,
		Key:   []byte("entry-scan"),
		Value: []byte("three"),
	}}, true); err != nil {
		t.Fatalf("AppendRawKVCommandWALOrderedEntries: %v", err)
	}

	stats := db.Stats()
	for key, want := range map[string]uint64{
		"treedb.command_wal.append.count_total":            3,
		"treedb.command_wal.append.point.count_total":      1,
		"treedb.command_wal.append.payload.count_total":    1,
		"treedb.command_wal.append.entry_scan.count_total": 1,
		"treedb.command_wal.append.intent.count_total":     0,
		"treedb.command_wal.flush.count_total":             2,
		"treedb.command_wal.sync.count_total":              1,
	} {
		if got := commandWALTestStatUint64(t, stats, key); got != want {
			t.Fatalf("%s=%d, want %d (stats=%#v)", key, got, want, stats)
		}
	}
	for _, key := range []string{
		"treedb.command_wal.append.ns_total",
		"treedb.command_wal.flush.ns_total",
		"treedb.command_wal.sync.ns_total",
	} {
		// Path-level count stats above prove the append/flush/sync paths ran.
		// Short operations can report zero elapsed nanoseconds on platforms
		// with coarser visible clock granularity, especially Windows runners.
		_ = commandWALTestStatUint64(t, stats, key)
	}
	for _, key := range []string{
		"treedb.command_wal.append.point.ns_total",
		"treedb.command_wal.append.payload.ns_total",
		"treedb.command_wal.append.entry_scan.ns_total",
	} {
		// Sub-path timers can round to zero when the observed work is shorter
		// than the platform clock's visible granularity; count stats above prove
		// the paths were reached.
		_ = commandWALTestStatUint64(t, stats, key)
	}
}

func TestCommandWALStatsDefaultDoesNotScanWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			_ = db.Close()
		}
	})
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := os.Rename(formatConfigPath(dir), formatConfigPath(dir)+".hidden"); err != nil {
		t.Fatalf("hide format config after open: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.required_feature"]; got != "true" {
		t.Fatalf("required feature stat=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.required_feature_error"]; got != "" {
		t.Fatalf("required feature error=%q, want cached open-time value without stat-time I/O (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.stats_scan"]; got != "false" {
		t.Fatalf("stats_scan=%q, want false (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.frames"]; got != "0" {
		t.Fatalf("frames=%q, want 0 without diagnostic scan (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.live_accepted_frames"]; got != "1" {
		t.Fatalf("live accepted frames=%q, want 1 without diagnostic scan (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.live_accepted_max_lsn"]; got != "1" {
		t.Fatalf("live accepted max lsn=%q, want 1 without diagnostic scan (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.live_covered_frames"]; got != "1" {
		t.Fatalf("live covered frames=%q, want 1 without diagnostic scan (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.live_covered_max_lsn"]; got != "1" {
		t.Fatalf("live covered max lsn=%q, want 1 without diagnostic scan (stats=%#v)", got, stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dbClosed = true
}

func TestCommandWALStatsExposeDeferredCommandBufferTrim(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			_ = db.Close()
		}
	})

	value := bytes.Repeat([]byte("x"), commandWALDeferredPointBufferRetainSize+8192)
	if _, err := db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("large"), value, false); err != nil {
		t.Fatalf("AppendRawKVPointCommandWALTrusted: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.command_wal.writer.command_buffer.length_bytes"]; got != "0" {
		t.Fatalf("command buffer len=%q, want 0 after append flush (stats=%#v)", got, stats)
	}
	if got, want := stats["treedb.command_wal.writer.command_buffer.capacity_bytes"], strconv.Itoa(commandWALDeferredPointBufferRetainSize); got != want {
		t.Fatalf("command buffer cap=%q, want retain cap %s (stats=%#v)", got, want, stats)
	}
	if got := stats["treedb.command_wal.writer.command_buffer.trim_count"]; got != "1" {
		t.Fatalf("command buffer trim count=%q, want 1 (stats=%#v)", got, stats)
	}
	dropped, err := strconv.ParseUint(stats["treedb.command_wal.writer.command_buffer.dropped_bytes_total"], 10, 64)
	if err != nil {
		t.Fatalf("parse dropped bytes: %v (stats=%#v)", err, stats)
	}
	if dropped == 0 {
		t.Fatalf("command buffer dropped bytes=0, want retained capacity drop accounted (stats=%#v)", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dbClosed = true
}

func TestCommandWALStatsReadOnlyReportsPersistedMode(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ro, err := Open(Options{Dir: dir, ReadOnly: true, CommandWALStatsScan: true})
	if err != nil {
		t.Fatalf("Open read-only command WAL: %v", err)
	}
	defer ro.Close()
	stats := ro.Stats()
	if got := stats["treedb.command_wal.enabled"]; got != "true" {
		t.Fatalf("read-only command WAL enabled=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.required_feature"]; got != "true" {
		t.Fatalf("read-only required_feature=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.frames"]; got != "1" {
		t.Fatalf("read-only frames=%q, want 1 (stats=%#v)", got, stats)
	}
}

func TestCommandWALStatsScanUsesDefaultWALMaxSegmentBytes(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			_ = db.Close()
		}
	})
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.stats_error"]; got != "" {
		t.Fatalf("unexpected stats error with default WALMaxSegmentBytes: %q (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.frames"]; got != "1" {
		t.Fatalf("frames=%q, want 1 with default WALMaxSegmentBytes (stats=%#v)", got, stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dbClosed = true
}

func TestCommandWALStatsScanRefreshesWithoutAppliedLSNChange(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	db := &DB{dir: dir, commandWAL: true, commandWALStatsScan: true}

	first := make(map[string]string)
	writeCommandWALStats(first, db)
	if got := first["treedb.command_wal.frames"]; got != "1" {
		t.Fatalf("first frames=%q, want 1 (stats=%#v)", got, first)
	}

	writeCommandWALFrame(t, dir, 2, 2)
	second := make(map[string]string)
	writeCommandWALStats(second, db)
	if got := second["treedb.command_wal.frames"]; got != "2" {
		t.Fatalf("second frames=%q, want refreshed scan with 2 frames (stats=%#v)", got, second)
	}
}

func TestCommandWALCleanupStatsCountConsumedBytes(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALSegmentFrames(t, dir, 2, 3, 2)
	coveredInfo, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("stat covered segment: %v", err)
	}
	activeInfo, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log"))
	if err != nil {
		t.Fatalf("stat active segment: %v", err)
	}

	db := &DB{dir: dir, commandWAL: true}
	db.state.Store(&DBState{AppliedCommandLSN: 1})

	if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
	}
	stats := make(map[string]string)
	writeCommandWALStats(stats, db)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN after cleanup: %v", err)
	}
	var active commandWALSegmentCleanupDecision
	for _, decision := range decisions {
		if filepath.Base(decision.Path) == "commit-l0-000002.log" {
			active = decision
			break
		}
	}
	if active.ScannedBytes <= 0 || active.ScannedBytes >= active.Size {
		t.Fatalf("active scanned bytes=%d size=%d, want partial scan decision", active.ScannedBytes, active.Size)
	}
	got := commandWALTestStatUint64(t, stats, "treedb.command_wal.cleanup.scanned_bytes_total")
	if got <= uint64(active.ScannedBytes) {
		t.Fatalf("scanned_bytes_total=%d, want covered segment plus active partial scan > %d", got, active.ScannedBytes)
	}
	fullSegmentBytes := uint64(coveredInfo.Size() + activeInfo.Size())
	if got >= fullSegmentBytes {
		t.Fatalf("scanned_bytes_total=%d full_segment_bytes=%d, want consumed bytes below full segment bytes", got, fullSegmentBytes)
	}
}

func TestCommandWALStatsDisabledDoesNotScanWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll WAL dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "commit-l0-000001.log"), []byte{0x01, 0x02, 0x03}, 0o600); err != nil {
		t.Fatalf("WriteFile bogus WAL: %v", err)
	}
	stats := make(map[string]string)
	writeCommandWALStats(stats, &DB{dir: dir})
	if got := stats["treedb.command_wal.stats_error"]; got != "" {
		t.Fatalf("disabled command WAL stats scanned WAL and errored: %q", got)
	}
	if got := stats["treedb.command_wal.frames"]; got != "0" {
		t.Fatalf("frames=%q, want 0 for disabled command WAL", got)
	}
	if got := stats["treedb.command_wal.stats_scan"]; got != "false" {
		t.Fatalf("stats_scan=%q, want false for disabled command WAL", got)
	}
	for _, key := range []string{
		"treedb.command_wal.append.count_total",
		"treedb.command_wal.flush.count_total",
		"treedb.command_wal.sync.count_total",
		"treedb.command_wal.cleanup.scan.count_total",
		"treedb.command_wal.cleanup.scan.ns_total",
		"treedb.command_wal.cleanup.scanned_bytes_total",
		"treedb.command_wal.cleanup.scanned_frames_total",
	} {
		if got := commandWALTestStatUint64(t, stats, key); got != 0 {
			t.Fatalf("%s=%d, want 0 for disabled command WAL (stats=%#v)", key, got, stats)
		}
	}
}
