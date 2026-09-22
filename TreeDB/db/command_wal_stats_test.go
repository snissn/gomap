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
		"treedb.command_wal.write.syscalls_total",
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
		"treedb.command_wal.flush.point.count_total":       1,
		"treedb.command_wal.flush.payload.count_total":     1,
		"treedb.command_wal.sync.entry_scan.count_total":   1,
		"treedb.command_wal.sync.barrier.count_total":      0,
		"treedb.command_wal.write.syscalls_total":          3,
		"treedb.command_wal.write.errors_total":            0,
		"treedb.command_wal.file_sync.calls_total":         1,
		"treedb.command_wal.file_sync.errors_total":        0,
	} {
		if got := commandWALTestStatUint64(t, stats, key); got != want {
			t.Fatalf("%s=%d, want %d (stats=%#v)", key, got, want, stats)
		}
	}
	for _, key := range []string{
		"treedb.command_wal.append.ns_total",
		"treedb.command_wal.flush.ns_total",
		"treedb.command_wal.sync.ns_total",
		"treedb.command_wal.write.ns_total",
		"treedb.command_wal.file_sync.ns_total",
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

func TestCommandWALStatsBoundV2ScratchAfterLargeFrame(t *testing.T) {
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
	if got, want := stats["treedb.command_wal.writer.scratch_capacity_bytes"], strconv.Itoa(commandWALDeferredPointBufferRetainSize); got != want {
		t.Fatalf("V2 scratch cap=%q, want retain cap %s after oversized append (stats=%#v)", got, want, stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dbClosed = true
}

func TestCommandWALStatsExposeDurablePrefixAndPendingDebt(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("relaxed"), []byte("value"), false); err != nil {
		t.Fatalf("append relaxed command: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "0" {
		t.Fatalf("durable_wal_lsn=%q, want 0 before barrier", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "1" {
		t.Fatalf("pending debt entries=%q, want 1", got)
	}
	if _, ok := stats["treedb.command_wal.dependency_debt.max_age_ns"]; !ok {
		t.Fatal("pending debt age stat missing")
	}

	if err := db.FlushCommandWALBarrier(true); err != nil {
		t.Fatalf("durable prefix barrier: %v", err)
	}
	stats = db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "2" {
		t.Fatalf("durable_wal_lsn=%q, want 2 after barrier", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
		t.Fatalf("pending debt entries=%q, want 0 after barrier", got)
	}
}

func TestPublishStagedCommandWALNoopSyncClosesExistingFrameDurablePrefix(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	intent, err := db.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewCommandWALIntent: %v", err)
	}
	lsn, err := db.AppendStagedCommandWALIntent(intent, false)
	if err != nil {
		t.Fatalf("AppendStagedCommandWALIntent relaxed: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("staged intent lsn=%d, want 1", lsn)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "0" {
		t.Fatalf("durable_wal_lsn=%q, want 0 before sync publish", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "1" {
		t.Fatalf("pending debt entries=%q, want 1 before sync publish", got)
	}

	if err := db.PublishStagedCommandWALNoop(intent, true); err != nil {
		t.Fatalf("PublishStagedCommandWALNoop sync: %v", err)
	}
	const barrierLSN = uint64(2)
	if got := db.State().AppliedCommandLSN; got != barrierLSN {
		t.Fatalf("AppliedCommandLSN=%d, want durable barrier lsn %d", got, barrierLSN)
	}
	stats = db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "2" {
		t.Fatalf("durable_wal_lsn=%q, want durable barrier lsn 2", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
		t.Fatalf("pending debt entries=%q, want 0 after sync publish", got)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.append.count_total"); got != 2 {
		t.Fatalf("append count=%d, want relaxed frame plus durable barrier", got)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.sync.barrier.count_total"); got != 1 {
		t.Fatalf("barrier sync count=%d, want 1", got)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.frames"); got != 2 {
		t.Fatalf("command WAL frames=%d, want relaxed frame plus durable barrier", got)
	}

	next := mustRawKVCommandWALIntent(t, db, "after/staged-sync", "value")
	if err := db.PublishCommandWALNoop(next, false); err != nil {
		t.Fatalf("PublishCommandWALNoop after staged sync barrier: %v", err)
	}
	if got := next.AssignedLSN(); got != barrierLSN+1 {
		t.Fatalf("next command lsn=%d, want %d", got, barrierLSN+1)
	}
	if got := db.State().AppliedCommandLSN; got != barrierLSN+1 {
		t.Fatalf("AppliedCommandLSN after next command=%d, want %d", got, barrierLSN+1)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close after sync publish: %v", err)
	}
	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen after sync publish: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	if got := reopen.State().AppliedCommandLSN; got != barrierLSN+1 {
		t.Fatalf("reopened AppliedCommandLSN=%d, want next command lsn %d", got, barrierLSN+1)
	}
	if got := reopen.Stats()["treedb.command_wal.durable_wal_lsn"]; got != "3" {
		t.Fatalf("reopened durable_wal_lsn=%q, want graceful close through next command lsn 3", got)
	}
}

func TestPublishStagedCommandWALNoopSyncDoesNotAppendBarrierForDurableFrame(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	intent, err := db.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewCommandWALIntent: %v", err)
	}
	lsn, err := db.AppendStagedCommandWALIntent(intent, true)
	if err != nil {
		t.Fatalf("AppendStagedCommandWALIntent durable: %v", err)
	}
	if err := db.PublishStagedCommandWALNoop(intent, true); err != nil {
		t.Fatalf("PublishStagedCommandWALNoop durable: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "1" {
		t.Fatalf("durable_wal_lsn=%q, want original durable frame lsn 1", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
		t.Fatalf("pending debt entries=%q, want 0", got)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.append.count_total"); got != 1 {
		t.Fatalf("append count=%d, want only original durable frame", got)
	}
	if got := commandWALTestStatUint64(t, stats, "treedb.command_wal.sync.barrier.count_total"); got != 0 {
		t.Fatalf("barrier sync count=%d, want 0", got)
	}
	if got := db.State().AppliedCommandLSN; got != lsn {
		t.Fatalf("AppliedCommandLSN=%d, want durable frame lsn %d", got, lsn)
	}
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
	writeCommandWALSegmentFrames(t, dir, 2, 2, 3)
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
	installCommandWALCleanupRootForTest(t, db, 1, 1)

	if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
	}
	stats := make(map[string]string)
	writeCommandWALStats(stats, db)
	for key, want := range map[string]uint64{
		"treedb.command_wal.cleanup.proof.count_total":                        1,
		"treedb.command_wal.cleanup.proof.frontier_lsn":                       1,
		"treedb.command_wal.cleanup.proof.durable_wal_lsn":                    1,
		"treedb.command_wal.cleanup.proof.selected_root_commit_seq":           1,
		"treedb.command_wal.cleanup.proof.older_root_commit_seq":              0,
		"treedb.command_wal.cleanup.covered_segments_total":                   1,
		"treedb.command_wal.cleanup.retained_segments_total":                  1,
		"treedb.command_wal.cleanup.retained.reason.active_segments_total":    0,
		"treedb.command_wal.cleanup.retained.reason.uncovered_segments_total": 1,
		"treedb.command_wal.cleanup.retained.reason.pinned_segments_total":    0,
		"treedb.command_wal.cleanup.retained.reason.error_segments_total":     0,
		"treedb.command_wal.cleanup.removed_segments_total":                   1,
		"treedb.command_wal.cleanup.retries_total":                            0,
		"treedb.command_wal.cleanup.namespace_sync.calls_total":               1,
		"treedb.command_wal.cleanup.namespace_sync.errors_total":              0,
		"treedb.command_wal.cleanup.oldest_pinned_lsn":                        0,
	} {
		if got := commandWALTestStatUint64(t, stats, key); got != want {
			t.Fatalf("%s=%d, want %d (stats=%#v)", key, got, want, stats)
		}
	}
	// The exact counts and byte totals are the observed-work signals. On
	// platforms with coarse monotonic clock resolution, both short phases can
	// legitimately measure 0ns; still require their stats to exist and parse.
	for _, key := range []string{
		"treedb.command_wal.cleanup.proof.ns_total",
		"treedb.command_wal.cleanup.ns_total",
	} {
		_ = commandWALTestStatUint64(t, stats, key)
	}
	for _, key := range []string{
		"treedb.command_wal.cleanup.covered_bytes_total",
		"treedb.command_wal.cleanup.retained_bytes_total",
		"treedb.command_wal.cleanup.removed_bytes_total",
	} {
		if got := commandWALTestStatUint64(t, stats, key); got == 0 {
			t.Fatalf("%s=0, want observed cleanup work (stats=%#v)", key, stats)
		}
	}

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
	if active.ScannedBytes != active.Size {
		t.Fatalf("active scanned bytes=%d size=%d, want full lineage scan", active.ScannedBytes, active.Size)
	}
	got := commandWALTestStatUint64(t, stats, "treedb.command_wal.cleanup.scanned_bytes_total")
	fullSegmentBytes := uint64(coveredInfo.Size() + activeInfo.Size())
	if got != fullSegmentBytes {
		t.Fatalf("scanned_bytes_total=%d full_segment_bytes=%d, want complete cleanup lineage scan", got, fullSegmentBytes)
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
		"treedb.command_wal.cleanup.proof.count_total",
		"treedb.command_wal.cleanup.covered_segments_total",
		"treedb.command_wal.cleanup.retained_segments_total",
		"treedb.command_wal.cleanup.removed_segments_total",
		"treedb.command_wal.cleanup.retries_total",
		"treedb.command_wal.cleanup.namespace_sync.calls_total",
	} {
		if got := commandWALTestStatUint64(t, stats, key); got != 0 {
			t.Fatalf("%s=%d, want 0 for disabled command WAL (stats=%#v)", key, got, stats)
		}
	}
}
