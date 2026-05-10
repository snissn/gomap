package db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompactStorageDeletesZeroByteValueLogFiles(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	emptyPath := filepath.Join(valueLogDir, "value-l42-000001.log")
	if err := os.WriteFile(emptyPath, nil, 0644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	plan, err := reopened.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := plan.RemainingDebt.ZeroByteValueLogFiles; got != 1 {
		t.Fatalf("zero-byte debt=%d want 1", got)
	}

	stats, err := reopened.CompactStorage(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("empty value-log file still exists or stat failed: %v", err)
	}
	if !stats.FullyCompacted {
		t.Fatalf("FullyCompacted=false remaining debt=%+v", stats.RemainingDebt)
	}
}

func TestCompactStorageDeletesManagerPinnedZeroByteValueLogFiles(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	livePath := filepath.Join(valueLogDir, "value-l0-000001.log")
	liveID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("live file id: %v", err)
	}
	liveWriter, err := valuelog.NewWriter(livePath, liveID)
	if err != nil {
		t.Fatalf("live writer: %v", err)
	}
	livePtr, err := liveWriter.Append(0, nil, 1, bytes.Repeat([]byte("v"), 256))
	if err != nil {
		_ = liveWriter.Close()
		t.Fatalf("append live value: %v", err)
	}
	if err := liveWriter.Close(); err != nil {
		t.Fatalf("close live writer: %v", err)
	}
	batch := d.NewBatch()
	ptrBatch, ok := batch.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch %T", batch)
	}
	if err := ptrBatch.SetPointer([]byte("k"), livePtr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("batch write: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	const emptyName = "value-l42-000001.log"
	emptyPath := filepath.Join(valueLogDir, emptyName)
	if err := os.WriteFile(emptyPath, nil, 0644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}
	fileID, ok := compactStorageValueLogFileID(emptyName)
	if !ok {
		t.Fatalf("parse %s failed", emptyName)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.valueLogManager == nil || !reopened.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected empty segment %d to be manager-registered", fileID)
	}
	state := reopened.State()
	if state == nil || state.ValueLogSet == nil {
		t.Fatal("expected state value-log set")
	}
	if _, ok := state.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("expected empty segment %d to be pinned by state value-log set", fileID)
	}

	stats, err := reopened.CompactStorage(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("manager-pinned empty value-log file still exists or stat failed: %v", err)
	}
	if !stats.FullyCompacted {
		t.Fatalf("FullyCompacted=false remaining debt=%+v", stats.RemainingDebt)
	}
}

func TestCompactStorageKeepsPinnedZombieZeroByteValueLogFiles(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.SetSync([]byte("live"), []byte("v")); err != nil {
		_ = d.Close()
		t.Fatalf("set live: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	const emptyName = "value-l42-000001.log"
	emptyPath := filepath.Join(valueLogDir, emptyName)
	if err := os.WriteFile(emptyPath, nil, 0644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}
	fileID, ok := compactStorageValueLogFileID(emptyName)
	if !ok {
		t.Fatalf("parse %s failed", emptyName)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.valueLogManager == nil || !reopened.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected empty segment %d to be manager-registered", fileID)
	}
	pinned := reopened.AcquireSnapshot()
	if pinned == nil {
		t.Fatal("expected pinned snapshot")
	}
	pinnedClosed := false
	defer func() {
		if !pinnedClosed {
			_ = pinned.Close()
		}
	}()

	if _, err := reopened.CompactStorage(context.Background(), CompactStorageOptions{}); err != nil {
		t.Fatalf("CompactStorage first: %v", err)
	}
	if _, err := os.Stat(emptyPath); err != nil {
		t.Fatalf("pinned zombie empty value-log file removed on first prune: %v", err)
	}
	if reopened.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected segment %d to be zombie after first prune", fileID)
	}

	if _, err := reopened.CompactStorage(context.Background(), CompactStorageOptions{}); err != nil {
		t.Fatalf("CompactStorage second: %v", err)
	}
	if _, err := os.Stat(emptyPath); err != nil {
		t.Fatalf("pinned zombie empty value-log file removed on second prune: %v", err)
	}

	if err := pinned.Close(); err != nil {
		t.Fatalf("close pinned snapshot: %v", err)
	}
	pinnedClosed = true
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("expected pinned zombie file to delete after release, stat err=%v", err)
	}
}

func TestCompactStoragePlanMatchesAppliedRewriteScopeForLiveOnlySegments(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	livePath := filepath.Join(valueLogDir, "value-l0-000001.log")
	liveID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("live file id: %v", err)
	}
	liveWriter, err := valuelog.NewWriter(livePath, liveID)
	if err != nil {
		t.Fatalf("live writer: %v", err)
	}
	livePtr, err := liveWriter.Append(0, nil, 1, bytes.Repeat([]byte("v"), 256))
	if err != nil {
		_ = liveWriter.Close()
		t.Fatalf("append live value: %v", err)
	}
	if err := liveWriter.Close(); err != nil {
		t.Fatalf("close live writer: %v", err)
	}

	batch := d.NewBatch()
	ptrBatch, ok := batch.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch %T", batch)
	}
	if err := ptrBatch.SetPointer([]byte("k"), livePtr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("batch write: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := plan.RemainingDebt.ValueLogRewriteSegments; got != 0 {
		t.Fatalf("plan rewrite segments=%d want 0, rewrite plan=%+v debt=%+v", got, plan.ValueLogRewritePlan, plan.RemainingDebt)
	}
	if plan.ValueLogRewritePlan.SegmentsTotal == 0 {
		t.Fatalf("expected rewrite plan to see live value-log segment, rewrite plan=%+v", plan.ValueLogRewritePlan)
	}
	if !plan.FullyCompacted {
		t.Fatalf("plan FullyCompacted=false for live-only segment, rewrite plan=%+v debt=%+v", plan.ValueLogRewritePlan, plan.RemainingDebt)
	}

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if got := stats.ValueLogRewrite.SourceSegmentsRequested; got != 0 {
		t.Fatalf("applied rewrite source segments=%d want 0, stats=%+v", got, stats.ValueLogRewrite)
	}
	if got := stats.ValueLogRewrite.ValueRecordsCopied; got != 0 {
		t.Fatalf("applied rewrite copied value records=%d want 0, stats=%+v", got, stats.ValueLogRewrite)
	}
	if !stats.FullyCompacted {
		t.Fatalf("applied FullyCompacted=false for live-only segment, rewrite plan=%+v debt=%+v", stats.ValueLogRewritePlan, stats.RemainingDebt)
	}
}

func TestCompactStorageReportsAppliedValueLogGCStats(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}

	path1 := filepath.Join(valueLogDir, "value-l0-000001.log")
	id1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("file id 1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer 1: %v", err)
	}
	ptr1, err := w1.Append(0, nil, 1, bytes.Repeat([]byte("stale-value|"), 64))
	if err != nil {
		_ = w1.Close()
		t.Fatalf("append 1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close writer 1: %v", err)
	}

	path2 := filepath.Join(valueLogDir, "value-l0-000002.log")
	id2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("file id 2: %v", err)
	}
	w2, err := valuelog.NewWriter(path2, id2)
	if err != nil {
		t.Fatalf("writer 2: %v", err)
	}
	ptr2, err := w2.Append(0, nil, 2, bytes.Repeat([]byte("live-value|"), 64))
	if err != nil {
		_ = w2.Close()
		t.Fatalf("append 2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close writer 2: %v", err)
	}

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("stale"), ptr1); err != nil {
		t.Fatalf("set stale: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("live"), ptr2); err != nil {
		t.Fatalf("set live: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("batch write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}
	if err := db.Delete([]byte("stale")); err != nil {
		t.Fatalf("delete stale: %v", err)
	}

	stats, err := db.CompactStorage(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if stats.ValueLogGC.SegmentsDeleted == 0 {
		t.Fatalf("applied ValueLogGC stats were not preserved: %+v", stats.ValueLogGC)
	}
	if stats.ValueLogGC.BytesDeleted == 0 {
		t.Fatalf("applied ValueLogGC bytes deleted were not preserved: %+v", stats.ValueLogGC)
	}
	if stats.RemainingDebt.ValueLogGCSegments != 0 || stats.RemainingDebt.ValueLogGCBytes != 0 {
		t.Fatalf("remaining GC debt=%+v, want none", stats.RemainingDebt)
	}
}

func TestCompactStorageKeepsProtectedZeroByteValueLogFiles(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	emptyPath := filepath.Join(valueLogDir, "value-l9-000001.log")
	if err := os.WriteFile(emptyPath, nil, 0644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	opts := CompactStorageOptions{ValueLogProtectedPaths: []string{emptyPath}}
	plan, err := reopened.CompactStoragePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := plan.RemainingDebt.ZeroByteValueLogFiles; got != 0 {
		t.Fatalf("protected zero-byte debt=%d want 0", got)
	}
	stats, err := reopened.CompactStorage(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if _, err := os.Stat(emptyPath); err != nil {
		t.Fatalf("protected empty value-log file was removed: %v", err)
	}
	if stats.ZeroByteValueLogFilesDeleted != 0 {
		t.Fatalf("deleted protected zero-byte files=%d want 0", stats.ZeroByteValueLogFilesDeleted)
	}
	if !stats.FullyCompacted {
		t.Fatalf("FullyCompacted=false remaining debt=%+v", stats.RemainingDebt)
	}
}

func TestCompactStorageDefaultProtectedPathsActivatesActiveProtection(t *testing.T) {
	paths := compactStorageValueLogProtectedPaths(CompactStorageOptions{})
	if len(paths) != 1 || paths[0] != "" {
		t.Fatalf("default protected paths=%q want sentinel", paths)
	}
}

func TestCompactStorageFencedProtectedPathsIncludeExplicitPaths(t *testing.T) {
	paths := compactStorageFencedValueLogProtectedPaths(CompactStorageOptions{
		ValueLogProtectedPaths: []string{"explicit-a", "shared"},
		ValueLogFencedProtectedPathsFunc: func() []string {
			return []string{"dynamic-a", "shared"}
		},
	})
	want := []string{"explicit-a", "shared", "dynamic-a"}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("fenced protected paths=%q want %q", paths, want)
	}
}

func TestZeroByteValueLogCleanupProtectsByFileID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000002.log")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("write zero-byte value log: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("encode file ID: %v", err)
	}
	count, err := zeroByteValueLogSegmentFiles(dir, nil, []uint32{fileID})
	if err != nil {
		t.Fatalf("zero-byte scan with protected file ID: %v", err)
	}
	if count != 0 {
		t.Fatalf("protected zero-byte count=%d want 0", count)
	}
	count, err = zeroByteValueLogSegmentFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("zero-byte scan without protection: %v", err)
	}
	if count != 1 {
		t.Fatalf("unprotected zero-byte count=%d want 1", count)
	}
}

func TestCompactStorageKeepsCurrentWritableZeroByteValueLogFiles(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	path := filepath.Join(valueLogDir, "value-l0-000002.log")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("write zero-byte value log: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("encode file ID: %v", err)
	}
	if err := d.valueLogManager.RegisterSegment(path, fileID); err != nil {
		t.Fatalf("register segment: %v", err)
	}
	if err := d.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
		t.Fatalf("promote current writable: %v", err)
	}

	plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := plan.RemainingDebt.ZeroByteValueLogFiles; got != 0 {
		t.Fatalf("current writable zero-byte debt=%d want 0", got)
	}
	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("current writable zero-byte file was removed: %v", err)
	}
	if stats.ZeroByteValueLogFilesDeleted != 0 {
		t.Fatalf("deleted current writable zero-byte files=%d want 0", stats.ZeroByteValueLogFilesDeleted)
	}
}

func TestCompactStoragePlanReadOnlyDoesNotDeleteZeroByteValueLogFiles(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	emptyPath := filepath.Join(valueLogDir, "value-l7-000001.log")
	if err := os.WriteFile(emptyPath, nil, 0644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}

	readonly, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}
	stats, err := readonly.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	closeErr := readonly.Close()
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("close readonly: %v", closeErr)
	}
	if got := stats.RemainingDebt.ZeroByteValueLogFiles; got != 1 {
		t.Fatalf("zero-byte debt=%d want 1", got)
	}
	if _, err := os.Stat(emptyPath); err != nil {
		t.Fatalf("read-only plan mutated empty value-log file: %v", err)
	}
}

func TestCompactStoragePlanIgnoresZeroByteValueLogFilesWhenCleanupDisabled(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	emptyPath := filepath.Join(valueLogDir, "value-l8-000001.log")
	if err := os.WriteFile(emptyPath, nil, 0644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	stats, err := reopened.CompactStoragePlan(context.Background(), CompactStorageOptions{
		DisableZeroByteValueLogCleanup: true,
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := stats.RemainingDebt.ZeroByteValueLogFiles; got != 0 {
		t.Fatalf("zero-byte debt=%d want 0 when cleanup is disabled", got)
	}
	if _, err := os.Stat(emptyPath); err != nil {
		t.Fatalf("plan mutated empty value-log file: %v", err)
	}
}

func TestCompactStorageRIDAllocatorIsSharedAcrossOfflineWriters(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(ValueLogDirPath(dir), 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	if err := os.MkdirAll(LeafLogDirPath(dir), 0o755); err != nil {
		t.Fatalf("mkdir leaf_vlog: %v", err)
	}

	origScanner := rewriteRIDStartScanner
	scanCalls := 0
	rewriteRIDStartScanner = func([]logSegment) (uint64, error) {
		scanCalls++
		return 500, nil
	}
	t.Cleanup(func() { rewriteRIDStartScanner = origScanner })

	d := &DB{
		dir:                        dir,
		indexOuterLeavesInValueLog: true,
		valueLogCompression:        ValueLogCompressionOff,
	}
	opts := CompactStorageOptions{}
	if err := d.prepareCompactStorageRIDAllocator(&opts); err != nil {
		t.Fatalf("prepareCompactStorageRIDAllocator: %v", err)
	}
	if opts.ReserveRIDs == nil {
		t.Fatal("expected shared ReserveRIDs callback")
	}
	if scanCalls != 1 {
		t.Fatalf("rid start scans=%d want 1", scanCalls)
	}

	_, cleanup, err := d.installCompactStorageLeafPageLog(opts)
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog: %v", err)
	}
	defer cleanup()
	if d.leafPageLog == nil {
		t.Fatal("expected installed leaf page log")
	}
	if _, err := d.leafPageLog.AppendLeafPage(bytes.Repeat([]byte("l"), page.PageSize)); err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}

	start, err := opts.ReserveRIDs(2)
	if err != nil {
		t.Fatalf("ReserveRIDs: %v", err)
	}
	if start != 501 {
		t.Fatalf("ReserveRIDs start=%d want 501 after leaf writer consumed rid 500", start)
	}
}

func TestCompactStorageRefreshesInstalledLeafWriterAfterPack(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(ValueLogDirPath(dir), 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	if err := os.MkdirAll(LeafLogDirPath(dir), 0755); err != nil {
		t.Fatalf("mkdir leaf_vlog: %v", err)
	}

	d := &DB{
		dir:                        dir,
		indexOuterLeavesInValueLog: true,
		valueLogCompression:        ValueLogCompressionOff,
	}
	opts := CompactStorageOptions{ReserveRIDs: newRewriteRIDAllocator(1, nil).Reserve}
	installed, cleanup, err := d.installCompactStorageLeafPageLog(opts)
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog: %v", err)
	}
	defer cleanup()
	if installed == nil {
		t.Fatal("expected installed compact leaf writer")
	}

	packWriter := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0)
	packWriter.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	if _, err := packWriter.AppendLeafPage(bytes.Repeat([]byte("p"), page.PageSize)); err != nil {
		_ = packWriter.Close()
		t.Fatalf("pack writer AppendLeafPage: %v", err)
	}
	if err := packWriter.Close(); err != nil {
		t.Fatalf("close pack writer: %v", err)
	}

	if err := d.refreshCompactStorageLeafPageLog(installed); err != nil {
		t.Fatalf("refreshCompactStorageLeafPageLog: %v", err)
	}
	if _, err := installed.AppendLeafPage(bytes.Repeat([]byte("v"), page.PageSize)); err != nil {
		t.Fatalf("installed AppendLeafPage: %v", err)
	}
	gotPath, _, ok := installed.CurrentValueLogSegment()
	if !ok {
		t.Fatal("installed writer did not report current leaf segment")
	}
	if got := filepath.Base(gotPath); got != "value-l255-000002.log" {
		t.Fatalf("installed writer segment=%s, want value-l255-000002.log", got)
	}
	if _, err := os.Stat(filepath.Join(LeafLogDirPath(dir), "value-l255-000001.log")); err != nil {
		t.Fatalf("expected packed leaf segment to remain: %v", err)
	}
}

func TestRegisterLeafPageLogSegmentsForPublishRegistersRewriteSegments(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 0)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	d.SetLeafPageLog(leafLog)
	defer func() { _ = leafLog.Close() }()

	if _, err := leafLog.AppendLeafPage(bytes.Repeat([]byte("v"), page.PageSize)); err != nil {
		t.Fatalf("AppendLeafPage: %v", err)
	}
	createdSegments, err := leafLog.createdSegmentsSnapshot()
	if err != nil {
		t.Fatalf("createdSegmentsSnapshot: %v", err)
	}
	if len(createdSegments) != 1 {
		t.Fatalf("created segments=%d want 1", len(createdSegments))
	}
	if d.valueLogManager.HasSegment(createdSegments[0].fileID) {
		t.Fatalf("segment %d was registered before publish helper", createdSegments[0].fileID)
	}

	registered, err := d.registerLeafPageLogSegmentsForPublish(7)
	if err != nil {
		t.Fatalf("registerLeafPageLogSegmentsForPublish: %v", err)
	}
	if !registered {
		t.Fatal("current leaf segment was not registered")
	}
	if !d.valueLogManager.HasSegment(createdSegments[0].fileID) {
		t.Fatalf("segment %d was not registered", createdSegments[0].fileID)
	}
	set := d.valueLogManager.CurrentSetNoRefresh()
	defer func() { _ = d.valueLogManager.Release(set) }()
	if set == nil {
		t.Fatal("missing value-log set")
	}
	if _, ok := set.Files[createdSegments[0].fileID]; !ok {
		t.Fatalf("published value-log set missing segment %d", createdSegments[0].fileID)
	}
}

func TestCompactStorageHoldsMaintenanceLockAcrossPhases(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	phaseChecks := 0
	d.compactStorageAfterPhase = func(name string) {
		phaseChecks++
		if d.maintenanceMu.TryLock() {
			d.maintenanceMu.Unlock()
			t.Fatalf("maintenance lock was not held after compact phase %q", name)
		}
	}
	t.Cleanup(func() {
		d.compactStorageAfterPhase = nil
	})

	if _, err := d.CompactStorage(context.Background(), CompactStorageOptions{}); err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if phaseChecks == 0 {
		t.Fatal("expected compact storage phase hook to run")
	}
}

func TestCompactStorageSettlesLeafGenerationGCAfterPinnedRetiring(t *testing.T) {
	d, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "k", 64, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	pinned := d.AcquireSnapshot()
	if pinned == nil {
		t.Fatal("expected pinned snapshot")
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, d, "k", 64, 'b')

	closedPinned := false
	d.compactStorageAfterPhase = func(name string) {
		if name != "leaf-generation-gc" || closedPinned {
			return
		}
		closedPinned = true
		if err := pinned.Close(); err != nil {
			t.Fatalf("close pinned snapshot: %v", err)
		}
	}
	t.Cleanup(func() {
		d.compactStorageAfterPhase = nil
		if !closedPinned {
			_ = pinned.Close()
		}
	})

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if !closedPinned {
		t.Fatal("phase hook did not close pinned snapshot")
	}
	if !stats.FullyCompacted {
		t.Fatalf("FullyCompacted=false remaining debt=%+v", stats.RemainingDebt)
	}
	if !compactStoragePhaseSeen(stats.Phases, "settle-leaf-generation-gc-1") {
		t.Fatalf("settle leaf GC phase missing: %+v", stats.Phases)
	}
	if err := waitForPathRemoval(path1, 5*time.Second); err != nil {
		t.Fatalf("waitForPathRemoval(%s): %v", path1, err)
	}
	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	for _, gen := range manifest.Generations {
		if gen.GenerationID == 0 {
			t.Fatalf("invalid generation in manifest: %+v", gen)
		}
		if gen.State == leafGenerationStateRetiring || gen.State == leafGenerationStateDeleted {
			t.Fatalf("generation %d state=%q after compact storage; manifest=%+v", gen.GenerationID, gen.State, manifest.Generations)
		}
		for _, fileID := range gen.FileIDs {
			if fileID == rawFileID1 {
				t.Fatalf("dead generation file %d still present in manifest: %+v", rawFileID1, manifest.Generations)
			}
		}
	}
}

func compactStoragePhaseSeen(phases []CompactStoragePhaseStats, name string) bool {
	for _, phase := range phases {
		if phase.Name == name {
			return true
		}
	}
	return false
}
