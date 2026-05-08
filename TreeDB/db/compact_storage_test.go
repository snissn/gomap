package db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
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

	cleanup, err := d.installCompactStorageLeafPageLog(opts)
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
