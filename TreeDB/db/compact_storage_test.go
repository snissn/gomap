package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompactStorageRepairsMissingCurrentLeafGenerationFile(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	leafDir := LeafLogDirPath(dir)
	_, fileID18 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 18)
	_, fileID19 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 19)
	_, fileID20 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 20)
	fileID17, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 17)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	raw17 := page.ValueLogSegmentID(fileID17)
	raw18 := page.ValueLogSegmentID(fileID18)
	raw19 := page.ValueLogSegmentID(fileID19)
	raw20 := page.ValueLogSegmentID(fileID20)
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 3,
		NextGenerationID:    4,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateSealed, FileIDs: []uint32{raw18}, CreatedCommitSeq: 10, SealedCommitSeq: 20, PublishedCommitSeq: 20},
			{GenerationID: 2, State: leafGenerationStateSealed, FileIDs: []uint32{raw19}, CreatedCommitSeq: 21, SealedCommitSeq: 30, PublishedCommitSeq: 30},
			{GenerationID: 3, State: leafGenerationStateWritable, FileIDs: []uint32{raw17}, CreatedCommitSeq: 31, PublishedCommitSeq: 31},
		},
	}
	db.writeMu.Lock()
	db.leafGenerationManifest = manifest
	db.writeMu.Unlock()
	if err := db.publishLeafGenerationState(false); err != nil {
		t.Fatalf("publishLeafGenerationState: %v", err)
	}
	if _, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{Mode: CompactStorageFull}); err != nil {
		t.Fatalf("CompactStoragePlan with stale missing leaf generation: %v", err)
	}
	if _, err := db.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageFull}); err != nil {
		t.Fatalf("CompactStorage with stale missing leaf generation: %v", err)
	}

	view := db.currentLeafGenerationView()
	if _, ok := view.FileToGeneration[raw17]; ok {
		t.Fatalf("missing raw file %d still visible after CompactStorage", raw17)
	}
	if _, ok := view.FileToGeneration[raw20]; !ok {
		t.Fatalf("existing writable raw file %d missing after CompactStorage", raw20)
	}
	for rawFileID := range view.FileToGeneration {
		if exists, err := leafGenerationRawFileExists(dir, rawFileID); err != nil || !exists {
			t.Fatalf("view references missing raw file %d: exists=%v err=%v", rawFileID, exists, err)
		}
	}
}

func TestCompactStorageConcurrentChildDeletionIsAbsentFromStorageScans(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "value-l0-000001.log")
	missingChild := &os.PathError{Op: "lstat", Path: child, Err: os.ErrNotExist}
	if !compactStorageConcurrentChildDeletion(root, child, missingChild) {
		t.Fatalf("post-enumeration child error=%v should be absent from usage, plan, and apply scans", missingChild)
	}
	missing := &os.PathError{Op: "lstat", Path: root, Err: os.ErrNotExist}
	if compactStorageConcurrentChildDeletion(root, root, missing) {
		t.Fatal("missing scan root should remain an error")
	}
	if compactStorageConcurrentChildDeletion(root, child, os.ErrPermission) {
		t.Fatal("non-ENOENT child error should remain an error")
	}
}

func TestZeroByteValueLogScansTreatAbsentDomainAsEmpty(t *testing.T) {
	root := t.TempDir()
	missing := filepath.Join(root, "missing")
	if count, err := zeroByteValueLogSegmentFiles(missing, nil, nil); err != nil || count != 0 {
		t.Fatalf("zero-byte plan absent domain: count=%d err=%v", count, err)
	}
	dbDir := filepath.Join(root, "db")
	db, err := Open(Options{Dir: dbDir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	valueLogDir := ValueLogDirPath(dbDir)
	if err := os.Remove(valueLogDir); err != nil {
		t.Fatalf("remove empty value-log domain: %v", err)
	}
	db.maintenanceMu.Lock()
	deleted, pruneErr := db.pruneZeroByteValueLogFiles(nil)
	db.maintenanceMu.Unlock()
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("restore value-log domain: %v", err)
	}
	if pruneErr != nil || deleted != 0 {
		t.Fatalf("zero-byte apply absent domain: deleted=%d err=%v", deleted, pruneErr)
	}
}

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
	if err := d.RegisterValueLogSegment(livePath, liveID); err != nil {
		t.Fatalf("register live value-log producer: %v", err)
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
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}
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

func TestCompactStorageManagerSegmentPostUnlinkCutRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	const emptyName = "value-l42-000001.log"
	emptyPath := filepath.Join(valueLogDir, emptyName)
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
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
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}
	if reopened.valueLogManager == nil || !reopened.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected empty segment %d to be manager-registered", fileID)
	}

	cutErr := errors.New("injected post-unlink cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceUnlink &&
			event.Resource == durabilitycut.ResourceValueLog &&
			filepath.Clean(event.OldPath) == filepath.Clean(emptyPath) {
			return cutErr
		}
		return nil
	})
	_, compactErr := reopened.CompactStorage(context.Background(), CompactStorageOptions{})
	restore()
	if !errors.Is(compactErr, cutErr) || !errors.Is(compactErr, ErrRecoveryRequired) {
		t.Fatalf("CompactStorage error=%v, want injected cut and ErrRecoveryRequired", compactErr)
	}
	if _, statErr := os.Stat(emptyPath); !os.IsNotExist(statErr) {
		t.Fatalf("post-unlink cut path stat=%v, want removed", statErr)
	}
}

func TestCompactStorageManagerSegmentDeletionSyncsNamespaceBeforeSuccess(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	const emptyName = "value-l42-000001.log"
	emptyPath := filepath.Join(valueLogDir, emptyName)
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
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
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}
	if reopened.valueLogManager == nil || !reopened.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected empty segment %d to be manager-registered", fileID)
	}

	var points []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && filepath.Clean(event.Path) == filepath.Clean(valueLogDir) {
			if event.Point == durabilitycut.BeforeDeletionDirectorySync || event.Point == durabilitycut.AfterDeletionDirectorySync {
				points = append(points, event.Point)
			}
		}
		return nil
	})
	_, compactErr := reopened.CompactStorage(context.Background(), CompactStorageOptions{})
	restore()
	if compactErr != nil {
		t.Fatalf("CompactStorage: %v", compactErr)
	}
	want := []durabilitycut.Point{durabilitycut.BeforeDeletionDirectorySync, durabilitycut.AfterDeletionDirectorySync}
	if !reflect.DeepEqual(points, want) {
		t.Fatalf("deletion namespace sync points=%v want=%v", points, want)
	}
	if _, statErr := os.Stat(emptyPath); !os.IsNotExist(statErr) {
		t.Fatalf("empty value-log file still exists or stat failed: %v", statErr)
	}
}

func TestCompactStorageManagerSegmentDeletionSyncFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	const emptyName = "value-l42-000001.log"
	emptyPath := filepath.Join(valueLogDir, emptyName)
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
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
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}
	if reopened.valueLogManager == nil || !reopened.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected empty segment %d to be manager-registered", fileID)
	}

	cutErr := errors.New("injected deletion directory sync cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog &&
			event.Point == durabilitycut.BeforeDeletionDirectorySync &&
			filepath.Clean(event.Path) == filepath.Clean(valueLogDir) {
			return cutErr
		}
		return nil
	})
	_, compactErr := reopened.CompactStorage(context.Background(), CompactStorageOptions{})
	restore()
	if !errors.Is(compactErr, cutErr) || !errors.Is(compactErr, ErrRecoveryRequired) {
		t.Fatalf("CompactStorage error=%v, want injected cut and ErrRecoveryRequired", compactErr)
	}
	if _, statErr := os.Stat(emptyPath); !os.IsNotExist(statErr) {
		t.Fatalf("post-unlink sync-cut path stat=%v, want removed", statErr)
	}
	if !reopened.publicationPoisoned.Load() {
		t.Fatal("deletion namespace sync cut did not poison publication")
	}
	if err := reopened.SetSync([]byte("post-cut"), []byte("blocked")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("SetSync after deletion namespace sync cut error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCompactStorageManagerSuccessfulUnlinkCloseErrorSyncsBeforeReturn(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	const emptyName = "value-l42-000001.log"
	emptyPath := filepath.Join(valueLogDir, emptyName)
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
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
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}
	if reopened.valueLogManager == nil || !reopened.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected empty segment %d to be manager-registered", fileID)
	}

	closeErr := errors.New("injected close after successful unlink")
	removeReturned := false
	reopened.testCompactStorageRemoveValueLogSegmentHook = func(gotFileID uint32) (bool, error) {
		if gotFileID != fileID {
			t.Fatalf("remove file ID=%d want=%d", gotFileID, fileID)
		}
		if err := os.Remove(emptyPath); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove value-log file: %v", err)
		}
		removeReturned = true
		return true, closeErr
	}

	var points []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if removeReturned && event.Resource == durabilitycut.ResourceValueLog && filepath.Clean(event.Path) == filepath.Clean(valueLogDir) {
			if event.Point == durabilitycut.BeforeDeletionDirectorySync || event.Point == durabilitycut.AfterDeletionDirectorySync {
				points = append(points, event.Point)
			}
		}
		return nil
	})
	deleted, compactErr := reopened.pruneZeroByteValueLogFiles(nil)
	restore()
	if !errors.Is(compactErr, closeErr) || !errors.Is(compactErr, ErrRecoveryRequired) {
		t.Fatalf("pruneZeroByteValueLogFiles error=%v, want close error and ErrRecoveryRequired", compactErr)
	}
	if deleted != 1 {
		t.Fatalf("pruneZeroByteValueLogFiles deleted=%d want=1; error=%v", deleted, compactErr)
	}
	want := []durabilitycut.Point{durabilitycut.BeforeDeletionDirectorySync, durabilitycut.AfterDeletionDirectorySync}
	if !reflect.DeepEqual(points, want) {
		t.Fatalf("deletion namespace sync points=%v want=%v", points, want)
	}
	if _, statErr := os.Stat(emptyPath); !os.IsNotExist(statErr) {
		t.Fatalf("post-unlink close-error path stat=%v, want removed", statErr)
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
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}
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

func TestCompactStoragePinnedZombieReleaseSyncsDeletionNamespaceBeforeSuccess(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	const emptyName = "value-l42-000001.log"
	emptyPath := filepath.Join(valueLogDir, emptyName)
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
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
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}
	if reopened.valueLogManager == nil || !reopened.valueLogManager.HasSegment(fileID) {
		t.Fatalf("expected empty segment %d to be manager-registered", fileID)
	}
	pinned := reopened.AcquireSnapshot()
	if pinned == nil {
		t.Fatal("expected pinned snapshot")
	}
	if _, err := reopened.CompactStorage(context.Background(), CompactStorageOptions{}); err != nil {
		_ = pinned.Close()
		t.Fatalf("CompactStorage: %v", err)
	}
	if _, err := os.Stat(emptyPath); err != nil {
		_ = pinned.Close()
		t.Fatalf("pinned zombie removed before release: %v", err)
	}

	var points []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && filepath.Clean(event.Path) == filepath.Clean(valueLogDir) {
			if event.Point == durabilitycut.BeforeDeletionDirectorySync || event.Point == durabilitycut.AfterDeletionDirectorySync {
				points = append(points, event.Point)
			}
		}
		return nil
	})
	closeErr := pinned.Close()
	restore()
	if closeErr != nil {
		t.Fatalf("close pinned snapshot: %v", closeErr)
	}
	want := []durabilitycut.Point{durabilitycut.BeforeDeletionDirectorySync, durabilitycut.AfterDeletionDirectorySync}
	if !reflect.DeepEqual(points, want) {
		t.Fatalf("deferred deletion namespace sync points=%v want=%v", points, want)
	}
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("expected pinned zombie file to delete after release, stat err=%v", err)
	}
}

func TestCompactStoragePinnedZombieReleaseSyncFailurePoisonsHandle(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	valueLogDir := ValueLogDirPath(dir)
	const emptyName = "value-l42-000001.log"
	emptyPath := filepath.Join(valueLogDir, emptyName)
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatalf("write empty value log: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if err := reopened.RefreshValueLogSet(); err != nil {
		t.Fatalf("explicit maintenance refresh: %v", err)
	}
	pinned := reopened.AcquireSnapshot()
	if pinned == nil {
		t.Fatal("expected pinned snapshot")
	}
	if _, err := reopened.CompactStorage(context.Background(), CompactStorageOptions{}); err != nil {
		_ = pinned.Close()
		t.Fatalf("CompactStorage: %v", err)
	}

	cutErr := errors.New("injected deferred deletion directory sync cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog &&
			event.Point == durabilitycut.BeforeDeletionDirectorySync &&
			filepath.Clean(event.Path) == filepath.Clean(valueLogDir) {
			return cutErr
		}
		return nil
	})
	closeErr := pinned.Close()
	restore()
	if !errors.Is(closeErr, cutErr) || !errors.Is(closeErr, ErrRecoveryRequired) {
		t.Fatalf("close pinned snapshot error=%v, want injected cut and ErrRecoveryRequired", closeErr)
	}
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("post-unlink sync-cut path stat=%v, want removed", err)
	}
	if err := reopened.SetSync([]byte("after-deferred-delete-cut"), []byte("blocked")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("SetSync after deferred deletion sync cut=%v, want ErrRecoveryRequired", err)
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
	registerTestValueLogProducer(t, dir, livePath, liveID)

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

func TestCompactStorageFullRewriteSkipsMostlyLiveValueLogSegment(t *testing.T) {
	db := openCompactStorageRewritePolicyFixture(t, 9, 1, 1024)
	defer func() { _ = db.Close() }()

	opts := CompactStorageOptions{
		Mode:                                CompactStorageFull,
		ValueLogRewriteMinSegmentStaleRatio: 0.30,
		ValueLogRewriteMinSegmentStaleBytes: 1,
		DisableZeroByteValueLogCleanup:      true,
	}
	plan, err := db.CompactStoragePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if plan.ValueLogRewritePlan.BytesStale <= 0 {
		t.Fatalf("expected byte-minimization stale bytes in mostly-live fixture, plan=%+v", plan.ValueLogRewritePlan)
	}
	if got := plan.ValueLogRewritePlan.SegmentsSelected; got != 0 {
		t.Fatalf("full policy selected mostly-live segment=%d want 0, plan=%+v", got, plan.ValueLogRewritePlan)
	}
	if got := plan.RemainingDebt.ValueLogRewriteSegments; got != 0 {
		t.Fatalf("policy rewrite debt=%d want 0, debt=%+v plan=%+v", got, plan.RemainingDebt, plan.ValueLogRewritePlan)
	}
	if !plan.FullyCompacted || !plan.PolicyFullyCompacted || plan.ByteMinimized {
		t.Fatalf("plan flags fully=%t policy=%t byte=%t debt=%+v plan=%+v", plan.FullyCompacted, plan.PolicyFullyCompacted, plan.ByteMinimized, plan.RemainingDebt, plan.ValueLogRewritePlan)
	}

	stats, err := db.CompactStorage(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if got := stats.ValueLogRewrite.SourceSegmentsRequested; got != 0 {
		t.Fatalf("applied rewrite source segments=%d want 0, stats=%+v", got, stats.ValueLogRewrite)
	}
	if got := stats.ValueLogRewrite.ValueBytesCopied; got != 0 {
		t.Fatalf("applied rewrite copied bytes=%d want 0, stats=%+v", got, stats.ValueLogRewrite)
	}
	if !stats.FullyCompacted || !stats.PolicyFullyCompacted || stats.ByteMinimized {
		t.Fatalf("stats flags fully=%t policy=%t byte=%t debt=%+v plan=%+v", stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized, stats.RemainingDebt, stats.ValueLogRewritePlan)
	}
}

func TestCompactStorageQuickAndDefaultRewriteSkipMostlyLiveValueLogSegment(t *testing.T) {
	for _, tt := range []struct {
		name     string
		opts     CompactStorageOptions
		wantMode CompactStorageMode
	}{
		{name: "quick", opts: CompactStorageOptions{Mode: CompactStorageQuick}, wantMode: CompactStorageQuick},
		{name: "zero_value_default", opts: CompactStorageOptions{}, wantMode: CompactStorageFull},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := openCompactStorageRewritePolicyFixture(t, 9, 1, 1024)
			defer func() { _ = db.Close() }()

			plan, err := db.CompactStoragePlan(context.Background(), tt.opts)
			if err != nil {
				t.Fatalf("CompactStoragePlan: %v", err)
			}
			if plan.Mode != tt.wantMode {
				t.Fatalf("plan mode=%q want %q", plan.Mode, tt.wantMode)
			}
			if plan.ValueLogRewritePlan.BytesStale <= 0 {
				t.Fatalf("expected physical stale bytes in mostly-live fixture, plan=%+v", plan.ValueLogRewritePlan)
			}
			if plan.ValueLogRewritePlan.SegmentsSelected != 0 || plan.RemainingDebt.ValueLogRewriteSegments != 0 || plan.RemainingDebt.ValueLogRewriteBytes != 0 {
				t.Fatalf("plan reported rewrite work for policy-skipped segment: plan=%+v debt=%+v", plan.ValueLogRewritePlan, plan.RemainingDebt)
			}
			if !plan.FullyCompacted || !plan.PolicyFullyCompacted || plan.ByteMinimized {
				t.Fatalf("plan flags fully=%t policy=%t byte=%t debt=%+v", plan.FullyCompacted, plan.PolicyFullyCompacted, plan.ByteMinimized, plan.RemainingDebt)
			}

			stats, err := db.CompactStorage(context.Background(), tt.opts)
			if err != nil {
				t.Fatalf("CompactStorage: %v", err)
			}
			if stats.Mode != tt.wantMode {
				t.Fatalf("stats mode=%q want %q", stats.Mode, tt.wantMode)
			}
			if stats.ValueLogRewrite.SourceSegmentsRequested != 0 || stats.ValueLogRewrite.ValueRecordsCopied != 0 || stats.ValueLogRewrite.ValueBytesCopied != 0 {
				t.Fatalf("applied rewrite copied policy-skipped segment: stats=%+v", stats.ValueLogRewrite)
			}
			if stats.RemainingDebt.ValueLogRewriteSegments != 0 || stats.RemainingDebt.ValueLogRewriteBytes != 0 {
				t.Fatalf("remaining policy rewrite debt=%+v want none", stats.RemainingDebt)
			}
			if !stats.FullyCompacted || !stats.PolicyFullyCompacted || stats.ByteMinimized {
				t.Fatalf("stats flags fully=%t policy=%t byte=%t debt=%+v", stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized, stats.RemainingDebt)
			}
		})
	}
}

func TestCompactStorageFullRewritePolicyRequiresStaleRatioAndBytes(t *testing.T) {
	cases := []struct {
		name         string
		opts         CompactStorageOptions
		wantSelected int
	}{
		{
			name: "below_ratio",
			opts: CompactStorageOptions{
				Mode:                                CompactStorageFull,
				ValueLogRewriteMinSegmentStaleRatio: 0.30,
				ValueLogRewriteMinSegmentStaleBytes: 1,
			},
			wantSelected: 0,
		},
		{
			name: "below_bytes",
			opts: CompactStorageOptions{
				Mode:                                CompactStorageFull,
				ValueLogRewriteMinSegmentStaleRatio: 0.01,
				ValueLogRewriteMinSegmentStaleBytes: 1 << 20,
			},
			wantSelected: 0,
		},
		{
			name: "clears_both",
			opts: CompactStorageOptions{
				Mode:                                CompactStorageFull,
				ValueLogRewriteMinSegmentStaleRatio: 0.01,
				ValueLogRewriteMinSegmentStaleBytes: 1,
			},
			wantSelected: 1,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			db := openCompactStorageRewritePolicyFixture(t, 9, 1, 1024)
			defer func() { _ = db.Close() }()
			plan, err := db.CompactStoragePlan(context.Background(), tt.opts)
			if err != nil {
				t.Fatalf("CompactStoragePlan: %v", err)
			}
			if got := plan.ValueLogRewritePlan.SegmentsSelected; got != tt.wantSelected {
				t.Fatalf("segments selected=%d want %d, rewrite plan=%+v debt=%+v", got, tt.wantSelected, plan.ValueLogRewritePlan, plan.RemainingDebt)
			}
			if got := plan.RemainingDebt.ValueLogRewriteSegments; got != tt.wantSelected {
				t.Fatalf("rewrite debt segments=%d want %d, rewrite plan=%+v debt=%+v", got, tt.wantSelected, plan.ValueLogRewritePlan, plan.RemainingDebt)
			}
		})
	}
}

func TestCompactStorageFullRewriteRewritesHighStaleValueLogSegment(t *testing.T) {
	db := openCompactStorageRewritePolicyFixture(t, 2, 8, 1024)
	defer func() { _ = db.Close() }()

	opts := CompactStorageOptions{
		Mode:                                CompactStorageFull,
		ValueLogRewriteMinSegmentStaleRatio: 0.30,
		ValueLogRewriteMinSegmentStaleBytes: 1,
	}
	plan, err := db.CompactStoragePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := plan.ValueLogRewritePlan.SegmentsSelected; got != 1 {
		t.Fatalf("full policy selected segments=%d want 1, plan=%+v debt=%+v", got, plan.ValueLogRewritePlan, plan.RemainingDebt)
	}
	if got := plan.RemainingDebt.ValueLogRewriteSegments; got != 1 {
		t.Fatalf("policy rewrite debt=%d want 1, debt=%+v plan=%+v", got, plan.RemainingDebt, plan.ValueLogRewritePlan)
	}
	if plan.FullyCompacted || plan.PolicyFullyCompacted || plan.ByteMinimized {
		t.Fatalf("plan flags fully=%t policy=%t byte=%t want all false before rewrite", plan.FullyCompacted, plan.PolicyFullyCompacted, plan.ByteMinimized)
	}

	stats, err := db.CompactStorage(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if got := stats.ValueLogRewrite.SourceSegmentsRequested; got != 1 {
		t.Fatalf("applied rewrite source segments=%d want 1, stats=%+v", got, stats.ValueLogRewrite)
	}
	if got := stats.ValueLogRewrite.ValueRecordsCopied; got != 2 {
		t.Fatalf("applied rewrite copied records=%d want 2, stats=%+v", got, stats.ValueLogRewrite)
	}
	if got := stats.ValueLogRewrite.ValueBytesCopied; got <= 0 {
		t.Fatalf("applied rewrite copied bytes=%d want >0, stats=%+v", got, stats.ValueLogRewrite)
	}
	if stats.RemainingDebt.ValueLogRewriteSegments != 0 || stats.RemainingDebt.ValueLogRewriteBytes != 0 {
		t.Fatalf("remaining policy rewrite debt=%+v want none", stats.RemainingDebt)
	}
	if stats.RemainingDebt.ValueLogGCSegments == 0 || stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
		t.Fatalf("rewrite must retain older-root GC debt: flags fully=%t policy=%t byte=%t debt=%+v", stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized, stats.RemainingDebt)
	}
	advancePastRetainedDurableSlotForTest(t, db)
	converged, err := db.CompactStorage(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStorage after durable horizon advance: %v", err)
	}
	if !converged.FullyCompacted || !converged.PolicyFullyCompacted || converged.ByteMinimized {
		t.Fatalf("converged flags fully=%t policy=%t byte=%t debt=%+v", converged.FullyCompacted, converged.PolicyFullyCompacted, converged.ByteMinimized, converged.RemainingDebt)
	}
}

func TestCompactStorageFullRewriteDoesNotExemptSmallHighStaleValueLogSegment(t *testing.T) {
	db := openCompactStorageRewritePolicyFixture(t, 2, 8, 128)
	defer func() { _ = db.Close() }()

	// The source segment is far below the default 8 MiB absolute floor, but it
	// is 80% stale. Full mode caps that floor at its 30% ratio threshold so this
	// high-stale small segment remains actionable.
	opts := CompactStorageOptions{Mode: CompactStorageFull}
	plan, err := db.CompactStoragePlan(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := plan.ValueLogRewritePlan.SegmentsSelected; got != 1 {
		t.Fatalf("full policy selected segments=%d want 1, plan=%+v debt=%+v", got, plan.ValueLogRewritePlan, plan.RemainingDebt)
	}
	if got := plan.RemainingDebt.ValueLogRewriteSegments; got != 1 {
		t.Fatalf("policy rewrite debt=%d want 1, debt=%+v plan=%+v", got, plan.RemainingDebt, plan.ValueLogRewritePlan)
	}

	stats, err := db.CompactStorage(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if got := stats.ValueLogRewrite.SourceSegmentsRequested; got != 1 {
		t.Fatalf("applied rewrite source segments=%d want 1, stats=%+v", got, stats.ValueLogRewrite)
	}
	if got := stats.ValueLogRewrite.ValueBytesCopied; got <= 0 {
		t.Fatalf("applied rewrite copied bytes=%d want >0, stats=%+v", got, stats.ValueLogRewrite)
	}
	if stats.RemainingDebt.ValueLogGCSegments == 0 || stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
		t.Fatalf("rewrite must retain older-root GC debt: flags fully=%t policy=%t byte=%t debt=%+v", stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized, stats.RemainingDebt)
	}
	advancePastRetainedDurableSlotForTest(t, db)
	converged, err := db.CompactStorage(context.Background(), opts)
	if err != nil {
		t.Fatalf("CompactStorage after durable horizon advance: %v", err)
	}
	if !converged.FullyCompacted || !converged.PolicyFullyCompacted || converged.ByteMinimized {
		t.Fatalf("converged flags fully=%t policy=%t byte=%t debt=%+v", converged.FullyCompacted, converged.PolicyFullyCompacted, converged.ByteMinimized, converged.RemainingDebt)
	}
}

func TestCompactStorageExhaustiveRewriteSelectsAnyStaleValueLogSegment(t *testing.T) {
	db := openCompactStorageRewritePolicyFixture(t, 9, 1, 128)
	defer func() { _ = db.Close() }()

	fullPlan, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{
		Mode:                                CompactStorageFull,
		ValueLogRewriteMinSegmentStaleRatio: 0.30,
		ValueLogRewriteMinSegmentStaleBytes: 1,
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan full: %v", err)
	}
	if got := fullPlan.ValueLogRewritePlan.SegmentsSelected; got != 0 {
		t.Fatalf("full policy selected mostly-live segment=%d want 0, plan=%+v", got, fullPlan.ValueLogRewritePlan)
	}

	exhaustivePlan, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("CompactStoragePlan exhaustive: %v", err)
	}
	if got := exhaustivePlan.ValueLogRewritePlan.SegmentsSelected; got != 1 {
		t.Fatalf("exhaustive selected segments=%d want 1 for stale bytes, plan=%+v debt=%+v", got, exhaustivePlan.ValueLogRewritePlan, exhaustivePlan.RemainingDebt)
	}
	if exhaustivePlan.FullyCompacted || exhaustivePlan.PolicyFullyCompacted || exhaustivePlan.ByteMinimized {
		t.Fatalf("exhaustive plan flags fully=%t policy=%t byte=%t want all false before rewrite", exhaustivePlan.FullyCompacted, exhaustivePlan.PolicyFullyCompacted, exhaustivePlan.ByteMinimized)
	}

	stats, err := db.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("CompactStorage exhaustive: %v", err)
	}
	if got := stats.ValueLogRewrite.SourceSegmentsRequested; got != 1 {
		t.Fatalf("exhaustive rewrite source segments=%d want 1, stats=%+v", got, stats.ValueLogRewrite)
	}
	if got := stats.ValueLogRewrite.ValueRecordsCopied; got != 9 {
		t.Fatalf("exhaustive rewrite copied records=%d want 9, stats=%+v", got, stats.ValueLogRewrite)
	}
	if stats.RemainingDebt.ValueLogGCSegments == 0 || stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
		t.Fatalf("exhaustive rewrite must retain older-root GC debt: flags fully=%t policy=%t byte=%t debt=%+v", stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized, stats.RemainingDebt)
	}
	advancePastRetainedDurableSlotForTest(t, db)
	converged, err := db.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("CompactStorage exhaustive after durable horizon advance: %v", err)
	}
	if runtime.GOOS == "windows" {
		phase := compactStorageIndexVacuumPhase(t, converged)
		if phase.Status != CompactStoragePhaseStatusUnsupported || !phase.Required {
			t.Fatalf("windows index-vacuum phase=%+v want required unsupported", phase)
		}
		if converged.FullyCompacted || converged.PolicyFullyCompacted || converged.ByteMinimized || !converged.RemainingDebt.IndexVacuumRequired {
			t.Fatalf("windows unsupported vacuum overstated convergence: flags fully=%t policy=%t byte=%t debt=%+v", converged.FullyCompacted, converged.PolicyFullyCompacted, converged.ByteMinimized, converged.RemainingDebt)
		}
		return
	}
	if !converged.FullyCompacted || !converged.PolicyFullyCompacted || !converged.ByteMinimized {
		t.Fatalf("exhaustive converged flags fully=%t policy=%t byte=%t debt=%+v", converged.FullyCompacted, converged.PolicyFullyCompacted, converged.ByteMinimized, converged.RemainingDebt)
	}
}

func TestCompactStorageCompactedFlagsTruthTable(t *testing.T) {
	debt := CompactStorageDebt{ValueLogRewriteSegments: 1, ValueLogRewriteBytes: 1}
	for _, tt := range []struct {
		name       string
		opts       CompactStorageOptions
		debt       CompactStorageDebt
		wantFully  bool
		wantPolicy bool
		wantByte   bool
	}{
		{name: "full_empty_policy_only", opts: CompactStorageOptions{Mode: CompactStorageFull}, wantFully: true, wantPolicy: true, wantByte: false},
		{name: "full_policy_debt", opts: CompactStorageOptions{Mode: CompactStorageFull}, debt: debt, wantFully: false, wantPolicy: false, wantByte: false},
		{name: "quick_empty_policy_only", opts: CompactStorageOptions{Mode: CompactStorageQuick}, wantFully: true, wantPolicy: true, wantByte: false},
		{name: "exhaustive_empty_byte_minimized", opts: CompactStorageOptions{Mode: CompactStorageExhaustive}, wantFully: true, wantPolicy: true, wantByte: true},
		{name: "exhaustive_byte_debt", opts: CompactStorageOptions{Mode: CompactStorageExhaustive}, debt: debt, wantFully: false, wantPolicy: false, wantByte: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fully, policy, byteMinimized := compactStorageCompactionFlags(tt.opts, tt.debt)
			if fully != tt.wantFully || policy != tt.wantPolicy || byteMinimized != tt.wantByte {
				t.Fatalf("flags fully=%t policy=%t byte=%t want fully=%t policy=%t byte=%t", fully, policy, byteMinimized, tt.wantFully, tt.wantPolicy, tt.wantByte)
			}
		})
	}
}

func TestCompactStorageRewritePolicyDefaultsAndOverrides(t *testing.T) {
	for _, tt := range []struct {
		name      string
		opts      CompactStorageOptions
		wantRatio float64
		wantBytes int64
		wantCap   float64
	}{
		{name: "default_is_full_policy", opts: normalizeCompactStorageOptions(CompactStorageOptions{}), wantRatio: compactStoragePolicyValueLogRewriteMinStaleRatio, wantBytes: compactStoragePolicyValueLogRewriteMinStaleBytes, wantCap: compactStoragePolicyValueLogRewriteMinStaleRatio},
		{name: "quick_uses_policy", opts: normalizeCompactStorageOptions(CompactStorageOptions{Mode: CompactStorageQuick}), wantRatio: compactStoragePolicyValueLogRewriteMinStaleRatio, wantBytes: compactStoragePolicyValueLogRewriteMinStaleBytes, wantCap: compactStoragePolicyValueLogRewriteMinStaleRatio},
		{name: "full_override", opts: normalizeCompactStorageOptions(CompactStorageOptions{Mode: CompactStorageFull, ValueLogRewriteMinSegmentStaleRatio: 0.42, ValueLogRewriteMinSegmentStaleBytes: 4096}), wantRatio: 0.42, wantBytes: 4096, wantCap: 0},
		{name: "exhaustive_byte_minimizing", opts: normalizeCompactStorageOptions(CompactStorageOptions{Mode: CompactStorageExhaustive, ValueLogRewriteMinSegmentStaleRatio: 0.99, ValueLogRewriteMinSegmentStaleBytes: 1 << 30}), wantRatio: 0, wantBytes: 1, wantCap: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rewriteOpts := compactStorageRewritePlanOptions(tt.opts, nil)
			if rewriteOpts.MinSegmentStaleRatio != tt.wantRatio || rewriteOpts.MinSegmentStaleBytes != tt.wantBytes || rewriteOpts.MinSegmentStaleBytesCapRatio != tt.wantCap {
				t.Fatalf("rewrite policy ratio=%v bytes=%d cap=%v want ratio=%v bytes=%d cap=%v", rewriteOpts.MinSegmentStaleRatio, rewriteOpts.MinSegmentStaleBytes, rewriteOpts.MinSegmentStaleBytesCapRatio, tt.wantRatio, tt.wantBytes, tt.wantCap)
			}
		})
	}
}

func TestCompactStorageRewritePolicyRatioOnlyOverrideCapsDefaultByteFloor(t *testing.T) {
	opts := normalizeCompactStorageOptions(CompactStorageOptions{
		Mode:                                CompactStorageFull,
		ValueLogRewriteMinSegmentStaleRatio: 0.10,
	})
	rewriteOpts := compactStorageRewritePlanOptions(opts, nil)
	if rewriteOpts.MinSegmentStaleRatio != 0.10 || rewriteOpts.MinSegmentStaleBytes != compactStoragePolicyValueLogRewriteMinStaleBytes || rewriteOpts.MinSegmentStaleBytesCapRatio != 0.10 {
		t.Fatalf("ratio-only rewrite policy=%+v", rewriteOpts)
	}

	files := map[uint32]*valuelog.File{
		1: rewriteSelectorTestFile(t, "ratio-equal.log", 100),
		2: rewriteSelectorTestFile(t, "ratio-below.log", 100),
	}
	selected := selectRewriteSourceSegments(rewriteOpts, files, map[uint32]struct{}{}, map[uint32]int64{
		1: 90, // 10 stale bytes: ratio and capped byte-floor equality.
		2: 91, // 9 stale bytes: below both effective thresholds.
	})
	if _, ok := selected[1]; !ok {
		t.Fatalf("ratio-only override did not select equality candidate: selected=%v", selected)
	}
	if _, ok := selected[2]; ok {
		t.Fatalf("ratio-only override selected below-threshold candidate: selected=%v", selected)
	}
}

func TestCompactStorageRewritePolicyBytesOnlyOverrideKeepsExactFloor(t *testing.T) {
	const customStaleBytes = int64(40)
	opts := normalizeCompactStorageOptions(CompactStorageOptions{
		Mode:                                CompactStorageFull,
		ValueLogRewriteMinSegmentStaleBytes: customStaleBytes,
	})
	rewriteOpts := compactStorageRewritePlanOptions(opts, nil)
	if rewriteOpts.MinSegmentStaleRatio != compactStoragePolicyValueLogRewriteMinStaleRatio || rewriteOpts.MinSegmentStaleBytes != customStaleBytes || rewriteOpts.MinSegmentStaleBytesCapRatio != 0 {
		t.Fatalf("bytes-only rewrite policy=%+v", rewriteOpts)
	}

	files := map[uint32]*valuelog.File{
		1: rewriteSelectorTestFile(t, "bytes-below.log", 100),
		2: rewriteSelectorTestFile(t, "bytes-equal.log", 100),
	}
	selected := selectRewriteSourceSegments(rewriteOpts, files, map[uint32]struct{}{}, map[uint32]int64{
		1: 61, // 39% stale clears the default ratio but misses the exact byte floor.
		2: 60, // 40% stale clears both thresholds at byte-floor equality.
	})
	if _, ok := selected[1]; ok {
		t.Fatalf("bytes-only override selected below exact byte floor: selected=%v", selected)
	}
	if _, ok := selected[2]; !ok {
		t.Fatalf("bytes-only override did not select equality candidate: selected=%v", selected)
	}
}

func openCompactStorageRewritePolicyFixture(t *testing.T, liveRecords, staleRecords, valueSize int) *DB {
	t.Helper()
	if liveRecords <= 0 || staleRecords <= 0 {
		t.Fatalf("liveRecords and staleRecords must be positive: live=%d stale=%d", liveRecords, staleRecords)
	}
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	total := liveRecords + staleRecords
	ptrs := appendPointersInNewSegment(t, db.dir, 0, 1, 360_000, total, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i%23)}, valueSize)
	})
	activePtrs := appendPointersInNewSegment(t, db.dir, 0, 2, 460_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("z"), valueSize)
	})
	b, ok := db.NewBatch().(*Batch)
	if !ok {
		_ = db.Close()
		t.Fatalf("NewBatch type assertion failed")
	}
	for i := 0; i < liveRecords; i++ {
		if err := b.SetPointer([]byte(fmt.Sprintf("source-live-%06d", i)), ptrs[i]); err != nil {
			_ = b.Close()
			_ = db.Close()
			t.Fatalf("SetPointer source %d: %v", i, err)
		}
	}
	if err := b.SetPointer([]byte("active-live"), activePtrs[0]); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("SetPointer active: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch close: %v", err)
	}
	if err := db.RefreshValueLogSet(); err != nil {
		_ = db.Close()
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	return db
}

func TestCompactStorageReclaimsCoalescedSupersededValueLogDependency(t *testing.T) {
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
	registerTestValueLogProducer(t, dir, path1, id1)

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
	registerTestValueLogProducer(t, dir, path2, id2)

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
	// CompactStorage checkpoints the visible frontier before GC. Both ordinary
	// writes above are therefore coalesced into one durable root that names only
	// the final live dependency; the superseded intermediate root never occupies
	// a recovery-selectable slot and must not pin ptr1.
	if stats.ValueLogGC.SegmentsEligible == 0 || stats.ValueLogGC.SegmentsDeleted == 0 {
		t.Fatalf("coalesced superseded value-log dependency was not reclaimed: %+v", stats.ValueLogGC)
	}
	if stats.ValueLogGC.SegmentsPending != 0 || stats.ValueLogGC.BytesPending != 0 {
		t.Fatalf("coalesced superseded value-log dependency remained pending: %+v", stats.ValueLogGC)
	}
	if stats.RemainingDebt.ValueLogGCSegments != 0 || stats.RemainingDebt.ValueLogGCBytes != 0 {
		t.Fatalf("remaining GC debt=%+v, want no debt from a non-selectable intermediate root", stats.RemainingDebt)
	}
	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("coalesced superseded segment still exists: %v", err)
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

func TestCompactStorageDefaultProtectedPathsAreEmpty(t *testing.T) {
	paths := compactStorageValueLogProtectedPaths(CompactStorageOptions{})
	if len(paths) != 0 {
		t.Fatalf("default protected paths=%q want none", paths)
	}
}

func TestCompactStorageOnlineRewriteProtectedPaths_DefaultSentinel(t *testing.T) {
	paths := compactStorageOnlineRewriteProtectedPaths(CompactStorageOptions{})
	if !reflect.DeepEqual(paths, []string{""}) {
		t.Fatalf("online rewrite protected paths=%q want sentinel [\"\"]", paths)
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

func TestCompactStorageFencedValueLogProtectedPathsDefaultWhenDynamicEmpty(t *testing.T) {
	paths := compactStorageFencedValueLogProtectedPaths(CompactStorageOptions{
		ValueLogFencedProtectedPathsFunc: func() []string {
			return nil
		},
	})
	if len(paths) != 0 {
		t.Fatalf("fenced protected paths=%q want none when both static and dynamic are empty", paths)
	}
}

func TestCompactStorageFencedReclaimProtectsByFileID(t *testing.T) {
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
	if err := os.WriteFile(path, []byte("value-log-bytes"), 0644); err != nil {
		t.Fatalf("write value log: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("encode file ID: %v", err)
	}
	if err := d.valueLogManager.RegisterSegment(path, fileID); err != nil {
		t.Fatalf("register segment: %v", err)
	}

	ids, _, err := d.compactStorageFencedUnreferencedValueLogIDs(context.Background(), CompactStorageOptions{
		UnsafeValueLogReclaimFencedUnreferenced: true,
		ValueLogFencedProtectedPathsFunc: func() []string {
			return []string{filepath.Join(t.TempDir(), "value-l0-000002.log")}
		},
	})
	if err != nil {
		t.Fatalf("compactStorageFencedUnreferencedValueLogIDs: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("fenced reclaim IDs=%v want none", ids)
	}
}

type compactStorageFencedValueLogRefCounters struct {
	tracker        atomic.Uint64
	fallbackScan   atomic.Uint64
	validationScan atomic.Uint64
	other          atomic.Uint64
}

func withCompactStorageFencedValueLogRefCounters(t *testing.T, db *DB) *compactStorageFencedValueLogRefCounters {
	t.Helper()
	counters := &compactStorageFencedValueLogRefCounters{}
	prev := db.compactStorageFencedValueLogRefHook
	db.compactStorageFencedValueLogRefHook = func(event compactStorageFencedValueLogRefEvent) {
		switch event.Source {
		case valueLogRefResolutionSourceTracker:
			counters.tracker.Add(1)
		case valueLogRefResolutionSourceFallbackScan:
			counters.fallbackScan.Add(1)
		case valueLogRefResolutionSourceValidationScan:
			counters.validationScan.Add(1)
		default:
			counters.other.Add(1)
		}
		if prev != nil {
			prev(event)
		}
	}
	t.Cleanup(func() {
		db.compactStorageFencedValueLogRefHook = prev
	})
	return counters
}

func withValueLogRefFullScanCounter(t *testing.T) *atomic.Uint64 {
	t.Helper()
	var counter atomic.Uint64
	unregister := registerScanValueLogRefCountsHook(func() {
		counter.Add(1)
	})
	t.Cleanup(func() {
		unregister()
	})
	return &counter
}

func TestCompactStoragePlanFencedReclaimUsesSharedAuditRefs(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	fixture := seedValueLogRefIncrementalParityFixture(t, db, dir, true)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	if _, ok := db.valueLogRefTracker.referencedSet(db.currentCommitSeq()); !ok {
		t.Fatalf("expected valid value-log ref tracker")
	}

	counters := withCompactStorageFencedValueLogRefCounters(t, db)
	fullScans := withValueLogRefFullScanCounter(t)
	stats, err := db.CompactStoragePlan(context.Background(), CompactStorageOptions{
		UnsafeValueLogReclaimFencedUnreferenced: true,
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if counters.tracker.Load() != 0 || counters.fallbackScan.Load() != 0 || counters.validationScan.Load() != 1 || counters.other.Load() != 0 {
		t.Fatalf("fenced refs counters tracker=%d fallback=%d validation=%d other=%d; want validation=1 only",
			counters.tracker.Load(), counters.fallbackScan.Load(), counters.validationScan.Load(), counters.other.Load())
	}
	if got := fullScans.Load(); got != 0 {
		t.Fatalf("scanValueLogRefCounts calls during tracker-valid CompactStoragePlan=%d want 0", got)
	}
	if stats.RemainingDebt.ValueLogGCSegments == 0 {
		// RemainingDebt carries both ordinary GC and fenced debt; the hook and scan
		// counter assertions above prove fenced debt used the shared validation scan.
		t.Fatalf("expected value-log debt including unreferenced file %d, stats=%+v", fixture.UnreferencedFileID, stats.RemainingDebt)
	}
}

func TestCompactStorageFencedReclaimFallsBackWhenTrackerStale(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	fixture := seedValueLogRefIncrementalParityFixture(t, db, dir, true)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	opts := CompactStorageOptions{UnsafeValueLogReclaimFencedUnreferenced: true}
	wantIDs, wantBytes, err := db.compactStorageFencedUnreferencedValueLogIDs(context.Background(), opts)
	if err != nil {
		t.Fatalf("compactStorageFencedUnreferencedValueLogIDs tracker path: %v", err)
	}
	if !compactStorageIDListContains(wantIDs, fixture.UnreferencedFileID) {
		t.Fatalf("tracker path fenced IDs=%v want unreferenced file %d", wantIDs, fixture.UnreferencedFileID)
	}

	seq := db.currentCommitSeq()
	db.valueLogRefTracker.mu.Lock()
	db.valueLogRefTracker.commitSeq = seq + 1
	db.valueLogRefTracker.valid = true
	db.valueLogRefTracker.mu.Unlock()
	if _, ok := db.valueLogRefTracker.referencedSet(seq); ok {
		t.Fatalf("expected stale tracker to be rejected for seq=%d", seq)
	}

	counters := withCompactStorageFencedValueLogRefCounters(t, db)
	fullScans := withValueLogRefFullScanCounter(t)
	gotIDs, gotBytes, err := db.compactStorageFencedUnreferencedValueLogIDs(context.Background(), opts)
	if err != nil {
		t.Fatalf("compactStorageFencedUnreferencedValueLogIDs fallback path: %v", err)
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) || gotBytes != wantBytes {
		t.Fatalf("fallback fenced IDs/bytes=(%v,%d) want (%v,%d)", gotIDs, gotBytes, wantIDs, wantBytes)
	}
	if counters.tracker.Load() != 0 || counters.fallbackScan.Load() != 1 || counters.validationScan.Load() != 0 || counters.other.Load() != 0 {
		t.Fatalf("fenced refs counters tracker=%d fallback=%d validation=%d other=%d; want fallback=1 only",
			counters.tracker.Load(), counters.fallbackScan.Load(), counters.validationScan.Load(), counters.other.Load())
	}
	if got := fullScans.Load(); got != 1 {
		t.Fatalf("scanValueLogRefCounts calls for stale tracker fallback=%d want 1", got)
	}
	if _, ok := db.valueLogRefTracker.referencedSet(seq); !ok {
		t.Fatalf("expected fallback scan to refresh tracker for seq=%d", seq)
	}
}

func TestCompactStorageFencedReclaimTrackerFullScanParityBeforeDeletion(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	fixture := seedValueLogRefIncrementalParityFixture(t, db, dir, true)
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	seq := db.currentCommitSeq()
	trackerRefs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected tracker refs for seq=%d", seq)
	}
	fullCounts, fullSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}
	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(trackerRefs, fullRefs) {
		t.Fatalf("tracker/full-scan refs differ: tracker=%v full=%v", trackerRefs, fullRefs)
	}

	opts := CompactStorageOptions{UnsafeValueLogReclaimFencedUnreferenced: true}
	trackerIDs, trackerBytes, err := db.compactStorageFencedUnreferencedValueLogIDs(context.Background(), opts)
	if err != nil {
		t.Fatalf("tracker fenced IDs: %v", err)
	}
	db.valueLogRefTracker.invalidate()
	scanIDs, scanBytes, err := db.compactStorageFencedUnreferencedValueLogIDs(context.Background(), opts)
	if err != nil {
		t.Fatalf("scan fenced IDs: %v", err)
	}
	if !reflect.DeepEqual(scanIDs, trackerIDs) || scanBytes != trackerBytes {
		t.Fatalf("tracker/scan fenced IDs differ: tracker=(%v,%d) scan=(%v,%d)", trackerIDs, trackerBytes, scanIDs, scanBytes)
	}
	fenced := compactStorageIDListSet(scanIDs)
	for ref := range fullRefs {
		if _, ok := fenced[ref]; ok {
			t.Fatalf("referenced file %d appeared in fenced IDs %v", ref, scanIDs)
		}
	}
	if _, ok := fenced[fixture.UnreferencedFileID]; !ok {
		t.Fatalf("unreferenced file %d missing from fenced IDs %v", fixture.UnreferencedFileID, scanIDs)
	}
}

func compactStorageIDListContains(ids []uint32, id uint32) bool {
	_, ok := compactStorageIDListSet(ids)[id]
	return ok
}

func compactStorageIDListSet(ids []uint32) map[uint32]struct{} {
	out := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
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

	_, handoff, err := d.installCompactStorageLeafPageLog(opts)
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog: %v", err)
	}
	defer func() {
		if handoff != nil {
			_ = handoff.cleanup()
		}
	}()
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
	installed, handoff, err := d.installCompactStorageLeafPageLog(opts)
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog: %v", err)
	}
	defer func() {
		if handoff != nil {
			_ = handoff.cleanup()
		}
	}()
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

func TestCompactStorageLeafPageLogHandoffRestoresPreviousAndAdvancesSeq(t *testing.T) {
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
	previousAppender, err := newReplayInlineAppenderWithNextRID(d, nil, 1)
	if err != nil {
		t.Fatalf("newReplayInlineAppenderWithNextRID: %v", err)
	}
	defer func() { _ = previousAppender.close() }()
	previousOwner := replayInlineLeafPageLog{appender: previousAppender}
	d.SetLeafPageLog(previousOwner)
	previousInstalled := d.leafPageLog
	opts := CompactStorageOptions{
		Mode:        CompactStorageExhaustive,
		ReserveRIDs: newRewriteRIDAllocator(1, nil).Reserve,
	}
	installed, handoff, err := d.installCompactStorageLeafPageLog(opts)
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog: %v", err)
	}
	cleanupDone := false
	defer func() {
		if !cleanupDone && handoff != nil {
			_ = handoff.cleanup()
		}
	}()
	if installed == nil {
		t.Fatal("expected installed compact leaf writer")
	}
	if d.leafPageLog == previousInstalled {
		t.Fatal("compact handoff did not install a replacement leaf-page log")
	}
	if _, err := installed.AppendLeafPage(bytes.Repeat([]byte("h"), page.PageSize)); err != nil {
		t.Fatalf("installed AppendLeafPage: %v", err)
	}
	if err := handoff.cleanup(); err != nil {
		t.Fatalf("cleanup handoff: %v", err)
	}
	cleanupDone = true
	if d.leafPageLog != previousInstalled {
		t.Fatalf("restored leaf-page log=%T want exact previous %T", d.leafPageLog, previousInstalled)
	}
	if _, err := d.leafPageLog.AppendLeafPage(bytes.Repeat([]byte("r"), page.PageSize)); err != nil {
		t.Fatalf("restored owner AppendLeafPage: %v", err)
	}
	gotPath, _, ok := previousOwner.CurrentValueLogSegment()
	if !ok {
		t.Fatal("restored owner did not report current leaf segment")
	}
	if got := filepath.Base(gotPath); got != "value-l255-000002.log" {
		t.Fatalf("restored owner segment=%s, want value-l255-000002.log", got)
	}
}

type compactStorageFailingHandoffLeafPageLog struct {
	err          error
	advanceCalls int
}

func (l *compactStorageFailingHandoffLeafPageLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, nil
}

func (l *compactStorageFailingHandoffLeafPageLog) Flush() error { return nil }

func (l *compactStorageFailingHandoffLeafPageLog) Sync() error { return nil }

func (l *compactStorageFailingHandoffLeafPageLog) AdvanceCompactStorageLeafPageLogSeqAtLeast(uint32) error {
	l.advanceCalls++
	return l.err
}

func TestCompactStorageLeafPageLogCleanupErrorPropagatesOnce(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)
	writeLeafGenerationKeys(t, d, "cleanup", 64, 'c')
	currentLeafSegmentOrFatal(t, leafLog)

	cleanupErr := errors.New("compact cleanup advance failure")
	failingHandoff := &compactStorageFailingHandoffLeafPageLog{err: cleanupErr}
	previousOwner := &leafPageLogLaneGroup{
		lanes: []LeafPageLog{
			replayInlineLeafPageLog{},
			failingHandoff,
		},
	}
	d.SetLeafPageLog(previousOwner)

	_, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("CompactStorage error=%v, want cleanup handoff error %v", err, cleanupErr)
	}
	if failingHandoff.advanceCalls != 1 {
		t.Fatalf("cleanup handoff calls=%d want 1", failingHandoff.advanceCalls)
	}
	if d.leafPageLog != nil {
		t.Fatalf("leaf-page log was not cleared after cleanup error: got %T want nil fail-closed writer", d.leafPageLog)
	}
}

func TestCompactStorageLeafPageLogHandoffAdvanceFailureFailsClosed(t *testing.T) {
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
	advanceErr := errors.New("injected advance failure")
	failingHandoff := &compactStorageFailingHandoffLeafPageLog{err: advanceErr}
	previousOwner := &leafPageLogLaneGroup{
		lanes: []LeafPageLog{
			replayInlineLeafPageLog{},
			failingHandoff,
		},
	}
	d.SetLeafPageLog(previousOwner)
	opts := CompactStorageOptions{
		Mode:        CompactStorageExhaustive,
		ReserveRIDs: newRewriteRIDAllocator(1, nil).Reserve,
	}
	installed, handoff, err := d.installCompactStorageLeafPageLog(opts)
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog: %v", err)
	}
	if installed == nil || handoff == nil {
		t.Fatal("expected installed compact handoff")
	}
	if _, err := installed.AppendLeafPage(bytes.Repeat([]byte("h"), page.PageSize)); err != nil {
		t.Fatalf("installed AppendLeafPage: %v", err)
	}
	err = handoff.cleanup()
	if !errors.Is(err, ErrCompactStorageLeafPageLogHandoffCleanup) {
		t.Fatalf("cleanup error=%v, want handoff cleanup sentinel", err)
	}
	if !errors.Is(err, advanceErr) {
		t.Fatalf("cleanup error=%v, want injected advance error", err)
	}
	var handoffErr *CompactStorageLeafPageLogHandoffError
	if !errors.As(err, &handoffErr) {
		t.Fatalf("cleanup error=%T, want CompactStorageLeafPageLogHandoffError", err)
	}
	if handoffErr.Stage != "restore previous owner" {
		t.Fatalf("handoff stage=%q want restore previous owner", handoffErr.Stage)
	}
	if d.leafPageLog != nil {
		t.Fatalf("leaf-page log=%T, want fail-closed nil active writer", d.leafPageLog)
	}
	if failingHandoff.advanceCalls != 1 {
		t.Fatalf("previous owner advance calls=%d want 1", failingHandoff.advanceCalls)
	}
}

func TestCompactStorageLeafPageLogHandoffCloseFailureFailsClosed(t *testing.T) {
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
	previousHandoff := &compactStorageFailingHandoffLeafPageLog{}
	previousOwner := &leafPageLogLaneGroup{
		lanes: []LeafPageLog{
			replayInlineLeafPageLog{},
			previousHandoff,
		},
	}
	d.SetLeafPageLog(previousOwner)
	opts := CompactStorageOptions{
		Mode:        CompactStorageExhaustive,
		ReserveRIDs: newRewriteRIDAllocator(1, nil).Reserve,
	}
	installed, handoff, err := d.installCompactStorageLeafPageLog(opts)
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog: %v", err)
	}
	if installed == nil || handoff == nil {
		t.Fatal("expected installed compact handoff")
	}
	if _, err := installed.AppendLeafPage(bytes.Repeat([]byte("c"), page.PageSize)); err != nil {
		t.Fatalf("installed AppendLeafPage: %v", err)
	}

	closeErr := errors.New("injected compact writer close failure")
	origClose := compactStorageLeafPageLogHandoffCloseWriter
	compactStorageLeafPageLogHandoffCloseWriter = func(w *rewriteWriter) error {
		_ = origClose(w)
		return closeErr
	}
	t.Cleanup(func() { compactStorageLeafPageLogHandoffCloseWriter = origClose })

	err = handoff.cleanup()
	if !errors.Is(err, ErrCompactStorageLeafPageLogHandoffCleanup) {
		t.Fatalf("cleanup error=%v, want handoff cleanup sentinel", err)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("cleanup error=%v, want injected close error", err)
	}
	var handoffErr *CompactStorageLeafPageLogHandoffError
	if !errors.As(err, &handoffErr) {
		t.Fatalf("cleanup error=%T, want CompactStorageLeafPageLogHandoffError", err)
	}
	if handoffErr.Stage != "close compact writer" {
		t.Fatalf("handoff stage=%q want close compact writer", handoffErr.Stage)
	}
	if d.leafPageLog != nil {
		t.Fatalf("leaf-page log=%T, want fail-closed nil active writer", d.leafPageLog)
	}
	if previousHandoff.advanceCalls != 0 {
		t.Fatalf("previous owner advance calls=%d want 0 after close-stage failure", previousHandoff.advanceCalls)
	}
	if err := handoff.cleanup(); err != nil {
		t.Fatalf("second cleanup should be idempotent, got %v", err)
	}
}

func TestCompactStorageLeafPageLogHandoffScanFailureFailsClosed(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(ValueLogDirPath(dir), 0755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	leafDir := LeafLogDirPath(dir)
	if err := os.MkdirAll(leafDir, 0755); err != nil {
		t.Fatalf("mkdir leaf_vlog: %v", err)
	}

	d := &DB{
		dir:                        dir,
		indexOuterLeavesInValueLog: true,
		valueLogCompression:        ValueLogCompressionOff,
	}
	previousOwner := replayInlineLeafPageLog{}
	d.SetLeafPageLog(previousOwner)
	opts := CompactStorageOptions{
		Mode:        CompactStorageExhaustive,
		ReserveRIDs: newRewriteRIDAllocator(1, nil).Reserve,
	}
	installed, handoff, err := d.installCompactStorageLeafPageLog(opts)
	if err != nil {
		t.Fatalf("installCompactStorageLeafPageLog: %v", err)
	}
	if installed == nil || handoff == nil {
		t.Fatal("expected installed compact handoff")
	}
	if _, err := installed.AppendLeafPage(bytes.Repeat([]byte("s"), page.PageSize)); err != nil {
		t.Fatalf("installed AppendLeafPage: %v", err)
	}

	scanErr := errors.New("injected compact leaf segment scan failure")
	origListSegments := compactStorageLeafPageLogHandoffListSegments
	compactStorageLeafPageLogHandoffListSegments = func(string) ([]logSegment, error) {
		return nil, scanErr
	}
	t.Cleanup(func() { compactStorageLeafPageLogHandoffListSegments = origListSegments })

	err = handoff.cleanup()
	if !errors.Is(err, ErrCompactStorageLeafPageLogHandoffCleanup) {
		t.Fatalf("cleanup error=%v, want handoff cleanup sentinel", err)
	}
	if !errors.Is(err, scanErr) {
		t.Fatalf("cleanup error=%v, want injected scan error", err)
	}
	var handoffErr *CompactStorageLeafPageLogHandoffError
	if !errors.As(err, &handoffErr) {
		t.Fatalf("cleanup error=%T, want CompactStorageLeafPageLogHandoffError", err)
	}
	if handoffErr.Stage != "scan compact leaf segments" {
		t.Fatalf("handoff stage=%q want scan compact leaf segments", handoffErr.Stage)
	}
	if d.leafPageLog != nil {
		t.Fatalf("leaf-page log=%T, want fail-closed nil active writer", d.leafPageLog)
	}
}

func TestCompactStoragePreInstallFailureDoesNotMutateLeafPageLogOwner(t *testing.T) {
	d, _, wantValue := openCompactStorageCommandWALLeafPageLogDB(t)
	defer func() { _ = d.Close() }()
	previousLeafPageLog := d.leafPageLog

	scanErr := errors.New("injected compact rid scan failure")
	origLister := rewriteWALSegmentsLister
	rewriteWALSegmentsLister = func(string) ([]logSegment, error) {
		return nil, scanErr
	}
	t.Cleanup(func() { rewriteWALSegmentsLister = origLister })

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if !errors.Is(err, scanErr) {
		t.Fatalf("CompactStorage error=%v, want injected scan failure", err)
	}
	if d.leafPageLog != previousLeafPageLog {
		t.Fatalf("pre-install failure mutated owner: got %T want %T", d.leafPageLog, previousLeafPageLog)
	}
	if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
		t.Fatalf("pre-install failure reported compact success flags: %+v", stats)
	}
	got, err := d.Get([]byte("canonical"))
	if err != nil {
		t.Fatalf("Get canonical after pre-install failure: %v", err)
	}
	if !bytes.Equal(got, wantValue) {
		t.Fatalf("canonical value mismatch after pre-install failure: got %q want %q", got, wantValue)
	}
	if err := d.SetSync([]byte("post-pre-install-failure"), []byte("ok")); err != nil {
		t.Fatalf("SetSync after pre-install failure: %v", err)
	}
}

func TestCompactStorageInterruptedHandoffRestoresPreviousLeafPageLog(t *testing.T) {
	tests := []struct {
		name        string
		cancelAfter string
	}{
		{
			name:        "during rewrite after compact writer install",
			cancelAfter: "checkpoint",
		},
		{
			name:        "after value log rewrite registration checkpoint",
			cancelAfter: "checkpoint-after-value-log-rewrite",
		},
		{
			name:        "after generation seal",
			cancelAfter: "seal-current-leaf-generation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, dir, wantValue := openCompactStorageCommandWALLeafPageLogDB(t)
			closed := false
			defer func() {
				if !closed {
					_ = d.Close()
				}
			}()
			previousLeafPageLog := d.leafPageLog
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d.compactStorageAfterPhase = func(name string) {
				if name == tt.cancelAfter {
					cancel()
				}
			}

			stats, err := d.CompactStorage(ctx, CompactStorageOptions{Mode: CompactStorageExhaustive})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("CompactStorage error=%v, want context.Canceled after %s", err, tt.cancelAfter)
			}
			if d.leafPageLog != previousLeafPageLog {
				t.Fatalf("interrupted compact restored leaf-page log=%T want exact previous %T", d.leafPageLog, previousLeafPageLog)
			}
			if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
				t.Fatalf("interrupted compact reported compact success flags: %+v", stats)
			}
			got, err := d.Get([]byte("canonical"))
			if err != nil {
				t.Fatalf("Get canonical after interrupted compact: %v", err)
			}
			if !bytes.Equal(got, wantValue) {
				t.Fatalf("canonical value mismatch after interrupted compact: got %q want %q", got, wantValue)
			}
			postKey := []byte("post-" + tt.name)
			if err := d.SetSync(postKey, []byte("ok")); err != nil {
				t.Fatalf("SetSync after interrupted compact: %v", err)
			}
			got, err = d.Get(postKey)
			if err != nil {
				t.Fatalf("Get post key after interrupted compact: %v", err)
			}
			if string(got) != "ok" {
				t.Fatalf("post value=%q want ok", got)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("close after interrupted compact: %v", err)
			}
			closed = true

			reopened, err := Open(compactStorageCommandWALLeafPageLogOptions(dir))
			if err != nil {
				t.Fatalf("reopen after interrupted compact: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			got, err = reopened.Get([]byte("canonical"))
			if err != nil {
				t.Fatalf("reopen Get canonical: %v", err)
			}
			if !bytes.Equal(got, wantValue) {
				t.Fatalf("reopen canonical value mismatch: got %q want %q", got, wantValue)
			}
			got, err = reopened.Get(postKey)
			if err != nil {
				t.Fatalf("reopen Get post key: %v", err)
			}
			if string(got) != "ok" {
				t.Fatalf("reopen post value=%q want ok", got)
			}
		})
	}
}

func openCompactStorageCommandWALLeafPageLogDB(t *testing.T) (*DB, string, []byte) {
	t.Helper()
	dir := t.TempDir()
	d, err := Open(compactStorageCommandWALLeafPageLogOptions(dir))
	if err != nil {
		t.Fatalf("Open command WAL value-log outer leaves: %v", err)
	}
	if d.leafPageLog == nil {
		_ = d.Close()
		t.Fatal("expected command WAL open to install replay-inline leaf page log")
	}
	for i := 0; i < 96; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		value := bytes.Repeat([]byte{byte('a' + i%26)}, 384)
		if err := d.SetSync(key, value); err != nil {
			_ = d.Close()
			t.Fatalf("SetSync(%d): %v", i, err)
		}
	}
	wantValue := bytes.Repeat([]byte("z"), 512)
	if err := d.SetSync([]byte("canonical"), wantValue); err != nil {
		_ = d.Close()
		t.Fatalf("SetSync canonical: %v", err)
	}
	return d, dir, wantValue
}

func compactStorageCommandWALLeafPageLogOptions(dir string) Options {
	return Options{
		Dir:                        dir,
		CommandWAL:                 true,
		Durability:                 DurabilityWALOnRelaxed,
		DisableSideStores:          true,
		IndexOuterLeavesInValueLog: true,
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

	registered, err := d.registerLeafPageLogSegmentsForPublish()
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
	rawFileID, ok := rawLeafGenerationFileID(createdSegments[0].fileID)
	if !ok {
		t.Fatalf("raw leaf generation file id missing for segment %d", createdSegments[0].fileID)
	}
	d.leafGenerationPendingMu.Lock()
	_, pending := d.leafGenerationPendingSet[rawFileID]
	pendingCommitSeq := d.leafGenerationPendingCommitSeq[rawFileID]
	d.leafGenerationPendingMu.Unlock()
	if !pending {
		t.Fatalf("raw leaf generation file id %d was not queued pending", rawFileID)
	}
	if pendingCommitSeq != 0 {
		t.Fatalf("pending commitSeq=%d want 0 before publish commit succeeds", pendingCommitSeq)
	}
}

func TestRegisterLeafPageLogSegmentsForPublishQueuesCurrentLeafGenerationSegment(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	path, fileID := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 9)
	d.SetLeafPageLog(&manifestTestLeafPageLog{path: path, fileID: fileID})
	if d.valueLogManager.HasSegment(fileID) {
		t.Fatalf("segment %d was registered before publish helper", fileID)
	}

	registered, err := d.registerLeafPageLogSegmentsForPublish()
	if err != nil {
		t.Fatalf("registerLeafPageLogSegmentsForPublish: %v", err)
	}
	if !registered {
		t.Fatal("current leaf segment was not registered")
	}
	if !d.valueLogManager.HasSegment(fileID) {
		t.Fatalf("segment %d was not registered", fileID)
	}

	rawFileID, ok := rawLeafGenerationFileID(fileID)
	if !ok {
		t.Fatalf("raw leaf generation file id missing for segment %d", fileID)
	}
	d.leafGenerationPendingMu.Lock()
	_, pending := d.leafGenerationPendingSet[rawFileID]
	pendingCommitSeq := d.leafGenerationPendingCommitSeq[rawFileID]
	d.leafGenerationPendingMu.Unlock()
	if !pending {
		t.Fatalf("raw leaf generation file id %d was not queued pending", rawFileID)
	}
	if pendingCommitSeq != 0 {
		t.Fatalf("pending commitSeq=%d want 0 before publish commit succeeds", pendingCommitSeq)
	}
	staged, changed, err := d.stagedLeafGenerationManifestWithPending(d.leafGenerationManifest, 0, 222)
	if err != nil {
		t.Fatalf("stagedLeafGenerationManifestWithPending: %v", err)
	}
	if !changed {
		t.Fatal("expected staged manifest change")
	}
	current := staged.Generations[staged.currentGenerationIndex()]
	if got, want := current.FileIDs, []uint32{rawFileID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged current FileIDs=%v want %v", got, want)
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

func TestCompactStorageReportsRetiringLeafGenerationAfterIndexVacuum(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("index vacuum is unsupported on Windows; skipped-vacuum guard intentionally keeps leaf sources pinned")
	}
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
	if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
		t.Fatalf("retiring leaf generation overstated completion: %+v", stats)
	}
	phase := compactStorageIndexVacuumPhase(t, stats)
	if phase.Status != CompactStoragePhaseStatusSucceeded || !phase.Required || phase.Reason != "leaf_generation" {
		t.Fatalf("index vacuum phase=%+v want required leaf-generation success", phase)
	}
	if stats.RemainingDebt.LeafGCGenerations == 0 {
		t.Fatalf("retiring leaf generation missing from final debt: %+v", stats.RemainingDebt)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("retiring leaf source removed while index vacuum is fenced: %v", err)
	}
	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	gen := findLeafGenerationByFileID(t, manifest, rawFileID1)
	if got, want := gen.State, leafGenerationStateRetiring; got != want {
		t.Fatalf("generation state after fenced vacuum=%q want %q manifest=%+v", got, want, manifest.Generations)
	}
}

func TestCompactStorageKeepsPackedLeafSourcesUntilIndexVacuum(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "k", 2048, 'a')
	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, d, "z", 32, 'z')
	d.SetLeafPageLog(nil)

	sawLeafGC := false
	d.compactStorageAfterPhase = func(name string) {
		if name != "leaf-generation-gc" {
			return
		}
		sawLeafGC = true
		if _, err := os.Stat(path1); err != nil {
			t.Fatalf("pre-vacuum leaf segment removed: %v", err)
		}
		manifest := loadLeafGenerationManifestOrFatal(t, dir)
		gen := findLeafGenerationByFileID(t, manifest, rawFileID1)
		if got, want := gen.State, leafGenerationStateRetiring; got != want {
			t.Fatalf("generation state after leaf gc=%q want %q manifest=%+v", got, want, manifest.Generations)
		}
	}
	t.Cleanup(func() { d.compactStorageAfterPhase = nil })

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{LeafPackMaxPasses: 4})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if !sawLeafGC {
		t.Fatal("leaf-generation-gc phase hook did not run")
	}
	if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.RemainingDebt.LeafGCGenerations == 0 {
		t.Fatalf("retained packed source must remain reported debt: %+v", stats)
	}
}

func TestCompactStorageWindowsKeepsPackedLeafSourcesWhenIndexVacuumUnsupported(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("index vacuum is supported on this platform")
	}
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "k", 2048, 'a')
	path1, _ := currentLeafSegmentOrFatal(t, leafLog)
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, d, "z", 32, 'z')
	d.SetLeafPageLog(nil)

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{LeafPackMaxPasses: 4})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	indexVacuumSkipped := false
	for _, phase := range stats.Phases {
		if phase.Name == "index-vacuum" && phase.Skipped {
			indexVacuumSkipped = true
			break
		}
	}
	if !indexVacuumSkipped {
		t.Fatalf("index-vacuum was not skipped on windows: %+v", stats.Phases)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("leaf source removed after skipped index-vacuum: %v", err)
	}
}

func TestCompactStoragePlan_ProtectedRootIDsKeepDetachedLeafGenerationLive(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	rootTable := mustFrozenSystemMemtable(t, systemRangeKVs(2048, nil)...)
	rootID, err := d.PublishOrderedRootIterator(0, rootTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	if rootID == 0 {
		t.Fatal("expected non-zero detached root")
	}
	if state := d.State(); state.RootPageID == rootID || state.SystemRootPageID == rootID {
		t.Fatalf("test requires detached root, got state roots user=%d system=%d detached=%d", state.RootPageID, state.SystemRootPageID, rootID)
	}

	path1, fileID1 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	refs := collectLeafRefIDsFromRoot(t, d, rootID)
	refsFile := false
	for ptr := range refs {
		if ptr.FileID == rawFileID1 {
			refsFile = true
			break
		}
	}
	if !refsFile {
		t.Fatalf("detached root refs do not include raw file id %d: %+v", rawFileID1, refs)
	}

	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeys(t, d, "current", 1, 'z')

	unprotectedPlan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan unprotected: %v", err)
	}
	if got := unprotectedPlan.RemainingDebt.LeafGCGenerations; got == 0 {
		t.Fatalf("LeafGCGenerations=%d want detached generation eligible without protection (debt=%+v)", got, unprotectedPlan.RemainingDebt)
	}

	protectedPlan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{
		LeafGenerationProtectedRootIDs: []uint64{0, rootID, rootID},
		LeafPackMaxPasses:              1,
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan protected: %v", err)
	}
	if got := protectedPlan.RemainingDebt.LeafGCGenerations; got != 0 {
		t.Fatalf("protected LeafGCGenerations=%d want 0 (debt=%+v)", got, protectedPlan.RemainingDebt)
	}
	if got := protectedPlan.LeafGenerationGC.GenerationsEligible; got != 0 {
		t.Fatalf("protected GenerationsEligible=%d want 0 (stats=%+v)", got, protectedPlan.LeafGenerationGC)
	}
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected protected leaf segment to remain: %v", err)
	}
	if got := collectLeafRefIDsFromRoot(t, d, rootID); len(got) == 0 {
		t.Fatalf("expected detached root %d leaf refs to remain readable", rootID)
	}
}

func TestCompactStorageLeafGenerationProtectedRootIDPairMergesSourcesOnce(t *testing.T) {
	d := &DB{}
	pairCalls := 0
	opts := CompactStorageOptions{
		LeafGenerationProtectedRootIDs:       []uint64{0, 1, 2, 1},
		LeafGenerationProtectedSystemRootIDs: []uint64{10, 0, 10},
		LeafGenerationProtectedRootIDsFunc: func() []uint64 {
			return []uint64{2, 3}
		},
		LeafGenerationProtectedSystemRootIDsFunc: func() []uint64 {
			return []uint64{10, 11}
		},
		LeafGenerationProtectedRootIDPairFunc: func() ([]uint64, []uint64) {
			pairCalls++
			return []uint64{3, 4}, []uint64{11, 12}
		},
	}

	rootIDs, systemRootIDs := d.compactStorageLeafGenerationProtectedRootIDPair(opts)
	if pairCalls != 1 {
		t.Fatalf("pair callback calls=%d want 1", pairCalls)
	}
	if want := []uint64{1, 2, 3, 4}; !reflect.DeepEqual(rootIDs, want) {
		t.Fatalf("rootIDs=%v want %v", rootIDs, want)
	}
	if want := []uint64{10, 11, 12}; !reflect.DeepEqual(systemRootIDs, want) {
		t.Fatalf("systemRootIDs=%v want %v", systemRootIDs, want)
	}
}

func TestCompactStorageReportsAndCompactsSelectableLeafPackDebt(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "k", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "k", 0, 1024, 'b')
	writeLeafGenerationKeys(t, d, "z", 32, 'z')
	d.SetLeafPageLog(nil)

	plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := plan.RemainingDebt.LeafPackGenerations; got == 0 {
		t.Fatalf("LeafPackGenerations=%d want selectable debt, plan=%+v debt=%+v", got, plan.LeafGenerationPlan, plan.RemainingDebt)
	}
	if got := plan.LeafGenerationPlan.ExpectedReclaimPerByteCopiedPPM; got < leafGenerationPackDefaultMinReclaimPerByteCopiedPPM {
		t.Fatalf("test setup produced low-yield plan per-copy=%d, want >= %d", got, leafGenerationPackDefaultMinReclaimPerByteCopiedPPM)
	}

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{LeafPackMaxPasses: 4})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if len(stats.LeafGenerationPacks) == 0 || !stats.LeafGenerationPacks[0].Ran {
		t.Fatalf("expected first leaf-generation pack to run, packs=%+v", stats.LeafGenerationPacks)
	}
	if stats.RemainingDebt.LeafPackGenerations != 0 || stats.RemainingDebt.LeafPackBytes != 0 {
		t.Fatalf("remaining leaf-pack debt=%+v, want none", stats.RemainingDebt)
	}
	if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.RemainingDebt.LeafGCGenerations == 0 {
		t.Fatalf("retained source must remain reported leaf-GC debt: %+v", stats)
	}
}

func TestCompactStoragePlanSkipsLowYieldLeafPackDebtByDefault(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "k", 32768, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "k", 0, 1, 'b')
	d.SetLeafPageLog(nil)

	plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if plan.LeafGenerationPlan.ExpectedReclaimBytes <= 0 || len(plan.LeafGenerationPlan.Candidates) == 0 {
		t.Fatalf("test setup produced no raw leaf-pack candidate: plan=%+v", plan.LeafGenerationPlan)
	}
	if got := plan.LeafGenerationPlan.ExpectedReclaimPerByteCopiedPPM; got >= leafGenerationPackDefaultMinReclaimPerByteCopiedPPM {
		t.Fatalf("test setup per-copy=%d, want below default %d", got, leafGenerationPackDefaultMinReclaimPerByteCopiedPPM)
	}
	if got := plan.RemainingDebt.LeafPackGenerations; got != 0 {
		t.Fatalf("low-yield LeafPackGenerations=%d want 0, plan=%+v debt=%+v", got, plan.LeafGenerationPlan, plan.RemainingDebt)
	}
	if got := plan.RemainingDebt.LeafPackBytes; got != 0 {
		t.Fatalf("low-yield LeafPackBytes=%d want 0, plan=%+v debt=%+v", got, plan.LeafGenerationPlan, plan.RemainingDebt)
	}

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{LeafPackMaxPasses: 1})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if len(stats.LeafGenerationPacks) == 0 || stats.LeafGenerationPacks[0].Ran {
		t.Fatalf("expected low-yield leaf pack to skip, packs=%+v", stats.LeafGenerationPacks)
	}
	if runtime.GOOS == "windows" {
		phase := compactStorageIndexVacuumPhase(t, stats)
		if !phase.Required || phase.Status != CompactStoragePhaseStatusUnsupported {
			t.Fatalf("windows index-vacuum phase=%+v want required unsupported", phase)
		}
		if stats.FullyCompacted || !stats.RemainingDebt.IndexVacuumRequired {
			t.Fatalf("windows unsupported vacuum overstated convergence: %+v", stats)
		}
		return
	}
	if !stats.FullyCompacted {
		t.Fatalf("FullyCompacted=false for low-yield residual, remaining debt=%+v", stats.RemainingDebt)
	}
}

func TestCompactStoragePlanLeafPackDebtUsesBoundedSelection(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "dense", 32768, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf dense: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "dense", 0, 1, 'b')

	writeLeafGenerationKeys(t, d, "sparse", 2048, 'c')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf sparse: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "sparse", 0, 1024, 'd')
	d.SetLeafPageLog(nil)

	const minPerCopy = 200000
	plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{
		LeafPackMaxGenerationsPerPass: 1,
		LeafPackMinReclaimPerCopyPPM:  minPerCopy,
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if got := plan.LeafGenerationPlan.ExpectedReclaimPerByteCopiedPPM; got >= minPerCopy {
		t.Fatalf("test setup whole-plan per-copy=%d, want below %d to prove bounded selection", got, minPerCopy)
	}
	if got := plan.RemainingDebt.LeafPackGenerations; got != 1 {
		t.Fatalf("bounded selection LeafPackGenerations=%d want 1, plan=%+v debt=%+v", got, plan.LeafGenerationPlan, plan.RemainingDebt)
	}
	if got := plan.RemainingDebt.LeafPackBytes; got <= 0 {
		t.Fatalf("bounded selection LeafPackBytes=%d want >0, debt=%+v", got, plan.RemainingDebt)
	}
}

func TestCompactStorageSettleHonorsLeafPackMaxPasses(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "left", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf left: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "left", 0, 1024, 'b')

	writeLeafGenerationKeys(t, d, "right", 2048, 'c')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf right: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "right", 0, 1024, 'd')
	writeLeafGenerationKeys(t, d, "current", 16, 'e')
	d.SetLeafPageLog(nil)

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{
		LeafPackMaxPasses:             1,
		LeafPackMaxGenerationsPerPass: 1,
		LeafPackMinReclaimPerCopyPPM:  1,
	})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if got := len(stats.LeafGenerationPacks); got != 1 {
		t.Fatalf("leaf pack runs=%d want exactly one capped pass, packs=%+v", got, stats.LeafGenerationPacks)
	}
	if !stats.LeafGenerationPacks[0].Ran {
		t.Fatalf("capped leaf pack did not run: %+v", stats.LeafGenerationPacks[0])
	}
	if compactStoragePhaseSeen(stats.Phases, "settle-leaf-generation-pack-1") {
		t.Fatalf("settle ran an extra leaf-pack pass despite exhausted LeafPackMaxPasses: phases=%+v", stats.Phases)
	}
	if stats.FullyCompacted {
		t.Fatalf("FullyCompacted=true want residual debt after capped pass")
	}
	if stats.RemainingDebt.LeafPackGenerations == 0 || stats.RemainingDebt.LeafPackBytes == 0 {
		t.Fatalf("remaining leaf-pack debt=%+v want residual capped debt", stats.RemainingDebt)
	}
}

func TestCompactStorageStopsLeafPackOnLowYieldResidualWithinPassBudget(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "dense", 32768, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf dense: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "dense", 0, 1, 'b')

	writeLeafGenerationKeys(t, d, "sparse", 2048, 'c')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf sparse: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "sparse", 0, 1024, 'd')
	d.SetLeafPageLog(nil)

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{
		LeafPackMaxPasses:             4,
		LeafPackMaxGenerationsPerPass: 1,
		LeafPackMinReclaimPerCopyPPM:  200000,
	})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if len(stats.LeafGenerationPacks) < 2 {
		t.Fatalf("expected run plus bounded low-yield skip, packs=%+v", stats.LeafGenerationPacks)
	}
	if !stats.LeafGenerationPacks[0].Ran {
		t.Fatalf("first pack did not run: %+v", stats.LeafGenerationPacks[0])
	}
	if stats.LeafGenerationPacks[1].Ran || stats.LeafGenerationPacks[1].SkipReason == "" {
		t.Fatalf("second pack should stop on low-yield residual, packs=%+v", stats.LeafGenerationPacks)
	}
	if len(stats.LeafGenerationPacks) > 2 {
		t.Fatalf("leaf pack did not stop after low-yield residual, packs=%+v", stats.LeafGenerationPacks)
	}
	if stats.RemainingDebt.LeafPackGenerations != 0 || stats.RemainingDebt.LeafPackBytes != 0 {
		t.Fatalf("expected bounded leaf-pack policy convergence, debt=%+v", stats.RemainingDebt)
	}
	if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.RemainingDebt.LeafGCGenerations == 0 {
		t.Fatalf("retained leaf-GC debt must keep completion false: %+v", stats)
	}
}

func TestCompactStorageLeafPackMultiPassReusesLiveScan(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "left", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf left: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "left", 0, 1024, 'b')

	writeLeafGenerationKeys(t, d, "right", 2048, 'c')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf right: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "right", 0, 1024, 'd')
	writeLeafGenerationKeys(t, d, "current", 16, 'e')
	d.SetLeafPageLog(nil)

	scans := withLeafGenerationLiveScanCounter(t)
	var scansAfterFirst, scansAfterSecond uint64
	d.compactStorageAfterPhase = func(name string) {
		switch name {
		case "leaf-generation-pack-1":
			scansAfterFirst = scans.Load()
		case "leaf-generation-pack-2":
			scansAfterSecond = scans.Load()
		}
	}
	t.Cleanup(func() { d.compactStorageAfterPhase = nil })
	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{
		LeafPackMaxPasses:             3,
		LeafPackMaxGenerationsPerPass: 1,
		LeafPackMinReclaimPerCopyPPM:  1,
	})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if got := compactStorageRanLeafPackCount(stats.LeafGenerationPacks); got < 2 {
		t.Fatalf("ran leaf-pack passes=%d want at least 2, packs=%+v", got, stats.LeafGenerationPacks)
	}
	if scansAfterFirst == 0 || scansAfterSecond == 0 {
		t.Fatalf("missing pack scan observations: after_first=%d after_second=%d", scansAfterFirst, scansAfterSecond)
	}
	if got, want := scansAfterSecond, scansAfterFirst; got != want {
		t.Fatalf("second unchanged pack scan count=%d want unchanged from first=%d", got, want)
	}
}

func TestCompactStorageLeafPackMultiPassRescansAfterForegroundCommit(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "left", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf left: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "left", 0, 1024, 'b')

	writeLeafGenerationKeys(t, d, "right", 2048, 'c')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf right: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "right", 0, 1024, 'd')
	writeLeafGenerationKeys(t, d, "current", 16, 'e')
	d.SetLeafPageLog(nil)

	scans := withLeafGenerationLiveScanCounter(t)
	injected := false
	var scansAfterFirst, scansAfterSecond uint64
	d.compactStorageAfterPhase = func(name string) {
		switch name {
		case "leaf-generation-pack-1":
			scansAfterFirst = scans.Load()
			if injected {
				return
			}
			injected = true
			state := d.State()
			if state == nil {
				t.Fatal("missing state before foreground commit")
			}
			if err := d.ForceCommit(state.RootPageID); err != nil {
				t.Fatalf("foreground same-root commit: %v", err)
			}
		case "leaf-generation-pack-2":
			scansAfterSecond = scans.Load()
		}
	}
	t.Cleanup(func() { d.compactStorageAfterPhase = nil })

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{
		LeafPackMaxPasses:             3,
		LeafPackMaxGenerationsPerPass: 1,
		LeafPackMinReclaimPerCopyPPM:  1,
	})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if !injected {
		t.Fatal("foreground commit hook did not run")
	}
	if got := compactStorageRanLeafPackCount(stats.LeafGenerationPacks); got < 2 {
		t.Fatalf("ran leaf-pack passes=%d want at least 2, packs=%+v", got, stats.LeafGenerationPacks)
	}
	if scansAfterFirst == 0 || scansAfterSecond == 0 {
		t.Fatalf("missing pack scan observations: after_first=%d after_second=%d", scansAfterFirst, scansAfterSecond)
	}
	if got, want := scansAfterSecond, scansAfterFirst+1; got != want {
		t.Fatalf("second dirty pack scan count=%d want one more than first=%d", got, scansAfterFirst)
	}
}

func TestCompactStorageLeafPackCarryMatchesFreshPlan(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "left", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf left: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "left", 0, 1024, 'b')
	writeLeafGenerationKeys(t, d, "right", 2048, 'c')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf right: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "right", 0, 1024, 'd')
	writeLeafGenerationKeys(t, d, "current", 16, 'e')
	d.SetLeafPageLog(nil)

	assertCompactStorageLeafPackCarryMatchesFreshPlan(t, d, nil, nil)
}

func TestCompactStorageLeafPackCarryMatchesFreshPlanWithProtectedResidual(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "main", 2048, 'a')
	detachedTable := mustFrozenSystemMemtable(t, systemRangeKVs(512, nil)...)
	detachedRoot, err := d.PublishOrderedRootIterator(0, detachedTable.NewIterator(nil, nil))
	if err != nil {
		t.Fatalf("PublishOrderedRootIterator: %v", err)
	}
	if detachedRoot == 0 {
		t.Fatal("expected detached protected root")
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "main", 0, 1024, 'b')
	writeLeafGenerationKeys(t, d, "current", 16, 'c')
	d.SetLeafPageLog(nil)

	assertCompactStorageLeafPackCarryMatchesFreshPlan(t, d, []uint64{detachedRoot}, nil)
}

func TestCompactStorageLeafPackCarryRescansWhenProtectedRootIsRewritten(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "main", 2048, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "main", 0, 1024, 'b')
	writeLeafGenerationKeys(t, d, "current", 16, 'c')
	d.SetLeafPageLog(nil)

	published := d.State()
	if published == nil || published.RootPageID == 0 {
		t.Fatal("expected published root")
	}
	opts := compactStorageLeafPackFromPlanOptions(normalizeCompactStorageOptions(CompactStorageOptions{
		LeafPackMaxPasses:             2,
		LeafPackMaxGenerationsPerPass: 1,
		LeafPackMinReclaimPerCopyPPM:  1,
	}), []uint64{published.RootPageID}, nil)
	ignored := make(map[uint32]struct{})
	var carry compactStorageLeafPackPlanState
	scans := withLeafGenerationLiveScanCounter(t)

	first, err := d.compactStorageLeafGenerationPackRunOnce(context.Background(), opts, true, ignored, &carry)
	if err != nil {
		t.Fatalf("first leaf pack: %v", err)
	}
	if !first.Ran {
		t.Fatalf("first leaf pack skipped: %+v", first)
	}
	compactStorageRememberLeafPackCreatedFileIDs(ignored, first)
	scansAfterPack := scans.Load()

	planOpts := leafGenerationPackFromPlanPlanOptions(opts)
	carried, err := d.compactStorageLeafGenerationPlan(context.Background(), planOpts, &carry)
	if err != nil {
		t.Fatalf("post-pack plan: %v", err)
	}
	if got, want := scans.Load(), scansAfterPack+1; got != want {
		t.Fatalf("post-pack live scans=%d want %d", got, want)
	}
	fresh, err := d.LeafGenerationPlan(context.Background(), planOpts)
	if err != nil {
		t.Fatalf("fresh plan: %v", err)
	}
	carried = compactStorageFilterIgnoredLeafPackPlan(carried, opts, ignored)
	fresh = compactStorageFilterIgnoredLeafPackPlan(fresh, opts, ignored)
	if !reflect.DeepEqual(compactStorageAuditPublicLeafPlan(carried), compactStorageAuditPublicLeafPlan(fresh)) {
		t.Fatalf("post-pack plan differs from fresh scan:\ncarried=%+v\nfresh=%+v", carried, fresh)
	}
}

func TestCompactStorageLeafPackCarryRescansWhenProtectedRootsChange(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, _ := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "main", 512, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationKeyRange(t, d, "main", 0, 256, 'b')
	d.SetLeafPageLog(nil)

	published := d.State()
	if published == nil || published.RootPageID == 0 {
		t.Fatal("expected published root")
	}
	opts := leafGenerationPackFromPlanPlanOptions(compactStorageLeafPackFromPlanOptions(
		normalizeCompactStorageOptions(CompactStorageOptions{
			LeafPackMaxGenerationsPerPass: 1,
			LeafPackMinReclaimPerCopyPPM:  1,
		}),
		nil,
		nil,
	))

	scans := withLeafGenerationLiveScanCounter(t)
	var carry compactStorageLeafPackPlanState
	if _, err := d.compactStorageLeafGenerationPlan(context.Background(), opts, &carry); err != nil {
		t.Fatalf("initial carried plan: %v", err)
	}
	initialScans := scans.Load()
	if initialScans == 0 {
		t.Fatal("initial plan did not perform a live scan")
	}

	opts.ProtectedRootIDs = []uint64{published.RootPageID}
	if _, err := d.compactStorageLeafGenerationPlan(context.Background(), opts, &carry); err != nil {
		t.Fatalf("changed protected-root plan: %v", err)
	}
	if got, want := scans.Load(), initialScans+1; got != want {
		t.Fatalf("changed protected roots live scans=%d want %d", got, want)
	}
}

func assertCompactStorageLeafPackCarryMatchesFreshPlan(t *testing.T, d *DB, protectedRootIDs, protectedSystemRootIDs []uint64) {
	t.Helper()
	opts := compactStorageLeafPackFromPlanOptions(normalizeCompactStorageOptions(CompactStorageOptions{
		LeafPackMaxPasses:             2,
		LeafPackMaxGenerationsPerPass: 1,
		LeafPackMinReclaimPerCopyPPM:  1,
	}), protectedRootIDs, protectedSystemRootIDs)
	ignored := make(map[uint32]struct{})
	var carry compactStorageLeafPackPlanState

	first, err := d.compactStorageLeafGenerationPackRunOnce(context.Background(), opts, true, ignored, &carry)
	if err != nil {
		t.Fatalf("first leaf pack: %v", err)
	}
	if !first.Ran {
		t.Fatalf("first leaf pack skipped: %+v", first)
	}
	compactStorageRememberLeafPackCreatedFileIDs(ignored, first)
	pin := d.AcquireSnapshot()
	if pin == nil {
		t.Fatal("expected snapshot pin before carried plan")
	}
	defer func() { _ = pin.Close() }()

	planOpts := leafGenerationPackFromPlanPlanOptions(opts)
	carried, err := d.compactStorageLeafGenerationPlan(context.Background(), planOpts, &carry)
	if err != nil {
		t.Fatalf("carried plan: %v", err)
	}
	fresh, err := d.LeafGenerationPlan(context.Background(), planOpts)
	if err != nil {
		t.Fatalf("fresh plan: %v", err)
	}
	carried = compactStorageFilterIgnoredLeafPackPlan(carried, opts, ignored)
	fresh = compactStorageFilterIgnoredLeafPackPlan(fresh, opts, ignored)

	if !reflect.DeepEqual(carried.Candidates, fresh.Candidates) {
		t.Fatalf("carried candidates differ from fresh scan:\ncarried=%+v\nfresh=%+v", carried.Candidates, fresh.Candidates)
	}
	if !reflect.DeepEqual(carried.CandidateGenerationIDs, fresh.CandidateGenerationIDs) ||
		carried.CandidateBytesTotal != fresh.CandidateBytesTotal ||
		carried.CandidateBytesLive != fresh.CandidateBytesLive ||
		carried.CandidateBytesDead != fresh.CandidateBytesDead ||
		carried.CandidateBytesToCopy != fresh.CandidateBytesToCopy ||
		carried.CandidateLivePages != fresh.CandidateLivePages ||
		carried.ExpectedReclaimBytes != fresh.ExpectedReclaimBytes ||
		carried.ExpectedReclaimRatioPPM != fresh.ExpectedReclaimRatioPPM ||
		carried.ExpectedReclaimPerByteCopiedPPM != fresh.ExpectedReclaimPerByteCopiedPPM ||
		carried.Admission != fresh.Admission {
		t.Fatalf("carried aggregate differs from fresh scan:\ncarried=%+v\nfresh=%+v", carried, fresh)
	}
}

func compactStorageRanLeafPackCount(packs []LeafGenerationPackRunOnceStats) int {
	ran := 0
	for _, pack := range packs {
		if pack.Ran {
			ran++
		}
	}
	return ran
}

func TestCompactStoragePlanIgnoresEmptyAndCurrentLeafGenerations(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)
	d.SetLeafPageLog(nil)

	empty, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan empty: %v", err)
	}
	if empty.RemainingDebt.LeafPackGenerations != 0 || empty.RemainingDebt.LeafGCGenerations != 0 {
		t.Fatalf("empty leaf generation reported debt=%+v", empty.RemainingDebt)
	}

	d.SetLeafPageLog(leafLog)
	writeLeafGenerationKeys(t, d, "current", 64, 'z')
	d.SetLeafPageLog(nil)
	current, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan current: %v", err)
	}
	if current.RemainingDebt.LeafPackGenerations != 0 || current.RemainingDebt.LeafGCGenerations != 0 {
		t.Fatalf("current writable leaf generation reported debt=%+v plan=%+v", current.RemainingDebt, current.LeafGenerationPlan)
	}
}

func TestCompactStorageExhaustiveSealsCurrentLeafGeneration(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	d, leafLog, dir := openLeafGenerationPackTestDB(t)

	writeLeafGenerationKeys(t, d, "current", 64, 'z')
	_, fileID := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID := page.ValueLogSegmentID(fileID)
	d.SetLeafPageLog(nil)

	fullPlan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan full: %v", err)
	}
	if fullPlan.RemainingDebt.LeafPackGenerations != 0 {
		t.Fatalf("full mode should ignore current writable generation debt=%+v", fullPlan.RemainingDebt)
	}

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("CompactStorage exhaustive: %v", err)
	}
	if !compactStoragePhaseSeen(stats.Phases, "seal-current-leaf-generation") {
		t.Fatalf("missing seal-current-leaf-generation phase: %+v", stats.Phases)
	}
	if stats.ByteMinimized || stats.FullyCompacted || stats.PolicyFullyCompacted || stats.RemainingDebt.LeafGCGenerations == 0 {
		t.Fatalf("exhaustive retained source overstated completion: byte=%t fully=%t policy=%t debt=%+v", stats.ByteMinimized, stats.FullyCompacted, stats.PolicyFullyCompacted, stats.RemainingDebt)
	}
	if len(stats.LeafGenerationPacks) == 0 || !stats.LeafGenerationPacks[0].Ran {
		t.Fatalf("expected exhaustive leaf pack to run after sealing current generation: %+v", stats.LeafGenerationPacks)
	}

	manifest := loadLeafGenerationManifestOrFatal(t, dir)
	for _, gen := range manifest.Generations {
		if gen.State == leafGenerationStateWritable {
			for _, got := range gen.FileIDs {
				if got == rawFileID {
					t.Fatalf("original current raw file %d remained writable after exhaustive compact: %+v", rawFileID, manifest.Generations)
				}
			}
		}
	}
}

func TestCompactStorageExhaustiveRefusesExternalLeafPageLog(t *testing.T) {
	d, _, _ := openLeafGenerationPackTestDB(t)
	writeLeafGenerationKeys(t, d, "current", 8, 'x')
	previousLeafPageLog := d.leafPageLog
	beforeSegments, err := listValueLogSegments(d.dir)
	if err != nil {
		t.Fatalf("listValueLogSegments before compact: %v", err)
	}

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if !errors.Is(err, ErrCompactStorageLeafPageLogOwnerUnsupported) {
		t.Fatalf("CompactStorage exhaustive with external leaf log error=%v, want owner unsupported", err)
	}
	var ownerErr *CompactStorageLeafPageLogOwnerError
	if !errors.As(err, &ownerErr) {
		t.Fatalf("CompactStorage exhaustive error=%T, want CompactStorageLeafPageLogOwnerError", err)
	}
	if ownerErr.Classification.Status != CompactStorageOwnerStatusExternalUnsupported {
		t.Fatalf("owner status=%q want %q", ownerErr.Classification.Status, CompactStorageOwnerStatusExternalUnsupported)
	}
	if ownerErr.Classification.Detail == "" {
		t.Fatal("owner refusal detail is empty")
	}
	if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
		t.Fatalf("external owner refusal reported compact success flags: %+v", stats)
	}
	if d.leafPageLog != previousLeafPageLog {
		t.Fatalf("external owner was mutated: got %T want %T", d.leafPageLog, previousLeafPageLog)
	}
	afterSegments, err := listValueLogSegments(d.dir)
	if err != nil {
		t.Fatalf("listValueLogSegments after compact: %v", err)
	}
	if !reflect.DeepEqual(afterSegments, beforeSegments) {
		t.Fatalf("external owner refusal mutated value-log segments:\nbefore=%+v\nafter=%+v", beforeSegments, afterSegments)
	}
}

func TestCompactStorageFullLeafPackDefaultUsesByteBudgetNotGenerationCap(t *testing.T) {
	opts := normalizeCompactStorageOptions(CompactStorageOptions{})
	if opts.Mode != CompactStorageFull {
		t.Fatalf("Mode=%q want full", opts.Mode)
	}
	if opts.LeafPackMaxGenerationsPerPass != 0 {
		t.Fatalf("LeafPackMaxGenerationsPerPass=%d want 0 (byte-budget only)", opts.LeafPackMaxGenerationsPerPass)
	}
	if opts.LeafPackMaxBytesToCopyPerPass != 1<<30 {
		t.Fatalf("LeafPackMaxBytesToCopyPerPass=%d want %d", opts.LeafPackMaxBytesToCopyPerPass, int64(1<<30))
	}

	plan := LeafGenerationPlan{Admission: leafGenerationPlanAdmissionEligible}
	for i := 0; i < 96; i++ {
		plan.Candidates = append(plan.Candidates, LeafGenerationPlanGeneration{
			GenerationID: uint64(i + 1),
			BytesTotal:   3,
			BytesLive:    1,
			BytesDead:    2,
			BytesToCopy:  1,
			LivePages:    1,
		})
	}

	gens, bytes, err := compactStorageLeafPackDebtFromPlan(plan, compactStorageLeafPackFromPlanOptions(opts, nil, nil))
	if err != nil {
		t.Fatalf("compactStorageLeafPackDebtFromPlan default: %v", err)
	}
	if gens != 96 || bytes != 192 {
		t.Fatalf("default full leaf-pack debt gens=%d bytes=%d want 96/192", gens, bytes)
	}

	capped := opts
	capped.LeafPackMaxGenerationsPerPass = 64
	gens, bytes, err = compactStorageLeafPackDebtFromPlan(plan, compactStorageLeafPackFromPlanOptions(capped, nil, nil))
	if err != nil {
		t.Fatalf("compactStorageLeafPackDebtFromPlan capped: %v", err)
	}
	if gens != 64 || bytes != 128 {
		t.Fatalf("explicit capped leaf-pack debt gens=%d bytes=%d want 64/128", gens, bytes)
	}
}

func TestCompactStorageLeafPackFilterIgnoresOnlyRunCreatedGenerations(t *testing.T) {
	opts := compactStorageLeafPackFromPlanOptions(normalizeCompactStorageOptions(CompactStorageOptions{
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	}), nil, nil)
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Generations: []LeafGenerationPlanGeneration{
			{GenerationID: 1, FileIDs: []uint32{11}, BytesTotal: 100, BytesLive: 80, BytesDead: 20, BytesToCopy: 80, LivePages: 1, Eligible: true},
			{GenerationID: 2, FileIDs: []uint32{21}, BytesTotal: 200, BytesLive: 150, BytesDead: 50, BytesToCopy: 150, LivePages: 1, Eligible: true},
			{GenerationID: 3, FileIDs: []uint32{31, 32}, BytesTotal: 300, BytesLive: 200, BytesDead: 100, BytesToCopy: 200, LivePages: 2, Eligible: true},
		},
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 1, FileIDs: []uint32{11}, BytesTotal: 100, BytesLive: 80, BytesDead: 20, BytesToCopy: 80, LivePages: 1, Eligible: true},
			{GenerationID: 2, FileIDs: []uint32{21}, BytesTotal: 200, BytesLive: 150, BytesDead: 50, BytesToCopy: 150, LivePages: 1, Eligible: true},
			{GenerationID: 3, FileIDs: []uint32{31, 32}, BytesTotal: 300, BytesLive: 200, BytesDead: 100, BytesToCopy: 200, LivePages: 2, Eligible: true},
		},
	}

	filtered := compactStorageFilterIgnoredLeafPackPlan(plan, opts, map[uint32]struct{}{11: {}, 31: {}})
	if got, want := filtered.CandidateGenerationIDs, []uint64{2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CandidateGenerationIDs=%v want %v", got, want)
	}
	if got, want := filtered.CandidateBytesTotal, int64(500); got != want {
		t.Fatalf("CandidateBytesTotal=%d want %d", got, want)
	}
	if got, want := filtered.CandidateBytesLive, int64(350); got != want {
		t.Fatalf("CandidateBytesLive=%d want %d", got, want)
	}
	if got, want := filtered.CandidateBytesDead, int64(150); got != want {
		t.Fatalf("CandidateBytesDead=%d want %d", got, want)
	}
	if got, want := filtered.CandidateBytesToCopy, int64(350); got != want {
		t.Fatalf("CandidateBytesToCopy=%d want %d", got, want)
	}
	if got, want := filtered.CandidateLivePages, 3; got != want {
		t.Fatalf("CandidateLivePages=%d want %d", got, want)
	}
	if got, want := filtered.ExpectedReclaimBytes, int64(150); got != want {
		t.Fatalf("ExpectedReclaimBytes=%d want %d", got, want)
	}
	if got, want := filtered.ExpectedReclaimRatioPPM, ratioPPM(150, 500); got != want {
		t.Fatalf("ExpectedReclaimRatioPPM=%d want %d", got, want)
	}
	if got, want := filtered.ExpectedReclaimPerByteCopiedPPM, ratioPPM(150, 350); got != want {
		t.Fatalf("ExpectedReclaimPerByteCopiedPPM=%d want %d", got, want)
	}
	if filtered.Generations[0].Eligible || filtered.Generations[0].SkipReason != compactStorageLeafPackSkipRunCreated {
		t.Fatalf("filtered generation 1=%+v want run-created skip", filtered.Generations[0])
	}
	if !filtered.Generations[2].Eligible {
		t.Fatalf("partially ignored generation was filtered: %+v", filtered.Generations[2])
	}
	if plan.Generations[0].SkipReason != "" || !plan.Generations[0].Eligible {
		t.Fatalf("original plan mutated: %+v", plan.Generations[0])
	}

	allIgnored := compactStorageFilterIgnoredLeafPackPlan(plan, opts, map[uint32]struct{}{11: {}, 21: {}, 31: {}, 32: {}})
	if allIgnored.Admission != leafGenerationPlanAdmissionNoCandidates || len(allIgnored.Candidates) != 0 || len(allIgnored.CandidateGenerationIDs) != 0 {
		t.Fatalf("allIgnored admission=%s candidates=%v ids=%v want no candidates", allIgnored.Admission, allIgnored.Candidates, allIgnored.CandidateGenerationIDs)
	}
	if allIgnored.CandidateBytesTotal != 0 || allIgnored.CandidateBytesDead != 0 || allIgnored.ExpectedReclaimBytes != 0 {
		t.Fatalf("allIgnored retained accounting: total=%d dead=%d reclaim=%d", allIgnored.CandidateBytesTotal, allIgnored.CandidateBytesDead, allIgnored.ExpectedReclaimBytes)
	}
}

func TestCompactStorageNormalizeExhaustiveForcesUnboundedLeafPack(t *testing.T) {
	opts := normalizeCompactStorageOptions(CompactStorageOptions{Mode: CompactStorageExhaustive})
	if opts.Mode != CompactStorageExhaustive {
		t.Fatalf("Mode=%q want exhaustive", opts.Mode)
	}
	if !opts.LeafPackForce {
		t.Fatal("exhaustive mode should force leaf pack thresholds")
	}
	if opts.LeafPackMaxPasses < 256 {
		t.Fatalf("LeafPackMaxPasses=%d want at least 256", opts.LeafPackMaxPasses)
	}
	if opts.LeafPackMaxGenerationsPerPass != 0 || opts.LeafPackMaxBytesToCopyPerPass != 0 {
		t.Fatalf("exhaustive leaf pack limits gens=%d bytes=%d want unbounded", opts.LeafPackMaxGenerationsPerPass, opts.LeafPackMaxBytesToCopyPerPass)
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

func compactStoragePhaseSkippedWithReason(phases []CompactStoragePhaseStats, name, reason string) bool {
	for _, phase := range phases {
		if phase.Name == name && phase.Skipped && strings.Contains(phase.SkipReason, reason) {
			return true
		}
	}
	return false
}
