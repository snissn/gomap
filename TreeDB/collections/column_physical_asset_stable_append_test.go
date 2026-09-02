package collections

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type stableColumnAppendTestRecoveryRetainer struct {
	cleanups []func() error
}

func (retainer *stableColumnAppendTestRecoveryRetainer) RetainStableResourceCaptureRecovery(cleanup func() error) error {
	retainer.cleanups = append(retainer.cleanups, cleanup)
	return nil
}

func (retainer *stableColumnAppendTestRecoveryRetainer) run() error {
	var errs []error
	for _, cleanup := range retainer.cleanups {
		errs = append(errs, cleanup())
	}
	retainer.cleanups = nil
	return errors.Join(errs...)
}

func stableColumnAppendTestConfig(namespace string) ColumnStoreConfig {
	return ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{
		Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: namespace,
	}}
}

func stableColumnAppendTestPath(t *testing.T, rootDir string, cfg ColumnStoreConfig, fileID uint32) string {
	t.Helper()
	namespace, err := columnAssetManagerNamespaceForRoot(rootDir, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureColumnAssetManagerNamespace(namespace); err != nil {
		t.Fatal(err)
	}
	path, err := columnAssetSegmentPath(rootDir, ColumnAssetRef{
		Kind: ColumnAssetKindTCS1PartImage, Namespace: cfg.AssetManager.Namespace, FileID: fileID, Length: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestColumnPhysicalAssetReservedPayloadStableOpenRejectsReplacement4429(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column append requires exact relative namespace support")
	}
	rootDir := t.TempDir()
	cfg := stableColumnAppendTestConfig("stable-reserved-replacement")
	appender, err := newColumnPhysicalAssetSegmentAppenderWithStableResources(rootDir, cfg, 4429, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = appender.abort()
		}
	}()

	orphanName := appender.stableChildName + ".orphan"
	if err := rootpublication.RenameStableChildFile(appender.stableParent, appender.stableChildName, orphanName); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement-must-remain-untouched")
	file, err := rootpublication.OpenStableChildFile(appender.stableParent, appender.stableChildName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(replacement); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	emitted := false
	if _, err := appender.appendKindWithReservedPayload(8, ColumnAssetKindTCS1HNSWSearchPack, 1, 1, 8, func(*columnAssetReservedPayload) error {
		emitted = true
		return nil
	}); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("reserved append error=%v want stable child conflict", err)
	}
	if emitted || !appender.failed || len(appender.stableRefs) != 0 {
		t.Fatalf("reserved append emitted=%t failed=%t refs=%v want no emission, failed, no refs", emitted, appender.failed, appender.stableRefs)
	}
	got, err := os.ReadFile(appender.assetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, replacement) {
		t.Fatalf("replacement changed: got=%q want=%q", got, replacement)
	}
	if err := appender.abort(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if appender.file != nil || appender.stableParent != nil {
		t.Fatalf("failed reserved append retained handles file=%v parent=%v", appender.file, appender.stableParent)
	}
}

func TestAppendColumnPhysicalAssetsWithStableResourcesValidationFailureRestoresAlignedAppendStart(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column append requires exact relative namespace support")
	}
	rootDir := t.TempDir()
	cfg := stableColumnAppendTestConfig("stable-append-aligned-rollback")
	const fileID = uint32(71)
	path := stableColumnAppendTestPath(t, rootDir, cfg, fileID)
	original := []byte("odd")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	recovery := &stableColumnAppendTestRecoveryRetainer{}
	var captured ColumnAssetRef
	restore := setColumnAssetStableObligationTestHook(func(ref ColumnAssetRef, _ rootpublication.StableLogicalObligation, _ *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
		captured = ref
		return columnAssetStableCaptureOmitObligation
	})
	t.Cleanup(restore)
	refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(
		rootDir,
		cfg,
		fileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload: []byte("rejected aligned value"), Kind: ColumnAssetKindTCS1Int64Values, Generation: 3, PartID: 5,
		}},
		registry, recovery,
	)
	restore()
	if refs != nil || resources != nil || !errors.Is(err, rootpublication.ErrUnresolvedResource) || errors.Is(err, ErrRecoveryRequired) {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("rejected append refs=%v resources=%v err=%v want nil authority, unresolved resource, and exact rollback", refs, resources, err)
	}
	if captured.Offset <= int64(len(original)) {
		t.Fatalf("captured aligned offset=%d want padding after original size=%d", captured.Offset, len(original))
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("rejected aligned append left bytes=%q want original=%q", got, original)
	}
	if pins, identities := registry.ActivePins(), registry.ActiveIdentities(); pins != 0 || identities != 0 {
		t.Fatalf("rejected append retained pins=%d identities=%d", pins, identities)
	}
}

func TestAppendColumnPhysicalAssetsWithStableResourcesValidationFailureRemovesFreshSegment(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column append requires exact relative namespace support")
	}
	rootDir := t.TempDir()
	cfg := stableColumnAppendTestConfig("stable-append-fresh-rollback")
	const fileID = uint32(72)
	path := stableColumnAppendTestPath(t, rootDir, cfg, fileID)
	registry := rootpublication.NewIdentityPinRegistry()
	recovery := &stableColumnAppendTestRecoveryRetainer{}
	restore := setColumnAssetStableObligationTestHook(func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
		return columnAssetStableCaptureOmitToken
	})
	t.Cleanup(restore)
	refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(
		rootDir,
		cfg,
		fileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload: []byte("rejected fresh value"), Kind: ColumnAssetKindTCS1PartImage, Generation: 4, PartID: 6,
		}},
		registry, recovery,
	)
	restore()
	if refs != nil || resources != nil || !errors.Is(err, rootpublication.ErrUnresolvedResource) || errors.Is(err, ErrRecoveryRequired) {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("rejected fresh append refs=%v resources=%v err=%v", refs, resources, err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("fresh rejected segment still exists: %v", statErr)
	}
	stats := registry.Stats()
	if stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("fresh rejected append retained registry state: %+v", stats)
	}
}

func TestAppendColumnPhysicalAssetsWithStableResourcesRollbackSyncAmbiguityRetainsProof(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column append requires exact relative namespace support")
	}
	rootDir := t.TempDir()
	cfg := stableColumnAppendTestConfig("stable-append-sync-ambiguity")
	const fileID = uint32(73)
	path := stableColumnAppendTestPath(t, rootDir, cfg, fileID)
	registry := rootpublication.NewIdentityPinRegistry()
	recovery := &stableColumnAppendTestRecoveryRetainer{}
	sentinel := errors.New("injected stable column rollback parent sync failure")
	originalSync := syncStableColumnAssetParentRollback
	syncStableColumnAssetParentRollback = func(*os.File) error { return sentinel }
	t.Cleanup(func() { syncStableColumnAssetParentRollback = originalSync })
	restore := setColumnAssetStableObligationTestHook(func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
		return columnAssetStableCaptureOmitToken
	})
	t.Cleanup(restore)
	_, resources, err := AppendColumnPhysicalAssetsWithStableResources(
		rootDir,
		cfg,
		fileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload: []byte("ambiguous fresh value"), Kind: ColumnAssetKindTCS1PartImage, Generation: 5, PartID: 7,
		}},
		registry, recovery,
	)
	restore()
	if resources != nil {
		resources.Release()
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !errors.Is(err, sentinel) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ambiguous rollback error=%v want validation, injected sync, and recovery required", err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("ambiguous rollback did not unlink exact segment: %v", statErr)
	}
	stats := registry.Stats()
	if stats.ActivePins == 0 || stats.ActiveIdentities == 0 || stats.ActiveStableNamespaceLinks != 1 || len(recovery.cleanups) != 1 {
		t.Fatalf("ambiguous rollback registry state=%+v cleanups=%d want retained exact authority", stats, len(recovery.cleanups))
	}
	syncStableColumnAssetParentRollback = originalSync
	if err := recovery.run(); err != nil {
		t.Fatalf("teardown rollback retry: %v", err)
	}
	stats = registry.Stats()
	if stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("teardown rollback registry state=%+v want release to zero", stats)
	}
}

func TestAppendColumnPhysicalAssetsWithStableResourcesRollbackAmbiguityRetriesAtDBShutdown(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column append requires exact relative namespace support")
	}
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = database.Close()
		}
	})
	lease, err := database.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	leaseReleased := false
	t.Cleanup(func() {
		if !leaseReleased {
			lease.Release()
		}
	})

	rootDir := database.ColumnAssetRootDir()
	cfg := stableColumnAppendTestConfig("stable-append-db-shutdown-retry")
	const fileID = uint32(75)
	path := stableColumnAppendTestPath(t, rootDir, cfg, fileID)
	registry := database.StableResourceIdentityPinRegistry()
	sentinel := errors.New("injected stable column DB shutdown retry")
	originalSync := syncStableColumnAssetParentRollback
	syncStableColumnAssetParentRollback = func(*os.File) error { return sentinel }
	t.Cleanup(func() { syncStableColumnAssetParentRollback = originalSync })
	restore := setColumnAssetStableObligationTestHook(func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
		return columnAssetStableCaptureOmitToken
	})
	t.Cleanup(restore)
	_, resources, err := AppendColumnPhysicalAssetsWithStableResources(
		rootDir,
		cfg,
		fileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload: []byte("shutdown retry value"), Kind: ColumnAssetKindTCS1PartImage, Generation: 7, PartID: 9,
		}},
		registry,
		lease,
	)
	restore()
	if resources != nil {
		resources.Release()
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !errors.Is(err, sentinel) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ambiguous rollback error=%v want validation, injected sync, and recovery required", err)
	}
	if setErr := database.SetSync([]byte("after-column-rollback-ambiguity"), []byte("blocked")); !errors.Is(setErr, backenddb.ErrRecoveryRequired) {
		t.Fatalf("SetSync after retained rollback=%v want DB recovery required", setErr)
	}
	if nextLease, acquireErr := database.AcquireStableResourceCaptureLease(); !errors.Is(acquireErr, backenddb.ErrRecoveryRequired) {
		if nextLease != nil {
			nextLease.Release()
		}
		t.Fatalf("capture admission after retained rollback=(%v, %v) want DB recovery required", nextLease, acquireErr)
	}
	stats := registry.Stats()
	if stats.ActivePins == 0 || stats.ActiveIdentities == 0 || stats.ActiveStableNamespaceLinks != 1 {
		t.Fatalf("retained DB rollback registry state=%+v", stats)
	}

	syncStableColumnAssetParentRollback = originalSync
	lease.Release()
	leaseReleased = true
	if closeErr := database.Close(); closeErr != nil {
		t.Fatalf("DB shutdown rollback retry: %v", closeErr)
	}
	closed = true
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("DB shutdown retained rejected segment: %v", statErr)
	}
	stats = registry.Stats()
	if stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("DB shutdown rollback registry state=%+v want release to zero", stats)
	}
}

func TestAppendColumnPhysicalAssetsWithStableResourcesRollbackRejectsReboundName(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column append requires exact relative namespace support")
	}
	rootDir := t.TempDir()
	cfg := stableColumnAppendTestConfig("stable-append-rebind")
	const fileID = uint32(74)
	path := stableColumnAppendTestPath(t, rootDir, cfg, fileID)
	registry := rootpublication.NewIdentityPinRegistry()
	recovery := &stableColumnAppendTestRecoveryRetainer{}
	replacement := []byte("replacement-must-survive")
	orphanName := filepath.Base(path) + ".rejected"
	restoreRollback := setColumnAssetStableBeforeRollbackTestHook(func(parent, _ *os.File, name string) {
		if err := rootpublication.RenameStableChildFile(parent, name, orphanName); err != nil {
			t.Fatal(err)
		}
		file, err := rootpublication.OpenStableChildFile(parent, name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(replacement); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(restoreRollback)
	restoreObligation := setColumnAssetStableObligationTestHook(func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
		return columnAssetStableCaptureOmitToken
	})
	t.Cleanup(restoreObligation)
	_, resources, err := AppendColumnPhysicalAssetsWithStableResources(
		rootDir,
		cfg,
		fileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload: []byte("rejected rebound value"), Kind: ColumnAssetKindTCS1PartImage, Generation: 6, PartID: 8,
		}},
		registry, recovery,
	)
	restoreObligation()
	restoreRollback()
	if resources != nil {
		resources.Release()
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("rebound rollback error=%v want validation, conflict, and recovery required", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, replacement) {
		t.Fatalf("rebound replacement changed: got=%q want=%q", got, replacement)
	}
	orphanPath := filepath.Join(filepath.Dir(path), orphanName)
	info, statErr := os.Stat(orphanPath)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Size() != 0 {
		t.Fatalf("exact rejected child retained %d appended bytes", info.Size())
	}
	stats := registry.Stats()
	if stats.ActivePins == 0 || stats.ActiveIdentities == 0 || stats.ActiveStableNamespaceLinks != 1 || len(recovery.cleanups) != 1 {
		t.Fatalf("rebound rollback registry state=%+v cleanups=%d want retained exact authority", stats, len(recovery.cleanups))
	}
	if cleanupErr := recovery.run(); !errors.Is(cleanupErr, rootpublication.ErrResourceConflict) || !errors.Is(cleanupErr, ErrRecoveryRequired) {
		t.Fatalf("rebound teardown cleanup error=%v want retained conflict", cleanupErr)
	}
	got, readErr = os.ReadFile(path)
	if readErr != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("rebound teardown changed replacement got=%q err=%v", got, readErr)
	}
	info, statErr = os.Stat(orphanPath)
	if statErr != nil || info.Size() != 0 {
		t.Fatalf("rebound teardown changed exact orphan info=%v err=%v", info, statErr)
	}
	stats = registry.Stats()
	if stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 1 {
		t.Fatalf("rebound teardown registry state=%+v want handles released and stale proof retained", stats)
	}
}

func TestAppendColumnPhysicalAssetsWithStableResourcesExistingSegmentRollbackRejectsReboundName(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column append requires exact relative namespace support")
	}
	rootDir := t.TempDir()
	cfg := stableColumnAppendTestConfig("stable-append-existing-rebind")
	const fileID = uint32(76)
	path := stableColumnAppendTestPath(t, rootDir, cfg, fileID)
	original := []byte("existing-prefix-must-be-restored")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	recovery := &stableColumnAppendTestRecoveryRetainer{}
	replacement := []byte("existing-replacement-must-survive")
	orphanName := filepath.Base(path) + ".rejected"
	restoreRollback := setColumnAssetStableBeforeRollbackTestHook(func(parent, _ *os.File, name string) {
		if err := rootpublication.RenameStableChildFile(parent, name, orphanName); err != nil {
			t.Fatal(err)
		}
		file, err := rootpublication.OpenStableChildFile(parent, name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(replacement); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(restoreRollback)
	restoreObligation := setColumnAssetStableObligationTestHook(func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
		return columnAssetStableCaptureOmitToken
	})
	t.Cleanup(restoreObligation)
	_, resources, err := AppendColumnPhysicalAssetsWithStableResources(
		rootDir,
		cfg,
		fileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload: []byte("rejected existing rebound value"), Kind: ColumnAssetKindTCS1PartImage, Generation: 8, PartID: 10,
		}},
		registry, recovery,
	)
	restoreObligation()
	restoreRollback()
	if resources != nil {
		resources.Release()
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) || !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("existing rebound rollback error=%v want validation, conflict, and recovery required", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("existing rebound replacement changed got=%q err=%v", got, readErr)
	}
	orphanPath := filepath.Join(filepath.Dir(path), orphanName)
	got, readErr = os.ReadFile(orphanPath)
	if readErr != nil || !bytes.Equal(got, original) {
		t.Fatalf("existing rebound exact child got=%q err=%v want original=%q", got, readErr, original)
	}
	stats := registry.Stats()
	if stats.ActivePins == 0 || stats.ActiveIdentities == 0 || stats.ActiveStableNamespaceLinks != 1 || len(recovery.cleanups) != 1 {
		t.Fatalf("existing rebound rollback registry state=%+v cleanups=%d want retained exact authority", stats, len(recovery.cleanups))
	}
	if cleanupErr := recovery.run(); !errors.Is(cleanupErr, rootpublication.ErrResourceConflict) || !errors.Is(cleanupErr, ErrRecoveryRequired) {
		t.Fatalf("existing rebound teardown cleanup error=%v want retained conflict", cleanupErr)
	}
	got, readErr = os.ReadFile(path)
	if readErr != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("existing rebound teardown changed replacement got=%q err=%v", got, readErr)
	}
	got, readErr = os.ReadFile(orphanPath)
	if readErr != nil || !bytes.Equal(got, original) {
		t.Fatalf("existing rebound teardown changed exact child got=%q err=%v want original=%q", got, readErr, original)
	}
	stats = registry.Stats()
	if stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 1 {
		t.Fatalf("existing rebound teardown registry state=%+v want handles released and stale proof retained", stats)
	}
}
