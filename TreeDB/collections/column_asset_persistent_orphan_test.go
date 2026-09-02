package collections

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func fileHandleClosedForTest(file *os.File) bool {
	return errors.Is(file.Close(), os.ErrClosed)
}

func persistentOrphanColumnStoreConfig(t testing.TB, namespace string) ColumnStoreConfig {
	t.Helper()
	input := testColumnStoreConfig(nil)
	input.AssetManager = &ColumnAssetManagerConfig{
		Kind:              ColumnAssetManagerValueLogShaped,
		IsolatedNamespace: true,
		Namespace:         namespace,
	}
	cfg, err := normalizeColumnStoreConfig("events", input)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	return *cfg
}

func persistentOrphanSegmentPath(t *testing.T, root string, cfg ColumnStoreConfig, fileID uint32) string {
	t.Helper()
	path, err := columnAssetSegmentPath(root, ColumnAssetRef{
		Namespace: cfg.AssetManager.Namespace,
		FileID:    fileID,
	})
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	return path
}

func TestColumnAssetCreatedOneShotFailureRetainsPrefixForIdenticalRetry(t *testing.T) {
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	cfg := persistentOrphanColumnStoreConfig(t, "one-shot-retry")
	assetPath := persistentOrphanSegmentPath(t, root, cfg, columnAssetM12ASegmentFileID)
	failedPayload := []byte("failed-unpublished-prefix")
	injected := errors.New("injected created segment sync failure")
	var failedFile *os.File
	originalSync := syncColumnAssetSegmentFileForPublish
	syncColumnAssetSegmentFileForPublish = func(file *os.File) error {
		failedFile = file
		return injected
	}
	t.Cleanup(func() { syncColumnAssetSegmentFileForPublish = originalSync })

	failedRef, err := writeColumnAssetToManagerSegment(root, cfg, failedPayload, ColumnAssetKindTCS1PartImage, 7, 1, columnAssetM12ASegmentFileID)
	if !errors.Is(err, injected) {
		t.Fatalf("created write error=%v want %v", err, injected)
	}
	if failedRef != (ColumnAssetRef{}) {
		t.Fatalf("created failed write returned ref=%+v", failedRef)
	}
	if failedFile == nil {
		t.Fatal("created failed write did not expose exact sync handle")
	}
	if !fileHandleClosedForTest(failedFile) {
		t.Fatal("created failed write leaked exact handle")
	}
	if got, err := os.ReadFile(assetPath); err != nil || !bytes.Equal(got, failedPayload) {
		t.Fatalf("created failed prefix=%q err=%v want %q", got, err, failedPayload)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("created failed write marked pathname directory-sync cache known")
	}

	syncColumnAssetSegmentFileForPublish = originalSync
	retryPayload := []byte("checksum-correct-retry")
	retryRef, err := writeColumnAssetToManagerSegment(root, cfg, retryPayload, ColumnAssetKindTCS1PartImage, 7, 1, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	if retryRef.Offset != int64(len(failedPayload)) {
		t.Fatalf("retry offset=%d want after failed prefix %d", retryRef.Offset, len(failedPayload))
	}
	got, err := readColumnPhysicalAssetFromManager(root, retryRef)
	if err != nil || !bytes.Equal(got, retryPayload) {
		t.Fatalf("retry ref payload=%q err=%v want %q", got, err, retryPayload)
	}
}

func TestColumnAssetCreatedOneShotFailureNeverUnlinksReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("renaming an open file is platform-specific; Windows behavior is compile-gated")
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	cfg := persistentOrphanColumnStoreConfig(t, "one-shot-replacement")
	assetPath := persistentOrphanSegmentPath(t, root, cfg, columnAssetM12ASegmentFileID)
	displacedPath := assetPath + ".displaced"
	failedPayload := []byte("exact-created-child")
	replacementPayload := []byte("adversarial-replacement")
	injected := errors.New("injected failure after replacement")
	var exactFile *os.File
	originalSync := syncColumnAssetSegmentFileForPublish
	syncColumnAssetSegmentFileForPublish = func(file *os.File) error {
		exactFile = file
		if err := os.Rename(assetPath, displacedPath); err != nil {
			t.Fatalf("rename exact child: %v", err)
		}
		if err := os.WriteFile(assetPath, replacementPayload, 0o600); err != nil {
			t.Fatalf("write replacement: %v", err)
		}
		return injected
	}
	t.Cleanup(func() { syncColumnAssetSegmentFileForPublish = originalSync })

	ref, err := writeColumnAssetToManagerSegment(root, cfg, failedPayload, ColumnAssetKindTCS1PartImage, 9, 1, columnAssetM12ASegmentFileID)
	if !errors.Is(err, injected) || ref != (ColumnAssetRef{}) {
		t.Fatalf("adversarial result ref=%+v err=%v want zero/injected", ref, err)
	}
	if !fileHandleClosedForTest(exactFile) {
		t.Fatal("adversarial cleanup leaked exact child handle")
	}
	if got, err := os.ReadFile(displacedPath); err != nil || !bytes.Equal(got, failedPayload) {
		t.Fatalf("displaced exact child=%q err=%v want %q", got, err, failedPayload)
	}
	if got, err := os.ReadFile(assetPath); err != nil || !bytes.Equal(got, replacementPayload) {
		t.Fatalf("replacement=%q err=%v want survivor %q", got, err, replacementPayload)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("adversarial failure marked replacement pathname cache known")
	}
}

func TestStableColumnAssetCreatedFailureClosesHandlesWithoutValidationOrUnlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("renaming an open file is platform-specific; Windows behavior is compile-gated")
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	cfg := persistentOrphanColumnStoreConfig(t, "stable-replacement")
	assetPath := persistentOrphanSegmentPath(t, root, cfg, columnAssetM12ASegmentFileID)
	displacedPath := assetPath + ".displaced"
	failedPayload := []byte("stable-exact-created-child")
	replacementPayload := []byte("stable-adversarial-replacement")
	injected := errors.New("injected stable capture failure")
	var exactFile, exactParent *os.File
	originalOpenParent := openStableColumnAssetParent
	openStableColumnAssetParent = func(path string) (*os.File, error) {
		parent, err := originalOpenParent(path)
		if err == nil {
			exactParent = parent
		}
		return parent, err
	}
	t.Cleanup(func() { openStableColumnAssetParent = originalOpenParent })
	originalResourceToken := stableColumnAssetResourceTokenForPublish
	stableColumnAssetResourceTokenForPublish = func(file *os.File, _ ColumnAssetRef, _ *rootpublication.StableNamespaceToken) (*rootpublication.StableResourceToken, error) {
		exactFile = file
		if err := os.Rename(assetPath, displacedPath); err != nil {
			t.Fatalf("rename stable exact child: %v", err)
		}
		if err := os.WriteFile(assetPath, replacementPayload, 0o600); err != nil {
			t.Fatalf("write stable replacement: %v", err)
		}
		return nil, injected
	}
	t.Cleanup(func() { stableColumnAssetResourceTokenForPublish = originalResourceToken })

	ref, token, err := writeColumnAssetToManagerWithStableResource(root, cfg, failedPayload, ColumnAssetKindTCS1TypedColumnPart, 11, 1)
	if !errors.Is(err, injected) || ref != (ColumnAssetRef{}) || token != nil {
		t.Fatalf("stable adversarial result ref=%+v token=%v err=%v", ref, token, err)
	}
	if errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("stable failure performed pathname validation after create: %v", err)
	}
	if exactFile == nil || exactParent == nil {
		t.Fatalf("stable failure did not capture exact handles file=%p parent=%p", exactFile, exactParent)
	}
	if !fileHandleClosedForTest(exactFile) {
		t.Fatal("stable failure leaked exact child handle")
	}
	if !fileHandleClosedForTest(exactParent) {
		t.Fatal("stable failure leaked exact parent handle")
	}
	if got, err := os.ReadFile(displacedPath); err != nil || !bytes.Equal(got, failedPayload) {
		t.Fatalf("stable displaced exact child=%q err=%v want %q", got, err, failedPayload)
	}
	if got, err := os.ReadFile(assetPath); err != nil || !bytes.Equal(got, replacementPayload) {
		t.Fatalf("stable replacement=%q err=%v want survivor %q", got, err, replacementPayload)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("stable adversarial failure marked replacement pathname cache known")
	}
}

func TestColumnAssetAppenderAbortNeverUnlinksReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("renaming an open file is platform-specific; Windows behavior is compile-gated")
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	cfg := persistentOrphanColumnStoreConfig(t, "appender-abort-replacement")
	appender, err := newNextColumnPhysicalAssetSegmentAppender(root, cfg)
	if err != nil {
		t.Fatalf("new appender: %v", err)
	}
	failedPayload := []byte("aborted-exact-child")
	if _, err := appender.append(failedPayload, 13, 1); err != nil {
		_ = appender.abort()
		t.Fatalf("append: %v", err)
	}
	exactFile := appender.file
	assetPath := appender.assetPath
	displacedPath := assetPath + ".displaced"
	replacementPayload := []byte("abort-replacement")
	markColumnAssetSegmentDirSynced(assetPath)
	if err := os.Rename(assetPath, displacedPath); err != nil {
		_ = appender.abort()
		t.Fatalf("rename exact appender child: %v", err)
	}
	if err := os.WriteFile(assetPath, replacementPayload, 0o600); err != nil {
		_ = appender.abort()
		t.Fatalf("write replacement: %v", err)
	}
	if err := appender.abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if !fileHandleClosedForTest(exactFile) {
		t.Fatal("abort leaked exact child handle")
	}
	if got, err := os.ReadFile(displacedPath); err != nil || !bytes.Equal(got, failedPayload) {
		t.Fatalf("aborted displaced child=%q err=%v want %q", got, err, failedPayload)
	}
	if got, err := os.ReadFile(assetPath); err != nil || !bytes.Equal(got, replacementPayload) {
		t.Fatalf("abort replacement=%q err=%v want survivor %q", got, err, replacementPayload)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("abort retained stale pathname directory-sync cache")
	}
}

func TestColumnAssetAppenderSyncFailureNeverUnlinksReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("renaming an open file is platform-specific; Windows behavior is compile-gated")
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	cfg := persistentOrphanColumnStoreConfig(t, "appender-close-replacement")
	appender, err := newNextColumnPhysicalAssetSegmentAppender(root, cfg)
	if err != nil {
		t.Fatalf("new appender: %v", err)
	}
	failedPayload := []byte("close-failed-exact-child")
	if _, err := appender.append(failedPayload, 15, 1); err != nil {
		_ = appender.abort()
		t.Fatalf("append: %v", err)
	}
	exactFile := appender.file
	assetPath := appender.assetPath
	displacedPath := assetPath + ".displaced"
	replacementPayload := []byte("close-failure-replacement")
	injected := errors.New("injected appender sync failure")
	originalSync := syncColumnAssetSegmentFileForPublish
	syncColumnAssetSegmentFileForPublish = func(file *os.File) error {
		if file != exactFile {
			t.Fatalf("sync file=%p want exact appender file=%p", file, exactFile)
		}
		if err := os.Rename(assetPath, displacedPath); err != nil {
			t.Fatalf("rename exact appender child: %v", err)
		}
		if err := os.WriteFile(assetPath, replacementPayload, 0o600); err != nil {
			t.Fatalf("write replacement: %v", err)
		}
		return injected
	}
	t.Cleanup(func() { syncColumnAssetSegmentFileForPublish = originalSync })

	if err := appender.close(); !errors.Is(err, injected) {
		t.Fatalf("close error=%v want %v", err, injected)
	}
	if !fileHandleClosedForTest(exactFile) {
		t.Fatal("failed close leaked exact child handle")
	}
	if got, err := os.ReadFile(displacedPath); err != nil || !bytes.Equal(got, failedPayload) {
		t.Fatalf("failed-close displaced child=%q err=%v want %q", got, err, failedPayload)
	}
	if got, err := os.ReadFile(assetPath); err != nil || !bytes.Equal(got, replacementPayload) {
		t.Fatalf("failed-close replacement=%q err=%v want survivor %q", got, err, replacementPayload)
	}
	if appender.closeStats.DirSync != 0 || appender.closeStats.Remove != 0 || appender.closeStats.RemoveDirSync != 0 || appender.closeStats.CleanupDuration() != 0 {
		t.Fatalf("failed close rollback accounting=%+v want no namespace cleanup", appender.closeStats)
	}
	if columnAssetSegmentDirSyncKnown(assetPath) {
		t.Fatal("failed close marked replacement pathname cache known")
	}
}

func TestColumnAssetAppenderCloseAndAbortErrorsRetainCreatedPath(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "close"
		if abort {
			name = "abort"
		}
		t.Run(name, func(t *testing.T) {
			root := backenddb.ColumnAssetRootDirPath(t.TempDir())
			cfg := persistentOrphanColumnStoreConfig(t, "appender-"+name+"-error")
			appender, err := newNextColumnPhysicalAssetSegmentAppender(root, cfg)
			if err != nil {
				t.Fatalf("new appender: %v", err)
			}
			assetPath := appender.assetPath
			if err := appender.file.Close(); err != nil {
				t.Fatalf("preclose exact file: %v", err)
			}
			if abort {
				err = appender.abort()
			} else {
				err = appender.close()
			}
			if err == nil {
				t.Fatalf("%s error=nil want exact-file close failure", name)
			}
			if _, statErr := os.Stat(assetPath); statErr != nil {
				t.Fatalf("%s removed created path: %v", name, statErr)
			}
			if columnAssetSegmentDirSyncKnown(assetPath) {
				t.Fatalf("%s marked created failure pathname cache known", name)
			}
			if !abort && (appender.closeStats.Remove != 0 || appender.closeStats.RemoveDirSync != 0 || appender.closeStats.CleanupDuration() != 0) {
				t.Fatalf("%s removal accounting=%+v want zero", name, appender.closeStats)
			}
		})
	}
}

func TestColumnAssetAppenderAbort64CycleHandleAndCachePlateau(t *testing.T) {
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	cfg := persistentOrphanColumnStoreConfig(t, "appender-abort-plateau")
	const cycles = 64
	var previousFileID uint32
	for cycle := 0; cycle < cycles; cycle++ {
		appender, err := newNextColumnPhysicalAssetSegmentAppender(root, cfg)
		if err != nil {
			t.Fatalf("cycle %d new appender: %v", cycle, err)
		}
		if appender.fileID <= previousFileID {
			_ = appender.abort()
			t.Fatalf("cycle %d file_id=%d want > %d", cycle, appender.fileID, previousFileID)
		}
		previousFileID = appender.fileID
		payload := []byte(fmt.Sprintf("unpublished-orphan-%02d", cycle))
		if _, err := appender.append(payload, uint64(cycle+1), 1); err != nil {
			_ = appender.abort()
			t.Fatalf("cycle %d append: %v", cycle, err)
		}
		exactFile := appender.file
		assetPath := appender.assetPath
		markColumnAssetSegmentDirSynced(assetPath)
		if err := appender.abort(); err != nil {
			t.Fatalf("cycle %d abort: %v", cycle, err)
		}
		if !fileHandleClosedForTest(exactFile) {
			t.Fatalf("cycle %d leaked exact handle", cycle)
		}
		if got, err := os.ReadFile(assetPath); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("cycle %d orphan=%q err=%v want %q", cycle, got, err, payload)
		}
		if columnAssetSegmentDirSyncKnown(assetPath) {
			t.Fatalf("cycle %d retained pathname cache authority", cycle)
		}
	}

	final, err := newNextColumnPhysicalAssetSegmentAppender(root, cfg)
	if err != nil {
		t.Fatalf("final new appender: %v", err)
	}
	if final.fileID <= previousFileID {
		_ = final.abort()
		t.Fatalf("final file_id=%d want > last orphan %d", final.fileID, previousFileID)
	}
	payload := []byte("published-after-64-orphans")
	ref, err := final.append(payload, cycles+1, 1)
	if err != nil {
		_ = final.abort()
		t.Fatalf("final append: %v", err)
	}
	if err := final.close(); err != nil {
		t.Fatalf("final close: %v", err)
	}
	got, err := readColumnPhysicalAssetFromManager(root, ref)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("final ref payload=%q err=%v want %q", got, err, payload)
	}
	if filepath.Clean(final.assetPath) == "" {
		t.Fatal("final appender path unexpectedly empty")
	}
}
