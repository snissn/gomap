//go:build !windows

package collections

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestDeleteColumnAssetSegmentStableRejectsPreValidationRebind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-000001.tca")
	moved := filepath.Join(dir, "segment-original.tca")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	restore := setColumnAssetStableDeleteBeforeValidationTestHook(func() {
		if err := os.Rename(path, moved); err != nil {
			t.Fatalf("rename original: %v", err)
		}
		if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
			t.Fatalf("write replacement: %v", err)
		}
	})
	defer restore()
	deleted, err := deleteColumnAssetSegmentStable(path, rootpublication.NewIdentityPinRegistry(), removeStableColumnAssetChild)
	if deleted || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("stable delete after rebind=(%v,%v) want (false,ErrResourceConflict)", deleted, err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "replacement" {
		t.Fatalf("replacement changed raw=%q err=%v", raw, err)
	}
	if raw, err := os.ReadFile(moved); err != nil || string(raw) != "original" {
		t.Fatalf("original changed raw=%q err=%v", raw, err)
	}
}

func TestDeleteColumnAssetSegmentStableWaitsForActiveAppender(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	root := backenddb.ColumnAssetRootDirPath(t.TempDir())
	appender, err := newColumnPhysicalAssetSegmentAppendWriter(root, *cfg, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("newColumnPhysicalAssetSegmentAppendWriter: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = appender.abort()
		}
	}()
	if _, err := appender.appendKind([]byte("append-in-progress"), ColumnAssetKindTCS1PartImage, 1, columnPhysicalRowAssetPartID); err != nil {
		t.Fatalf("appendKind: %v", err)
	}

	lockAttempted := make(chan struct{})
	validationReached := make(chan struct{})
	restoreLockHook := setColumnAssetStableDeleteBeforeSegmentLockTestHook(func() { close(lockAttempted) })
	defer restoreLockHook()
	restoreValidationHook := setColumnAssetStableDeleteBeforeValidationTestHook(func() { close(validationReached) })
	defer restoreValidationHook()
	type deleteResult struct {
		deleted bool
		err     error
	}
	segmentLockProved := make(chan struct{}, 1)
	remover := func(parent *os.File, name, diagnosticPath string) error {
		lock := columnAssetSegmentWriteLock(diagnosticPath)
		if lock.TryLock() {
			lock.Unlock()
			return errors.New("stable delete remover ran without canonical segment lock")
		}
		segmentLockProved <- struct{}{}
		return removeStableColumnAssetChild(parent, name, diagnosticPath)
	}
	result := make(chan deleteResult, 1)
	go func() {
		deleted, err := deleteColumnAssetSegmentStable(appender.assetPath, rootpublication.NewIdentityPinRegistry(), remover)
		result <- deleteResult{deleted: deleted, err: err}
	}()
	<-lockAttempted
	select {
	case <-validationReached:
		t.Fatal("stable delete passed the segment lock while appender still held it")
	default:
	}
	select {
	case got := <-result:
		t.Fatalf("stable delete completed during append: %+v", got)
	default:
	}
	if raw, err := os.ReadFile(appender.assetPath); err != nil || string(raw) != "append-in-progress" {
		t.Fatalf("active append target changed raw=%q err=%v", raw, err)
	}

	if err := appender.close(); err != nil {
		t.Fatalf("appender close: %v", err)
	}
	closed = true
	select {
	case got := <-result:
		if got.err != nil || !got.deleted {
			t.Fatalf("stable delete after writer release=(%v,%v) want (true,nil)", got.deleted, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stable delete remained blocked after writer released segment lock")
	}
	select {
	case <-segmentLockProved:
	default:
		t.Fatal("stable delete remover did not prove canonical segment lock ownership")
	}
	select {
	case <-validationReached:
	default:
		t.Fatal("stable delete did not validate retained link after writer release")
	}
	if _, err := os.Stat(appender.assetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment after stable delete stat error=%v want not exist", err)
	}
}
