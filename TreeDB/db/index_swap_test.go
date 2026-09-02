package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoverIndexSwap_CleanIndexDoesNotSyncDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, indexFileName), []byte("index"), 0600); err != nil {
		t.Fatalf("WriteFile(index): %v", err)
	}

	origSync := syncDirFn
	defer func() { syncDirFn = origSync }()
	wantErr := errors.New("unexpected directory sync")
	syncDirFn = func(_ string) error { return wantErr }

	if err := recoverIndexSwap(dir); err != nil {
		t.Fatalf("recoverIndexSwap clean index: %v", err)
	}
}

func TestRecoverIndexSwap_FailedBestEffortRemovalDoesNotSyncDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, indexFileName), []byte("index"), 0600); err != nil {
		t.Fatalf("WriteFile(index): %v", err)
	}
	newPath := filepath.Join(dir, indexNewFileName)
	if err := os.Mkdir(newPath, 0700); err != nil {
		t.Fatalf("Mkdir(new artifact): %v", err)
	}
	if err := os.WriteFile(filepath.Join(newPath, "child"), []byte("blocks removal"), 0600); err != nil {
		t.Fatalf("WriteFile(new artifact child): %v", err)
	}

	origSync := syncDirFn
	defer func() { syncDirFn = origSync }()
	wantErr := errors.New("unexpected directory sync")
	syncDirFn = func(_ string) error { return wantErr }

	if err := recoverIndexSwap(dir); err != nil {
		t.Fatalf("recoverIndexSwap failed best-effort removal: %v", err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("failed best-effort removal should retain artifact: %v", err)
	}
}

func TestRecoverIndexSwap_RemovedArtifactSyncsDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, indexFileName), []byte("index"), 0600); err != nil {
		t.Fatalf("WriteFile(index): %v", err)
	}
	newPath := filepath.Join(dir, indexNewFileName)
	if err := os.WriteFile(newPath, []byte("stale"), 0600); err != nil {
		t.Fatalf("WriteFile(new artifact): %v", err)
	}

	origSync := syncDirFn
	defer func() { syncDirFn = origSync }()
	calls := 0
	syncDirFn = func(_ string) error {
		calls++
		return nil
	}

	if err := recoverIndexSwap(dir); err != nil {
		t.Fatalf("recoverIndexSwap removed artifact: %v", err)
	}
	if calls != 1 {
		t.Fatalf("directory sync calls=%d, want 1 after artifact removal", calls)
	}
	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Fatalf("removed artifact stat error=%v, want not-exist", err)
	}
}

func TestRecoverIndexSwap_SyncsAfterNewReadyRename(t *testing.T) {
	dir := t.TempDir()
	newPath := filepath.Join(dir, indexNewFileName)
	readyPath := filepath.Join(dir, indexReadyFileName)

	if err := os.WriteFile(newPath, []byte("new"), 0600); err != nil {
		t.Fatalf("WriteFile(new): %v", err)
	}
	if err := os.WriteFile(readyPath, []byte("ready"), 0600); err != nil {
		t.Fatalf("WriteFile(ready): %v", err)
	}

	origSync := syncDirFn
	defer func() { syncDirFn = origSync }()
	calls := 0
	syncDirFn = func(_ string) error {
		calls++
		return nil
	}

	if err := recoverIndexSwap(dir); err != nil {
		t.Fatalf("recoverIndexSwap: %v", err)
	}

	if calls == 0 {
		t.Fatalf("expected directory sync after rename")
	}

	if _, err := os.Stat(filepath.Join(dir, indexFileName)); err != nil {
		t.Fatalf("expected index.db to exist: %v", err)
	}
	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Fatalf("expected index.db.new removed, got %v", err)
	}
	if _, err := os.Stat(readyPath); !os.IsNotExist(err) {
		t.Fatalf("expected ready marker removed, got %v", err)
	}
}

func TestRecoverIndexSwap_SyncsAfterBakRestore(t *testing.T) {
	dir := t.TempDir()
	bakPath := filepath.Join(dir, indexBakFileName)

	if err := os.WriteFile(bakPath, []byte("bak"), 0600); err != nil {
		t.Fatalf("WriteFile(bak): %v", err)
	}

	origSync := syncDirFn
	defer func() { syncDirFn = origSync }()
	calls := 0
	syncDirFn = func(_ string) error {
		calls++
		return nil
	}

	if err := recoverIndexSwap(dir); err != nil {
		t.Fatalf("recoverIndexSwap: %v", err)
	}

	if calls == 0 {
		t.Fatalf("expected directory sync after bak rename")
	}

	if _, err := os.Stat(filepath.Join(dir, indexFileName)); err != nil {
		t.Fatalf("expected index.db to exist: %v", err)
	}
	if _, err := os.Stat(bakPath); !os.IsNotExist(err) {
		t.Fatalf("expected index.db.bak removed, got %v", err)
	}
}
