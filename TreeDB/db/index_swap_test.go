package db

import (
	"os"
	"path/filepath"
	"testing"
)

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
