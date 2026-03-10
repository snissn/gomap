package treedb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveOpenDirLayout_RootDir(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	dictDir := filepath.Join(root, "dictdb")
	templateDir := filepath.Join(root, "templatedb")
	for _, dir := range []string{mainDir, dictDir, templateDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(filepath.Join(mainDir, "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write maindb/index.db: %v", err)
	}

	layout, err := resolveOpenDirLayout(root, false)
	if err != nil {
		t.Fatalf("resolve root layout: %v", err)
	}
	if layout.rootDir != root || layout.mainDir != mainDir || layout.dictdbDir != dictDir || layout.templatedbDir != templateDir || layout.disableSideStores {
		t.Fatalf("unexpected root layout: %+v", layout)
	}
}

func TestResolveOpenDirLayout_MainDirWithSideStores(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "dictdb"), 0o755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write maindb/index.db: %v", err)
	}

	layout, err := resolveOpenDirLayout(mainDir, false)
	if err != nil {
		t.Fatalf("resolve main dir layout: %v", err)
	}
	if layout.rootDir != root || layout.mainDir != mainDir || layout.dictdbDir != filepath.Join(root, "dictdb") {
		t.Fatalf("unexpected main-dir layout: %+v", layout)
	}
}

func TestResolveOpenDirLayout_FlatDirDisablesSideStores(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write index.db: %v", err)
	}

	layout, err := resolveOpenDirLayout(root, false)
	if err != nil {
		t.Fatalf("resolve flat layout: %v", err)
	}
	if layout.rootDir != root || layout.mainDir != root || !layout.disableSideStores {
		t.Fatalf("unexpected flat layout: %+v", layout)
	}
}

func TestResolveOpenDirLayout_IndexPathDirectoryReturnsError(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "index.db"), 0o755); err != nil {
		t.Fatalf("mkdir index.db: %v", err)
	}

	if _, err := resolveOpenDirLayout(root, false); err == nil {
		t.Fatal("expected index.db directory error")
	}
}

func TestResolveOpenDirLayout_DisableSideStoresRejectsRootDir(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "maindb"), 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if _, err := resolveOpenDirLayout(root, true); err == nil {
		t.Fatalf("expected DisableSideStores root-layout error")
	}
}
