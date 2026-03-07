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
	dictDir := filepath.Join(root, "dictdb")
	if err := os.MkdirAll(dictDir, 0o755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dictDir, "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write dictdb/index.db: %v", err)
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

func TestResolveOpenDirLayout_MainDirWithUninitializedSiblingSideStoreStaysFlat(t *testing.T) {
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
	if layout.rootDir != mainDir || layout.mainDir != mainDir || !layout.disableSideStores {
		t.Fatalf("unexpected flat main-dir layout: %+v", layout)
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

func TestResolveOpenDirLayout_DisableSideStoresRejectsInitializedSideStoreRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "dictdb"), 0o755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "dictdb", "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write dictdb/index.db: %v", err)
	}
	if _, err := resolveOpenDirLayout(root, true); err == nil {
		t.Fatalf("expected DisableSideStores initialized-root error")
	}
}

func TestResolveOpenDirLayout_NewMaindbPathStaysUnderProvidedDir(t *testing.T) {
	parent := t.TempDir()
	provided := filepath.Join(parent, "maindb")

	layout, err := resolveOpenDirLayout(provided, false)
	if err != nil {
		t.Fatalf("resolve new maindb path: %v", err)
	}
	if layout.rootDir != provided || layout.mainDir != filepath.Join(provided, "maindb") {
		t.Fatalf("unexpected layout for new maindb path: %+v", layout)
	}
	if layout.dictdbDir != filepath.Join(provided, "dictdb") || layout.templatedbDir != filepath.Join(provided, "templatedb") {
		t.Fatalf("unexpected side-store layout for new maindb path: %+v", layout)
	}
}

func TestResolveOpenDirLayout_NewMaindbPathUsesInitializedParentRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "dictdb"), 0o755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "dictdb", "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write dictdb/index.db: %v", err)
	}
	provided := filepath.Join(root, "maindb")

	layout, err := resolveOpenDirLayout(provided, false)
	if err != nil {
		t.Fatalf("resolve initialized parent maindb path: %v", err)
	}
	if layout.rootDir != root || layout.mainDir != provided {
		t.Fatalf("unexpected initialized-parent layout: %+v", layout)
	}
}
