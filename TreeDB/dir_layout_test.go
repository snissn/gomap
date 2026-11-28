package treedb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveOpenDir_FlatDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write index.db: %v", err)
	}

	got, err := resolveOpenDir(dir)
	if err != nil {
		t.Fatalf("resolve flat dir: %v", err)
	}
	if got != filepath.Clean(dir) {
		t.Fatalf("unexpected resolved dir: got=%q want=%q", got, filepath.Clean(dir))
	}
}

func TestResolveOpenDir_IndexPathDirectoryReturnsError(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "index.db"), 0o755); err != nil {
		t.Fatalf("mkdir index.db: %v", err)
	}

	if _, err := resolveOpenDir(root); err == nil {
		t.Fatal("expected index.db directory error")
	}
}

func TestResolveOpenDir_EmptyReturnsError(t *testing.T) {
	if _, err := resolveOpenDir(""); err == nil {
		t.Fatal("expected empty dir error")
	}
}
