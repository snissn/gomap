//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRenameStableChildFileUsesRetainedParentAfterPathRebind(t *testing.T) {
	root := t.TempDir()
	originalPath := filepath.Join(root, "leaf")
	if err := os.Mkdir(originalPath, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "manifest.tmp", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	if _, err := child.WriteString("stable"); err != nil {
		t.Fatal(err)
	}

	retainedPath := filepath.Join(root, "retained")
	if err := os.Rename(originalPath, retainedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(originalPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(originalPath, "manifest.tmp"), []byte("rogue"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := RenameStableChildFile(parent, "manifest.tmp", "manifest.json"); err != nil {
		t.Fatalf("RenameStableChildFile: %v", err)
	}
	if err := ValidateStableChildLink(parent, child, "manifest.json"); err != nil {
		t.Fatalf("ValidateStableChildLink: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(retainedPath, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "stable" {
		t.Fatalf("retained manifest=%q want stable", got)
	}
	if _, err := os.Stat(filepath.Join(originalPath, "manifest.json")); !os.IsNotExist(err) {
		t.Fatalf("diagnostic path replacement was mutated: %v", err)
	}
}

func TestRenameStableChildFileRejectsNonBaseNames(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	for _, name := range []string{"", ".", "..", "a/b"} {
		if err := RenameStableChildFile(parent, name, "manifest.json"); err == nil {
			t.Fatalf("old name %q unexpectedly accepted", name)
		}
		if err := RenameStableChildFile(parent, "manifest.tmp", name); err == nil {
			t.Fatalf("new name %q unexpectedly accepted", name)
		}
	}
}
