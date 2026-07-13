//go:build windows

package rootpublication

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenStableChildFileSupportsCreateAndAppend(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child.WriteString("first"); err != nil {
		t.Fatal(err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	child, err = OpenStableChildFile(parent, "child", os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child.WriteString("-second"); err != nil {
		t.Fatal(err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(parent.Name(), "child"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "first-second"; got != want {
		t.Fatalf("content=%q want %q", got, want)
	}
}

func TestRenameAndRemoveStableChildFile(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	if err := os.WriteFile(filepath.Join(dir, "old"), []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "new"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameStableChildFile(parent, "old", "new"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "new"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "replacement"; got != want {
		t.Fatalf("renamed content=%q want %q", got, want)
	}
	if err := RemoveStableChildFile(parent, "new"); err != nil {
		t.Fatal(err)
	}
	if err := RemoveStableChildFile(parent, "new"); err != nil {
		t.Fatalf("idempotent removal: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new")); !os.IsNotExist(err) {
		t.Fatalf("removed child stat error=%v want not-exist", err)
	}
}

func TestStableNamespaceRegistrationCertifiesExactWindowsHandles(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource, err := OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Close()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "child", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
}
