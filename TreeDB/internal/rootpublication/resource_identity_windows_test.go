//go:build windows

package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenStableChildFileUsesExactParentHandleWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenStableChildFile: %v", err)
	}
	if _, err := child.Write([]byte("stable")); err != nil {
		_ = child.Close()
		t.Fatalf("write stable child: %v", err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "child")); err != nil || string(got) != "stable" {
		t.Fatalf("read stable child got=%q err=%v", got, err)
	}
	if duplicate, err := OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600); !errors.Is(err, os.ErrExist) {
		if duplicate != nil {
			_ = duplicate.Close()
		}
		t.Fatalf("duplicate OpenStableChildFile error=%v want os.ErrExist", err)
	}
	if missing, err := OpenStableChildFile(parent, "missing", os.O_RDONLY, 0); !errors.Is(err, os.ErrNotExist) {
		if missing != nil {
			_ = missing.Close()
		}
		t.Fatalf("missing OpenStableChildFile error=%v want os.ErrNotExist", err)
	}
}

func TestStableNamespaceRegistrationSyncsExactParentWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource, err := os.OpenFile(filepath.Join(dir, "child"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Close()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "child", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatalf("NewStableNamespaceToken: %v", err)
	}
	defer namespace.Release()
	if err := namespace.Stabilize(); err != nil {
		t.Fatalf("Stabilize: %v", err)
	}
}

func TestStableNamespaceCreationProofSyncsExactParentWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.OpenFile(filepath.Join(dir, "child-proof"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	proof, err := NewStableNamespaceCreationProof(parent, child, "child-proof")
	if err != nil {
		t.Fatalf("NewStableNamespaceCreationProof: %v", err)
	}
	proof.Release()
}

func TestRenameStableChildFileFailsTypedWithoutVisibilityWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	oldPath := filepath.Join(dir, "old")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameStableChildFile(parent, "old", "new"); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("RenameStableChildFile error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("unsupported rename changed source: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported rename exposed destination: %v", err)
	}
}
