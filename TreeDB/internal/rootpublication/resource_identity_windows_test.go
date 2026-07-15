//go:build windows

package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenStableChildFileFailsTypedWhenRelativePrimitiveUnavailable(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		if child != nil {
			_ = child.Close()
		}
		t.Fatalf("OpenStableChildFile error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if child != nil {
		_ = child.Close()
		t.Fatal("unsupported relative open returned a child handle")
	}
	if _, err := os.Stat(filepath.Join(parent.Name(), "child")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported relative open exposed child: %v", err)
	}
}

func TestStableNamespaceRegistrationFailsTypedBeforeCertification(t *testing.T) {
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
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		if namespace != nil {
			namespace.Release()
		}
		t.Fatalf("NewStableNamespaceToken error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if namespace != nil {
		namespace.Release()
		t.Fatal("unsupported namespace registration returned a certifying token")
	}
}

func TestStableNamespaceCreationProofFailsTypedOnWindows(t *testing.T) {
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
	if proof != nil {
		proof.Release()
		t.Fatal("unsupported namespace creation returned a proof")
	}
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("NewStableNamespaceCreationProof error=%v want ErrNamespacePersistenceUnsupported", err)
	}
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
