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

func TestRenameStableChildFileFailsTypedWhenRelativePrimitiveUnavailable(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	if err := RenameStableChildFile(parent, "old", "new"); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("RenameStableChildFile error=%v want ErrNamespacePersistenceUnsupported", err)
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
