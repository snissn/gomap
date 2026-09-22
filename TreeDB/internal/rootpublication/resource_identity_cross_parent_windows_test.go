//go:build windows

package rootpublication

import (
	"errors"
	"os"
	"testing"
)

func TestMoveStableChildFileNoReplaceFailsClosedOnWindows(t *testing.T) {
	source, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destination, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if installed, err := MoveStableChildFileNoReplace(source, source, "old", destination, "new"); installed || !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("installed=%v err=%v want typed unsupported", installed, err)
	}
}

func TestInstallStableFileHandleNoReplaceFailsClosedOnWindows(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	if installed, err := InstallStableFileHandleNoReplace(parent, parent, "new"); installed || !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("installed=%v err=%v want typed unsupported", installed, err)
	}
}

func TestOpenStableAnonymousFileFailsClosedOnWindows(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	if file, err := OpenStableAnonymousFile(parent, 0o600); file != nil || !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("file=%v err=%v want typed unsupported", file, err)
	}
	entries, err := parent.ReadDir(-1)
	if err != nil || len(entries) != 0 {
		t.Fatalf("anonymous unsupported entries=%v err=%v", entries, err)
	}
}
