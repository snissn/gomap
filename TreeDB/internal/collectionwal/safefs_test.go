package collectionwal

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestValidateClassRootDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod safe dir: %v", err)
	}
	if err := ValidateClassRootDir(dir); err != nil {
		t.Fatalf("ValidateClassRootDir(safe): %v", err)
	}

	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := ValidateClassRootDir(file); !errors.Is(err, ErrCollectionWALUnsafePath) {
		t.Fatalf("ValidateClassRootDir(file)=%v want unsafe path", err)
	}
}

func TestValidateClassRootDirRejectsWritableModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode bits are not authoritative on windows")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o770); err != nil {
		t.Fatalf("chmod group writable: %v", err)
	}
	if err := ValidateClassRootDir(dir); !errors.Is(err, ErrCollectionWALUnsafePath) {
		t.Fatalf("ValidateClassRootDir(group writable)=%v want unsafe path", err)
	}
	if err := os.Chmod(dir, 0o707); err != nil {
		t.Fatalf("chmod world writable: %v", err)
	}
	if err := ValidateClassRootDir(dir); !errors.Is(err, ErrCollectionWALUnsafePath) {
		t.Fatalf("ValidateClassRootDir(world writable)=%v want unsafe path", err)
	}
}

func TestValidateClassRootDirRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink privileges are environment-dependent on windows")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "link")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := ValidateClassRootDir(link); !errors.Is(err, ErrCollectionWALUnsafePath) {
		t.Fatalf("ValidateClassRootDir(symlink)=%v want unsafe path", err)
	}
}
