//go:build linux

package rootpublication

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestOpenStableAnonymousFileLinuxPublishesOnlyExactHandle(t *testing.T) {
	parent, err := OpenStableParent(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	file, err := OpenStableAnonymousFile(parent, 0o600)
	if err != nil {
		if errors.Is(err, ErrNamespacePersistenceUnsupported) {
			t.Skipf("O_TMPFILE unavailable on test filesystem: %v", err)
		}
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("anonymous file info=%v err=%v", info, err)
	}
	if _, err := file.Write([]byte("anonymous lifecycle")); err != nil {
		t.Fatal(err)
	}
	if err := SyncStableFile(file); err != nil {
		t.Fatal(err)
	}
	entries, err := parent.ReadDir(-1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("anonymous file created namespace entries: %v", entries)
	}
	installed, err := InstallStableFileHandleNoReplace(file, parent, "installed.vlc")
	if err != nil || !installed {
		t.Fatalf("InstallStableFileHandleNoReplace installed=%v err=%v", installed, err)
	}
	if err := SyncStableNamespace(parent); err != nil {
		t.Fatal(err)
	}
	installedFile, err := OpenStableChildFile(parent, "installed.vlc", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer installedFile.Close()
	got := make([]byte, len("anonymous lifecycle"))
	if _, err := installedFile.Read(got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("anonymous lifecycle")) {
		t.Fatalf("installed bytes=%q", got)
	}
	occupied, err := OpenStableChildFile(parent, "occupied.vlc", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := occupied.Write([]byte("keep")); err != nil {
		_ = occupied.Close()
		t.Fatal(err)
	}
	if err := occupied.Close(); err != nil {
		t.Fatal(err)
	}
	if installed, err := InstallStableFileHandleNoReplace(file, parent, "occupied.vlc"); installed || !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("occupied install installed=%v err=%v", installed, err)
	}
	preserved, err := OpenStableChildFile(parent, "occupied.vlc", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer preserved.Close()
	got = make([]byte, len("keep"))
	if _, err := preserved.Read(got); err != nil || !bytes.Equal(got, []byte("keep")) {
		t.Fatalf("occupied bytes=%q err=%v", got, err)
	}
}
