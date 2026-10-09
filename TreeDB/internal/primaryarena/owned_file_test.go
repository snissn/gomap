package primaryarena

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenCapsuleOwnedFileUsesTransferredHandle(t *testing.T) {
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, "owned"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := filepath.Join(dir, "foreign")
	if err := os.WriteFile(diagnostic, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	a, err := OpenCapsuleOwnedFile(f, diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	if !a.CapsuleFormatV6() || a.MetadataOwner() == nil {
		t.Fatal("missing genuine capsule owner")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(diagnostic)
	if err != nil || string(b) != "foreign" {
		t.Fatalf("diagnostic child changed: %q %v", b, err)
	}
}

func TestOpenCapsuleOwnedFileFailedFormatClosesHandle(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "malformed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("malformed")); err != nil {
		t.Fatal(err)
	}
	a, err := OpenCapsuleOwnedFile(f, "unused")
	if !errors.Is(err, ErrFormat) || a != nil {
		t.Fatalf("a=%p err=%v", a, err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("handle not closed: %v", err)
	}
}

func TestOpenCapsuleOwnedFileFailedPagerRetainsGenuineOwner(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	a, err := OpenCapsuleOwnedFile(f, "unused")
	if a == nil || err == nil || a.MetadataOwner() == nil {
		t.Fatalf("missing failure owner: a=%p err=%v", a, err)
	}
	if _, _, err := a.PrepareClaim(Component, nil); err == nil {
		t.Fatal("failed arena allows allocation")
	}
	if err := a.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("lost original close error: %v", err)
	}
}
