//go:build windows

package stableio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestDuplicateCustodyWindowsPreservesAmbiguousRawClose(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fault := errors.New("injected duplication failure after raw result")
	closeFault := errors.New("injected close failure")
	closes := 0
	var c DuplicateCustody
	_, err = duplicateFileWithCustodyWindows(f, "fake", &c, duplicateCustodyWindowsOps{
		duplicate: func(_, _ windows.Handle, out *windows.Handle) error { *out = 217; return fault },
		close: func(handle windows.Handle) error {
			closes++
			if handle != 217 {
				t.Fatalf("handle=%d", handle)
			}
			return closeFault
		},
		wrap: func(uintptr, string) *os.File { t.Fatal("unexpected wrap"); return nil },
	})
	if err != &c || !errors.Is(err, fault) || !errors.Is(err, closeFault) || c.OperationError() != fault || c.CleanupError() != closeFault || !c.Ambiguous() || c.raw != 217 || closes != 1 {
		t.Fatalf("err=%v closes=%d", err, closes)
	}
	copied := c
	if _, err = DuplicateFileWithCustody(f, "again", &copied); !errors.Is(err, ErrDuplicateCustodyUsed) {
		t.Fatalf("copied custody: %v", err)
	}
	if closes != 1 {
		t.Fatalf("raw cleanup repeated: %d", closes)
	}
}

func TestDuplicateCustodyWindowsWrapFailure(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	closeFault := errors.New("injected close failure")
	var c DuplicateCustody
	_, err = duplicateFileWithCustodyWindows(f, "fake", &c, duplicateCustodyWindowsOps{
		duplicate: func(_, _ windows.Handle, out *windows.Handle) error { *out = 218; return nil },
		close:     func(windows.Handle) error { return closeFault },
		wrap:      func(uintptr, string) *os.File { return nil },
	})
	if err != &c || !errors.Is(err, os.ErrInvalid) || !errors.Is(err, closeFault) || !c.Ambiguous() || c.raw != 218 {
		t.Fatalf("err=%v", err)
	}
}

func TestDuplicateCustodyWindowsSurvivesSourceClose(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte("abc"), 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	var c DuplicateCustody
	dup, err := DuplicateFileWithCustody(f, "duplicate", &c)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	defer dup.Close()
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if _, err = dup.ReadAt(buf, 0); err != nil || string(buf) != "abc" {
		t.Fatalf("read=%q err=%v", buf, err)
	}
	if c.Ambiguous() || c.OperationError() != nil || c.CleanupError() != nil {
		t.Fatal("success retained failure")
	}
}
