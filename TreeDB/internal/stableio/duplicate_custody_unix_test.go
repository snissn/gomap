//go:build darwin || linux || freebsd || netbsd || openbsd

package stableio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDuplicateCustodyPreservesAmbiguousRawClose(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fault := errors.New("injected duplicate failure")
	closeFault := errors.New("injected ambiguous close failure")
	closes := 0
	var c DuplicateCustody
	_, err = duplicateFileWithCustodyUnix(f, "fake", &c, duplicateCustodyUnixOps{
		dup: func(int) (int, error) { return 217, fault },
		close: func(fd int) error {
			closes++
			if fd != 217 {
				t.Fatalf("close fd=%d", fd)
			}
			return closeFault
		},
		wrap: func(uintptr, string) *os.File { t.Fatal("unexpected wrap"); return nil },
	})
	if err != &c || !errors.Is(err, fault) || !errors.Is(err, closeFault) || c.OperationError() != fault || c.CleanupError() != closeFault || !c.Ambiguous() || c.raw != 217 || closes != 1 {
		t.Fatalf("err=%v operation=%v cleanup=%v ambiguous=%v raw=%d closes=%d", err, c.OperationError(), c.CleanupError(), c.Ambiguous(), c.raw, closes)
	}
	copied := c
	if _, err = DuplicateFileWithCustody(f, "again", &copied); !errors.Is(err, ErrDuplicateCustodyUsed) {
		t.Fatalf("copied custody: %v", err)
	}
	if _, err = DuplicateFileWithCustody(f, "again", &c); !errors.Is(err, ErrDuplicateCustodyUsed) {
		t.Fatalf("reused custody: %v", err)
	}
	if closes != 1 {
		t.Fatalf("raw cleanup repeated: %d", closes)
	}
}

func TestDuplicateCustodyCleanFailureAndSourceClose(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("injected duplicate failure")
	closes := 0
	var failed DuplicateCustody
	_, err = duplicateFileWithCustodyUnix(f, "fake", &failed, duplicateCustodyUnixOps{
		dup:   func(int) (int, error) { return 218, fault },
		close: func(int) error { closes++; return nil },
		wrap:  func(uintptr, string) *os.File { t.Fatal("unexpected wrap"); return nil },
	})
	if err != &failed || !errors.Is(err, fault) || failed.Ambiguous() || failed.CleanupError() != nil || closes != 1 {
		t.Fatalf("failure=%v closes=%d", err, closes)
	}
	if _, err = f.WriteAt([]byte("abc"), 0); err != nil {
		t.Fatal(err)
	}
	var succeeded DuplicateCustody
	dup, err := DuplicateFileWithCustody(f, "duplicate", &succeeded)
	if err != nil {
		t.Fatal(err)
	}
	defer dup.Close()
	var flags int
	var flagsErr error
	conn, err := dup.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.Control(func(fd uintptr) { flags, flagsErr = unix.FcntlInt(fd, unix.F_GETFD, 0) }); err != nil {
		t.Fatal(err)
	}
	if flagsErr != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("FD_CLOEXEC flags=%d err=%v", flags, flagsErr)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if _, err = dup.ReadAt(buf, 0); err != nil || string(buf) != "abc" {
		t.Fatalf("read=%q err=%v", buf, err)
	}
	if succeeded.Ambiguous() || succeeded.OperationError() != nil || succeeded.CleanupError() != nil {
		t.Fatal("success retained failure")
	}
}

func TestDuplicateCustodyWrapFailureRetainsCause(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	closeFault := errors.New("injected close error")
	var c DuplicateCustody
	_, err = duplicateFileWithCustodyUnix(f, "fake", &c, duplicateCustodyUnixOps{
		dup:   func(int) (int, error) { return 219, nil },
		close: func(int) error { return closeFault },
		wrap:  func(uintptr, string) *os.File { return nil },
	})
	if err != &c || !errors.Is(err, os.ErrInvalid) || !errors.Is(err, closeFault) || !c.Ambiguous() || c.raw != 219 {
		t.Fatalf("err=%v, ambiguous=%v", err, c.Ambiguous())
	}
}
