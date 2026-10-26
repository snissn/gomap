//go:build darwin || linux || freebsd || netbsd || openbsd

package stableio

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDuplicateFilePreservesCloseOnExec(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	duplicate, err := DuplicateFile(file, "duplicate")
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Close()
	flags, err := unix.FcntlInt(duplicate.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("duplicate not close-on-exec: flags%d %v", flags, err)
	}
}
