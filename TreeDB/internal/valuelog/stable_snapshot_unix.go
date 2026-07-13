//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package valuelog

import (
	"os"

	"golang.org/x/sys/unix"
)

func duplicateStableFile(f *os.File) (*os.File, error) {
	fd, err := unix.Dup(int(f.Fd()))
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fd)
	return os.NewFile(uintptr(fd), f.Name()+" (stable)"), nil
}
