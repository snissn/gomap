//go:build darwin || linux || freebsd || netbsd || openbsd

package stableio

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// DuplicateFile owns an independent descriptor for the exact open object.
// diagnosticName is provenance only; no path is opened or resolved.
func DuplicateFile(file *os.File, diagnosticName string) (*os.File, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	fd, err := unix.Dup(int(file.Fd()))
	runtime.KeepAlive(file)
	if err != nil {
		return nil, err
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return nil, errors.Join(err, unix.Close(fd))
	}
	duplicate := os.NewFile(uintptr(fd), diagnosticName)
	if duplicate == nil {
		return nil, errors.Join(os.ErrInvalid, unix.Close(fd))
	}
	return duplicate, nil
}
