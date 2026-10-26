//go:build darwin || linux || freebsd || netbsd || openbsd

package stableio

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// DuplicateFile owns an independent descriptor for the exact open object.
// diagnosticName is provenance only; no path is opened or resolved.
func DuplicateFile(file *os.File, diagnosticName string) (*os.File, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, err
	}
	fd := -1
	var duplicateErr error
	// Control holds the source descriptor live through duplication, including
	// against a concurrent Close and descriptor-number reuse.
	controlErr := raw.Control(func(source uintptr) {
		fd, duplicateErr = unix.Dup(int(source))
		if duplicateErr == nil {
			_, duplicateErr = unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC)
		}
	})
	if controlErr != nil || duplicateErr != nil {
		err := errors.Join(controlErr, duplicateErr)
		if fd >= 0 {
			err = errors.Join(err, unix.Close(fd))
		}
		return nil, err
	}
	duplicate := os.NewFile(uintptr(fd), diagnosticName)
	if duplicate == nil {
		return nil, errors.Join(os.ErrInvalid, unix.Close(fd))
	}
	return duplicate, nil
}
