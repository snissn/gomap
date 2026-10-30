//go:build darwin || linux || freebsd || netbsd || openbsd

package stableio

import (
	"os"

	"golang.org/x/sys/unix"
)

type duplicateCustodyUnixOps struct {
	dup   func(int) (int, error)
	close func(int) error
	wrap  func(uintptr, string) *os.File
}

func DuplicateFileWithCustody(file *os.File, diagnosticName string, custody *DuplicateCustody) (*os.File, error) {
	return duplicateFileWithCustodyUnix(file, diagnosticName, custody, duplicateCustodyUnixOps{
		dup:   func(fd int) (int, error) { return unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0) },
		close: unix.Close,
		wrap:  os.NewFile,
	})
}

func duplicateFileWithCustodyUnix(file *os.File, diagnosticName string, c *DuplicateCustody, ops duplicateCustodyUnixOps) (*os.File, error) {
	if !c.begin() {
		return nil, ErrDuplicateCustodyUsed
	}
	if file == nil {
		c.operationErr = os.ErrInvalid
		return nil, c
	}
	source, err := file.SyscallConn()
	if err != nil {
		c.operationErr = err
		return nil, c
	}
	fd := -1
	c.controlErr = source.Control(func(raw uintptr) {
		fd, c.operationErr = ops.dup(int(raw))
	})
	if c.controlErr == nil && c.operationErr == nil {
		owned := ops.wrap(uintptr(fd), diagnosticName)
		if owned != nil {
			return owned, nil
		}
		c.operationErr = os.ErrInvalid
	}
	if fd >= 0 {
		c.raw = uintptr(fd)
		c.cleanupErr = ops.close(fd)
		c.ambiguous = c.cleanupErr != nil
	}
	return nil, c
}
