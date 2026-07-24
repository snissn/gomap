//go:build linux

package rootpublication

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openStableAnonymousFile(parent *os.File, perm os.FileMode) (*os.File, error) {
	var fd int
	var err error
	for {
		fd, err = unix.Openat(int(parent.Fd()), ".", unix.O_RDWR|unix.O_CLOEXEC|unix.O_TMPFILE, uint32(perm.Perm()))
		if err != unix.EINTR {
			break
		}
	}
	if err != nil {
		switch err {
		case unix.ENOSYS, unix.EINVAL, unix.EOPNOTSUPP, unix.EPERM, unix.EISDIR, unix.ENOENT, unix.ENODEV:
			return nil, fmt.Errorf("%w: anonymous O_TMPFILE: %v", ErrNamespacePersistenceUnsupported, err)
		default:
			return nil, err
		}
	}
	file := os.NewFile(uintptr(fd), "stable-anonymous")
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: anonymous O_TMPFILE descriptor", ErrNamespacePersistenceUnsupported)
	}
	return file, nil
}
