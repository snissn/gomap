//go:build darwin

package stableio

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// SyncFile uses F_FULLFSYNC because Darwin fsync alone does not require the
// storage device to flush volatile write caches.
func SyncFile(file *os.File) error {
	if file == nil {
		return os.ErrInvalid
	}
	for {
		_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
		if err == nil {
			return nil
		}
		if err == unix.EINTR {
			continue
		}
		if err == unix.EINVAL || err == unix.ENOTSUP || err == unix.EPERM {
			return fmt.Errorf("%w: F_FULLFSYNC: %v", ErrFilePersistenceUnsupported, err)
		}
		return err
	}
}
