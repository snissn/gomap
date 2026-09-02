//go:build linux

package pager

import (
	"os"

	"golang.org/x/sys/unix"
)

func preallocateFile(f *os.File, size int64) error {
	// Best-effort: on some filesystems fallocate may fail with EOPNOTSUPP.
	// If it's unsupported, fall back to Truncate only.
	if err := unix.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		if err == unix.EOPNOTSUPP || err == unix.ENOSYS || err == unix.EINVAL {
			return nil
		}
		return err
	}
	return nil
}
