//go:build linux

package collections

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func syncColumnAssetSegmentFile(file *os.File) error {
	if file == nil {
		return nil
	}
	fd := int(file.Fd())
	for {
		err := unix.Fdatasync(fd)
		runtime.KeepAlive(file)
		if err == unix.EINTR {
			continue
		}
		if err == unix.ENOSYS || err == unix.EINVAL {
			return file.Sync()
		}
		return err
	}
}
