//go:build darwin

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
	fd := file.Fd()
	for {
		_, _, errno := unix.Syscall(unix.SYS_FDATASYNC, fd, 0, 0)
		runtime.KeepAlive(file)
		if errno == 0 {
			return nil
		}
		if errno == unix.EINTR {
			continue
		}
		if errno == unix.ENOSYS || errno == unix.EINVAL {
			return file.Sync()
		}
		return errno
	}
}
