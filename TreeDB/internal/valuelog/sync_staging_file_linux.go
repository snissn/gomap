//go:build linux

package valuelog

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func syncStagingFileData(file *os.File) error {
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
