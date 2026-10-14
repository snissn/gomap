//go:build linux

package pager

import (
	"github.com/snissn/gomap/TreeDB/internal/nativeprunemetrics"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func syncPageFileData(file *os.File) error {
	fd := int(file.Fd())
	for {
		err := unix.Fdatasync(fd)
		nativeprunemetrics.Barrier(nativeprunemetrics.File, err)
		runtime.KeepAlive(file)
		if err == unix.EINTR {
			continue
		}
		if err == unix.ENOSYS || err == unix.EINVAL {
			return nativeprunemetrics.SyncFile(file, nativeprunemetrics.File)
		}
		return err
	}
}
