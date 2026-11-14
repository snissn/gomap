//go:build linux

package stableio

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func writebackRange(file *os.File, bytes int64) error {
	err := unix.SyncFileRange(int(file.Fd()), 0, bytes, unix.SYNC_FILE_RANGE_WRITE)
	runtime.KeepAlive(file)
	return err
}
