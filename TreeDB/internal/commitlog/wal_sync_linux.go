//go:build linux

package commitlog

import (
	"golang.org/x/sys/unix"
	"os"
)

// syncWALData is used only for initialized fixed-capacity command WAL files.
// Metadata and namespace initialization use the full stable-file primitive.
func syncWALData(file *os.File) error {
	if file == nil {
		return os.ErrInvalid
	}
	for {
		err := unix.Fdatasync(int(file.Fd()))
		if err != unix.EINTR {
			return err
		}
	}
}

// hintWALInitWriteback starts best-effort asynchronous kernel writeback for a
// chunk of an unpublished fixed-WAL reserve so multi-MiB zero-fill pipelines
// with NVMe writeback instead of dumping the entire file into fsync at once.
// Durability remains solely guaranteed by the subsequent full SyncFile.
func hintWALInitWriteback(file *os.File, offset, length int64) {
	if file == nil || length <= 0 {
		return
	}
	_ = unix.SyncFileRange(int(file.Fd()), offset, length, unix.SYNC_FILE_RANGE_WRITE)
}
