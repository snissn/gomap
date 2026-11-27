//go:build linux

package db

import (
	"os"

	"golang.org/x/sys/unix"
)

// hintStableFileWriteback triggers writeback and waits for dirty pages to
// finish delayed-allocation extent conversion on Linux (without issuing a disk
// flush barrier) so a subsequent fdatasync on this file coalesces into the same
// ext4 JBD2 commit as a concurrent directory fsync.
func hintStableFileWriteback(file *os.File) {
	if file == nil {
		return
	}
	rawConn, err := file.SyscallConn()
	if err != nil {
		return
	}
	_ = rawConn.Control(func(fd uintptr) {
		_ = unix.SyncFileRange(int(fd), 0, 0, unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
	})
}
