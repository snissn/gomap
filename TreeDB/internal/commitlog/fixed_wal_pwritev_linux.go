//go:build linux

package commitlog

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

const fixedWALSupportsPwritev = true

func writeFixedAtParts(file *os.File, offset int64, header, frame, tail []byte) (uint64, error) {
	if file == nil {
		return 0, os.ErrInvalid
	}
	rawConn, err := file.SyscallConn()
	if err != nil {
		return 0, err
	}
	parts := [3][]byte{header, frame, tail}
	remaining := parts[:]
	var syscalls uint64
	var opErr error
	ctrlErr := rawConn.Control(func(fd uintptr) {
		for len(remaining) > 0 {
			for len(remaining) > 0 && len(remaining[0]) == 0 {
				remaining = remaining[1:]
			}
			if len(remaining) == 0 {
				return
			}
			syscalls++
			n, werr := unix.Pwritev(int(fd), remaining, offset)
			if werr != nil {
				if werr == unix.EINTR || werr == unix.EAGAIN {
					continue
				}
				opErr = werr
				return
			}
			if n <= 0 {
				opErr = io.ErrShortWrite
				return
			}
			offset += int64(n)
			written := n
			for written > 0 && len(remaining) > 0 {
				if written >= len(remaining[0]) {
					written -= len(remaining[0])
					remaining = remaining[1:]
					continue
				}
				remaining[0] = remaining[0][written:]
				written = 0
			}
		}
	})
	if ctrlErr != nil {
		return syscalls, ctrlErr
	}
	return syscalls, opErr
}

// hintFixedWALWriteback initiates asynchronous writeback on a freshly written
// fixed-WAL initialization range so NVMe DMA and ext4 extent conversion overlap
// with subsequent zero-fill writes before the mandatory fsync barrier.
func hintFixedWALWriteback(file *os.File, offset, length int64) {
	if file == nil || length <= 0 {
		return
	}
	rawConn, err := file.SyscallConn()
	if err != nil {
		return
	}
	_ = rawConn.Control(func(fd uintptr) {
		_ = unix.SyncFileRange(int(fd), offset, length, unix.SYNC_FILE_RANGE_WRITE)
	})
}
