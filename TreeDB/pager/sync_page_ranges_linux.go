//go:build linux

package pager

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func syncPageRangesData(file *os.File, chunks [][]byte, ranges []syncPageRange, chunkSize int64) (bool, error) {
	if file == nil {
		return true, nil
	}
	if !useDurableRangeWrites(ranges) {
		return false, nil
	}
	fd := int(file.Fd())
	for _, r := range ranges {
		buf := chunks[r.chunk][r.start:r.end]
		offset := int64(r.chunk)*chunkSize + int64(r.start)
		for len(buf) > 0 {
			n, err := unix.Pwritev2(fd, [][]byte{buf}, offset, unix.RWF_DSYNC)
			runtime.KeepAlive(file)
			runtime.KeepAlive(chunks)
			if err == unix.EINTR {
				continue
			}
			if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
				return false, nil
			}
			if err != nil {
				return true, err
			}
			if n <= 0 {
				return true, unix.EIO
			}
			buf = buf[n:]
			offset += int64(n)
		}
	}
	return true, nil
}
