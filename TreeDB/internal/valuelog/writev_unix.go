//go:build unix

package valuelog

import (
	"errors"

	"golang.org/x/sys/unix"
)

const writevSupported = true

// Conservative iovec limit used for batching in userspace.
//
// Many unix platforms have IOV_MAX/UIO_MAXIOV of 1024. We cap below this and
// flush earlier to reduce the risk of hitting a smaller platform limit.
const writevMaxIovs = 1024

func writevAll(fd int, iovs [][]byte) error {
	for len(iovs) > 0 {
		n, err := unix.Writev(fd, iovs)
		if err != nil {
			if err == unix.EINTR || err == unix.EAGAIN {
				continue
			}
			return err
		}
		if n <= 0 {
			return errors.New("valuelog: short writev")
		}
		written := n
		for written > 0 && len(iovs) > 0 {
			if written >= len(iovs[0]) {
				written -= len(iovs[0])
				iovs = iovs[1:]
				continue
			}
			iovs[0] = iovs[0][written:]
			written = 0
		}
	}
	return nil
}
