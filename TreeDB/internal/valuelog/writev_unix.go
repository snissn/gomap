//go:build unix

package valuelog

import (
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

const writevSupported = true

// Conservative iovec limit used for batching in userspace.
//
// Many unix platforms have IOV_MAX/UIO_MAXIOV of 1024. We cap below this and
// flush earlier to reduce the risk of hitting a smaller platform limit.
const writevMaxIovs = 1024

type writevIovec = unix.Iovec

// writevAll writes all iovecs to fd, retrying on partial writes.
//
// Callers must treat iovs as immutable for the duration of this call. The
// generated unix.Iovec entries point at the backing arrays in iovs until
// writevAll returns.
func writevAll(fd int, iovs [][]byte, scratch []writevIovec) ([]writevIovec, writevCallStats, error) {
	var stats writevCallStats
	if len(iovs) == 0 {
		return scratch[:0], stats, nil
	}
	if cap(scratch) < len(iovs) {
		scratch = make([]writevIovec, len(iovs))
	}
	vecs := scratch[:len(iovs)]
	for i, b := range iovs {
		if len(b) > 0 {
			vecs[i].Base = &b[0]
			vecs[i].SetLen(len(b))
			continue
		}
		vecs[i].Base = nil
		vecs[i].SetLen(0)
	}
	for len(vecs) > 0 {
		stats.syscalls++
		stats.iovecs += uint64(len(vecs))
		n, err := rawWritev(fd, vecs)
		if err != nil {
			if err == unix.EINTR || err == unix.EAGAIN {
				continue
			}
			return scratch, stats, err
		}
		if n <= 0 {
			return scratch, stats, errors.New("valuelog: short writev")
		}
		stats.bytes += uint64(n)
		written := n
		for written > 0 && len(vecs) > 0 {
			iovLen := int(vecs[0].Len)
			if iovLen <= 0 {
				vecs = vecs[1:]
				continue
			}
			if written >= iovLen {
				written -= iovLen
				vecs = vecs[1:]
				continue
			}
			vecs[0].Base = (*byte)(unsafe.Add(unsafe.Pointer(vecs[0].Base), written))
			vecs[0].SetLen(iovLen - written)
			written = 0
		}
	}
	return scratch, stats, nil
}

func rawWritev(fd int, iovs []writevIovec) (int, error) {
	if len(iovs) == 0 {
		return 0, nil
	}
	n, _, errno := unix.Syscall(unix.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iovs[0])), uintptr(len(iovs)))
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}
