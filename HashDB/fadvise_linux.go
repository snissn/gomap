//go:build linux
// +build linux

package hashdb

import "golang.org/x/sys/unix"

func applyFadvise(fd int, size int64) {
	_ = unix.Fadvise(fd, 0, size, unix.FADV_RANDOM)
}

func applyMadvise(data []byte) {
	_ = unix.Madvise(data, unix.MADV_WILLNEED)
}
