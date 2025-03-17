//go:build linux
// +build linux

package gomap

import "golang.org/x/sys/unix"

func applyFadvise(fd int, size int64) {
	_ = unix.Fadvise(fd, 0, size, unix.FADV_RANDOM|unix.FADV_DONTNEED)
}

func applyMadvise(data []byte) {
	_ = unix.Madvise(data, unix.MADV_WILLNEED)
}
