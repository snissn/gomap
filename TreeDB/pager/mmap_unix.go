//go:build !windows
// +build !windows

package pager

import "golang.org/x/sys/unix"

func mmapFile(fd uintptr, offset int64, length int) ([]byte, error) {
	return unix.Mmap(int(fd), offset, length, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
}

func mmapFileReadOnly(fd uintptr, offset int64, length int) ([]byte, error) {
	return unix.Mmap(int(fd), offset, length, unix.PROT_READ, unix.MAP_SHARED)
}

func mmapAvailable() error {
	return nil
}

func munmapFile(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Munmap(b)
}

func msyncFile(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Msync(b, unix.MS_SYNC)
}
