//go:build !windows
// +build !windows

package pager

import "golang.org/x/sys/unix"

func mmapFile(fd uintptr, offset int64, length int, populate bool) ([]byte, error) {
	flags := unix.MAP_SHARED
	if populate {
		flags |= mmapPopulateFlag()
	}
	return unix.Mmap(int(fd), offset, length, unix.PROT_READ|unix.PROT_WRITE, flags)
}

func mmapFileReadOnly(fd uintptr, offset int64, length int, populate bool) ([]byte, error) {
	flags := unix.MAP_SHARED
	if populate {
		flags |= mmapPopulateFlag()
	}
	return unix.Mmap(int(fd), offset, length, unix.PROT_READ, flags)
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
