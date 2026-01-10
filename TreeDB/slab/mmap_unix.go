//go:build !windows

package slab

import (
	"os"

	"golang.org/x/sys/unix"
)

func mmapReadOnly(f *os.File, length int) ([]byte, error) {
	b, err := unix.Mmap(int(f.Fd()), 0, length, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	adviseHugepage(b)
	return b, nil
}

func munmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Munmap(b)
}
