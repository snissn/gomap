//go:build !windows

package valuelog

import (
	"os"

	"golang.org/x/sys/unix"
)

func mmapReadOnly(f *os.File, length int) ([]byte, error) {
	return unix.Mmap(int(f.Fd()), 0, length, unix.PROT_READ, unix.MAP_SHARED)
}

func munmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Munmap(b)
}
