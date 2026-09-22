//go:build linux

package hashdb

import (
	"os"

	"golang.org/x/sys/unix"
)

func readAt(f *os.File, b []byte, off int64) (int, error) {
	for {
		n, err := unix.Pread(int(f.Fd()), b, off)
		if err == unix.EINTR {
			continue
		}
		return n, err
	}
}
