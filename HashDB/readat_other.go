//go:build !linux

package hashdb

import "os"

func readAt(f *os.File, b []byte, off int64) (int, error) {
	return f.ReadAt(b, off)
}
