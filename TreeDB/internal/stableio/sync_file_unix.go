//go:build linux || freebsd || netbsd || openbsd

package stableio

import "os"

func SyncFile(file *os.File) error {
	if file == nil {
		return os.ErrInvalid
	}
	return file.Sync()
}
