//go:build linux

package commitlog

import (
	"golang.org/x/sys/unix"
	"os"
)

// syncWALData is used only for initialized fixed-capacity command WAL files.
// Metadata and namespace initialization use the full stable-file primitive.
func syncWALData(file *os.File) error {
	if file == nil {
		return os.ErrInvalid
	}
	for {
		err := unix.Fdatasync(int(file.Fd()))
		if err != unix.EINTR {
			return err
		}
	}
}
