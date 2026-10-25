//go:build linux

package commitlog

import (
	"golang.org/x/sys/unix"
	"os"
)

func walFilesystemType(file *os.File) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &stat); err != nil {
		return 0, err
	}
	// Linux filesystem magic values are unsigned 32-bit identifiers, including
	// RAMFS_MAGIC whose high bit is set on 32-bit platforms.
	return uint64(uint32(stat.Type)), nil
}
