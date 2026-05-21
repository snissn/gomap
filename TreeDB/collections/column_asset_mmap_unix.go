//go:build darwin || linux || freebsd || netbsd || openbsd

package collections

import (
	"os"
	"syscall"
)

func mmapColumnPhysicalAssetFile(file *os.File) ([]byte, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	fd := int(file.Fd())
	var stat syscall.Stat_t
	for {
		err := syscall.Fstat(fd, &stat)
		if err == nil {
			break
		}
		if err != syscall.EINTR {
			return nil, err
		}
	}
	size := stat.Size
	if size <= 0 || size > int64(maxCollectionInt) {
		return nil, os.ErrInvalid
	}
	return syscall.Mmap(fd, 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmapColumnPhysicalAssetFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
