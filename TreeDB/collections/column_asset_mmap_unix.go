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
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size <= 0 || size > int64(maxCollectionInt) {
		return nil, os.ErrInvalid
	}
	return syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmapColumnPhysicalAssetFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
