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

func columnAssetVerifiedChecksumFileIdentityFromFile(file *os.File) columnAssetVerifiedChecksumFileIdentity {
	if file == nil {
		return columnAssetVerifiedChecksumFileIdentity{}
	}
	fd := int(file.Fd())
	var stat syscall.Stat_t
	for {
		err := syscall.Fstat(fd, &stat)
		if err == nil {
			return columnAssetVerifiedChecksumFileIdentity{
				dev:             uint64(stat.Dev),
				ino:             uint64(stat.Ino),
				size:            stat.Size,
				modTimeUnixNano: columnAssetStatModTimeUnixNano(&stat),
				valid:           true,
			}
		}
		if err != syscall.EINTR {
			return columnAssetVerifiedChecksumFileIdentity{}
		}
	}
}

func munmapColumnPhysicalAssetFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
