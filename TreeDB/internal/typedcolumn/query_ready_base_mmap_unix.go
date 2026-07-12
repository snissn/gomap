//go:build darwin || linux || freebsd || netbsd || openbsd

package typedcolumn

import (
	"os"
	"syscall"
)

func queryReadyBaseMmapSupported() bool { return true }

func mmapQueryReadyBaseFile(file *os.File) ([]byte, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() <= 0 || info.Size() > int64(^uint(0)>>1) {
		return nil, os.ErrInvalid
	}
	return syscall.Mmap(int(file.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmapQueryReadyBaseFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
