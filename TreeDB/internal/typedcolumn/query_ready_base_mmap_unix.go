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

func mmapQueryReadyBaseFileRange(file *os.File, offset int64, length int) (view, mapping []byte, err error) {
	if file == nil || offset < 0 || length <= 0 {
		return nil, nil, os.ErrInvalid
	}
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if offset > info.Size() || int64(length) > info.Size()-offset {
		return nil, nil, os.ErrInvalid
	}
	page := int64(os.Getpagesize())
	aligned := offset - offset%page
	delta64 := offset - aligned
	if delta64 > int64(^uint(0)>>1) || int64(length) > int64(^uint(0)>>1)-delta64 {
		return nil, nil, os.ErrInvalid
	}
	mappingLength := int(delta64) + length
	mapping, err = syscall.Mmap(int(file.Fd()), aligned, mappingLength, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	delta := int(delta64)
	return mapping[delta : delta+length], mapping, nil
}

func munmapQueryReadyBaseFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
