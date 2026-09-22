//go:build darwin || linux || freebsd || netbsd || openbsd

package mappedresource

import (
	"fmt"
	"os"
	"syscall"
)

func mmapFileRange(file *os.File, offset, length int64) ([]byte, []byte, error) {
	if file == nil {
		return nil, nil, fmt.Errorf("mappedresource: nil file")
	}
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	end := offset + length
	if offset < 0 || length < 0 || end < offset || end > info.Size() {
		return nil, nil, fmt.Errorf("mappedresource: range offset=%d length=%d outside file bytes=%d", offset, length, info.Size())
	}
	if length == 0 && info.Size() == 0 {
		return nil, nil, fmt.Errorf("mappedresource: cannot mmap empty file")
	}
	pageSize := int64(os.Getpagesize())
	alignedOffset := offset - offset%pageSize
	delta := offset - alignedOffset
	mappedLength := delta + length
	if length == 0 {
		if offset < info.Size() {
			mappedLength++
		} else if mappedLength == 0 {
			alignedOffset = offset - 1 - (offset-1)%pageSize
			delta = offset - alignedOffset
			mappedLength = delta
		}
	}
	if mappedLength < length || mappedLength > int64(^uint(0)>>1) {
		return nil, nil, fmt.Errorf("mappedresource: mapped range too large bytes=%d", mappedLength)
	}
	fd := int(file.Fd())
	mapped, err := syscall.Mmap(fd, alignedOffset, int(mappedLength), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return mapped, mapped[delta : delta+length], nil
}

func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
