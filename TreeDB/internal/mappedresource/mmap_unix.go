//go:build darwin || linux || freebsd || netbsd || openbsd

package mappedresource

import (
	"fmt"
	"os"
	"syscall"
)

func mmapFile(file *os.File) ([]byte, error) {
	if file == nil {
		return nil, fmt.Errorf("mappedresource: nil file")
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("mappedresource: cannot mmap empty file")
	}
	fd := int(file.Fd())
	return syscall.Mmap(fd, 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
