//go:build !(darwin || linux || freebsd || netbsd || openbsd || windows)

package mappedresource

import (
	"fmt"
	"os"
)

func mmapFile(file *os.File) ([]byte, error) {
	return nil, fmt.Errorf("mappedresource: mmap unsupported on this platform")
}

func munmapFile(data []byte) error {
	return nil
}
