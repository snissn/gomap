//go:build windows

package mappedresource

import (
	"fmt"
	"os"
)

func mmapFile(file *os.File) ([]byte, error) {
	return nil, fmt.Errorf("%w on windows", ErrMmapUnsupported)
}

func munmapFile(data []byte) error {
	return nil
}
