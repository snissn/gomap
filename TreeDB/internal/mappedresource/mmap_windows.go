//go:build windows

package mappedresource

import (
	"fmt"
	"os"
)

func mmapFileRange(file *os.File, offset, length int64) ([]byte, []byte, error) {
	return nil, nil, fmt.Errorf("%w on windows", ErrMmapUnsupported)
}

func munmapFile(data []byte) error {
	return nil
}
