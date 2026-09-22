//go:build windows

package valuelog

import (
	"errors"
	"os"
)

func mmapReadOnly(_ *os.File, _ int) ([]byte, error) {
	return nil, errors.New("mmap not supported on windows")
}

func munmap(_ []byte) error {
	return nil
}
