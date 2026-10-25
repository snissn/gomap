//go:build !linux

package commitlog

import (
	"errors"
	"os"
)

func walFilesystemType(file *os.File) (uint64, error) {
	return 0, errors.New("commitlog: filesystem classification unavailable")
}
