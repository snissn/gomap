//go:build !(darwin || linux || freebsd || netbsd || openbsd)

package collections

import (
	"errors"
	"os"
)

func mmapColumnPhysicalAssetFile(_ *os.File) ([]byte, error) {
	return nil, errors.New("collections: column asset mmap is not supported on this platform")
}

func munmapColumnPhysicalAssetFile(_ []byte) error {
	return nil
}
