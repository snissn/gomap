//go:build !(darwin || linux || freebsd || netbsd || openbsd)

package collections

import (
	"errors"
	"os"
)

var errColumnPhysicalAssetMmapUnsupported = errors.New("collections: column asset mmap is not supported on this platform")

func mmapColumnPhysicalAssetFile(_ *os.File) ([]byte, error) {
	return nil, errColumnPhysicalAssetMmapUnsupported
}

func mmapColumnPhysicalAssetFilePrefix(_ *os.File, _ int64) ([]byte, error) {
	return nil, errColumnPhysicalAssetMmapUnsupported
}

func munmapColumnPhysicalAssetFile(_ []byte) error {
	return nil
}
