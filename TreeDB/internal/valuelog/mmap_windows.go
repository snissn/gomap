//go:build windows

package valuelog

import (
	"errors"
	"os"
	"unsafe"
)

func mmapReadOnly(_ *os.File, _ int) ([]byte, error) {
	return nil, errors.New("mmap not supported on windows")
}

func munmap(_ []byte) error {
	return nil
}

func mapOwnedReadOnlyV6(_ *os.File, _ int) (ownedMmapV6, error) {
	return ownedMmapV6{}, errors.New("mmap not supported on windows")
}
func unmapOwnedRangeV6(_ *ownedMmapV6, _ uintptr) (uintptr, error) { return 0, errOwnedMappingV6 }

var unmapOwnedPhysicalV6 = func(_ unsafe.Pointer, _ uintptr) error { return errOwnedMappingV6 }
