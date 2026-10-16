//go:build !windows

package valuelog

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

func mmapReadOnly(f *os.File, length int) ([]byte, error) {
	return unix.Mmap(int(f.Fd()), 0, length, unix.PROT_READ, unix.MAP_SHARED)
}

func munmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return unix.Munmap(b)
}

// Only this constructor issues pointer-unmap authority. Existing legacy helpers
// retain x/sys's exact whole-slice registry, including invalid-slice rejection.
func mapOwnedReadOnlyV6(f *os.File, length int) (ownedMmapV6, error) {
	extent, e := mappingExtentV6(length)
	if e != nil {
		return ownedMmapV6{}, e
	}
	address, e := unix.MmapPtr(int(f.Fd()), 0, nil, uintptr(length), unix.PROT_READ, unix.MAP_SHARED)
	if e != nil {
		return ownedMmapV6{}, e
	}
	return ownedMmapV6{data: unsafe.Slice((*byte)(address), length), extent: extent, constructor: true}, nil
}
func unmapOwnedRangeV6(m *ownedMmapV6, length uintptr) (uintptr, error) {
	page := uintptr(os.Getpagesize())
	if m == nil || !m.constructor || len(m.data) == 0 || length == 0 || m.offset%page != 0 || length%page != 0 || length > m.remainingV6() {
		return 0, errOwnedMappingV6
	}
	address := unsafe.Add(unsafe.Pointer(unsafe.SliceData(m.data)), m.offset)
	if e := unmapOwnedPhysicalV6(address, length); e != nil {
		return 0, e
	}
	m.offset += length // Persist success before every possible yield/failure.
	if m.offset == m.extent {
		m.data = nil
	}
	return length, nil
}

// One actual physical operation; the owning descriptor supplies all authority.
var unmapOwnedPhysicalV6 = unix.MunmapPtr
