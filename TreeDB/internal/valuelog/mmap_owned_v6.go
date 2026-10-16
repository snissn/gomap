package valuelog

import (
	"errors"
	"os"
	"unsafe"
)

// ownedMmapV6 is File's actual constructor-owned mapping, including the exact
// remaining OS extent. mmapData is only a read view. Bare/ad-hoc slices never
// acquire pointer-unmap authority from their address or length.
type ownedMmapV6 struct {
	data           []byte
	extent, offset uintptr
	constructor    bool
}

var errOwnedMappingV6 = errors.New("valuelog: missing constructor mapping authority")

func mappingExtentV6(length int) (uintptr, error) {
	if length <= 0 {
		return 0, errOwnedMappingV6
	}
	size := uintptr(os.Getpagesize())
	n := uintptr(length)
	if size == 0 || n > ^uintptr(0)-(size-1) {
		return 0, errOwnedMappingV6
	}
	return (n + size - 1) / size * size, nil
}
func (m *ownedMmapV6) remainingV6() uintptr {
	if m == nil || m.offset > m.extent {
		return 0
	}
	return m.extent - m.offset
}
func (m *ownedMmapV6) matchesViewV6(data []byte) bool {
	return m.constructor && m.offset == 0 && len(data) == len(m.data) && len(data) != 0 && unsafe.SliceData(data) == unsafe.SliceData(m.data)
}
func (f *File) detachMmapOwnerV6Locked(data []byte) ownedMmapV6 {
	owner := f.currentMappingV6
	f.currentMappingV6 = ownedMmapV6{}
	if owner.constructor {
		return owner
	}
	return ownedMmapV6{data: data} // only legacy whole-slice Munmap may consume it
}
func (m *ownedMmapV6) closeV6() error {
	if !m.constructor {
		e := munmap(m.data)
		if e == nil {
			m.data = nil
		}
		return e
	}
	if m.remainingV6() == 0 {
		return nil
	}
	_, e := unmapOwnedRangeV6(m, m.remainingV6())
	return e
}
