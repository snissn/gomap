//go:build !windows

package pager

import (
	"golang.org/x/sys/unix"
	"os"
	"unsafe"
)

func nativePartialMappingSupported() bool { return true }
func mapConstructor(fd uintptr, offset int64, length int, populate, readOnly bool) (mappingOwner, error) {
	extent, e := mappingExtent(length)
	if e != nil {
		return mappingOwner{}, e
	}
	flags := unix.MAP_SHARED
	if populate {
		flags |= mmapPopulateFlag()
	}
	prot := unix.PROT_READ
	if !readOnly {
		prot |= unix.PROT_WRITE
	}
	address, e := unix.MmapPtr(int(fd), offset, nil, uintptr(length), prot, flags)
	if e != nil {
		return mappingOwner{}, e
	}
	return mappingOwner{data: unsafe.Slice((*byte)(address), length), extent: extent, issued: true}, nil
}

var unmapConstructorPhysical = unix.MunmapPtr

func unmapConstructor(m *mappingOwner, amount uintptr) error {
	page := uintptr(os.Getpagesize())
	if m == nil || !m.issued || len(m.data) == 0 || amount == 0 || m.offset%page != 0 || amount%page != 0 || amount > m.remaining() {
		return ErrMappingAuthority
	}
	if e := unmapConstructorPhysical(unsafe.Add(unsafe.Pointer(unsafe.SliceData(m.data)), m.offset), amount); e != nil {
		return e
	}
	m.offset += amount
	if m.remaining() == 0 {
		m.data = nil
	}
	return nil
}
