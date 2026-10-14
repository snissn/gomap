package pager

import (
	"errors"
	"os"
	"unsafe"
)

var ErrMappingAuthority = errors.New("pager: missing constructor mapping authority")

// mappingOwner is issued only by the physical mapping constructor. chunks is a
// read view, never a source of partial-unmap authority.
type mappingOwner struct {
	data           []byte
	extent, offset uintptr
	issued         bool
}

func mappingExtent(length int) (uintptr, error) {
	page := uintptr(os.Getpagesize())
	n := uintptr(length)
	if length <= 0 || n > ^uintptr(0)-(page-1) {
		return 0, ErrMappingAuthority
	}
	return (n + page - 1) / page * page, nil
}
func (m *mappingOwner) remaining() uintptr {
	if m == nil || m.offset > m.extent {
		return 0
	}
	return m.extent - m.offset
}
func (m *mappingOwner) matches(data []byte) bool {
	return m.issued && m.offset == 0 && len(data) == len(m.data) && len(data) > 0 && unsafe.SliceData(data) == unsafe.SliceData(m.data)
}
