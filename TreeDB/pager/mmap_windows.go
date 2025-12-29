//go:build windows
// +build windows

package pager

import (
	"reflect"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var allocationGranularity = func() int64 {
	var info syscall.SystemInfo
	syscall.GetSystemInfo(&info)
	return int64(info.AllocationGranularity)
}()

func mmapFile(fd uintptr, offset int64, length int) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	if allocationGranularity != 0 && offset%allocationGranularity != 0 {
		return nil, windows.ERROR_MAPPED_ALIGNMENT
	}
	size := offset + int64(length)
	h, err := windows.CreateFileMapping(windows.Handle(fd), nil, windows.PAGE_READWRITE, uint32(size>>32), uint32(size), nil)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(h)

	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_WRITE, uint32(offset>>32), uint32(offset), uintptr(length))
	if err != nil {
		return nil, err
	}
	hdr := &reflect.SliceHeader{Data: addr, Len: length, Cap: length}
	return *(*[]byte)(unsafe.Pointer(hdr)), nil
}

func mmapAvailable() error {
	return nil
}

func munmapFile(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&b[0])))
}

func msyncFile(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return windows.FlushViewOfFile(uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
}
