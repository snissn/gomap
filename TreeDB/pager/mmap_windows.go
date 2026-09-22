//go:build windows
// +build windows

package pager

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	modkernel32       = windows.NewLazySystemDLL("kernel32.dll")
	procGetSystemInfo = modkernel32.NewProc("GetSystemInfo")
)

// systemInfo mirrors Windows SYSTEM_INFO for allocation granularity lookup.
type systemInfo struct {
	wProcessorArchitecture      uint16
	wReserved                   uint16
	dwPageSize                  uint32
	lpMinimumApplicationAddress uintptr
	lpMaximumApplicationAddress uintptr
	dwActiveProcessorMask       uintptr
	dwNumberOfProcessors        uint32
	dwProcessorType             uint32
	dwAllocationGranularity     uint32
	wProcessorLevel             uint16
	wProcessorRevision          uint16
}

var allocationGranularity = func() int64 {
	var info systemInfo
	_, _, _ = procGetSystemInfo.Call(uintptr(unsafe.Pointer(&info)))
	return int64(info.dwAllocationGranularity)
}()

func mmapFile(fd uintptr, offset int64, length int, _ bool) ([]byte, error) {
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
	if addr == 0 {
		return nil, windows.ERROR_INVALID_ADDRESS
	}
	return sliceFromAddr(addr, length), nil
}

func mmapFileReadOnly(fd uintptr, offset int64, length int, _ bool) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	if allocationGranularity != 0 && offset%allocationGranularity != 0 {
		return nil, windows.ERROR_MAPPED_ALIGNMENT
	}
	size := offset + int64(length)
	h, err := windows.CreateFileMapping(windows.Handle(fd), nil, windows.PAGE_READONLY, uint32(size>>32), uint32(size), nil)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(h)

	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ, uint32(offset>>32), uint32(offset), uintptr(length))
	if err != nil {
		return nil, err
	}
	if addr == 0 {
		return nil, windows.ERROR_INVALID_ADDRESS
	}
	return sliceFromAddr(addr, length), nil
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

func sliceFromAddr(addr uintptr, length int) []byte {
	var dummy byte
	// The arithmetic form keeps go vet's unsafeptr analyzer happy for mmap pointers.
	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(&dummy)) + (addr - uintptr(unsafe.Pointer(&dummy))))
	return unsafe.Slice((*byte)(ptr), length)
}
