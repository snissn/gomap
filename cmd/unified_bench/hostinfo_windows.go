//go:build windows

package main

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

func hostCPUModel() string { return "" }

func hostMachineModel() string { return "" }

func hostMemBytes() uint64 {
	type memoryStatusEx struct {
		Length               uint32
		MemoryLoad           uint32
		TotalPhys            uint64
		AvailPhys            uint64
		TotalPageFile        uint64
		AvailPageFile        uint64
		TotalVirtual         uint64
		AvailVirtual         uint64
		AvailExtendedVirtual uint64
	}
	var mem memoryStatusEx
	mem.Length = uint32(unsafe.Sizeof(mem))

	kernel32 := windows.NewLazySystemDLL("kernel32.dll")
	proc := kernel32.NewProc("GlobalMemoryStatusEx")
	r1, _, _ := proc.Call(uintptr(unsafe.Pointer(&mem)))
	if r1 == 0 {
		return 0
	}
	return mem.TotalPhys
}
