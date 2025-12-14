//go:build windows

package main

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

func hostCPUModel() string { return "" }

func hostMachineModel() string { return "" }

func hostMemBytes() uint64 {
	var mem windows.MemStatusEx
	mem.Length = uint32(unsafe.Sizeof(mem))
	if err := windows.GlobalMemoryStatusEx(&mem); err != nil {
		return 0
	}
	return mem.TotalPhys
}
