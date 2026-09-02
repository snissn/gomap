//go:build windows

package hashdb

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

func lockBytes(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return windows.VirtualLock(uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
}

func unlockBytes(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return windows.VirtualUnlock(uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
}

func adviseWillNeed(b []byte) error {
	return nil
}

func adviseRandom(b []byte) error {
	return nil
}
