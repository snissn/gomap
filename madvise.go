package gomap

// #cgo CFLAGS: -std=c99
// #include <sys/mman.h>
import "C"

import (
	"unsafe"
	_ "unsafe" // required for go:linkname

	"golang.org/x/sys/unix"
)

//go:linkname madvise runtime.madvise
func madvise(addr unsafe.Pointer, n uintptr, behav int32) int32

func (h *Hashmap) adviseMemDoneDuringResize(index uint64) {
	madvise(unsafe.Pointer(h.oldKeys), uintptr(index), unix.MADV_DONTNEED)
}
