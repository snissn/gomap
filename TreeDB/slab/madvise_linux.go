//go:build linux

package slab

import "golang.org/x/sys/unix"

func adviseHugepage(b []byte) {
	if len(b) == 0 {
		return
	}
	_ = unix.Madvise(b, unix.MADV_HUGEPAGE)
}
