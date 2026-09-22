//go:build linux

package pager

import "golang.org/x/sys/unix"

func madviseWillNeedChunk(data []byte) {
	if len(data) == 0 {
		return
	}
	_ = unix.Madvise(data, unix.MADV_WILLNEED)
}
