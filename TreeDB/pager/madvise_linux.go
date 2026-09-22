//go:build linux

package pager

import "golang.org/x/sys/unix"

func madviseChunk(data []byte) {
	if len(data) == 0 {
		return
	}
	// Best-effort hint: try to back file mappings with transparent huge pages.
	// This can reduce minor-fault overhead and dTLB misses when touching many pages.
	_ = unix.Madvise(data, unix.MADV_HUGEPAGE)
}
