//go:build !windows

package mvcc

import (
	"io/fs"
	"syscall"
)

func managedHistoryAllocatedBytes(info fs.FileInfo) (int64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return st.Blocks * 512, true
}
