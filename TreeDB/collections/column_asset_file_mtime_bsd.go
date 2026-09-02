//go:build darwin || freebsd || netbsd

package collections

import (
	"syscall"
	"time"
)

func columnAssetStatModTimeUnixNano(stat *syscall.Stat_t) int64 {
	if stat == nil {
		return 0
	}
	return int64(stat.Mtimespec.Sec)*int64(time.Second) + int64(stat.Mtimespec.Nsec)
}
