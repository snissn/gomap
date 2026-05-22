//go:build linux || openbsd

package collections

import (
	"syscall"
	"time"
)

func columnAssetStatModTimeUnixNano(stat *syscall.Stat_t) int64 {
	if stat == nil {
		return 0
	}
	return int64(stat.Mtim.Sec)*int64(time.Second) + int64(stat.Mtim.Nsec)
}
