//go:build darwin || linux || freebsd || netbsd || openbsd

package nativewire

import (
	"errors"
	"os"
	"syscall"
)

func liveLifecycleRetainedFileLinkCountV1(info os.FileInfo) (uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("retained snapshot link count unavailable")
	}
	return uint64(stat.Nlink), nil
}
