//go:build windows

package hashdb

import (
	"golang.org/x/sys/windows"
)

func replaceFileAtomic(dst, src string) error {
	dstp, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}
	srcp, err := windows.UTF16PtrFromString(src)
	if err != nil {
		return err
	}

	return windows.MoveFileEx(srcp, dstp, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}
