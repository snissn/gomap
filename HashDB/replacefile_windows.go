//go:build windows

package hashdb

import (
	"os"

	"golang.org/x/sys/windows"
)

func replaceFileAtomic(dst, src string) error {
	// ReplaceFile requires the destination to exist. If it doesn't, fall back to Rename.
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		return os.Rename(src, dst)
	}

	dstp, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}
	srcp, err := windows.UTF16PtrFromString(src)
	if err != nil {
		return err
	}

	if err := windows.ReplaceFile(dstp, srcp, nil, 0, nil, nil); err != nil {
		return err
	}
	_ = os.Remove(src)
	return nil
}
