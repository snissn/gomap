//go:build windows

package valuelog

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// openSegmentReadHandle allows another manager to quarantine a sealed segment
// while this read handle remains open. Identity leases still decide whether the
// rename is permitted; FILE_SHARE_DELETE only removes the Windows pathname
// restriction after that decision has been made.
func openSegmentReadHandle(path string) (*os.File, error) {
	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	handle, err := windows.CreateFile(
		pathp,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "open", Path: path, Err: errors.New("invalid Windows file handle")}
	}
	return file, nil
}
