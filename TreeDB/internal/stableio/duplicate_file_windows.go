//go:build windows

package stableio

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/windows"
)

// DuplicateFile preserves the exact object's existing access mask. It does not
// perform the distinct ReOpenFile access upgrade used by retained sync pins.
func DuplicateFile(file *os.File, diagnosticName string) (*os.File, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	process, err := windows.GetCurrentProcess()
	if err != nil {
		return nil, err
	}
	var duplicate windows.Handle
	err = windows.DuplicateHandle(process, windows.Handle(file.Fd()), process, &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS)
	runtime.KeepAlive(file)
	if err != nil {
		return nil, err
	}
	owned := os.NewFile(uintptr(duplicate), diagnosticName)
	if owned == nil {
		return nil, errors.Join(os.ErrInvalid, windows.CloseHandle(duplicate))
	}
	return owned, nil
}
