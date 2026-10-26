//go:build windows

package stableio

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// DuplicateFile preserves the exact object's existing access mask. It does not
// perform the distinct ReOpenFile access upgrade used by retained sync pins.
func DuplicateFile(file *os.File, diagnosticName string) (*os.File, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, err
	}
	process, err := windows.GetCurrentProcess()
	if err != nil {
		return nil, err
	}
	var duplicate windows.Handle
	var duplicateErr error
	// A closed File.Fd is InvalidHandle, which DuplicateHandle interprets as
	// the current-process pseudo-handle. Control rejects closed files and pins
	// the source handle against concurrent Close/reuse during the syscall.
	controlErr := raw.Control(func(source uintptr) {
		duplicateErr = windows.DuplicateHandle(process, windows.Handle(source), process, &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS)
	})
	if controlErr != nil || duplicateErr != nil {
		err := errors.Join(controlErr, duplicateErr)
		if duplicate != 0 && duplicate != windows.InvalidHandle {
			err = errors.Join(err, windows.CloseHandle(duplicate))
		}
		return nil, err
	}
	owned := os.NewFile(uintptr(duplicate), diagnosticName)
	if owned == nil {
		return nil, errors.Join(os.ErrInvalid, windows.CloseHandle(duplicate))
	}
	return owned, nil
}
