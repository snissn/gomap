//go:build windows

package stableio

import (
	"os"

	"golang.org/x/sys/windows"
)

type duplicateCustodyWindowsOps struct {
	duplicate func(windows.Handle, windows.Handle, *windows.Handle) error
	close     func(windows.Handle) error
	wrap      func(uintptr, string) *os.File
}

// DuplicateFileWithCustody duplicates the exact open object and records any
// ambiguous raw cleanup in the caller-owned single-use custody record.
func DuplicateFileWithCustody(file *os.File, diagnosticName string, c *DuplicateCustody) (*os.File, error) {
	return duplicateFileWithCustodyWindows(file, diagnosticName, c, duplicateCustodyWindowsOps{
		duplicate: func(process, source windows.Handle, dest *windows.Handle) error {
			return windows.DuplicateHandle(process, source, process, dest, 0, false, windows.DUPLICATE_SAME_ACCESS)
		},
		close: windows.CloseHandle,
		wrap:  os.NewFile,
	})
}

func duplicateFileWithCustodyWindows(file *os.File, diagnosticName string, c *DuplicateCustody, ops duplicateCustodyWindowsOps) (*os.File, error) {
	if !c.begin() {
		return nil, ErrDuplicateCustodyUsed
	}
	if file == nil {
		c.operationErr = os.ErrInvalid
		return nil, c
	}
	source, err := file.SyscallConn()
	if err != nil {
		c.operationErr = err
		return nil, c
	}
	process, err := windows.GetCurrentProcess()
	if err != nil {
		c.operationErr = err
		return nil, c
	}
	var handle windows.Handle
	c.controlErr = source.Control(func(raw uintptr) {
		c.operationErr = ops.duplicate(process, windows.Handle(raw), &handle)
	})
	if c.controlErr == nil && c.operationErr == nil {
		owned := ops.wrap(uintptr(handle), diagnosticName)
		if owned != nil {
			return owned, nil
		}
		c.operationErr = os.ErrInvalid
	}
	if handle != 0 && handle != windows.InvalidHandle {
		c.raw = uintptr(handle)
		c.cleanupErr = ops.close(handle)
		c.ambiguous = c.cleanupErr != nil
	}
	return nil, c
}
