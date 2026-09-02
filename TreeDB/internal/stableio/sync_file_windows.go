//go:build windows

package stableio

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func SyncFile(file *os.File) error {
	if file == nil {
		return os.ErrInvalid
	}
	err := windows.FlushFileBuffers(windows.Handle(file.Fd()))
	if err == nil {
		return nil
	}
	if err == windows.ERROR_INVALID_HANDLE || err == windows.ERROR_ACCESS_DENIED || err == windows.ERROR_NOT_SUPPORTED {
		return fmt.Errorf("%w: FlushFileBuffers(file): %v", ErrFilePersistenceUnsupported, err)
	}
	return err
}
