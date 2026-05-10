//go:build windows

package db

import (
	"errors"
	"syscall"
)

func compactStorageIsBusyRemoveError(err error) bool {
	return errors.Is(err, syscall.ERROR_SHARING_VIOLATION) || errors.Is(err, syscall.ERROR_ACCESS_DENIED)
}
