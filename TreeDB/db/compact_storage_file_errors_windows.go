//go:build windows

package db

import (
	"errors"
	"syscall"
)

const (
	windowsErrorAccessDenied     syscall.Errno = 5
	windowsErrorSharingViolation syscall.Errno = 32
)

func compactStorageIsBusyRemoveError(err error) bool {
	return errors.Is(err, windowsErrorSharingViolation) || errors.Is(err, windowsErrorAccessDenied)
}
