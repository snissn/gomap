package atomicfile

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

// Write atomically replaces path with data using a temp file in the same
// directory. On Windows it retries rename on transient sharing/access errors.
func Write(path string, data []byte, perm os.FileMode) error {
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	f, err := os.CreateTemp(dir, base+".tmp.*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if err := f.Chmod(perm); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	const attempts = 8
	sleep := 5 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := os.Rename(tmp, path); err == nil {
			return nil
		}
		lastErr = err
		if runtime.GOOS != "windows" {
			return err
		}
		if isWindowsRenameRetryable(err) {
			time.Sleep(sleep)
			if sleep < 100*time.Millisecond {
				sleep *= 2
			}
			continue
		}
		return err
	}
	return lastErr
}

func isWindowsRenameRetryable(err error) bool {
	const (
		// See https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes
		windowsErrAccessDenied     = syscall.Errno(5)  // ERROR_ACCESS_DENIED
		windowsErrSharingViolation = syscall.Errno(32) // ERROR_SHARING_VIOLATION
		windowsErrLockViolation    = syscall.Errno(33) // ERROR_LOCK_VIOLATION
	)
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case windowsErrAccessDenied, windowsErrSharingViolation, windowsErrLockViolation:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "used by another process") || strings.Contains(msg, "access is denied")
}
