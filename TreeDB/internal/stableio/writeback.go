package stableio

import (
	"errors"
	"os"
	"syscall"
)

// BeginWriteback optionally initiates writeback through bytes on the retained
// file. It is not a persistence barrier and never replaces SyncFile.
func BeginWriteback(file *os.File, bytes uint64) error {
	return beginWriteback(file, bytes, writebackRange)
}

// The operation argument is a synchronous borrow, not retained callback state.
// Keeping injection here avoids mutable global hooks and per-call allocations.
func beginWriteback(file *os.File, bytes uint64, operation func(*os.File, int64) error) error {
	// Linux length zero means through EOF, so never pass it to the adapter.
	if bytes == 0 {
		return nil
	}
	if file == nil || bytes > uint64(1<<63-1) {
		return os.ErrInvalid
	}
	for {
		err := operation(file, int64(bytes))
		switch {
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.ENOSYS), errors.Is(err, syscall.EOPNOTSUPP):
			return nil
		default:
			return err
		}
	}
}
