package atomicfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

// ReplacementMayHaveCommitted reports whether an atomic replacement reached
// the rename boundary before returning an error. Callers that cache the old
// document must treat this outcome as ambiguous until they reopen it.
func ReplacementMayHaveCommitted(err error) bool {
	var committed *replacementMayHaveCommittedError
	return errors.As(err, &committed)
}

type replacementMayHaveCommittedError struct {
	err error
}

func (e *replacementMayHaveCommittedError) Error() string {
	return fmt.Sprintf("atomic replacement may have committed: %v", e.err)
}

func (e *replacementMayHaveCommittedError) Unwrap() error { return e.err }

// Write atomically replaces path with data using a temp file in the same
// directory. On Windows it retries rename on transient sharing/access errors.
func Write(path string, data []byte, perm os.FileMode) error {
	return write(path, data, perm, false)
}

// WriteDurable atomically replaces path and persists its parent namespace.
// Success covers both the replacement bytes and their recovery-selectable name.
// A failure after rename is ambiguous, including a failed parent-directory sync.
// Write retains its existing file-sync-only contract.
func WriteDurable(path string, data []byte, perm os.FileMode) error {
	return write(path, data, perm, true)
}

func write(path string, data []byte, perm os.FileMode, durable bool) error {
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
	defer func() { _ = removeObserved(dir, tmp) }()
	if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceAuxiliary, dir, "", tmp); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Chmod(perm); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceAuxiliary, dir, tmp, f); err != nil {
		_ = f.Close()
		return err
	}
	syncFile := (*os.File).Sync
	if durable {
		syncFile = stableio.SyncFile
	}
	if err := syncFile(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := durabilitycut.EmitStablePath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceAuxiliary, dir, tmp, f); err != nil {
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
			if err := durabilitycut.EmitNamespace(durabilitycut.NamespaceRename, durabilitycut.ResourceAuxiliary, dir, tmp, path); err != nil {
				return &replacementMayHaveCommittedError{err: err}
			}
			if durable {
				if err := SyncDirectory(dir); err != nil {
					return &replacementMayHaveCommittedError{err: err}
				}
			}
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

// SyncDirectory persists the namespace through the exact directory handle and
// exposes the same before/after boundary used by the stable-storage model.
func SyncDirectory(path string) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceAuxiliary, path, path, f); err != nil {
		return err
	}
	if err := stableio.SyncFile(f); err != nil {
		return err
	}
	return durabilitycut.EmitStablePath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceAuxiliary, path, path, f)
}

func removeObserved(root, path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return durabilitycut.EmitNamespace(durabilitycut.NamespaceUnlink, durabilitycut.ResourceAuxiliary, root, path, "")
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
