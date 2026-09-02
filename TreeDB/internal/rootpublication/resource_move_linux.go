//go:build linux

package rootpublication

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func stableCrossParentMoveNoReplaceSupported() bool { return true }

func moveStableChildFileNoReplace(sourceParent, expected *os.File, oldName string, destinationParent *os.File, newName string) (bool, error) {
	return moveStableChildFileNoReplaceLinux(sourceParent, expected, oldName, destinationParent, newName, nil)
}

func installStableFileHandleNoReplace(expected, destinationParent *os.File, name string) (bool, error) {
	return linkStableFileHandleNoReplace(expected, destinationParent, name)
}

func moveStableChildFileNoReplaceLinux(sourceParent, expected *os.File, oldName string, destinationParent *os.File, newName string, afterValidation func()) (bool, error) {
	// Reject an already-rebound staging name. The later mutation uses only the
	// retained child handle, so a rebind after this check cannot redirect the
	// installed identity or cause a source-path unlink.
	if err := validateStableChildLink(sourceParent, expected, oldName); err != nil {
		return false, err
	}
	if afterValidation != nil {
		afterValidation()
	}
	installed, err := linkStableFileHandleNoReplace(expected, destinationParent, newName)
	if err != nil {
		return installed, err
	}
	if err := validateStableChildLink(destinationParent, expected, newName); err != nil {
		return true, errors.Join(err, ErrResourceConflict)
	}
	// Never unlink oldName here. A pathname unlink would recreate the exact
	// identity-check/use race this primitive exists to avoid. The private
	// staging namespace remains recovery-owned until publication completes.
	if err := validateStableChildLink(sourceParent, expected, oldName); err != nil {
		return true, errors.Join(err, ErrResourceConflict)
	}
	return true, nil
}

func linkStableFileHandleNoReplace(expected, destinationParent *os.File, newName string) (bool, error) {
	link := func(oldDirFD int, oldName string, flags int) error {
		for {
			err := unix.Linkat(oldDirFD, oldName, int(destinationParent.Fd()), newName, flags)
			if err != unix.EINTR {
				return err
			}
		}
	}
	err := link(int(expected.Fd()), "", unix.AT_EMPTY_PATH)
	if err == unix.EPERM || err == unix.ENOENT || err == unix.EINVAL {
		// Older kernels may privilege AT_EMPTY_PATH. The proc descriptor symlink
		// is still bound to the retained handle and cannot be pathname-rebound.
		procPath := fmt.Sprintf("/proc/self/fd/%d", expected.Fd())
		err = link(unix.AT_FDCWD, procPath, unix.AT_SYMLINK_FOLLOW)
	}
	switch {
	case err == nil:
		return true, nil
	case err == unix.EEXIST:
		return false, fmt.Errorf("%w: destination child %q already exists", ErrResourceConflict, newName)
	case err == unix.ENOSYS || err == unix.EINVAL || err == unix.EOPNOTSUPP || err == unix.EPERM || err == unix.ENOENT:
		return false, fmt.Errorf("%w: exact-handle cross-parent no-replace link: %v", ErrNamespacePersistenceUnsupported, err)
	default:
		return false, err
	}
}
