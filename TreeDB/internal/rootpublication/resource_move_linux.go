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
	// Bind the pathname to the retained creation handle immediately before the
	// one atomic namespace mutation. A path rebound discovered here must not
	// install or unlink anything.
	if err := validateStableChildLink(sourceParent, expected, oldName); err != nil {
		return false, err
	}
	for {
		err := unix.Renameat2(
			int(sourceParent.Fd()), oldName,
			int(destinationParent.Fd()), newName,
			unix.RENAME_NOREPLACE,
		)
		switch {
		case err == nil:
			if validateErr := validateStableChildLink(destinationParent, expected, newName); validateErr != nil {
				return true, errors.Join(validateErr, ErrResourceConflict)
			}
			return true, nil
		case err == unix.EINTR:
			continue
		case err == unix.EEXIST:
			return false, fmt.Errorf("%w: destination child %q already exists", ErrResourceConflict, newName)
		case err == unix.ENOSYS || err == unix.EINVAL || err == unix.EOPNOTSUPP:
			return false, fmt.Errorf("%w: atomic cross-parent no-replace move: %v", ErrNamespacePersistenceUnsupported, err)
		default:
			return false, err
		}
	}
}
