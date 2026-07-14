//go:build darwin || freebsd || netbsd || openbsd

package rootpublication

import "os"

func stableCrossParentMoveNoReplaceSupported() bool { return false }

func moveStableChildFileNoReplace(*os.File, *os.File, string, *os.File, string) (bool, error) {
	return false, ErrNamespacePersistenceUnsupported
}
