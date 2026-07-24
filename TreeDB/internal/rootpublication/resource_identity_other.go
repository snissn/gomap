//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !windows

package rootpublication

import "os"

func linkStableChildFileNoReplace(*os.File, string, string) error {
	return ErrNamespacePersistenceUnsupported
}

func stableRelativeNamespaceSupported() bool { return false }

func stableNamespaceCreationPersistsThroughChild() bool { return false }

func openStableParent(path string) (*os.File, error) { return os.Open(path) }

func openStableChildFile(*os.File, string, int, os.FileMode) (*os.File, error) {
	return nil, ErrNamespacePersistenceUnsupported
}

func openStableAnonymousFile(*os.File, os.FileMode) (*os.File, error) {
	return nil, ErrNamespacePersistenceUnsupported
}

func openOrCreateStableChildDirectory(*os.File, string, os.FileMode) (*os.File, error) {
	return nil, ErrNamespacePersistenceUnsupported
}

func removeStableChildFile(*os.File, string) error {
	return ErrNamespacePersistenceUnsupported
}

func renameStableChildFile(*os.File, string, string) error {
	return ErrNamespacePersistenceUnsupported
}

func stableCrossParentMoveNoReplaceSupported() bool { return false }

func moveStableChildFileNoReplace(*os.File, *os.File, string, *os.File, string) (bool, error) {
	return false, ErrNamespacePersistenceUnsupported
}

func installStableFileHandleNoReplace(*os.File, *os.File, string) (bool, error) {
	return false, ErrNamespacePersistenceUnsupported
}

func duplicateStableFile(*os.File) (*os.File, error) {
	return nil, ErrStableIdentityUnsupported
}

func duplicateStableSyncFile(file *os.File) (*os.File, error) {
	return duplicateStableFile(file)
}

func platformStableIdentityFromFile(*os.File) (StableIdentity, error) {
	return StableIdentity{}, ErrStableIdentityUnsupported
}

func syncStableNamespace(*os.File) error {
	return ErrNamespacePersistenceUnsupported
}
