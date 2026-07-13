//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !windows

package rootpublication

import "os"

func openStableChildFile(*os.File, string, int, os.FileMode) (*os.File, error) {
	return nil, ErrNamespacePersistenceUnsupported
}

func removeStableChildFile(*os.File, string) error {
	return ErrNamespacePersistenceUnsupported
}

func renameStableChildFile(*os.File, string, string) error {
	return ErrNamespacePersistenceUnsupported
}

func duplicateStableFile(*os.File) (*os.File, error) {
	return nil, ErrStableIdentityUnsupported
}

func stableIdentityFromFile(*os.File) (StableIdentity, error) {
	return StableIdentity{}, ErrStableIdentityUnsupported
}

func syncStableNamespace(*os.File) error {
	return ErrNamespacePersistenceUnsupported
}
