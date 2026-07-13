//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !windows

package rootpublication

import "os"

func duplicateStableFile(*os.File) (*os.File, error) {
	return nil, ErrStableIdentityUnsupported
}

func stableIdentityFromFile(*os.File) (StableIdentity, error) {
	return StableIdentity{}, ErrStableIdentityUnsupported
}

func syncStableNamespace(*os.File) error {
	return ErrNamespacePersistenceUnsupported
}
