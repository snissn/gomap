//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package rootpublication

import "os"

// Non-Unix platforms retain the original exact-handle acquisition and identity
// behavior, including unsupported-platform failure and test identity overrides.
func platformStableChildIdentity(parent *os.File, name string) (StableIdentity, error) {
	linked, err := OpenStableChildFile(parent, name, os.O_RDONLY, 0)
	if err != nil {
		return StableIdentity{}, stableChildIdentityOpenError(name, err)
	}
	defer linked.Close()
	return stableIdentityFromFile(linked)
}
