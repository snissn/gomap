//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package collections

import "os"

func columnAssetVerifiedChecksumFileIdentityFromFile(file *os.File) columnAssetVerifiedChecksumFileIdentity {
	// Unsupported platforms do not expose a stable dev/inode identity here.
	// Disable cached-verify rather than keying it by size/mtime alone.
	return columnAssetVerifiedChecksumFileIdentity{}
}
