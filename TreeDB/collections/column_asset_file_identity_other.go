//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package collections

import "os"

func columnAssetVerifiedChecksumFileIdentityFromFile(file *os.File) columnAssetVerifiedChecksumFileIdentity {
	if file == nil {
		return columnAssetVerifiedChecksumFileIdentity{}
	}
	info, err := file.Stat()
	if err != nil {
		return columnAssetVerifiedChecksumFileIdentity{}
	}
	return columnAssetVerifiedChecksumFileIdentity{
		size:            info.Size(),
		modTimeUnixNano: info.ModTime().UnixNano(),
		valid:           true,
	}
}
