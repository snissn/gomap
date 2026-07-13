package valuelog

import (
	"errors"
	"fmt"
	"os"
)

var ErrStableFileIdentityUnsupported = errors.New("valuelog: stable file identity unsupported")

// StableFileIdentity is an opaque comparable identity obtained from an open
// descriptor. Its fields are intentionally private so paths and logical file
// IDs cannot be substituted as authority.
type StableFileIdentity struct {
	platform uint8
	volume   uint64
	fileHigh uint64
	fileLow  uint64
}

func (i StableFileIdentity) valid() bool {
	return i.platform != 0 && (i.volume != 0 || i.fileHigh != 0 || i.fileLow != 0)
}

// Token returns the deterministic serialization used by StableResourceToken.
func (i StableFileIdentity) Token() string {
	if !i.valid() {
		return ""
	}
	return fmt.Sprintf("file:%02x:%016x:%016x:%016x", i.platform, i.volume, i.fileHigh, i.fileLow)
}

func StableFileIdentityFromFile(f *os.File) (StableFileIdentity, error) {
	if f == nil {
		return StableFileIdentity{}, ErrStableFileIdentityUnsupported
	}
	identity, err := captureStableFileIdentity(f)
	if err != nil {
		return StableFileIdentity{}, fmt.Errorf("%w: %v", ErrStableFileIdentityUnsupported, err)
	}
	if !identity.valid() {
		return StableFileIdentity{}, ErrStableFileIdentityUnsupported
	}
	return identity, nil
}

func (f *File) StableFileIdentity() (StableFileIdentity, error) {
	if f == nil {
		return StableFileIdentity{}, ErrStableFileIdentityUnsupported
	}
	return StableFileIdentityFromFile(f.File)
}
