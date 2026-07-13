//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package valuelog

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func captureStableNamespaceDirectoryPlatform(path string) (*os.File, StableFileIdentity, error) {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, StableFileIdentity{}, err
	}
	identity, err := StableFileIdentityFromFile(directory)
	if err != nil {
		_ = directory.Close()
		return nil, StableFileIdentity{}, fmt.Errorf("%w: %v", ErrStableNamespaceUnsupported, err)
	}
	return directory, identity, nil
}

func stableNamespaceTargetIdentity(directory *os.File, name string) (StableFileIdentity, error) {
	if directory == nil || name == "" || name == "." || filepath.Base(name) != name {
		return StableFileIdentity{}, ErrStableNamespaceUnsupported
	}
	var stat unix.Stat_t
	if err := unix.Fstatat(int(directory.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return StableFileIdentity{}, err
	}
	identity := StableFileIdentity{
		platform: stableFileIdentityPlatformUnix,
		volume:   uint64(stat.Dev),
		fileLow:  uint64(stat.Ino),
	}
	if !identity.valid() {
		return StableFileIdentity{}, ErrStableFileIdentityUnsupported
	}
	return identity, nil
}
