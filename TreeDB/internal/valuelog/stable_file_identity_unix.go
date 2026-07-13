//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package valuelog

import (
	"os"

	"golang.org/x/sys/unix"
)

const stableFileIdentityPlatformUnix = 1

func captureStableFileIdentity(f *os.File) (StableFileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return StableFileIdentity{}, err
	}
	return StableFileIdentity{
		platform: stableFileIdentityPlatformUnix,
		volume:   uint64(stat.Dev),
		fileLow:  uint64(stat.Ino),
	}, nil
}
