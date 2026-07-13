//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package valuelog

import (
	"fmt"
	"os"
	"path/filepath"
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
