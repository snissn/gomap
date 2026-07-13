//go:build windows

package valuelog

import "os"

func captureStableNamespaceDirectoryPlatform(string) (*os.File, StableFileIdentity, error) {
	return nil, StableFileIdentity{}, ErrStableNamespaceUnsupported
}
