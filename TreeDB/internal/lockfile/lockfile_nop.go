//go:build !unix && !windows

package lockfile

import "os"

func lockFile(_ *os.File) error { return ErrUnsupported }
func lockFileShared(_ *os.File) error {
	return ErrUnsupported
}
func unlockFile(_ *os.File) error { return nil }
