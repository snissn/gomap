//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !windows

package stableio

import "os"

func DuplicateFile(*os.File, string) (*os.File, error) {
	return nil, ErrFilePersistenceUnsupported
}
