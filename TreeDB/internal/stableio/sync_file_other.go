//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !windows

package stableio

import "os"

func SyncFile(*os.File) error { return ErrFilePersistenceUnsupported }
