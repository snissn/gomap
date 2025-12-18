//go:build !linux

package pager

import "os"

func preallocateFile(_ *os.File, _ int64) error {
	// Not supported on this platform in this codebase; Truncate is still used.
	return nil
}
