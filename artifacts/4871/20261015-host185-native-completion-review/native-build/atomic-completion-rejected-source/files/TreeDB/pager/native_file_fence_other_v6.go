//go:build !linux

package pager

import "os"

func nativeFileFenceSupportedV6() bool             { return false }
func nativeFileFenceAdmissionV6() (uint64, uint64) { return 0, 0 }
func nativeFileFenceOnceV6(*os.File) (int64, error) {
	return 0, ErrNativeFileFenceUnsupportedV6
}
