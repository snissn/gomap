//go:build !linux

package osadapter

import (
	"os"
)

func stableOSHandlesSupported() bool { return false }

func duplicateOpenHandle(*os.File) (*os.File, error) { return nil, ErrUnsupportedPlatform }

func inspectOpenHandle(*os.File) (openSnapshot, error) {
	return openSnapshot{}, ErrUnsupportedPlatform
}

func syncOpenResource(*os.File) error { return ErrUnsupportedPlatform }

func validateNamespacePersistence(*os.File) error { return ErrUnsupportedPlatform }

func syncOpenNamespace(*os.File) error { return ErrUnsupportedPlatform }
