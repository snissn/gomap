//go:build !windows

package collections

import (
	"errors"
	"os"
)

func fileHandleClosedForTest(err error) bool {
	return errors.Is(err, os.ErrClosed)
}
