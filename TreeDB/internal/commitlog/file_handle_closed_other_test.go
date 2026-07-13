//go:build !windows

package commitlog

import (
	"errors"
	"os"
)

func fileHandleClosedForTest(err error) bool {
	return errors.Is(err, os.ErrClosed)
}
