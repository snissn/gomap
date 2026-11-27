//go:build !windows

package valuelog

import (
	"errors"
	"os"
)

func openSegmentReadHandle(path string) (*os.File, error) {
	return os.Open(path)
}

func isPendingDeleteSegmentPath(_ *File, _ string, pathErr error, _ bool) bool {
	return errors.Is(pathErr, os.ErrNotExist)
}

func isPendingDeleteOrSameSegmentPath(_ *File, _ string, _ bool) bool {
	return false
}
