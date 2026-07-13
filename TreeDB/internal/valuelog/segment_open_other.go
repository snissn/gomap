//go:build !windows

package valuelog

import "os"

func openSegmentReadHandle(path string) (*os.File, error) {
	return os.Open(path)
}
