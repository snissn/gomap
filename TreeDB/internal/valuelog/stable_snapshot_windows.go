//go:build windows

package valuelog

import "os"

func duplicateStableFile(*os.File) (*os.File, error) {
	return nil, ErrStableSnapshotUnsupported
}
