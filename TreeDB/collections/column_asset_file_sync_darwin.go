//go:build darwin

package collections

import "os"

func syncColumnAssetSegmentFile(file *os.File) error {
	if file == nil {
		return nil
	}
	// Go uses F_FULLFSYNC for os.File.Sync on Darwin; keep that publish barrier.
	return file.Sync()
}
