//go:build !linux && !darwin

package collections

import "os"

func syncColumnAssetSegmentFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return file.Sync()
}
