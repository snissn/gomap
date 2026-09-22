//go:build !linux

package valuelog

import "os"

func syncStagingFileData(file *os.File) error {
	return file.Sync()
}
