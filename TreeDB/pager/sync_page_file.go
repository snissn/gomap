package pager

import "os"

var syncPageFileFn = syncPageFileData

func syncPageFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return syncPageFileFn(file)
}
