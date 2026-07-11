//go:build !linux

package pager

import "os"

func syncPageFileData(file *os.File) error {
	return file.Sync()
}
