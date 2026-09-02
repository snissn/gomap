//go:build !linux

package pager

import (
	"os"

	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

func syncPageFileData(file *os.File) error {
	return stableio.SyncFile(file)
}
