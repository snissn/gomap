//go:build !windows

package hashdb

import (
	"os"
	"path/filepath"
)

func replaceFileAtomic(dst, src string) error {
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dst))
}
