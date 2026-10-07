//go:build !windows

package valuelog

import "os"

func retirementTestRenameParent(dir, saved string, files []*File) error { return os.Rename(dir, saved) }
