//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !windows

package stableio

import "os"

func DuplicateFileWithCustody(_ *os.File, _ string, c *DuplicateCustody) (*os.File, error) {
	if !c.begin() {
		return nil, ErrDuplicateCustodyUsed
	}
	c.operationErr = ErrFilePersistenceUnsupported
	return nil, c
}
