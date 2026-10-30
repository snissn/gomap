//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !windows

package stableio

import (
	"errors"
	"testing"
)

func TestDuplicateCustodyUnsupported(t *testing.T) {
	var c DuplicateCustody
	_, err := DuplicateFileWithCustody(nil, "", &c)
	if err != &c || !errors.Is(err, ErrFilePersistenceUnsupported) || c.Ambiguous() {
		t.Fatalf("err=%v", err)
	}
}
