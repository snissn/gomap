//go:build windows

package commitlog

import (
	"errors"
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func fileHandleClosedForTest(err error) bool {
	return errors.Is(err, os.ErrClosed) || errors.Is(err, windows.ERROR_INVALID_HANDLE)
}

func TestFileHandleClosedForTestWindows(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "closed-handle-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })

	_, statErr := file.Stat()
	if fileHandleClosedForTest(statErr) {
		t.Fatal("live file handle reported closed")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, statErr = file.Stat()
	if !errors.Is(statErr, windows.ERROR_INVALID_HANDLE) {
		t.Fatalf("closed file Stat error=%v want ERROR_INVALID_HANDLE", statErr)
	}
	if !fileHandleClosedForTest(statErr) {
		t.Fatalf("closed file Stat error=%v not recognized as closed", statErr)
	}
}
