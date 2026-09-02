//go:build windows

package collections

import (
	"os"
	"testing"
)

func TestFileHandleClosedForTestWindows(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "closed-handle-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })

	if fileHandleClosedForTest(file) {
		t.Fatal("live file handle reported closed")
	}
	if !fileHandleClosedForTest(file) {
		t.Fatal("handle closed by live negative control not recognized as closed")
	}
}
