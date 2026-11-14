//go:build linux

package stableio

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestBeginWritebackLinuxReadOnlyAndClosed(t *testing.T) {
	path := t.TempDir() + "/data"
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := BeginWriteback(file, 0); err != nil {
		t.Fatal(err)
	}
	if err := BeginWriteback(file, 4); err != nil {
		t.Fatalf("read-only hint=%v", err)
	}
	if err := SyncFile(file); err != nil {
		t.Fatalf("read-only full sync=%v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := BeginWriteback(file, 4); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed hint=%v", err)
	}
}
