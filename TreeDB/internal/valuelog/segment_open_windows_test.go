//go:build windows

package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSegmentReadHandleAllowsQuarantineRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	if err := os.WriteFile(path, []byte("sealed segment"), 0o600); err != nil {
		t.Fatal(err)
	}

	first, err := openSegmentReadHandle(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := openSegmentReadHandle(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	quarantine := filepath.Join(dir, ".value-l0-000001.log.delete-test")
	if err := os.Rename(path, quarantine); err != nil {
		t.Fatalf("rename with manager read handles open: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical path stat after rename = %v, want not exist", err)
	}
	buf := make([]byte, len("sealed segment"))
	if _, err := first.ReadAt(buf, 0); err != nil {
		t.Fatalf("read through renamed segment handle: %v", err)
	}
	if string(buf) != "sealed segment" {
		t.Fatalf("read through renamed segment = %q", buf)
	}
}
