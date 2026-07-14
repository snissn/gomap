//go:build !windows

package collections

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestDeleteColumnAssetSegmentStableRejectsPreValidationRebind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-000001.tca")
	moved := filepath.Join(dir, "segment-original.tca")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	restore := setColumnAssetStableDeleteBeforeValidationTestHook(func() {
		if err := os.Rename(path, moved); err != nil {
			t.Fatalf("rename original: %v", err)
		}
		if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
			t.Fatalf("write replacement: %v", err)
		}
	})
	defer restore()
	deleted, err := deleteColumnAssetSegmentStable(path, rootpublication.NewIdentityPinRegistry(), removeStableColumnAssetChild)
	if deleted || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("stable delete after rebind=(%v,%v) want (false,ErrResourceConflict)", deleted, err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "replacement" {
		t.Fatalf("replacement changed raw=%q err=%v", raw, err)
	}
	if raw, err := os.ReadFile(moved); err != nil || string(raw) != "original" {
		t.Fatalf("original changed raw=%q err=%v", raw, err)
	}
}
