//go:build linux

package collections

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestVectorPartitionLifecycleStoreV1RejectsSymlinkAndFIFO(t *testing.T) {
	for _, tc := range []struct {
		name   string
		create func(t *testing.T, path string)
	}{
		{"symlink", func(t *testing.T, path string) {
			t.Helper()
			if err := os.Symlink("/dev/null", path); err != nil {
				t.Fatal(err)
			}
		}},
		{"fifo", func(t *testing.T, path string) {
			t.Helper()
			if err := unix.Mkfifo(path, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			store, err := OpenVectorPartitionStoreV1(root)
			if err != nil {
				t.Fatal(err)
			}
			name, err := vectorPartitionLifecycleNameV1("docs", "embedding", 1)
			if err != nil {
				t.Fatal(err)
			}
			tc.create(t, filepath.Join(store.dir, name))
			if _, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("load err=%v", err)
			}
		})
	}
}
