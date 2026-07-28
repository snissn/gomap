//go:build windows

package powerlossoracle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureOpenFileSnapshotWindowsRetainedHandleBlocksPathRebind(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "resource")
	if err := os.WriteFile(path, []byte("captured"), 0o600); err != nil {
		t.Fatal(err)
	}
	captured, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Close()
	capturedIdentity, err := rootpublication.StableIdentityFromFile(captured)
	if err != nil {
		t.Fatal(err)
	}

	// os.Open does not grant delete sharing on Windows. The retained handle
	// therefore prevents the pathname from being rebound while the snapshot is
	// captured, which is the platform's equivalent safety property.
	if err := os.Rename(path, filepath.Join(root, "captured-inode")); err == nil {
		t.Fatal("rename unexpectedly rebound a path while its snapshot handle was retained")
	}

	info, data, identity, err := captureOpenFileSnapshot(captured)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || string(data) != "captured" {
		t.Fatalf("snapshot mode=%s data=%q want retained regular file bytes", info.Mode(), data)
	}
	if !rootpublication.SamePhysicalIdentity(identity, capturedIdentity) {
		t.Fatalf("snapshot identity=%+v want captured identity=%+v", identity, capturedIdentity)
	}
}
