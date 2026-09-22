//go:build darwin || linux || freebsd || netbsd || openbsd

package powerlossoracle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureOpenFileSnapshotKeepsBytesAndIdentityCoupledAcrossPathRebind(t *testing.T) {
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

	if err := os.Rename(path, filepath.Join(root, "captured-inode")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	replacement, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	replacementIdentity, err := rootpublication.StableIdentityFromFile(replacement)
	if closeErr := replacement.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
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
	if rootpublication.SamePhysicalIdentity(identity, replacementIdentity) {
		t.Fatalf("snapshot identity=%+v unexpectedly followed replacement=%+v", identity, replacementIdentity)
	}
}
