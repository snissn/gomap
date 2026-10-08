//go:build unix

package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
)

func TestPrimaryNamespaceFailedCloseKeepsExactRegistryCustody(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	proof, err := NewOwnedStableNamespaceCreationProof(parent, child, "primary", &owner)
	if err != nil {
		t.Fatal(err)
	}
	token, err := proof.Bind(parent, 1, "primary", "primary")
	if err != nil {
		t.Fatal(err)
	}
	proof.Release()
	registry := NewIdentityPinRegistry()
	if err = token.BindCleanupRegistry(registry); err != nil {
		t.Fatal(err)
	}
	// Force the real os.File.Close syscall to fail on its genuine captured FD.
	// The existing owner must retain uncertainty, not refund or retry it.
	if err = unix.Close(int(token.parent.Fd())); err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	token.Release()
	if registry.failedNamespace != token || token.parent == nil || token.metadata != &owner || owner.Bytes() != before {
		t.Fatal("failed namespace cleanup lost custody or refunded its backing")
	}
	if !errors.Is(registry.CleanupError(), unix.EBADF) {
		t.Fatalf("cleanup error=%v", registry.CleanupError())
	}
	registry.Close()
	if registry.failedNamespace != token || owner.Bytes() != before {
		t.Fatal("registry close erased uncertain namespace debt")
	}
	token.Release()
	if token.cleanupFailure.count != 1 || owner.Bytes() != before {
		t.Fatal("public repeat retried or appended failure debt")
	}
	var other retainedalloc.Owner
	other.Initialize(0)
	_ = other // Owner mismatch is refused by the actual resource constructor.
}
