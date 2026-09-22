//go:build windows

package rootpublication

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStableResourceTokenSyncReopensReadOnlyHandleWithWriteAccessWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := OpenStableParent(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	created, err := OpenStableChildFile(parent, "readonly.vlog", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := created.Write([]byte("durable")); err != nil {
		_ = created.Close()
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := OpenStableChildFile(parent, "readonly.vlog", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "1", Generation: 1,
		DiagnosticPath: "value_vlog/readonly.vlog", File: readOnly,
		Frontier: DurableFrontier{Bytes: uint64(len("durable"))}, Digest: sha256.Sum256([]byte("durable")),
		Reachability: ReachabilityValueLogPointer,
	})
	if err != nil {
		t.Fatalf("NewStableResourceToken from read-only handle: %v", err)
	}
	defer token.Release()
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("SyncThrough read-only source handle: %v", err)
	}
}

func TestStableRelativeNamespaceCapabilityRemainsFailClosedWindows(t *testing.T) {
	if StableRelativeNamespaceSupported() {
		t.Fatal("create-only Windows evidence must not advertise rename/remove namespace support")
	}
}

func TestOpenStableChildFileUsesExactParentHandleWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := OpenStableParent(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenStableChildFile: %v", err)
	}
	if _, err := child.Write([]byte("stable")); err != nil {
		_ = child.Close()
		t.Fatal(err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "child")); err != nil || string(got) != "stable" {
		t.Fatalf("read stable child got=%q err=%v", got, err)
	}
	if duplicate, err := OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600); !errors.Is(err, os.ErrExist) {
		if duplicate != nil {
			_ = duplicate.Close()
		}
		t.Fatalf("duplicate OpenStableChildFile error=%v want os.ErrExist", err)
	}
	if missing, err := OpenStableChildFile(parent, "missing", os.O_RDONLY, 0); !errors.Is(err, os.ErrNotExist) {
		if missing != nil {
			_ = missing.Close()
		}
		t.Fatalf("missing OpenStableChildFile error=%v want os.ErrNotExist", err)
	}
	moved := dir + "-moved"
	if err := os.Rename(dir, moved); err != nil {
		t.Fatalf("rename exact parent: %v", err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	reboundChild, err := OpenStableChildFile(parent, "after-rebind", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenStableChildFile after parent rebind: %v", err)
	}
	if err := reboundChild.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(moved, "after-rebind")); err != nil {
		t.Fatalf("exact parent did not receive rebound child: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "after-rebind")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement diagnostic path received child: %v", err)
	}
}

func TestEnsureStableChildDirectoryUsesExactParentHandleWindows(t *testing.T) {
	root := t.TempDir()
	originalDir := filepath.Join(root, "parent")
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := OpenStableParent(originalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	movedDir := filepath.Join(root, "parent-moved")
	if err := os.Rename(originalDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	child, err := EnsureStableChildDirectory(parent, "nested", 0o700, registry)
	if err != nil {
		t.Fatalf("EnsureStableChildDirectory: %v", err)
	}
	defer child.Close()
	if _, err := os.Stat(filepath.Join(movedDir, "nested")); err != nil {
		t.Fatalf("captured-parent directory child: %v", err)
	}
	if _, err := os.Stat(filepath.Join(originalDir, "nested")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement path unexpectedly received directory child: %v", err)
	}
	known, err := registry.StableDirectoryLinkKnown(parent, child, "nested")
	if err != nil || !known {
		t.Fatalf("stable directory link known=%t err=%v", known, err)
	}
	if got := registry.Stats(); got != (IdentityPinRegistryStats{}) {
		t.Fatalf("directory ancestry polluted stable resource accounting: %+v", got)
	}
	if got := registry.CachedStableDirectoryLinks(); got != 1 {
		t.Fatalf("stable directory cache entries=%d want 1", got)
	}
	registry.mu.Lock()
	var retained stableDirectoryLinkAuthority
	for _, authority := range registry.stableDirectoryLinks {
		retained = authority
		break
	}
	registry.mu.Unlock()
	if retained.parent == nil || retained.child == nil {
		t.Fatal("stable directory proof did not retain parent and child authority")
	}
	if _, err := retained.parent.Stat(); err != nil {
		t.Fatalf("retained stable directory parent: %v", err)
	}
	if _, err := retained.child.Stat(); err != nil {
		t.Fatalf("retained stable directory child: %v", err)
	}
	second, err := EnsureStableChildDirectory(parent, "nested", 0o700, registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.CachedStableDirectoryLinks(); got != 1 {
		t.Fatalf("repeated stable directory cache entries=%d want 1", got)
	}
	registry.ClearStableNamespaceLinks()
	known, err = registry.StableDirectoryLinkKnown(parent, child, "nested")
	if err != nil || known {
		t.Fatalf("cleared stable directory link known=%t err=%v", known, err)
	}
	if got := registry.CachedStableDirectoryLinks(); got != 0 {
		t.Fatalf("cleared stable directory cache entries=%d want 0", got)
	}
	if err := retained.parent.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cleared stable directory parent authority: %v", err)
	}
	if err := retained.child.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cleared stable directory child authority: %v", err)
	}
}

func TestStableNamespaceCreationSyncsExactChildWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	created, err := OpenStableChildFile(parent, "child", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	resource, err := OpenStableChildFile(parent, "child", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Close()
	parentGeneration, err := StableNamespaceParentGeneration(parent)
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: parentGeneration, Operation: NamespaceCreate,
		NewName: "child", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatalf("NewStableNamespaceToken: %v", err)
	}
	defer namespace.Release()
	if err := namespace.Stabilize(); err != nil {
		t.Fatalf("Stabilize exact child creation: %v", err)
	}
	if err := namespace.validateStable(); err != nil {
		t.Fatalf("creation token did not retain exact-link evidence: %v", err)
	}
}

func TestStableNamespaceCreationProofSyncsExactChildWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "child-proof", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	proof, err := NewStableNamespaceCreationProof(parent, child, "child-proof")
	if err != nil {
		t.Fatalf("NewStableNamespaceCreationProof: %v", err)
	}
	proof.Release()
}

func TestRenameStableChildFileFailsTypedWithoutVisibilityWindows(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	oldPath := filepath.Join(dir, "old")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameStableChildFile(parent, "old", "new"); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("RenameStableChildFile error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("unsupported rename changed source: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported rename exposed destination: %v", err)
	}
}
