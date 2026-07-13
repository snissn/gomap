//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStableNamespaceParentGenerationTracksExactParentIdentity(t *testing.T) {
	firstDir := t.TempDir()
	first, err := os.Open(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	duplicate, err := duplicateStableFile(first)
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Close()
	second, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	firstGeneration, err := StableNamespaceParentGeneration(first)
	if err != nil || firstGeneration == 0 {
		t.Fatalf("first generation=%d err=%v", firstGeneration, err)
	}
	duplicateGeneration, err := StableNamespaceParentGeneration(duplicate)
	if err != nil {
		t.Fatal(err)
	}
	if duplicateGeneration != firstGeneration {
		t.Fatalf("duplicate generation=%d first=%d", duplicateGeneration, firstGeneration)
	}
	secondGeneration, err := StableNamespaceParentGeneration(second)
	if err != nil {
		t.Fatal(err)
	}
	if secondGeneration == firstGeneration {
		t.Fatalf("distinct parent generations collided: %d", firstGeneration)
	}
}

func TestOpenStableChildFileUsesCapturedParentAfterPathReplacement(t *testing.T) {
	root := t.TempDir()
	originalDir := filepath.Join(root, "segments")
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(originalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	movedDir := filepath.Join(root, "segments-moved")
	if err := os.Rename(originalDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	child, err := OpenStableChildFile(parent, "000001.vlog", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	if _, err := os.Stat(filepath.Join(movedDir, "000001.vlog")); err != nil {
		t.Fatalf("captured-parent child: %v", err)
	}
	if _, err := os.Stat(filepath.Join(originalDir, "000001.vlog")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement path unexpectedly received child: %v", err)
	}
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: child, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "000001.vlog", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveStableChildFileUsesCapturedParentAfterPathReplacement(t *testing.T) {
	root := t.TempDir()
	originalDir := filepath.Join(root, "segments")
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(originalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "000001.vlog", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Close(); err != nil {
		t.Fatal(err)
	}
	movedDir := filepath.Join(root, "segments-moved")
	if err := os.Rename(originalDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(originalDir, "000001.vlog"), []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RemoveStableChildFile(parent, "000001.vlog"); err != nil {
		t.Fatal(err)
	}
	if err := parent.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(movedDir, "000001.vlog")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("captured-parent child still exists: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(originalDir, "000001.vlog")); err != nil || string(got) != "replacement" {
		t.Fatalf("replacement child changed: data=%q err=%v", got, err)
	}
}

func TestStableNamespaceRejectsChildReplacementBeforeStabilize(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	original, err := OpenStableChildFile(parent, "000001.vlog", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: original, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "000001.vlog", DiagnosticPath: "display-only",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	if err := os.Rename(filepath.Join(dir, "000001.vlog"), filepath.Join(dir, "original.vlog")); err != nil {
		t.Fatal(err)
	}
	replacement, err := OpenStableChildFile(parent, "000001.vlog", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_ = replacement.Close()
	if err := namespace.Stabilize(); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("Stabilize after child replacement error=%v want ErrResourceConflict", err)
	}
}

func TestStableNamespaceCreationProofRejectsReplacementBeforeBind(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	original, err := OpenStableChildFile(parent, "000001.vlog", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	proof, err := NewStableNamespaceCreationProof(parent, original, "000001.vlog")
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Release()
	if err := os.Rename(filepath.Join(dir, "000001.vlog"), filepath.Join(dir, "original.vlog")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000001.vlog"), []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	token, err := proof.Bind(parent, 1, "000001.vlog", "display-only")
	if token != nil {
		token.Release()
		t.Fatal("replacement child received a bound namespace token")
	}
	if !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("Bind after child replacement error=%v want ErrResourceConflict", err)
	}
}

func TestStableNamespaceCreationProofRejectsWrongParentAndName(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := OpenStableChildFile(parent, "000001.vlog", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	proof, err := NewStableNamespaceCreationProof(parent, child, "000001.vlog")
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Release()
	wrongParent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wrongParent.Close()
	for _, tc := range []struct {
		name   string
		parent *os.File
		child  string
	}{
		{name: "parent", parent: wrongParent, child: "000001.vlog"},
		{name: "name", parent: parent, child: "000002.vlog"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token, err := proof.Bind(tc.parent, 1, tc.child, "display-only")
			if token != nil {
				token.Release()
				t.Fatal("mismatched proof binding returned a token")
			}
			if !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("Bind error=%v want ErrResourceConflict", err)
			}
		})
	}
}
