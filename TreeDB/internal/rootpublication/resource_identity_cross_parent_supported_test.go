//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMoveStableChildFileNoReplaceCrossParentPreservesIdentity(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	destinationDir := filepath.Join(root, "destination")
	for _, dir := range []string{sourceDir, destinationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatalf("Mkdir %s: %v", dir, err)
		}
	}
	sourceParent, err := os.Open(sourceDir)
	if err != nil {
		t.Fatalf("open source parent: %v", err)
	}
	defer sourceParent.Close()
	destinationParent, err := os.Open(destinationDir)
	if err != nil {
		t.Fatalf("open destination parent: %v", err)
	}
	defer destinationParent.Close()
	child, err := OpenStableChildFile(sourceParent, "segment.pack", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create source child: %v", err)
	}
	defer child.Close()
	if _, err := child.WriteString("packed"); err != nil {
		t.Fatalf("write source child: %v", err)
	}
	wantIdentity, err := StableIdentityFromFile(child)
	if err != nil {
		t.Fatalf("source identity: %v", err)
	}

	installed, err := MoveStableChildFileNoReplace(sourceParent, "segment.pack", destinationParent, "segment.pack")
	if err != nil || !installed {
		t.Fatalf("MoveStableChildFileNoReplace installed=%v err=%v", installed, err)
	}
	if _, err := OpenStableChildFile(sourceParent, "segment.pack", os.O_RDONLY, 0); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source child remains: %v", err)
	}
	if err := ValidateStableChildLink(destinationParent, child, "segment.pack"); err != nil {
		t.Fatalf("destination link: %v", err)
	}
	destinationChild, err := OpenStableChildFile(destinationParent, "segment.pack", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open destination child: %v", err)
	}
	defer destinationChild.Close()
	gotIdentity, err := StableIdentityFromFile(destinationChild)
	if err != nil {
		t.Fatalf("destination identity: %v", err)
	}
	if !SamePhysicalIdentity(gotIdentity, wantIdentity) {
		t.Fatalf("destination identity=%+v want physical %+v", gotIdentity, wantIdentity)
	}
}

func TestMoveStableChildFileNoReplaceRejectsReplacement(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	destinationDir := filepath.Join(root, "destination")
	for _, dir := range []string{sourceDir, destinationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatalf("Mkdir %s: %v", dir, err)
		}
	}
	sourceParent, err := os.Open(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceParent.Close()
	destinationParent, err := os.Open(destinationDir)
	if err != nil {
		t.Fatal(err)
	}
	defer destinationParent.Close()
	source, err := OpenStableChildFile(sourceParent, "segment.pack", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destination, err := OpenStableChildFile(destinationParent, "segment.pack", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	destinationIdentity, _ := StableIdentityFromFile(destination)

	installed, err := MoveStableChildFileNoReplace(sourceParent, "segment.pack", destinationParent, "segment.pack")
	if installed || !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("MoveStableChildFileNoReplace installed=%v err=%v want conflict before mutation", installed, err)
	}
	if err := ValidateStableChildLink(sourceParent, source, "segment.pack"); err != nil {
		t.Fatalf("source link changed: %v", err)
	}
	got, err := OpenStableChildFile(destinationParent, "segment.pack", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	gotIdentity, _ := StableIdentityFromFile(got)
	if !SamePhysicalIdentity(gotIdentity, destinationIdentity) {
		t.Fatal("destination was replaced")
	}
}
