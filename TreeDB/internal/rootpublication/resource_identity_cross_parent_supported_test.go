//go:build linux

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

	installed, err := MoveStableChildFileNoReplace(sourceParent, child, "segment.pack", destinationParent, "segment.pack")
	if err != nil || !installed {
		t.Fatalf("MoveStableChildFileNoReplace installed=%v err=%v", installed, err)
	}
	if err := ValidateStableChildLink(sourceParent, child, "segment.pack"); err != nil {
		t.Fatalf("recovery-owned source alias changed: %v", err)
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

func TestInstallStableFileHandleNoReplacePreservesExactHandleAcrossSourceRebind(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	destinationDir := filepath.Join(root, "destination")
	for _, dir := range []string{sourceDir, destinationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
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
	expected, err := OpenStableChildFile(sourceParent, "source.tmp", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Close()
	if _, err := expected.WriteString("owned-by-retained-handle"); err != nil {
		t.Fatal(err)
	}
	want, err := StableIdentityFromFile(expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(sourceDir, "source.tmp"), filepath.Join(sourceDir, "source.saved")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "source.tmp"), []byte("rebound-decoy"), 0o600); err != nil {
		t.Fatal(err)
	}
	installed, err := InstallStableFileHandleNoReplace(expected, destinationParent, "installed")
	if err != nil || !installed {
		t.Fatalf("InstallStableFileHandleNoReplace installed=%v err=%v", installed, err)
	}
	got, err := OpenStableChildFile(destinationParent, "installed", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	identity, err := StableIdentityFromFile(got)
	if err != nil || !SamePhysicalIdentity(identity, want) {
		t.Fatalf("installed identity=%+v err=%v want=%+v", identity, err, want)
	}
	raw, err := os.ReadFile(filepath.Join(destinationDir, "installed"))
	if err != nil || string(raw) != "owned-by-retained-handle" {
		t.Fatalf("installed bytes=%q err=%v", raw, err)
	}
	raw, err = os.ReadFile(filepath.Join(sourceDir, "source.tmp"))
	if err != nil || string(raw) != "rebound-decoy" {
		t.Fatalf("rebound source bytes=%q err=%v", raw, err)
	}
}

func TestInstallStableFileHandleNoReplaceLeavesExistingTargetUntouched(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	destinationDir := filepath.Join(root, "destination")
	for _, dir := range []string{sourceDir, destinationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
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
	expected, err := OpenStableChildFile(sourceParent, "source.tmp", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Close()
	if _, err := expected.WriteString("new"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destinationDir, "installed"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	installed, err := InstallStableFileHandleNoReplace(expected, destinationParent, "installed")
	if installed || !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("InstallStableFileHandleNoReplace installed=%v err=%v want conflict", installed, err)
	}
	raw, err := os.ReadFile(filepath.Join(destinationDir, "installed"))
	if err != nil || string(raw) != "old" {
		t.Fatalf("existing target bytes=%q err=%v", raw, err)
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

	installed, err := MoveStableChildFileNoReplace(sourceParent, source, "segment.pack", destinationParent, "segment.pack")
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

func TestMoveStableChildFileNoReplaceRejectsReboundSourceBeforeMutation(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	destinationDir := filepath.Join(root, "destination")
	for _, dir := range []string{sourceDir, destinationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
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
	original, err := OpenStableChildFile(sourceParent, "segment.pack", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	if _, err := original.WriteString("original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(sourceDir, "segment.pack"), filepath.Join(sourceDir, "original.saved")); err != nil {
		t.Fatal(err)
	}
	replacement, err := OpenStableChildFile(sourceParent, "segment.pack", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if _, err := replacement.WriteString("replacement"); err != nil {
		t.Fatal(err)
	}

	installed, err := MoveStableChildFileNoReplace(sourceParent, original, "segment.pack", destinationParent, "segment.pack")
	if installed || !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("rebound move installed=%v err=%v want pre-mutation conflict", installed, err)
	}
	for path, want := range map[string]string{
		filepath.Join(sourceDir, "original.saved"): "original",
		filepath.Join(sourceDir, "segment.pack"):   "replacement",
	} {
		got, readErr := os.ReadFile(path)
		if readErr != nil || string(got) != want {
			t.Fatalf("%s data=%q err=%v want %q", path, got, readErr, want)
		}
	}
	if _, statErr := os.Stat(filepath.Join(destinationDir, "segment.pack")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination appeared after rebound rejection: %v", statErr)
	}
}

func TestMoveStableChildFileNoReplaceUsesRetainedIdentityAcrossRebindWindow(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	destinationDir := filepath.Join(root, "destination")
	for _, dir := range []string{sourceDir, destinationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
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
	original, err := OpenStableChildFile(sourceParent, "segment.pack", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	if _, err := original.WriteString("original"); err != nil {
		t.Fatal(err)
	}
	afterValidation := func() {
		if err := os.Rename(filepath.Join(sourceDir, "segment.pack"), filepath.Join(sourceDir, "original.saved")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sourceDir, "segment.pack"), []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	installed, err := moveStableChildFileNoReplaceLinux(sourceParent, original, "segment.pack", destinationParent, "segment.pack", afterValidation)
	if !installed || !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("interleaved move installed=%v err=%v want exact install plus ambiguity", installed, err)
	}
	for path, want := range map[string]string{
		filepath.Join(sourceDir, "original.saved"):    "original",
		filepath.Join(sourceDir, "segment.pack"):      "replacement",
		filepath.Join(destinationDir, "segment.pack"): "original",
	} {
		got, readErr := os.ReadFile(path)
		if readErr != nil || string(got) != want {
			t.Fatalf("%s data=%q err=%v want %q", path, got, readErr, want)
		}
	}
}

func TestProbeStableChildFileNoReplaceInstallExercisesTargetAndCleansAliases(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	if err := ProbeStableChildFileNoReplaceInstall(parent); err != nil {
		t.Fatalf("ProbeStableChildFileNoReplaceInstall: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("probe aliases remain: %v", entries)
	}
}

func TestProbeStableChildFileNoReplaceInstallCleansPostInstallAmbiguity(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	testErr := errors.New("post-install ambiguity")
	move := func(sourceParent, expected *os.File, oldName string, destinationParent *os.File, newName string) (bool, error) {
		installed, err := MoveStableChildFileNoReplace(sourceParent, expected, oldName, destinationParent, newName)
		if err != nil {
			return installed, err
		}
		return installed, testErr
	}
	err = probeStableChildFileNoReplaceInstall(parent, ".probe-source", ".probe-installed", move, RemoveStableChildFile)
	if !errors.Is(err, testErr) {
		t.Fatalf("probe error=%v want ambiguity", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("ambiguous probe aliases remain: %v", entries)
	}
}

func TestProbeStableChildFileNoReplaceInstallCleanupFailureBlocks(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	cleanupErr := errors.New("cleanup blocked")
	removeCalls := 0
	remove := func(parent *os.File, name string) error {
		removeCalls++
		if removeCalls == 1 {
			return cleanupErr
		}
		return RemoveStableChildFile(parent, name)
	}
	err = probeStableChildFileNoReplaceInstall(parent, ".probe-source", ".probe-installed", MoveStableChildFileNoReplace, remove)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("probe error=%v want cleanup failure", err)
	}
}
