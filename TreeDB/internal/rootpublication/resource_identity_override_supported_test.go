//go:build darwin || linux || freebsd || netbsd || openbsd || windows

package rootpublication

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStableIdentityOverridesForTestingAreScopedToPhysicalObjects(t *testing.T) {
	sourceDir := t.TempDir()
	materializedDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "resource.vlog")
	materializedPath := filepath.Join(materializedDir, "resource.vlog")
	if err := os.WriteFile(sourcePath, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(materializedPath, []byte("materialized"), 0o600); err != nil {
		t.Fatal(err)
	}

	sourceIdentity := nativeStableIdentityForPath(t, sourcePath)
	sourceDirIdentity := nativeStableIdentityForPath(t, sourceDir)
	materializedIdentity := nativeStableIdentityForPath(t, materializedPath)
	materializedDirIdentity := nativeStableIdentityForPath(t, materializedDir)
	if SamePhysicalIdentity(sourceIdentity, materializedIdentity) {
		t.Fatal("independently materialized files unexpectedly share a physical identity")
	}
	if SamePhysicalIdentity(sourceDirIdentity, materializedDirIdentity) {
		t.Fatal("independently materialized directories unexpectedly share a physical identity")
	}

	release, err := InstallStableIdentityOverridesForTesting(map[string]StableIdentity{
		materializedDir:  sourceDirIdentity,
		materializedPath: sourceIdentity,
	})
	if err != nil {
		t.Fatal(err)
	}

	assertStableIdentityForPath(t, materializedDir, sourceDirIdentity)
	resource, err := os.Open(materializedPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Close()
	assertStableIdentityForFile(t, resource, sourceIdentity)
	duplicate, err := duplicateStableFile(resource)
	if err != nil {
		t.Fatal(err)
	}
	assertStableIdentityForFile(t, duplicate, sourceIdentity)
	if err := duplicate.Close(); err != nil {
		t.Fatal(err)
	}

	unrelatedPath := filepath.Join(materializedDir, "replacement.vlog")
	if err := os.WriteFile(unrelatedPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	unrelatedIdentity := nativeStableIdentityForPath(t, unrelatedPath)
	assertStableIdentityForPath(t, unrelatedPath, unrelatedIdentity)
	if SamePhysicalIdentity(unrelatedIdentity, sourceIdentity) {
		t.Fatal("an unregistered object inherited an override from its parent path")
	}

	if unexpectedRelease, err := InstallStableIdentityOverridesForTesting(map[string]StableIdentity{
		materializedPath: sourceIdentity,
	}); err == nil {
		unexpectedRelease()
		t.Fatal("overlapping physical-identity override unexpectedly succeeded")
	}

	release()
	release()
	assertStableIdentityForPath(t, materializedDir, materializedDirIdentity)
	assertStableIdentityForFile(t, resource, materializedIdentity)

	reinstallRelease, err := InstallStableIdentityOverridesForTesting(map[string]StableIdentity{
		materializedPath: sourceIdentity,
	})
	if err != nil {
		t.Fatalf("reinstall after release: %v", err)
	}
	reinstallRelease()
}

func nativeStableIdentityForPath(t *testing.T, path string) StableIdentity {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	identity, err := platformStableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func assertStableIdentityForPath(t *testing.T, path string, want StableIdentity) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	assertStableIdentityForFile(t, file, want)
}

func assertStableIdentityForFile(t *testing.T, file *os.File, want StableIdentity) {
	t.Helper()
	got, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if !SamePhysicalIdentity(got, want) {
		t.Fatalf("stable identity=%+v want physical identity=%+v", got, want)
	}
}
