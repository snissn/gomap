//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStableIdentityOverrideForTestingDoesNotFollowPathRebind(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "source.vlog")
	materializedPath := filepath.Join(t.TempDir(), "materialized.vlog")
	if err := os.WriteFile(sourcePath, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(materializedPath, []byte("materialized"), 0o600); err != nil {
		t.Fatal(err)
	}
	sourceIdentity := nativeStableIdentityForPath(t, sourcePath)
	materializedIdentity := nativeStableIdentityForPath(t, materializedPath)

	release, err := InstallStableIdentityOverridesForTesting(map[string]StableIdentity{
		materializedPath: sourceIdentity,
	})
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := os.Open(materializedPath)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	assertStableIdentityForFile(t, pinned, sourceIdentity)

	if err := os.Remove(materializedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(materializedPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	replacementIdentity := nativeStableIdentityForPath(t, materializedPath)
	if SamePhysicalIdentity(replacementIdentity, materializedIdentity) {
		t.Fatal("replacement unexpectedly reused the still-open materialized object")
	}
	assertStableIdentityForFile(t, pinned, sourceIdentity)
	assertStableIdentityForPath(t, materializedPath, replacementIdentity)
	if SamePhysicalIdentity(replacementIdentity, sourceIdentity) {
		t.Fatal("replacement at an overridden path inherited the old modeled identity")
	}

	release()
	assertStableIdentityForFile(t, pinned, materializedIdentity)
}
