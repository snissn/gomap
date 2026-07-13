//go:build windows

package db

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableLeafGenerationManifestUnsupportedBeforeVisibilityWindows(t *testing.T) {
	leafDir := t.TempDir()
	store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	defer store.Close()
	if _, err := store.Replace(newLeafGenerationManifest(1)); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Replace error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	entries, err := os.ReadDir(leafDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unsupported stable replacement created files: %v", entries)
	}
}

func TestCompatibleLeafGenerationManifestSaveWorksWindows(t *testing.T) {
	leafDir := t.TempDir()
	store := newLeafGenerationManifestStore(leafDir, nil, leafGenerationManifestCompatibility, nil)
	defer store.Close()
	token, err := store.Replace(newLeafGenerationManifest(1))
	if err != nil {
		t.Fatal(err)
	}
	if token != nil {
		t.Fatal("compatibility save returned stable token")
	}
}
