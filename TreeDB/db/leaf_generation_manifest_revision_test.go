package db

import (
	"os"
	"testing"
)

func TestSaveLeafGenerationManifestAdvancesPersistentRevision(t *testing.T) {
	leafDir := t.TempDir()
	manifest := newLeafGenerationManifest(1)
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		t.Fatal(err)
	}
	firstRevision := manifest.Revision
	manifest.NextGenerationID++
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Revision <= firstRevision {
		t.Fatalf("revision=%d want > %d", manifest.Revision, firstRevision)
	}
	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || loaded.Revision != manifest.Revision {
		t.Fatalf("loaded revision=%d ok=%t want %d", loaded.Revision, ok, manifest.Revision)
	}
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); err != nil {
		t.Fatal(err)
	}
}
