package db

import (
	"os"
	"testing"
)

func TestLeafGenerationManifest_SaveLoadRoundTrip(t *testing.T) {
	leafDir := t.TempDir()
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 2,
		NextGenerationID:    3,
		Generations: []leafGenerationRecord{
			{
				GenerationID:       1,
				State:              leafGenerationStateSealed,
				FileIDs:            []uint32{101, 102},
				CreatedCommitSeq:   11,
				SealedCommitSeq:    19,
				PublishedCommitSeq: 19,
			},
			{
				GenerationID:       2,
				State:              leafGenerationStateWritable,
				FileIDs:            []uint32{201},
				CreatedCommitSeq:   20,
				PublishedCommitSeq: 20,
			},
		},
	}
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		t.Fatalf("saveLeafGenerationManifest: %v", err)
	}

	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatalf("expected manifest to exist")
	}
	if loaded.CurrentGenerationID != manifest.CurrentGenerationID {
		t.Fatalf("CurrentGenerationID=%d, want %d", loaded.CurrentGenerationID, manifest.CurrentGenerationID)
	}
	if loaded.NextGenerationID != manifest.NextGenerationID {
		t.Fatalf("NextGenerationID=%d, want %d", loaded.NextGenerationID, manifest.NextGenerationID)
	}
	if len(loaded.Generations) != len(manifest.Generations) {
		t.Fatalf("len(Generations)=%d, want %d", len(loaded.Generations), len(manifest.Generations))
	}
	if got, want := loaded.Generations[0].FileIDs[1], manifest.Generations[0].FileIDs[1]; got != want {
		t.Fatalf("generation[0].FileIDs[1]=%d, want %d", got, want)
	}
}

func TestLeafGenerationManifest_LoadRejectsUnsupportedVersion(t *testing.T) {
	leafDir := t.TempDir()
	path := leafGenerationManifestPath(leafDir)
	if err := os.WriteFile(path, []byte(`{"version":999,"current_generation_id":1,"next_generation_id":2,"generations":[{"generation_id":1,"state":"writable"}]}`), 0o600); err != nil {
		t.Fatalf("WriteFile(manifest): %v", err)
	}
	_, ok, err := loadLeafGenerationManifest(leafDir)
	if err == nil {
		t.Fatalf("expected loadLeafGenerationManifest error")
	}
	if ok {
		t.Fatalf("expected ok=false on unsupported version")
	}
}

func TestLoadOrCreateLeafGenerationManifest_ReadOnlyMissingReturnsSynthetic(t *testing.T) {
	leafDir := t.TempDir()
	manifest, err := loadOrCreateLeafGenerationManifest(leafDir, 77, true)
	if err != nil {
		t.Fatalf("loadOrCreateLeafGenerationManifest: %v", err)
	}
	if manifest == nil {
		t.Fatalf("expected manifest")
	}
	if got, want := manifest.CurrentGenerationID, uint64(1); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		t.Fatalf("expected no manifest file written for read-only open, got %v", err)
	}
}

func TestOpen_IndexOuterLeavesInValueLog_CreatesLeafGenerationManifest(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if db.leafGenerationManifest == nil {
		t.Fatalf("expected in-memory leaf generation manifest")
	}
	loaded, ok, err := loadLeafGenerationManifest(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatalf("expected manifest file to exist")
	}
	if got, want := loaded.CurrentGenerationID, uint64(1); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
}

func TestOpen_WithoutLeafVLog_DoesNotCreateLeafGenerationManifest(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if db.leafGenerationManifest != nil {
		t.Fatalf("expected no leaf generation manifest when outer leaves stay inline")
	}
	if _, err := os.Stat(leafGenerationManifestPath(LeafLogDirPath(dir))); !os.IsNotExist(err) {
		t.Fatalf("expected no manifest file, got %v", err)
	}
}

func TestOpenReadOnly_IndexOuterLeavesInValueLog_MissingManifestUsesSynthetic(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open writeable: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close writeable: %v", err)
	}
	manifestPath := leafGenerationManifestPath(LeafLogDirPath(dir))
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("Remove(manifest): %v", err)
	}

	ro, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	defer func() { _ = ro.Close() }()

	if ro.leafGenerationManifest == nil {
		t.Fatalf("expected synthetic in-memory manifest on read-only open")
	}
	if got, want := ro.leafGenerationManifest.CurrentGenerationID, uint64(1); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("expected read-only open to leave manifest absent, got %v", err)
	}
}
