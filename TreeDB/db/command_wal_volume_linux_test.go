package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALDedicatedVolumeRejectsSharedColumnAssets(t *testing.T) {
	// Keep the actual page index and data roles on tmpfs and use the normal
	// test filesystem as the volume presented by the existing WAL descriptor.
	dir, err := os.MkdirTemp("/dev/shm", "gomap-wal-credit-assets-")
	if err != nil {
		t.Skipf("separate tmpfs unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	walVolume := t.TempDir()
	file, err := os.Create(filepath.Join(walVolume, "wal-volume-id"))
	if err != nil {
		t.Fatal(err)
	}
	walID, idErr := rootpublication.StableIdentityFromFile(file)
	closeErr := file.Close()
	if idErr != nil || closeErr != nil {
		t.Fatalf("WAL identity: %v, close: %v", idErr, closeErr)
	}
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	check := func() bool {
		t.Helper()
		selected, err := database.commandWALHasDedicatedVolume(walID)
		if err != nil {
			t.Fatal(err)
		}
		return selected
	}
	volDir, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	dataID, idErr := rootpublication.StableIdentityFromFile(volDir)
	closeErr = volDir.Close()
	if idErr != nil || closeErr != nil {
		t.Fatalf("data identity: %v, close: %v", idErr, closeErr)
	}
	if dataID.VolumeID == walID.VolumeID {
		t.Skip("test and tmpfs volumes coincide")
	}
	if !check() {
		t.Fatal("dedicated WAL layout declined")
	}
	assets := resolveStorageLayout(dir).columnAssetDir
	namespace := filepath.Join(assets, "events")
	segments := filepath.Join(namespace, "nested", "assets", "segments")
	if err := os.MkdirAll(filepath.Dir(segments), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(walVolume, segments); err != nil {
		t.Fatal(err)
	}
	if check() {
		t.Fatal("nested column assets sharing the WAL device selected dedicated WAL credit")
	}
	if err := os.RemoveAll(namespace); err != nil {
		t.Fatal(err)
	}
	if !check() {
		t.Fatal("dedicated WAL layout declined after clearing column assets")
	}
	if err := os.Remove(assets); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(walVolume, assets); err != nil {
		t.Fatal(err)
	}
	if check() {
		t.Fatal("column assets sharing the WAL device selected dedicated WAL credit")
	}
}
