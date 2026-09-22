package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadOnlyOpenRejectsDirtyCollectionWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeSyntheticCollectionWALSegment(t, dir)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-only error=%v, want ErrRecoveryRequired", err)
	}
	_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("openReadOnlyNoLock error=%v, want ErrRecoveryRequired", err)
	}
}

func TestOfflineMaintenanceRejectsDirtyCollectionWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeSyntheticCollectionWALSegment(t, dir)

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ValueLogRewriteOffline error=%v, want ErrRecoveryRequired", err)
	}
	if err := VacuumIndexOffline(Options{Dir: dir}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("VacuumIndexOffline error=%v, want ErrRecoveryRequired", err)
	}
}

func writeSyntheticCollectionWALSegment(t *testing.T, dir string) {
	t.Helper()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "collection-l0-000001.log"), []byte("dirty collection wal"), 0o600); err != nil {
		t.Fatalf("write collection WAL segment: %v", err)
	}
}
