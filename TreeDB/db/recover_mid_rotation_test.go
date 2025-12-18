package db

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRecover_MidRotationCrash_PrunesGhostSlab(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	key := []byte("k1")
	val := bytes.Repeat([]byte("a"), 1024) // force slab pointers
	if err := d.SetSync(key, val); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Simulate a crash after slab rotation but before any commit updates meta:
	// the new slab file exists on disk, but the meta page still points at the
	// previous ActiveSlabID.
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	ghost := filepath.Join(dir, "data-0001.slab")
	if _, err := os.Stat(ghost); err != nil {
		t.Fatalf("expected ghost slab to exist: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()

	if gotID := d2.SlabManager().ActiveSlabID(); gotID != 0 {
		t.Fatalf("expected ActiveSlabID=0 after recovery, got %d", gotID)
	}
	if _, err := os.Stat(ghost); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected ghost slab pruned, stat err=%v", err)
	}

	got, err := d2.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch after recovery")
	}
}

