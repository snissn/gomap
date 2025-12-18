package db

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRecover_TornSlabTail_RollsBackMeta(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	key1 := []byte("k1")
	key2 := []byte("k2")
	val1 := bytes.Repeat([]byte("a"), 1024) // force slab pointers
	val2 := bytes.Repeat([]byte("b"), 1024)

	// Two committed versions so we have an older meta page to fall back to.
	if err := d.SetSync(key1, val1); err != nil {
		t.Fatalf("set1: %v", err)
	}
	if err := d.SetSync(key2, val2); err != nil {
		t.Fatalf("set2: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	slabPath := filepath.Join(dir, "data-0000.slab")
	info, err := os.Stat(slabPath)
	if err != nil {
		t.Fatalf("stat slab: %v", err)
	}
	before := info.Size()
	if before == 0 {
		t.Fatalf("expected non-empty slab")
	}

	// Corrupt the last byte of the file (simulates a torn tail record).
	f, err := os.OpenFile(slabPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open slab: %v", err)
	}
	defer f.Close()

	var b [1]byte
	if _, err := f.ReadAt(b[:], before-1); err != nil {
		t.Fatalf("read tail: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], before-1); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync slab: %v", err)
	}
	_ = f.Close()

	d2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()

	got1, err := d2.Get(key1)
	if err != nil {
		t.Fatalf("get1: %v", err)
	}
	if !bytes.Equal(got1, val1) {
		t.Fatalf("key1 mismatch after recovery")
	}
	got2, err := d2.Get(key2)
	if err != nil {
		t.Fatalf("get2: %v", err)
	}
	if got2 != nil {
		t.Fatalf("expected key2 rolled back (nil), got %d bytes", len(got2))
	}

	info2, err := os.Stat(slabPath)
	if err != nil {
		t.Fatalf("stat slab2: %v", err)
	}
	after := info2.Size()
	if after >= before {
		t.Fatalf("expected slab truncated after repair, before=%d after=%d", before, after)
	}
}

