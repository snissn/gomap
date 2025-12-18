package db

import (
	"bytes"
	"testing"
)

func TestCompactIndexRetiresOldPages(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, KeepRecent: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("x"), 10)
	for i := 0; i < 2000; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		if err := d.SetSync(k, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	oldHead := d.allocator.Head()

	if err := d.CompactIndex(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Advance commit seq enough for KeepRecent=1 pruning to kick in for the
	// compaction-retired pages.
	if err := d.SetSync([]byte{0xFF, 0xFE}, val); err != nil {
		t.Fatalf("set1: %v", err)
	}
	if err := d.SetSync([]byte{0xFF, 0xFD}, val); err != nil {
		t.Fatalf("set2: %v", err)
	}

	newHead := d.allocator.Head()
	if oldHead == 0 && newHead == 0 {
		t.Fatalf("expected freelist to become non-empty after compact + pruning")
	}
}
