package db

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRecover_RollsBackMetaWhenSlabTailMissing(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Commit #1 (durable): write a pointer record and sync.
	valA := bytes.Repeat([]byte("A"), 300)
	if err := d.SetSync([]byte("A"), valA); err != nil {
		t.Fatalf("SetSync(A): %v", err)
	}
	tail1 := d.SlabManager().ActiveSlabTail()

	// Commit #2 (not guaranteed durable): write another pointer record without
	// syncing the slab, but do sync the pager to simulate meta reaching disk.
	valB := bytes.Repeat([]byte("B"), 300)
	if err := d.Set([]byte("B"), valB); err != nil {
		t.Fatalf("Set(B): %v", err)
	}
	// Flush async writer so tail advances in s.Size
	if err := d.SlabManager().Sync(); err != nil {
		t.Fatalf("slab sync: %v", err)
	}
	tail2 := d.SlabManager().ActiveSlabTail()
	if tail2 <= tail1 {
		t.Fatalf("expected tail to advance, got tail1=%d tail2=%d", tail1, tail2)
	}
	if err := d.Pager().Sync(); err != nil {
		t.Fatalf("pager sync: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a crash where slab bytes for commit #2 were not durable by
	// truncating back to tail1, leaving index meta on disk.
	slabPath := filepath.Join(dir, "data-0000.slab")
	if err := os.Truncate(slabPath, int64(tail1)); err != nil {
		t.Fatalf("truncate slab: %v", err)
	}

	// Reopen: recovery should reject the newer meta page and roll back.
	d2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()

	gotA, err := d2.Get([]byte("A"))
	if err != nil {
		t.Fatalf("Get(A): %v", err)
	}
	if !bytes.Equal(gotA, valA) {
		t.Fatalf("A mismatch after recovery rollback")
	}

	gotB, err := d2.Get([]byte("B"))
	if err != nil {
		t.Fatalf("Get(B): %v", err)
	}
	if gotB != nil {
		t.Fatalf("expected B to be lost after rollback (async write), got len=%d", len(gotB))
	}
}
