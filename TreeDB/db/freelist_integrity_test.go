package db

import (
	"bytes"
	"testing"
)

func TestFreelistHeadValidAfterChurn(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const keys = 4000
	valA := bytes.Repeat([]byte("a"), 32)
	valB := bytes.Repeat([]byte("b"), 32)

	for round := 0; round < 3; round++ {
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valA); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
		b = d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valB); err != nil {
				t.Fatalf("overwrite: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("overwrite write: %v", err)
		}
		_ = b.Close()
		d.Prune()
	}

	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("missing allocator")
	}
	if _, err := idx.allocator.Stats(d.Pager().PageCount()); err != nil {
		t.Fatalf("freelist stats error: %v", err)
	}
}

func TestFreelistCountsDecreaseAfterReuse(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const keys = 4000
	valA := bytes.Repeat([]byte("a"), 32)
	valB := bytes.Repeat([]byte("b"), 32)

	// Seed and overwrite to create retired pages.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valA); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("seed write: %v", err)
		}
		_ = b.Close()
	}
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valB); err != nil {
				t.Fatalf("overwrite: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("overwrite write: %v", err)
		}
		_ = b.Close()
	}
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Delete(k); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("delete write: %v", err)
		}
		_ = b.Close()
		d.Prune()
		d.Prune()
	}

	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("missing allocator")
	}
	statsBefore, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats: %v", err)
	}
	if statsBefore.ReclaimablePages() == 0 {
		t.Fatalf("expected reclaimable pages before reuse")
	}
	// Rewrite to consume freelist.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valA); err != nil {
				t.Fatalf("rewrite: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("rewrite write: %v", err)
		}
		_ = b.Close()
		d.Prune()
	}

	statsAfter, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats after: %v", err)
	}
	if statsAfter.ReclaimablePages() >= statsBefore.ReclaimablePages() {
		t.Fatalf("expected reclaimable pages to decrease after reuse (before=%d after=%d)", statsBefore.ReclaimablePages(), statsAfter.ReclaimablePages())
	}
}
