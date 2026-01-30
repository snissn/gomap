package db

import (
	"testing"
)

func TestFreelistReclaimablePersistsAcrossClose(t *testing.T) {
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

	const keys = 2000

	// Seed, overwrite, and delete to create freelist pages.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, []byte("v")); err != nil {
				t.Fatalf("seed set: %v", err)
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
			if err := b.Set(k, []byte("w")); err != nil {
				t.Fatalf("overwrite set: %v", err)
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
	}
	// Force a commit after prune so freelist head is persisted.
	{
		b := d.NewBatch().(*Batch)
		if err := b.Set([]byte("persist"), []byte("1")); err != nil {
			t.Fatalf("persist set: %v", err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("persist write: %v", err)
		}
		_ = b.Close()
		d.Prune()
	}

	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("missing allocator")
	}
	statsBefore, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats before: %v", err)
	}
	if statsBefore.ReclaimablePages() == 0 {
		t.Fatalf("expected reclaimable pages before close")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()

	idx2 := d2.idx.Load()
	if idx2 == nil || idx2.allocator == nil {
		t.Fatalf("missing allocator after reopen")
	}
	statsAfter, err := idx2.allocator.Stats(d2.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats after: %v", err)
	}
	if statsBefore.ReclaimablePages() != statsAfter.ReclaimablePages() {
		t.Fatalf("reclaimable pages changed across close: before=%d after=%d", statsBefore.ReclaimablePages(), statsAfter.ReclaimablePages())
	}
}
