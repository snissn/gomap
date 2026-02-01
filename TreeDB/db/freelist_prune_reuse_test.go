package db

import (
	"bytes"
	"testing"
)

func TestFreelistReuseAfterPrune(t *testing.T) {
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

	// Seed keys.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valA); err != nil {
				t.Fatalf("seed set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("seed write: %v", err)
		}
		_ = b.Close()
	}

	// Overwrite once to create retired pages.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valB); err != nil {
				t.Fatalf("overwrite set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("overwrite write: %v", err)
		}
		_ = b.Close()
	}

	// Delete and prune.
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

	// Advance the commit sequence so pages retired during the delete phase become
	// eligible under KeepRecent=1, then prune again to populate the freelist.
	{
		b := d.NewBatch().(*Batch)
		if err := b.Set([]byte{0, 0}, valA); err != nil {
			t.Fatalf("advance set: %v", err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("advance write: %v", err)
		}
		_ = b.Close()
		d.Prune()
	}

	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("missing allocator")
	}
	stats, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats: %v", err)
	}
	if stats.ReclaimablePages() == 0 {
		t.Fatalf("expected reclaimable pages after delete+prune")
	}
	freelistBefore, _ := idx.allocator.AllocCounters()

	// Rewrite to reuse freelist.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valA); err != nil {
				t.Fatalf("rewrite set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("rewrite write: %v", err)
		}
		_ = b.Close()
		d.Prune()
	}

	freelistAfter, _ := idx.allocator.AllocCounters()
	if freelistAfter <= freelistBefore {
		t.Fatalf("expected freelist reuse to increase (before=%d after=%d)", freelistBefore, freelistAfter)
	}
}
