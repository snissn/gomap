package db

import (
	"bytes"
	"testing"
)

func TestPageCountBoundedAfterReuse(t *testing.T) {
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
	valA := bytes.Repeat([]byte("a"), 64)
	valB := bytes.Repeat([]byte("b"), 64)

	// Seed.
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
	basePages := d.Pager().PageCount()
	if basePages == 0 {
		t.Fatalf("expected non-zero base page count")
	}

	// Overwrite to retire pages.
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

	// Rewrite to reuse freelist.
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

	finalPages := d.Pager().PageCount()
	if finalPages > basePages*5 {
		t.Fatalf("expected bounded page count after reuse (base=%d final=%d)", basePages, finalPages)
	}
}
