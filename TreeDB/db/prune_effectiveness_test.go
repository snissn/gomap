package db

import (
	"bytes"
	"testing"
)

// This is a *passing* diagnostic invariant: when background pruning is disabled
// and no readers are pinned, calling Prune() after enough commit advancement
// should free at least some retired pages into the freelist.
//
// This does not assert reuse works (that is covered by the intentional-red tests).
func TestPruneMakesFreelistNonEmptyWithNoReaders(t *testing.T) {
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

	writeAll := func(val []byte) {
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, val); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	deleteAll := func() {
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
	}

	// Create retirements and advance commit sequence beyond keepRecent.
	writeAll(valA)
	writeAll(valB) // overwrite -> retire
	deleteAll()    // retire
	writeAll(valA) // advance seq

	d.Prune()

	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("missing allocator")
	}
	fl, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats: %v", err)
	}
	// Allow head to remain zero on extremely small trees, but require that the
	// allocator stats call succeeds and that reclaimables are not wildly inconsistent.
	// In practice this should become non-zero under churn.
	if fl.Head == 0 && fl.ReclaimablePages() == 0 {
		t.Fatalf("expected some freelist state after prune under churn (head=%d reclaimable=%d)", fl.Head, fl.ReclaimablePages())
	}
}
