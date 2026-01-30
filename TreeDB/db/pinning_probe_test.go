package db

import (
	"bytes"
	"math"
	"testing"
)

func TestPruneNotBlockedByReadersWhenNoSnapshots(t *testing.T) {
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

	const keys = 20000
	valA := bytes.Repeat([]byte("a"), 128)
	valB := bytes.Repeat([]byte("b"), 128)

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

	// Create churn and advance commit seq beyond keepRecent.
	writeAll(valA)
	writeAll(valB) // overwrite -> retires pages
	deleteAll()    // retires pages
	writeAll(valA) // rewrite -> advances seq

	idx := d.idx.Load()
	if idx == nil || idx.registry == nil || idx.allocator == nil {
		t.Fatalf("missing index internals")
	}

	minPinned := idx.registry.MinPinnedSeq()
	if minPinned != uint64(math.MaxUint64) {
		// With no snapshots/iterators held by the test, the registry should be empty.
		t.Fatalf("expected no pinned readers (minPinned=MaxUint64), got %d", minPinned)
	}

	// Prune should now be able to extract from graveyard if keepRecent window allows.
	d.Prune()

	fl, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats: %v", err)
	}
	t.Logf("freelist head=%d reclaimable=%d pages=%d", fl.Head, fl.ReclaimablePages(), d.Pager().PageCount())
}
