package db

import (
	"bytes"
	"testing"
)

func TestPruneCadenceRespectsKeepRecent(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             2,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const keys = 20000
	valA := bytes.Repeat([]byte("a"), 256)
	valB := bytes.Repeat([]byte("b"), 256)
	valC := bytes.Repeat([]byte("c"), 256)
	valD := bytes.Repeat([]byte("d"), 256)

	// Commit 1.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valA); err != nil {
				t.Fatalf("commit1 set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("commit1 write: %v", err)
		}
		_ = b.Close()
	}

	// Commit 2 (overwrite).
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valB); err != nil {
				t.Fatalf("commit2 set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("commit2 write: %v", err)
		}
		_ = b.Close()
	}

	d.Prune()
	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("missing allocator")
	}
	stats1, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats after commit2: %v", err)
	}
	reclaim1 := stats1.ReclaimablePages()

	// Commit 3 (overwrite again).
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valC); err != nil {
				t.Fatalf("commit3 set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("commit3 write: %v", err)
		}
		_ = b.Close()
	}

	// Commit 4 (overwrite again) should allow pruning older pages.
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valD); err != nil {
				t.Fatalf("commit4 set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("commit4 write: %v", err)
		}
		_ = b.Close()
	}

	d.Prune()
	stats2, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats after commit3: %v", err)
	}
	reclaim2 := stats2.ReclaimablePages()
	if reclaim2 == 0 || reclaim2 <= reclaim1 {
		t.Fatalf("expected reclaimable pages to increase after keepRecent window (before=%d after=%d)", reclaim1, reclaim2)
	}
}
