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

	// Seed and overwrite+delete to create retired pages, then prune to populate
	// the freelist.
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
	for round := 0; round < 5; round++ {
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

		b = d.NewBatch().(*Batch)
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
	d.Prune()

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
	t.Logf("before reuse: commit=%d head=%d reclaimable=%d pages=%d", d.State().CommitSeq, statsBefore.Head, statsBefore.ReclaimablePages(), d.Pager().PageCount())

	fbFreelist, fbAppend := idx.allocator.AllocCounters()
	pagesBefore := d.Pager().PageCount()

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
	}
	faFreelist, faAppend := idx.allocator.AllocCounters()
	pagesAfter := d.Pager().PageCount()

	statsAfter, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats after: %v", err)
	}
	freelistDelta := faFreelist - fbFreelist
	appendDelta := faAppend - fbAppend
	t.Logf("after rewrite: commit=%d head=%d reclaimable=%d pages=%d->%d alloc.freelist=%d->%d alloc.append=%d->%d",
		d.State().CommitSeq,
		statsAfter.Head,
		statsAfter.ReclaimablePages(),
		pagesBefore, pagesAfter,
		fbFreelist, faFreelist,
		fbAppend, faAppend,
	)

	// If we have reclaimables and PreferAppendAlloc=false, the rewrite should
	// primarily reuse the freelist rather than extend the file.
	//
	// This assertion is expected to FAIL until the bloat/reuse bug is fixed.
	if appendDelta > freelistDelta/4 {
		t.Fatalf("expected rewrite to prefer freelist reuse over append (reclaimable=%d freelist_delta=%d append_delta=%d)",
			statsBefore.ReclaimablePages(), freelistDelta, appendDelta)
	}
}
