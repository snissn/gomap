package db

import (
	"bytes"
	"testing"
)

func TestPrunePhaseProbe(t *testing.T) {
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

	// Phase 1: batch write
	{
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
	}
	d.Prune()
	pagesWrite := d.Pager().PageCount()
	idx := d.idx.Load()
	if idx == nil || idx.allocator == nil {
		t.Fatalf("missing allocator")
	}
	flWrite, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats after write: %v", err)
	}

	// Phase 2: batch delete
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
	}
	d.Prune()
	flDelete, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats after delete: %v", err)
	}

	// Phase 3: rewrite
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valB); err != nil {
				t.Fatalf("rewrite: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("rewrite write: %v", err)
		}
		_ = b.Close()
	}
	d.Prune()
	pagesRewrite := d.Pager().PageCount()
	flRewrite, err := idx.allocator.Stats(d.Pager().PageCount())
	if err != nil {
		t.Fatalf("freelist stats after rewrite: %v", err)
	}

	reclaimableDelete := flDelete.ReclaimablePages()
	reclaimableRewrite := flRewrite.ReclaimablePages()
	reclaimableWrite := flWrite.ReclaimablePages()

	t.Logf("pages write=%d rewrite=%d reclaimable write=%d delete=%d rewrite=%d",
		pagesWrite, pagesRewrite, reclaimableWrite, reclaimableDelete, reclaimableRewrite)

	// Diagnostic probe only. Do not assert reuse/bounds here; keep the strict
	// behavioral regressions in dedicated tests.
}
