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
	repWrite, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("report after write: %v", err)
	}
	pagesWrite := d.Pager().PageCount()

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
	repDelete, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("report after delete: %v", err)
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
	repRewrite, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("report after rewrite: %v", err)
	}
	pagesRewrite := d.Pager().PageCount()

	reclaimableDelete := parseReportUintReuseDB(t, repDelete, "treedb.freelist.reclaimable_pages")
	reclaimableRewrite := parseReportUintReuseDB(t, repRewrite, "treedb.freelist.reclaimable_pages")
	reclaimableWrite := parseReportUintReuseDB(t, repWrite, "treedb.freelist.reclaimable_pages")

	t.Logf("pages write=%d rewrite=%d reclaimable write=%d delete=%d rewrite=%d",
		pagesWrite, pagesRewrite, reclaimableWrite, reclaimableDelete, reclaimableRewrite)

	if reclaimableDelete == 0 {
		t.Fatalf("expected reclaimable pages after delete, got 0")
	}
	if pagesRewrite > pagesWrite+1024 {
		t.Fatalf("expected rewrite to reuse pages (write=%d rewrite=%d)", pagesWrite, pagesRewrite)
	}
	if reclaimableRewrite >= reclaimableDelete {
		t.Fatalf("expected reclaimable pages to decrease after rewrite (delete=%d rewrite=%d)", reclaimableDelete, reclaimableRewrite)
	}
}
